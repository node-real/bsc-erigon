package freezeblocks

import (
	"context"
	"encoding/binary"
	"fmt"
	coresnaptype "github.com/erigontech/erigon-db/snaptype"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/snaptype"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync"
	"os"
	"reflect"
	"time"
)

const (
	bscMinSegFrom    = 39_700_000
	chapelMinSegFrom = 39_500_000
)

func (br *BlockRetire) dbHasEnoughDataForBscRetire(ctx context.Context) (bool, error) {
	return true, nil
}

func (br *BlockRetire) retireBscBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []snapshotsync.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	startTime := time.Now()
	snapshots := br.bscSnapshots()
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers.Load()

	var minimumBlob uint64
	if br.chainConfig.ChainName == networkname.BSC {
		minimumBlob = bscMinSegFrom
	} else {
		minimumBlob = chapelMinSegFrom
	}

	blocksRetired := false
	totalSegments := 0
	var blockFrom uint64

	for _, snap := range blockReader.BscSnapshots().Types() {
		minSnapBlockNum := max(snapshots.DirtyBlocksAvailable(snap.Enum()), minBlockNum, minimumBlob)

		if maxBlockNum <= minSnapBlockNum || maxBlockNum-minSnapBlockNum < snaptype.Erigon2OldMergeLimit {
			continue
		}

		blockFrom = minSnapBlockNum + 1
		blockTo := maxBlockNum

		blocksRetired = true

		logger.Log(lvl, "[bsc snapshots] Retire BSC Blobs", "type", snap,
			"range", fmt.Sprintf("%s-%s", common.PrettyCounter(blockFrom), common.PrettyCounter(blockTo)))

		segmentStartTime := time.Now()
		for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, snap.Enum(), br.chainConfig) {
			end := chooseSegmentEnd(i, blockTo, snap.Enum(), br.chainConfig)
			if end-i < snaptype.Erigon2OldMergeLimit {
				break
			}
			dumpStartTime := time.Now()
			if err := DumpBlobs(ctx, i, end, br.chainConfig, tmpDir, snapshots.Dir(), db, int(workers), lvl, blockReader, br.bs, logger); err != nil {
				return blocksRetired, fmt.Errorf("DumpBlobs: %d-%d: %w", i, end, err)
			}
			dumpDuration := time.Since(dumpStartTime)
			logger.Log(lvl, "[bsc snapshots] Segment dumped", "range", fmt.Sprintf("%s-%s", common.PrettyCounter(i), common.PrettyCounter(end)), "duration", dumpDuration)
			totalSegments++
		}
		segmentDuration := time.Since(segmentStartTime)
		logger.Log(lvl, "[bsc snapshots] All segments dumped for type", "type", snap, "duration", segmentDuration, "segments", totalSegments)
	}

	if blocksRetired {
		if err := snapshots.OpenFolder(); err != nil {
			return blocksRetired, fmt.Errorf("reopen: %w", err)
		}
		snapshots.LogStat("bsc:retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		// now prune blobs from the database
		blockTo := (maxBlockNum / snaptype.Erigon2OldMergeLimit) * snaptype.Erigon2OldMergeLimit
		roTx, err := db.BeginRo(ctx)
		if err != nil {
			return false, nil
		}
		defer roTx.Rollback()
		cleanupStart := time.Now()
		for i := blockFrom; i < blockTo; i++ {
			blockHash, _, err := blockReader.CanonicalHash(ctx, roTx, i)
			if err != nil {
				return false, err
			}
			if err = br.bs.RemoveBlobSidecars(ctx, i, blockHash); err != nil {
				logger.Error("remove sidecars", "blockNum", i, "err", err)
			}
		}
		cleanupDuration := time.Since(cleanupStart)
		logger.Log(lvl, "[bsc snapshots] Blob cleanup completed", "duration", cleanupDuration)

		if seedNewSnapshots != nil {
			downloadRequest := []snapshotsync.DownloadRequest{
				snapshotsync.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return blocksRetired, err
			}
		}
	}

	retireDuration := time.Since(startTime)
	mergeStartTime := time.Now()
	merged, err := br.MergeBscBlocks(ctx, lvl, seedNewSnapshots, onDelete)
	mergeDuration := time.Since(mergeStartTime)

	if merged {
		logger.Log(lvl, "[bsc snapshots] BSC merge completed", "duration", mergeDuration)
	}

	totalDuration := time.Since(startTime)
	if blocksRetired || merged {
		logger.Log(lvl, "[bsc snapshots] BSC total operation completed", "totalDuration", totalDuration, "retireDuration", retireDuration, "mergeDuration", mergeDuration)
	}

	return blocksRetired || merged, err
}

func (br *BlockRetire) MergeBscBlocks(ctx context.Context, lvl log.Lvl, seedNewSnapshots func(downloadRequest []snapshotsync.DownloadRequest) error, onDelete func(l []string) error) (mergedBlocks bool, err error) {
	startTime := time.Now()
	notifier, logger, _, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers.Load()
	snapshots := br.bscSnapshots()
	merger := snapshotsync.NewMerger(tmpDir, int(workers), lvl, db, br.chainConfig, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges(), snapshots.BlocksAvailable())
	if len(rangesToMerge) > 0 {
		logger.Log(lvl, "[bsc snapshots] Retire Bsc Blocks", "rangesToMerge", snapshotsync.Ranges(rangesToMerge))
	}
	if len(rangesToMerge) == 0 {
		return false, nil
	}

	mergeOperationStart := time.Now()
	onMerge := func(r snapshotsync.Range) error {
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		if seedNewSnapshots != nil {
			downloadRequest := []snapshotsync.DownloadRequest{
				snapshotsync.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return err
			}
		}
		return nil
	}
	if err := merger.Merge(ctx, &snapshots.RoSnapshots, coresnaptype.BscSnapshotTypes, rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, onDelete); err != nil {
		return false, err
	}
	mergeOperationDuration := time.Since(mergeOperationStart)
	logger.Log(lvl, "[bsc snapshots] Merge operation completed", "duration", mergeOperationDuration, "rangesCount", len(rangesToMerge))

	cleanupStart := time.Now()
	{
		files, _, err := snapshotsync.TypedSegments(br.bscSnapshots().Dir(), br.bscSnapshots().SegmentsMin(), coresnaptype.BscSnapshotTypes, false)
		if err != nil {
			return true, err
		}

		// this is one off code to fix an issue in 2.49.x->2.52.x which missed
		// removal of intermediate segments after a merge operation
		removeBscOverlaps(br.bscSnapshots().Dir(), files, br.bscSnapshots().BlocksAvailable())
	}
	cleanupDuration := time.Since(cleanupStart)
	logger.Log(lvl, "[bsc snapshots] Cleanup completed", "duration", cleanupDuration)

	totalDuration := time.Since(startTime)
	logger.Log(lvl, "[bsc snapshots] MergeBscBlocks completed", "totalDuration", totalDuration, "mergeOperation", mergeOperationDuration, "cleanup", cleanupDuration)

	return true, nil
}

// this is one off code to fix an issue in 2.49.x->2.52.x which missed
// removal of intermediate segments after a merge operation
func removeBscOverlaps(dir string, active []snaptype.FileInfo, _max uint64) {
	list, err := snaptype.Segments(dir)

	if err != nil {
		return
	}

	var toDel []string
	l := make([]snaptype.FileInfo, 0, len(list))

	for _, f := range list {
		if !(f.Type.Enum() == coresnaptype.Enums.BscBlobs) {
			continue
		}
		l = append(l, f)
	}

	// added overhead to make sure we don't delete in the
	// current 500k block segment
	if _max > 500_001 {
		_max -= 500_001
	}

	for _, f := range l {
		if _max < f.From {
			continue
		}

		for _, a := range active {
			if a.Type.Enum() != coresnaptype.Enums.BscBlobs {
				continue
			}

			if f.From < a.From {
				continue
			}

			if f.From == a.From {
				if f.To < a.To {
					toDel = append(toDel, f.Path)
				}

				break
			}

			if f.From < a.To {
				toDel = append(toDel, f.Path)
				break
			}
		}
	}

	for _, f := range toDel {
		_ = os.Remove(f)
		_ = os.Remove(f + ".torrent")
		withoutExt := "blocksidecars"
		_ = os.Remove(withoutExt + ".idx")
		_ = os.Remove(withoutExt + ".idx.torrent")
	}
}

type BscRoSnapshots struct {
	snapshotsync.RoSnapshots
}

// NewBscSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewBscRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, segmentsMin uint64, logger log.Logger) *BscRoSnapshots {
	return &BscRoSnapshots{*snapshotsync.NewRoSnapshots(cfg, snapDir, coresnaptype.BscSnapshotTypes, segmentsMin, false, logger)}
}

func (s *BscRoSnapshots) Ranges() []snapshotsync.Range {
	view := s.View()
	defer view.Close()
	return view.base.Ranges()
}

type BscView struct {
	base *snapshotsync.View
}

func (s *BscRoSnapshots) View() *BscView {
	v := &BscView{base: s.RoSnapshots.View().WithBaseSegType(coresnaptype.BlobSidecars)}
	return v
}

func (v *BscView) Close() {
	v.base.Close()
}

func (v *BscView) BlobSidecars() []*snapshotsync.VisibleSegment {
	return v.base.Segments(coresnaptype.BlobSidecars)
}

func (v *BscView) BlobSidecarsSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(coresnaptype.BlobSidecars, blockNum)
}

func dumpBlobsRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, chainDB kv.RoDB, blobStore services.BlobStorage, blockReader services.FullBlockReader, chainConfig *chain.Config, workers int, lvl log.Lvl, logger log.Logger) (err error) {
	startTime := time.Now()
	f := coresnaptype.BlobSidecars.FileInfo(snapDir, blockFrom, blockTo)
	sn, err := seg.NewCompressor(ctx, "Snapshot "+f.Type.Name(), f.Path, tmpDir, seg.DefaultCfg, log.LvlTrace, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	// Use BigChunks pattern to avoid long transactions
	from := hexutil.EncodeTs(blockFrom)

	dataProcessingStart := time.Now()
	processedBlocks := uint64(0)
	emptyBlocks := uint64(0)
	blobBlocks := uint64(0)

	if err := kv.BigChunks(chainDB, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}

		blockHash := common.BytesToHash(v)

		blobTxCount, err := blobStore.BlobTxCount(ctx, blockHash)
		if err != nil {
			return false, err
		}

		if blobTxCount == 0 {
			emptyBlocks++
			if err := sn.AddWord(nil); err != nil {
				return false, err
			}
			processedBlocks++
			return true, nil
		}

		blobBlocks++
		sidecars, found, err := blobStore.ReadBlobSidecars(ctx, blockNum, blockHash)
		if err != nil {
			return false, fmt.Errorf("read blob sidecars: blockNum = %d, blobTxcount = %d, err = %v", blockNum, blobTxCount, err)
		}
		if !found {
			return false, fmt.Errorf("blob sidecars not found for block %d", blockNum)
		}

		dataRLP, err := rlp.EncodeToBytes(sidecars)
		if err != nil {
			return false, err
		}

		if err := sn.AddWord(dataRLP); err != nil {
			return false, err
		}

		processedBlocks++
		if blockNum%20_000 == 0 {
			logger.Log(lvl, "Dumping bsc blobs", "progress", blockNum)
		}

		return true, nil
	}); err != nil {
		return err
	}

	dataProcessingDuration := time.Since(dataProcessingStart)
	logger.Log(lvl, "[bsc snapshots] Data processing completed", "duration", dataProcessingDuration, "processedBlocks", processedBlocks, "emptyBlocks", emptyBlocks, "blobBlocks", blobBlocks, "blocks/sec", float64(processedBlocks)/dataProcessingDuration.Seconds())

	compressionStart := time.Now()
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	compressionDuration := time.Since(compressionStart)
	logger.Log(lvl, "[bsc snapshots] Compression completed", "duration", compressionDuration)

	// Generate .idx file, which is the slot => offset mapping.
	indexingStart := time.Now()
	p := &background.Progress{}
	if err := f.Type.BuildIndexes(ctx, f, nil, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return err
	}
	indexingDuration := time.Since(indexingStart)
	logger.Log(lvl, "[bsc snapshots] Indexing completed", "duration", indexingDuration)

	totalDuration := time.Since(startTime)
	logger.Log(lvl, "[bsc snapshots] dumpBlobsRange completed", "totalDuration", totalDuration, "dataProcessing", dataProcessingDuration, "compression", compressionDuration, "indexing", indexingDuration)

	return nil
}

func DumpBlobs(ctx context.Context, blockFrom, blockTo uint64, chainConfig *chain.Config, tmpDir, snapDir string, chainDB kv.RoDB, workers int, lvl log.Lvl, blockReader services.FullBlockReader, blobStore services.BlobStorage, logger log.Logger) error {
	startTime := time.Now()
	logger.Log(lvl, "Dumping blobs sidecars", "from", blockFrom, "to", blockTo)

	err := dumpBlobsRange(ctx, blockFrom, blockTo, tmpDir, snapDir, chainDB, blobStore, blockReader, chainConfig, workers, lvl, logger)

	duration := time.Since(startTime)
	blockCount := blockTo - blockFrom
	logger.Log(lvl, "Dumping blobs sidecars completed", "from", blockFrom, "to", blockTo, "duration", duration, "blocks", blockCount, "blocks/sec", float64(blockCount)/duration.Seconds())

	return err
}

func (s *BscRoSnapshots) ReadBlobSidecars(blockNum uint64) ([]*types.BlobSidecar, error) {
	view := s.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.BlobSidecarsSegment(blockNum)
	if !ok {
		return nil, nil
	}

	idxNum := seg.Src().Index()

	if idxNum == nil {
		return nil, nil
	}
	blockOffset := idxNum.OrdinalLookup(blockNum - idxNum.BaseDataID())

	gg := seg.Src().MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}

	buf, _ = gg.Next(buf[:0])
	if len(buf) == 0 {
		return nil, nil
	}

	var sidecars []*types.BlobSidecar
	err := rlp.DecodeBytes(buf, &sidecars)
	if err != nil {
		return nil, err
	}

	return sidecars, nil
}
