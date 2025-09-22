package freezeblocks

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
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

		logger.Log(lvl, "[bsc snapshots] Retire BSC Blobs", "type", snap,
			"blockFrom", blockFrom, "blockTo", blockTo)
		segmentStartTime := time.Now()
		for i := blockFrom; i < blockTo; {
			if blockTo-i < snaptype.Erigon2OldMergeLimit {
				break
			}
			to := chooseSegmentEnd(i, blockTo, snap.Enum(), br.chainConfig)
			logger.Log(lvl, "[bsc snapshots] Dumping blobs sidecars", "from", i, "to", to)
			blocksRetired = true
			if err := DumpBlobs(ctx, i, to, br.chainConfig, tmpDir, snapshots.Dir(), db, int(workers), lvl, blockReader, br.bs, logger); err != nil {
				return blocksRetired, fmt.Errorf("[bsc snapshots] DumpBlobs: %d-%d: %w", i, to, err)
			}
			logger.Log(lvl, "[bsc snapshots] Segment dumped", "i", i, "to", to)
			totalSegments++

			// Manually update loop variable
			i = to
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

	totalDuration := time.Since(startTime)
	if blocksRetired {
		logger.Log(lvl, "[bsc snapshots] BSC total operation completed", "totalDuration", totalDuration, "retireDuration", retireDuration)
	}

	return blocksRetired, nil
}

type BscRoSnapshots struct {
	snapshotsync.RoSnapshots
}

// NewBscSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewBscRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, logger log.Logger) *BscRoSnapshots {
	return &BscRoSnapshots{*snapshotsync.NewRoSnapshots(cfg, snapDir, snaptype2.BscSnapshotTypes, false, logger)}
}

func (s *BscRoSnapshots) Ranges() []snapshotsync.Range {
	view := s.View()
	defer view.Close()
	return view.base.Ranges(false)
}

type BscView struct {
	base *snapshotsync.View
}

func (s *BscRoSnapshots) View() *BscView {
	v := &BscView{base: s.RoSnapshots.View().WithBaseSegType(snaptype2.BlobSidecars)}
	return v
}

func (v *BscView) Close() {
	v.base.Close()
}

func (v *BscView) BlobSidecars() []*snapshotsync.VisibleSegment {
	return v.base.Segments(snaptype2.BlobSidecars)
}

func (v *BscView) BlobSidecarsSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(snaptype2.BlobSidecars, blockNum)
}

func dumpBlobsRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, chainDB kv.RoDB, blobStore services.BlobStorage, blockReader services.FullBlockReader, chainConfig *chain.Config, workers int, lvl log.Lvl, logger log.Logger) (err error) {
	startTime := time.Now()
	f := snaptype2.BlobSidecars.FileInfo(snapDir, blockFrom, blockTo)
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
			logger.Log(lvl, "[bsc snapshots] Dumping bsc blobs", "progress", blockNum)
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
	err := dumpBlobsRange(ctx, blockFrom, blockTo, tmpDir, snapDir, chainDB, blobStore, blockReader, chainConfig, workers, lvl, logger)

	duration := time.Since(startTime)
	blockCount := blockTo - blockFrom
	logger.Log(lvl, "[bsc snapshots] Dumping blobs sidecars completed", "from", blockFrom, "to", blockTo, "duration", duration, "blocks", blockCount, "blocks/sec", float64(blockCount)/duration.Seconds())

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
