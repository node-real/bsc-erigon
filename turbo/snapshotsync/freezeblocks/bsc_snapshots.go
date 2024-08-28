package freezeblocks

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/services"
	"path/filepath"
	"reflect"
)

var BscProduceFiles = dbg.EnvBool("BSC_PRODUCE_FILES", false)

const (
	bscMinSegFrom    = 39_700_000
	chapelMinSegFrom = 39_500_000
)

func (br *BlockRetire) dbHasEnoughDataForBscRetire(ctx context.Context) (bool, error) {
	return true, nil
}

func (br *BlockRetire) retireBscBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []services.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	if !BscProduceFiles {
		return false, nil
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	snapshots := br.bscSnapshots()

	chainConfig := fromdb.ChainConfig(br.db)
	var minimumBlob uint64
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	if chainConfig.ChainName == networkname.BSCChainName {
		minimumBlob = bscMinSegFrom
	} else {
		minimumBlob = chapelMinSegFrom
	}
	blockFrom := max(blockReader.FrozenBscBlobs(), minimumBlob)
	blocksRetired := false
	for _, snap := range blockReader.BscSnapshots().Types() {
		if maxBlockNum <= blockFrom || maxBlockNum-blockFrom < snaptype.Erigon2MergeLimit {
			continue
		}

		blockTo := maxBlockNum

		logger.Log(lvl, "[bsc snapshot] Retire Bsc Blobs", "type", snap,
			"range", fmt.Sprintf("%d-%d", blockFrom, blockTo))

		blocksRetired = true
		if err := DumpBlobs(ctx, blockFrom, blockTo, br.chainConfig, tmpDir, snapshots.Dir(), db, workers, lvl, blockReader, br.bs, logger); err != nil {
			return true, fmt.Errorf("DumpBlobs: %w", err)
		}
	}

	if blocksRetired {
		if err := snapshots.ReopenFolder(); err != nil {
			return true, fmt.Errorf("reopen: %w", err)
		}
		snapshots.LogStat("bsc:retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		// now prune blobs from the database
		blockTo := (maxBlockNum / snaptype.Erigon2MergeLimit) * snaptype.Erigon2MergeLimit
		roTx, err := db.BeginRo(ctx)
		if err != nil {
			return false, nil
		}
		defer roTx.Rollback()

		for i := blockFrom; i < blockTo; i++ {
			if i%10000 == 0 {
				log.Info("remove sidecars", "blockNum", i)
			}
			blockHash, err := blockReader.CanonicalHash(ctx, roTx, i)
			if err != nil {
				return false, err
			}
			if err = br.bs.RemoveBlobSidecars(ctx, i, blockHash); err != nil {
				log.Error("remove sidecars", "blockNum", i, "err", err)
			}

		}
		if seedNewSnapshots != nil {
			downloadRequest := []services.DownloadRequest{
				services.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return false, err
			}
		}
	}

	return blocksRetired, nil
}

type BscRoSnapshots struct {
	RoSnapshots
}

// NewBscSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewBscRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, segmentsMin uint64, logger log.Logger) *BscRoSnapshots {
	return &BscRoSnapshots{*newRoSnapshots(cfg, snapDir, coresnaptype.BscSnapshotTypes, segmentsMin, logger)}
}

func (s *BscRoSnapshots) Ranges() []Range {
	view := s.View()
	defer view.Close()
	return view.base.Ranges()
}

func (s *BscRoSnapshots) ReopenFolder() error {
	files, _, err := typedSegments(s.dir, s.segmentsMin.Load(), coresnaptype.BscSnapshotTypes, true)
	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	return s.ReopenList(list, false)
}

type BscView struct {
	base *View
}

func (s *BscRoSnapshots) View() *BscView {
	v := &BscView{base: s.RoSnapshots.View()}
	v.base.baseSegType = coresnaptype.BlobSidecars
	return v
}

func (v *BscView) Close() {
	v.base.Close()
}

func (v *BscView) BlobSidecars() []*Segment { return v.base.segments(coresnaptype.BlobSidecars) }

func (v *BscView) BlobSidecarsSegment(blockNum uint64) (*Segment, bool) {
	return v.base.Segment(coresnaptype.BlobSidecars, blockNum)
}

func dumpBlobsRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, chainDB kv.RoDB, blobStore services.BlobStorage, blockReader services.FullBlockReader, chainConfig *chain.Config, workers int, lvl log.Lvl, logger log.Logger) (err error) {
	f := coresnaptype.BlobSidecars.FileInfo(snapDir, blockFrom, blockTo)
	sn, err := seg.NewCompressor(ctx, "Snapshot "+f.Type.Name(), f.Path, tmpDir, seg.MinPatternScore, workers, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := chainDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Generate .seg file, which is just the list of beacon blocks.
	for i := blockFrom; i < blockTo; i++ {
		// read root.
		blockHash, err := blockReader.CanonicalHash(ctx, tx, i)
		if err != nil {
			return err
		}

		blobTxCount, err := blobStore.BlobTxCount(ctx, blockHash)
		if err != nil {
			return err
		}
		if blobTxCount == 0 {
			sn.AddWord(nil)
			continue
		}
		sidecars, found, err := blobStore.ReadBlobSidecars(ctx, i, blockHash)
		if err != nil {
			return fmt.Errorf("read blob sidecars: blockNum = %d, blobTxcount = %d, err = %v", i, blobTxCount, err)
		}
		if !found {
			return fmt.Errorf("blob sidecars not found for block %d", i)
		}
		dataRLP, err := rlp.EncodeToBytes(sidecars)
		if err != nil {
			return err
		}
		if err := sn.AddWord(dataRLP); err != nil {
			return err
		}
		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping bsc blobs", "progress", i)
		}

	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	if err := f.Type.BuildIndexes(ctx, f, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return err
	}

	return nil
}

func DumpBlobs(ctx context.Context, blockFrom, blockTo uint64, chainConfig *chain.Config, tmpDir, snapDir string, chainDB kv.RoDB, workers int, lvl log.Lvl, blockReader services.FullBlockReader, blobStore services.BlobStorage, logger log.Logger) error {
	//if checkBlobs(ctx, blockFrom, blockTo, chainDB, blobStore, blockReader, logger) == false {
	//	return fmt.Errorf("check blobs failed")
	//}
	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, coresnaptype.Enums.BscBlobs, chainConfig) {
		blocksPerFile := snapcfg.MergeLimit("", coresnaptype.Enums.BscBlobs, i)
		if blockTo-i < blocksPerFile {
			break
		}
		logger.Log(lvl, "Dumping blobs sidecars", "from", i, "to", blockTo)
		if err := dumpBlobsRange(ctx, i, chooseSegmentEnd(i, blockTo, coresnaptype.Enums.BscBlobs, chainConfig), tmpDir, snapDir, chainDB, blobStore, blockReader, chainConfig, workers, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

func (s *BscRoSnapshots) ReadBlobSidecars(blockNum uint64) ([]*types.BlobSidecar, error) {
	view := s.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.BlobSidecarsSegment(blockNum)
	if !ok {
		return nil, nil
	}

	idxNum := seg.Index()

	if idxNum == nil {
		return nil, nil
	}
	blockOffset := idxNum.OrdinalLookup(blockNum - idxNum.BaseDataID())

	gg := seg.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}

	buf, _ = gg.Next(buf)
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

//
//func checkBlobs(ctx context.Context, blockFrom, blockTo uint64, chainDB kv.RoDB, blobStore services.BlobStorage, blockReader services.FullBlockReader, logger log.Logger) bool {
//	tx, err := chainDB.BeginRo(ctx)
//	if err != nil {
//		return false
//	}
//	defer tx.Rollback()
//	var missedBlobs []uint64
//	noErr := true
//	for i := blockFrom; i < blockTo; i++ {
//		block, err := blockReader.BlockByNumber(ctx, tx, i)
//		if err != nil {
//			log.Error("ReadCanonicalHash", "blockNum", i, "blockHash", block.Hash(), "err", err)
//			noErr = false
//		}
//		var blobTxCount uint64
//
//		for _, tx := range block.Transactions() {
//			if tx.Type() != types.BlobTxType {
//				continue
//			}
//			blobTxCount++
//		}
//		if blobTxCount == 0 {
//			continue
//		}
//		blobs, found, err := blobStore.ReadBlobSidecars(ctx, i, block.Hash())
//		if err != nil {
//			noErr = false
//			missedBlobs = append(missedBlobs, i)
//			log.Error("read blob sidecars:", "blockNum", i, "blobTxCount", blobTxCount, "err", err)
//			err := blobStore.RemoveBlobSidecars(ctx, i, block.Hash())
//			log.Error("Remove blob sidecars:", "blockNum", i, "blobTxCount", blobTxCount, "err", err)
//			continue
//		}
//		if !found {
//			noErr = false
//			missedBlobs = append(missedBlobs, i)
//			log.Error("blob sidecars not found for block ", "blockNumber", i, "count", blobTxCount)
//			continue
//		}
//
//		if uint64(len(blobs)) != blobTxCount {
//			missedBlobs = append(missedBlobs, i)
//			noErr = false
//			log.Error("blob sidecars not found for block ", "blockNumber", i, "want", blobTxCount, "actual", len(blobs))
//			continue
//		}
//
//		if i%20_000 == 0 {
//			logger.Info("Dumping bsc blobs", "progress", i)
//		}
//	}
//
//	log.Info("Start query missedBlobs from http")
//	for _, num := range missedBlobs {
//		blobs := GetBlobSidecars(num)
//		hash, err := blockReader.CanonicalHash(ctx, tx, num)
//		if err != nil {
//			log.Error("GetBlobSidecars failed", "num", num, "err", err)
//			return noErr
//		}
//		if err = blobStore.WriteBlobSidecars(ctx, hash, blobs); err != nil {
//			log.Error("WriteBlobSidecars failed", "num", num, "err", err)
//		}
//		log.Info("WriteBlobSidecars blobs", "num", num, "hash", hash, "blobs", len(blobs))
//		time.Sleep(1 * time.Second)
//	}
//
//	return noErr
//}
