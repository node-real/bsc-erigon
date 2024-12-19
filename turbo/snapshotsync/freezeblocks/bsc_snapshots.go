package freezeblocks

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync"
	"reflect"
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

	snapshots := br.bscSnapshots()

	chainConfig := fromdb.ChainConfig(br.db)
	var minimumBlob uint64
	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	if chainConfig.ChainName == networkname.BSC {
		minimumBlob = bscMinSegFrom
	} else {
		minimumBlob = chapelMinSegFrom
	}
	blockFrom := max(blockReader.FrozenBscBlobs()+1, minimumBlob)
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
		if err := snapshots.OpenFolder(); err != nil {
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
				logger.Info("remove sidecars", "blockNum", i)
			}
			blockHash, _, err := blockReader.CanonicalHash(ctx, roTx, i)
			if err != nil {
				return false, err
			}
			if err = br.bs.RemoveBlobSidecars(ctx, i, blockHash); err != nil {
				logger.Error("remove sidecars", "blockNum", i, "err", err)
			}

		}
		if seedNewSnapshots != nil {
			downloadRequest := []snapshotsync.DownloadRequest{
				snapshotsync.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return false, err
			}
		}
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
	f := coresnaptype.BlobSidecars.FileInfo(snapDir, blockFrom, blockTo)
	sn, err := seg.NewCompressor(ctx, "Snapshot "+f.Type.Name(), f.Path, tmpDir, seg.DefaultCfg, log.LvlTrace, logger)
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
		blockHash, _, err := blockReader.CanonicalHash(ctx, tx, i)
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

	if err := f.Type.BuildIndexes(ctx, f, nil, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return err
	}

	return nil
}

func DumpBlobs(ctx context.Context, blockFrom, blockTo uint64, chainConfig *chain.Config, tmpDir, snapDir string, chainDB kv.RoDB, workers int, lvl log.Lvl, blockReader services.FullBlockReader, blobStore services.BlobStorage, logger log.Logger) error {
	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, coresnaptype.Enums.BscBlobs, chainConfig) {
		blocksPerFile := snapcfg.MergeLimitFromCfg(snapcfg.KnownCfg(""), coresnaptype.Enums.BscBlobs, i)
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
