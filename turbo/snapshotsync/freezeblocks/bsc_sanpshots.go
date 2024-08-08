package freezeblocks

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/erigon/core/blob_storage"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
	"sync/atomic"
)

type BscSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Salt uint32

	BlobSidecars *segments

	dir         string
	tmpdir      string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
	// allows for pruning segments - this is the min availible segment
	segmentsMin atomic.Uint64
}

func retireBlobSidecarsRange(ctx context.Context, db kv.RoDB, store blob_storage.BlobStore, from uint64, to uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	tmpDir, snapDir := dirs.Tmp, dirs.Snap

	segName := snaptype.BlobSidecars.FileName(0, from, to)
	f, _, _ := snaptype.ParseFileName(snapDir, segName)

	sn, err := seg.NewCompressor(ctx, "Snapshot BlobSidecars", f.Path, tmpDir, seg.MinPatternScore, workers, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	reusableBuf := []byte{}

	// Generate .seg file, which is just the list of beacon blocks.
	for i := from; i < to; i++ {
		// read root.
		blockHash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			return err
		}

		blobTxCount, err := store.BlobTxCount(ctx, blockHash)
		if err != nil {
			return err
		}
		if blobTxCount == 0 {
			sn.AddWord(nil)
			continue
		}
		sidecars, found, err := store.ReadBlobSidecars(ctx, i, blockHash)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("blob sidecars not found for block %d", i)
		}
		reusableBuf = reusableBuf[:0]
		// Make a concatenated SSZ of all sidecars.
		for _, sidecar := range sidecars {
			reusableBuf, err = sidecar.EncodeRLP(reusableBuf)
			if err != nil {
				return err
			}
		}

		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping beacon blobs", "progress", i)
		}
		if err := sn.AddWord(reusableBuf); err != nil {
			return err
		}

	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return BeaconSimpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}
