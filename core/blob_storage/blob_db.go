package blob_storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/spf13/afero"
)

const subdivision = 10_000

type BlobStore struct {
	db          kv.RwDB
	fs          afero.Fs
	chainConfig *chain.Config
	blocksKept  uint64
	blockReader services.BlockReader
}

func NewBlobStore(db kv.RwDB, fs afero.Fs, blocksKept uint64, chainConfig *chain.Config, blockReader services.BlockReader) services.BlobStorage {
	return &BlobStore{fs: fs, db: db, blocksKept: blocksKept, chainConfig: chainConfig, blockReader: blockReader}
}

func blobSidecarFilePath(blockNumber uint64, index uint64, hash libcommon.Hash) (folderpath, filepath string) {
	subdir := blockNumber / subdivision
	folderpath = strconv.FormatUint(subdir, 10)
	filepath = fmt.Sprintf("%s/%s_%d", folderpath, hash.String(), index)
	return
}

/*
file system layout: <blockNumber/subdivision>/<blockHash>_<index>
indicies:
- <blockRoot> -> kzg_commitments_length // block
*/

// WriteBlobSidecars writes the sidecars on the database. it assumes that all blobSidecars are for the same block and we have all of them.
func (bs *BlobStore) WriteBlobSidecars(ctx context.Context, blockHash libcommon.Hash, blobSidecars []*types.BlobSidecar) error {
	for i, blobSidecar := range blobSidecars {
		folderPath, filePath := blobSidecarFilePath(blobSidecar.BlockNumber.Uint64(), uint64(i), blobSidecar.BlockHash)
		// mkdir the whole folder and subfolders
		bs.fs.MkdirAll(folderPath, 0755)
		// create the file
		file, err := bs.fs.Create(filePath)
		if err != nil {
			return err
		}
		defer file.Close()

		data, err := rlp.EncodeToBytes(blobSidecar)
		if err != nil {
			return err
		}

		if err := snappyWrite(file, data); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
	}
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, uint32(len(blobSidecars)))
	tx, err := bs.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Put(kv.BlobTxCount, blockHash[:], val); err != nil {
		return err
	}
	return tx.Commit()
}

// ReadBlobSidecars reads the sidecars from the database. it assumes that all blobSidecars are for the same blockRoot and we have all of them.
func (bs *BlobStore) ReadBlobSidecars(ctx context.Context, number uint64, hash libcommon.Hash) (types.BlobSidecars, bool, error) {
	tx, err := bs.db.BeginRo(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	val, err := tx.GetOne(kv.BlobTxCount, hash[:])
	if err != nil {
		return nil, false, err
	}
	if len(val) == 0 {
		return nil, false, nil
	}
	blobTxCount := binary.LittleEndian.Uint32(val)

	var blobSidecars types.BlobSidecars
	for i := uint32(0); i < blobTxCount; i++ {
		_, filePath := blobSidecarFilePath(number, uint64(i), hash)
		file, err := bs.fs.Open(filePath)
		if err != nil {
			if errors.Is(err, afero.ErrFileNotFound) {
				return nil, false, nil
			}
			return nil, false, err
		}
		defer file.Close()

		blobSidecar := &types.BlobSidecar{}
		if err := snappyReader(file, blobSidecar); err != nil {
			return nil, false, err
		}
		blobSidecars = append(blobSidecars, blobSidecar)
	}
	return blobSidecars, true, nil
}

// Do a bit of pruning
func (bs *BlobStore) Prune(current uint64) error {
	if bs.blocksKept == math.MaxUint64 {
		return nil
	}
	currentBlock := current - bs.blocksKept
	currentBlock = (currentBlock / subdivision) * subdivision
	var startPrune uint64
	if currentBlock >= 1_000_000 {
		startPrune = currentBlock - 1_000_000
	}
	// delete all the folders that are older than slotsKept
	for i := startPrune; i < currentBlock; i += subdivision {
		bs.fs.RemoveAll(strconv.FormatUint(i/subdivision, 10))
	}
	return nil
}

func (bs *BlobStore) WriteStream(w io.Writer, number uint64, hash libcommon.Hash, idx uint64) error {
	_, filePath := blobSidecarFilePath(number, idx, hash)
	file, err := bs.fs.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(w, file)
	return err
}

func (bs *BlobStore) BlobTxCount(ctx context.Context, hash libcommon.Hash) (uint32, error) {
	tx, err := bs.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	val, err := tx.GetOne(kv.BlobTxCount, hash[:])
	if err != nil {
		return 0, err
	}
	if len(val) != 4 {
		return 0, nil
	}
	return binary.LittleEndian.Uint32(val), nil
}

func (bs *BlobStore) RemoveBlobSidecars(ctx context.Context, number uint64, hash libcommon.Hash) error {
	tx, err := bs.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	val, err := tx.GetOne(kv.BlobTxCount, hash[:])
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return nil
	}
	blobTxCount := binary.LittleEndian.Uint32(val)
	for i := uint32(0); i < blobTxCount; i++ {
		_, filePath := blobSidecarFilePath(number, uint64(i), hash)
		if err := bs.fs.Remove(filePath); err != nil {
			return err
		}
		tx.Delete(kv.BlobTxCount, hash[:])
	}
	return tx.Commit()
}

func (bs *BlobStore) BlobKept() bool {
	return bs.blocksKept == math.MaxUint64
}
