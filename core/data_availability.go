package core

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

// IsDataAvailable it checks that the blobTx block has available blob data
func IsDataAvailable(chain consensus.ChainHeaderReader, header *types.Header, body *types.RawBody, latest uint64) (err error) {
	if !chain.Config().IsCancun(header.Number.Uint64(), header.Time) {
		if body.Sidecars != nil {
			return errors.New("sidecars present in block body before cancun")
		}
		return nil
	}

	current := chain.CurrentHeader()
	if header.Time+params.MinTimeDurationForBlobRequests < current.Time { // if we needn't check DA of this block, just clean it
		body.CleanSidecars()
		return nil
	}

	// if sidecar is nil, just clean it. And it will be used for saving in ancient.
	if body.Sidecars == nil {
		body.CleanSidecars()
	}

	// alloc block's versionedHashes
	sidecars := body.Sidecars
	for _, s := range sidecars {
		if err := s.SanityCheck(header.Number, header.Hash()); err != nil {
			return err
		}
	}

	blobTxs := make([]types.Transaction, 0, len(sidecars))
	blobTxIndexes := make([]uint64, 0, len(sidecars))
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		return err
	}
	for i, tx := range txs {
		if tx.Type() != types.BlobTxType {
			continue
		}
		blobTxs = append(blobTxs, tx)
		blobTxIndexes = append(blobTxIndexes, uint64(i))
	}

	if len(blobTxs) != len(sidecars) {
		return fmt.Errorf("number %d, hash %v, blob info mismatch: have %d, want:%d", header.Number.Uint64(), header.Hash(), len(sidecars), len(blobTxs))
	}

	// check blob amount
	blobCnt := 0
	for _, s := range sidecars {
		blobCnt += len(s.Blobs)
	}

	maxBlobPerBlock := chain.Config().GetMaxBlobsPerBlock(header.Time)
	if blobCnt > int(maxBlobPerBlock) {
		return fmt.Errorf("too many blobs in block: have %d, permitted %d", blobCnt, maxBlobPerBlock)
	}

	for i, tx := range blobTxs {
		// check sidecar tx related
		if sidecars[i].TxHash != tx.Hash() {
			return fmt.Errorf("sidecar's TxHash mismatch with expected transaction, want: %v, have: %v", sidecars[i].TxHash, tx.Hash())
		}
		if sidecars[i].TxIndex != blobTxIndexes[i] {
			return fmt.Errorf("sidecar's TxIndex mismatch with expected transaction, want: %v, have: %v", sidecars[i].TxIndex, blobTxIndexes[i])
		}
		if err := sidecars[i].ValidateBlobTxSidecar(tx.GetBlobHashes()); err != nil {
			return err
		}
	}
	return nil
}
