package core

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

// IsDataAvailable it checks that the blobTx block has available blob data
func IsDataAvailable(chain consensus.ChainHeaderReader, header *types.Header, body *types.RawBody) (err error) {
	if !chain.Config().IsCancun(header.Number.Uint64(), header.Time) {
		if body.Sidecars != nil {
			return errors.New("sidecars present in block body before cancun")
		}
		return nil
	}

	current := chain.CurrentHeader()
	if header.Number.Uint64()+params.MinBlocksForBlobRequests < current.Number.Uint64() {
		// if we needn't check DA of this block, just clean it
		body.CleanSidecars()
		return nil
	}

	// if sidecar is nil, just clean it. And it will be used for saving in ancient.
	if body.Sidecars == nil {
		body.CleanSidecars()
	}

	// alloc block's versionedHashes
	sidecars := body.Sidecars
	blobIndex := 0
	txs, err := types.DecodeTransactions(body.Transactions)
	if err != nil {
		return err
	}
	for _, tx := range txs {
		if tx.Type() != types.BlobTxType {
			continue
		}
		if err := sidecars[blobIndex].ValidateBlobTxSidecar(tx.GetBlobHashes()); err != nil {
			return err
		}
		blobIndex++
	}
	if blobIndex != len(sidecars) {
		return fmt.Errorf("blob sidecars count mismatch with blob txs count %d  sidecars: %d", blobIndex, len(sidecars))
	}
	return nil
}
