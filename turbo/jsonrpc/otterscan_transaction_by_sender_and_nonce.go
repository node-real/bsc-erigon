// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"fmt"
	"sort"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/types/accounts"
)

func (api *OtterscanAPIImpl) GetTransactionBySenderAndNonce(ctx context.Context, addr common.Address, nonce uint64) (*common.Hash, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var acc accounts.Account
	it, err := tx.IndexRange(kv.AccountsHistoryIdx, addr[:], -1, -1, order.Asc, kv.Unlim)
	if err != nil {
		return nil, err
	}

	var prevTxnID, nextTxnID uint64
	for i := 0; it.HasNext(); i++ {
		txnID, err := it.Next()
		if err != nil {
			return nil, err
		}

		if i%4096 != 0 { // probe history periodically, not on every change
			nextTxnID = txnID
			continue
		}

		v, ok, err := tx.HistorySeek(kv.AccountsDomain, addr[:], txnID)
		if err != nil {
			log.Error("Unexpected error, couldn't find changeset", "txNum", i, "addr", addr)
			return nil, err
		}
		if !ok {
			err = fmt.Errorf("couldn't find history txnID=%v addr=%v", txnID, addr)
			log.Error("[rpc] Unexpected error", "err", err)
			return nil, err
		}

		if len(v) == 0 { // creation, but maybe not our Incarnation
			prevTxnID = txnID
			continue
		}

		if err := accounts.DeserialiseV3(&acc, v); err != nil {
			return nil, err
		}
		// Desired nonce was found in this chunk
		if acc.Nonce > nonce {
			break
		}
		prevTxnID = txnID
	}

	// The sort.Search function finds the first block where the incarnation has
	// changed to the desired one, so we get the previous block from the bitmap;
	// however if the creationTxnID block is already the first one from the bitmap, it means
	// the block we want is the max block from the previous shard.
	var creationTxnID uint64
	var searchErr error

	if nextTxnID == 0 {
		nextTxnID = prevTxnID + 1
	}
	// Binary search in [prevTxnID, nextTxnID] range; get first block where desired incarnation appears
	// can be replaced by full-scan over ttx.HistoryRange([prevTxnID, nextTxnID])?
	idx := sort.Search(int(nextTxnID-prevTxnID), func(i int) bool {
		txnID := uint64(i) + prevTxnID
		v, ok, err := tx.HistorySeek(kv.AccountsDomain, addr[:], txnID)
		if err != nil {
			log.Error("[rpc] Unexpected error, couldn't find changeset", "txNum", i, "addr", addr)
			panic(err)
		}
		if !ok {
			return false
		}
		if len(v) == 0 {
			creationTxnID = max(creationTxnID, txnID)
			return false
		}

		if err := accounts.DeserialiseV3(&acc, v); err != nil {
			searchErr = err
			return false
		}

		// Since the state contains the nonce BEFORE the block changes, we look for
		// the block when the nonce changed to be > the desired once, which means the
		// previous history block contains the actual change; it may contain multiple
		// nonce changes.
		if acc.Nonce <= nonce {
			creationTxnID = max(creationTxnID, txnID)
			return false
		}
		return true
	})

	if searchErr != nil {
		return nil, searchErr
	}
	if creationTxnID == 0 {
		return nil, nil
	}
	bn, ok, err := api._txNumReader.FindBlockNum(tx, creationTxnID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("block not found by txnID=%d", creationTxnID)
	}
	minTxNum, err := api._txNumReader.Min(tx, bn)
	if err != nil {
		return nil, err
	}
	txIndex := int(creationTxnID) - int(minTxNum) - 1 /* system-tx */
	if txIndex == -1 {
		txIndex = (idx + int(prevTxnID)) - int(minTxNum) - 1
	}
	txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, bn, txIndex)
	if err != nil {
		return nil, err
	}
	if txn == nil {
		log.Warn("[rpc] txn is nil", "blockNum", bn, "txIndex", txIndex)
		return nil, nil
	}
	found := txn.GetNonce() == nonce
	if !found {
		return nil, nil
	}
	txHash := txn.Hash()
	return &txHash, nil
}
