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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/polygon/bor/borcfg"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/finality/whitelist"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
)

type Snapshot struct {
	config *borcfg.BorConfig // Consensus engine parameters to fine tune behavior

	Number       uint64        `json:"number"`       // Block number where the snapshot was created
	Hash         common.Hash   `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *ValidatorSet `json:"validatorSet"` // Validator set at this moment
}

// GetSnapshot retrieves the state snapshot at a given block.
func (api *BorImpl) GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		header, _ = getHeaderByNumber(ctx, *number, api, tx)
	}
	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	// init consensus db
	borEngine, err := api.bor()
	if err != nil {
		return nil, err
	}

	borTx, err := borEngine.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()

	if api.useSpanProducersReader {
		validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
		if err != nil {
			return nil, err
		}

		snap := &Snapshot{
			Number:       header.Number.Uint64(),
			Hash:         header.Hash(),
			ValidatorSet: validatorSet,
		}

		return snap, nil
	}

	return snapshot(ctx, api, tx, borTx, header)
}

// GetAuthor retrieves the author a block.
func (api *BorImpl) GetAuthor(blockNrOrHash *rpc.BlockNumberOrHash) (*common.Address, error) {
	// init consensus db
	borEngine, err := api.bor()

	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header

	//nolint:nestif
	if blockNrOrHash == nil {
		latestBlockNum, err2 := rpchelper.GetLatestBlockNumber(tx)
		if err2 != nil {
			return nil, err2
		}
		header, err = api._blockReader.HeaderByNumber(ctx, tx, latestBlockNum)
	} else {
		if blockNr, ok := blockNrOrHash.Number(); ok {
			header, err = api._blockReader.HeaderByNumber(ctx, tx, uint64(blockNr))
		} else {
			if blockHash, ok := blockNrOrHash.Hash(); ok {
				header, err = rawdb.ReadHeaderByHash(tx, blockHash)
			}
		}
	}

	// Ensure we have an actually valid block and return its snapshot
	if header == nil || err != nil {
		return nil, errUnknownBlock
	}

	author, err := borEngine.Author(header)

	return &author, err
}

// GetSnapshotAtHash retrieves the state snapshot at a given block.
func (api *BorImpl) GetSnapshotAtHash(hash common.Hash) (*Snapshot, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retreive the header
	header, _ := getHeaderByHash(ctx, api, tx, hash)

	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	// init consensus db
	borEngine, err := api.bor()

	if err != nil {
		return nil, err
	}

	borTx, err := borEngine.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()

	if api.useSpanProducersReader {
		validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
		if err != nil {
			return nil, err
		}

		snap := &Snapshot{
			Number:       header.Number.Uint64(),
			Hash:         header.Hash(),
			ValidatorSet: validatorSet,
		}

		return snap, nil
	}

	return snapshot(ctx, api, tx, borTx, header)
}

// GetSigners retrieves the list of authorized signers at the specified block.
func (api *BorImpl) GetSigners(number *rpc.BlockNumber) ([]common.Address, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		header, _ = getHeaderByNumber(ctx, *number, api, tx)
	}
	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	// init consensus db
	borEngine, err := api.bor()

	if err != nil {
		return nil, err
	}

	borTx, err := borEngine.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()

	if api.useSpanProducersReader {
		validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
		if err != nil {
			return nil, err
		}

		snap := &Snapshot{
			Number:       header.Number.Uint64(),
			Hash:         header.Hash(),
			ValidatorSet: validatorSet,
		}

		return snap.signers(), nil
	}

	snap, err := snapshot(ctx, api, tx, borTx, header)
	if err != nil {
		return nil, err
	}

	return snap.signers(), err
}

// GetSignersAtHash retrieves the list of authorized signers at the specified block.
func (api *BorImpl) GetSignersAtHash(hash common.Hash) ([]common.Address, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retreive the header
	header, _ := getHeaderByHash(ctx, api, tx, hash)

	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	// init consensus db
	borEngine, err := api.bor()

	if err != nil {
		return nil, err
	}

	borTx, err := borEngine.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()

	if api.useSpanProducersReader {
		validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
		if err != nil {
			return nil, err
		}

		snap := &Snapshot{
			Number:       header.Number.Uint64(),
			Hash:         header.Hash(),
			ValidatorSet: validatorSet,
		}

		return snap.signers(), nil
	}

	snap, err := snapshot(ctx, api, tx, borTx, header)
	if err != nil {
		return nil, err
	}

	return snap.signers(), nil
}

// GetCurrentProposer gets the current proposer
func (api *BorImpl) GetCurrentProposer() (common.Address, error) {
	snap, err := api.GetSnapshot(nil)
	if err != nil {
		return common.Address{}, err
	}
	return snap.ValidatorSet.GetProposer().Address, nil
}

// GetCurrentValidators gets the current validators
func (api *BorImpl) GetCurrentValidators() ([]*valset.Validator, error) {
	snap, err := api.GetSnapshot(nil)
	if err != nil {
		return make([]*valset.Validator, 0), err
	}
	return snap.ValidatorSet.Validators, nil
}

// GetVoteOnHash gets the vote on milestone hash
func (api *BorImpl) GetVoteOnHash(ctx context.Context, starBlockNr uint64, endBlockNr uint64, hash string, milestoneId string) (bool, error) {
	tx, err := api.db.BeginRo(context.Background())
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	//Confirmation of 16 blocks on the endblock
	tipConfirmationBlockNr := endBlockNr + uint64(16)

	//Check if tipConfirmation block exit
	_, err = api._blockReader.BlockByNumber(ctx, tx, tipConfirmationBlockNr)
	if err != nil {
		return false, errors.New("failed to get tip confirmation block")
	}

	//Check if end block exist
	localEndBlock, err := api._blockReader.BlockByNumber(ctx, tx, endBlockNr)
	if err != nil {
		return false, errors.New("failed to get end block")
	}

	localEndBlockHash := localEndBlock.Hash().String()

	// TODO whitelisting service is pending removal - https://github.com/erigontech/erigon/issues/12855
	if service := whitelist.GetWhitelistingService(); service != nil {
		isLocked := service.LockMutex(endBlockNr)

		if !isLocked {
			service.UnlockMutex(false, "", endBlockNr, common.Hash{})
			return false, errors.New("whitelisted number or locked sprint number is more than the received end block number")
		}

		if localEndBlockHash != hash {
			service.UnlockMutex(false, "", endBlockNr, common.Hash{})
			return false, fmt.Errorf("hash mismatch: localChainHash %s, milestoneHash %s", localEndBlockHash, hash)
		}

		service.UnlockMutex(true, milestoneId, endBlockNr, localEndBlock.Hash())

		return true, nil
	}

	return localEndBlockHash == hash, nil
}

type BlockSigners struct {
	Signers []difficultiesKV
	Diff    int
	Author  common.Address
}

type difficultiesKV struct {
	Signer     common.Address
	Difficulty uint64
}

// GetSnapshotProposer retrieves the in-turn signer at a given block.
func (api *BorImpl) GetSnapshotProposer(blockNrOrHash *rpc.BlockNumberOrHash) (common.Address, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return common.Address{}, err
	}
	defer tx.Rollback()

	var header *types.Header
	//nolint:nestif
	if blockNrOrHash == nil {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		if blockNr, ok := blockNrOrHash.Number(); ok {
			if blockNr == rpc.LatestBlockNumber {
				header = rawdb.ReadCurrentHeader(tx)
			} else {
				header, err = getHeaderByNumber(ctx, blockNr, api, tx)
			}
		} else {
			if blockHash, ok := blockNrOrHash.Hash(); ok {
				header, err = getHeaderByHash(ctx, api, tx, blockHash)
			}
		}
	}

	if header == nil || err != nil {
		return common.Address{}, errUnknownBlock
	}

	borEngine, err := api.bor()
	if err != nil {
		return common.Address{}, err
	}

	borTx, err := borEngine.DB.BeginRo(ctx)
	if err != nil {
		return common.Address{}, err
	}
	defer borTx.Rollback()

	var snap *Snapshot
	if api.useSpanProducersReader {
		validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
		if err != nil {
			return common.Address{}, err
		}

		snap = &Snapshot{
			Number:       header.Number.Uint64(),
			Hash:         header.Hash(),
			ValidatorSet: validatorSet,
		}
	} else {
		parent, err := getHeaderByNumber(ctx, rpc.BlockNumber(int64(header.Number.Uint64()-1)), api, tx)
		if parent == nil || err != nil {
			return common.Address{}, errUnknownBlock
		}

		snap, err = snapshot(ctx, api, tx, borTx, parent)
		if err != nil {
			return common.Address{}, err
		}
	}

	return snap.ValidatorSet.GetProposer().Address, nil
}

func (api *BorImpl) GetSnapshotProposerSequence(blockNrOrHash *rpc.BlockNumberOrHash) (BlockSigners, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return BlockSigners{}, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if blockNrOrHash == nil {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		if blockNr, ok := blockNrOrHash.Number(); ok {
			if blockNr == rpc.LatestBlockNumber {
				header = rawdb.ReadCurrentHeader(tx)
			} else {
				header, err = getHeaderByNumber(ctx, blockNr, api, tx)
			}
		} else {
			if blockHash, ok := blockNrOrHash.Hash(); ok {
				header, err = getHeaderByHash(ctx, api, tx, blockHash)
			}
		}
	}

	// Ensure we have an actually valid block
	if header == nil || err != nil {
		return BlockSigners{}, errUnknownBlock
	}

	// init consensus db
	borEngine, err := api.bor()

	if err != nil {
		return BlockSigners{}, err
	}

	borTx, err := borEngine.DB.BeginRo(ctx)
	if err != nil {
		return BlockSigners{}, err
	}
	defer borTx.Rollback()

	var snap *Snapshot
	if api.useSpanProducersReader {
		validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
		if err != nil {
			return BlockSigners{}, err
		}

		snap = &Snapshot{
			Number:       header.Number.Uint64(),
			Hash:         header.Hash(),
			ValidatorSet: validatorSet,
		}
	} else {
		parent, err := getHeaderByNumber(ctx, rpc.BlockNumber(int64(header.Number.Uint64()-1)), api, tx)
		if parent == nil || err != nil {
			return BlockSigners{}, errUnknownBlock
		}

		snap, err = snapshot(ctx, api, tx, borTx, parent)
		if err != nil {
			return BlockSigners{}, err
		}
	}

	var difficulties = make(map[common.Address]uint64)

	proposer := snap.ValidatorSet.GetProposer().Address
	proposerIndex, _ := snap.ValidatorSet.GetByAddress(proposer)

	signers := snap.signers()
	for i := 0; i < len(signers); i++ {
		tempIndex := i
		if tempIndex < proposerIndex {
			tempIndex = tempIndex + len(signers)
		}

		difficulties[signers[i]] = uint64(len(signers) - (tempIndex - proposerIndex))
	}

	rankedDifficulties := rankMapDifficulties(difficulties)

	author, err := author(api, tx, header)
	if err != nil {
		return BlockSigners{}, err
	}

	diff := int(difficulties[author])
	blockSigners := BlockSigners{
		Signers: rankedDifficulties,
		Diff:    diff,
		Author:  author,
	}

	return blockSigners, nil
}

// GetRootHash returns the merkle root of the start to end block headers
func (api *BorImpl) GetRootHash(start, end uint64) (string, error) {
	borEngine, err := api.bor()

	if err != nil {
		return "", err
	}

	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	return borEngine.GetRootHash(ctx, tx, start, end)
}

// Helper functions for Snapshot Type

// copy creates a deep copy of the snapshot, though not the individual votes.
func (s *Snapshot) copy() *Snapshot {
	cpy := &Snapshot{
		config:       s.config,
		Number:       s.Number,
		Hash:         s.Hash,
		ValidatorSet: s.ValidatorSet.Copy(),
	}
	return cpy
}

// signers retrieves the list of authorized signers in ascending order.
func (s *Snapshot) signers() []common.Address {
	sigs := make([]common.Address, 0, len(s.ValidatorSet.Validators))
	for _, sig := range s.ValidatorSet.Validators {
		sigs = append(sigs, sig.Address)
	}
	return sigs
}

// apply header changes on snapshot
func (s *Snapshot) apply(headers []*types.Header) (*Snapshot, error) {
	// Allow passing in no headers for cleaner code
	if len(headers) == 0 {
		return s, nil
	}
	// Sanity check that the headers can be applied
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			return nil, errOutOfRangeChain
		}
	}
	if headers[0].Number.Uint64() != s.Number+1 {
		return nil, errOutOfRangeChain
	}
	// Iterate through the headers and create a new snapshot
	snap := s.copy()

	for _, header := range headers {
		// Remove any votes on checkpoint blocks
		number := header.Number.Uint64()

		// Resolve the authorization key and check against signers
		signer, err := ecrecover(header, s.config)
		if err != nil {
			return nil, err
		}

		// check if signer is in validator set
		if !snap.ValidatorSet.HasAddress(signer) {
			return nil, &valset.UnauthorizedSignerError{Number: number, Signer: signer.Bytes()}
		}

		// change validator set and change proposer
		if number > 0 && s.config.IsSprintEnd(number) {
			if err := bor.ValidateHeaderExtraLength(header.Extra); err != nil {
				return nil, err
			}
			validatorBytes := bor.GetValidatorBytes(header, s.config)

			// get validators from headers and use that for new validator set
			newVals, _ := valset.ParseValidators(validatorBytes)
			v := getUpdatedValidatorSet(snap.ValidatorSet.Copy(), newVals)
			v.IncrementProposerPriority(1)
			snap.ValidatorSet = v
		}
	}

	snap.Number += uint64(len(headers))
	snap.Hash = headers[len(headers)-1].Hash()

	return snap, nil
}

// snapshot retrieves the authorization snapshot at a given point in time.
func snapshot(ctx context.Context, api *BorImpl, db kv.Tx, borDb kv.Tx, header *types.Header) (*Snapshot, error) {
	// Search for a snapshot on disk or build it from checkpoint
	var (
		headers []*types.Header
		snap    *Snapshot
	)

	number := header.Number.Uint64()
	hash := header.Hash()

	for snap == nil { //nolint:govet
		// If an on-disk checkpoint snapshot can be found, use that
		if number%checkpointInterval == 0 {
			if s, err := loadSnapshot(api, db, borDb, hash); err == nil {
				log.Info("Loaded snapshot from disk", "number", number, "hash", hash)
				snap = s
			}
			break
		}

		// No snapshot for this header, move backward and check parent snapshots
		if header == nil {
			header, _ = getHeaderByNumber(ctx, rpc.BlockNumber(number), api, db)
			if header == nil {
				return nil, consensus.ErrUnknownAncestor
			}
		}
		headers = append(headers, header)
		number, hash = number-1, header.ParentHash
		header = nil
	}

	if snap == nil {
		return nil, fmt.Errorf("unknown error while retrieving snapshot at block number %v", number)
	}

	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	snap, err := snap.apply(headers)
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// loadSnapshot loads an existing snapshot from the database.
func loadSnapshot(api *BorImpl, db kv.Tx, borDb kv.Tx, hash common.Hash) (*Snapshot, error) {
	blob, err := borDb.GetOne(kv.BorSeparate, append([]byte("bor-"), hash[:]...))
	if err != nil {
		return nil, err
	}
	snap := new(Snapshot)
	if err := json.Unmarshal(blob, snap); err != nil {
		return nil, err
	}
	borEngine, _ := api.bor()
	snap.config = borEngine.Config()

	// update total voting power
	if err := snap.ValidatorSet.UpdateTotalVotingPower(); err != nil {
		return nil, err
	}

	return snap, nil
}
