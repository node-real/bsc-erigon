// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package core implements the Ethereum consensus protocol.
package core

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/erigontech/erigon/core/systemcontracts"

	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"

	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/ethutils"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
)

var (
	blockExecutionTimer = metrics.GetOrCreateSummary("chain_execution_seconds")
)

type SyncMode string

const (
	TriesInMemory = 128

	// See gas_limit in https://github.com/gnosischain/specs/blob/master/execution/withdrawals.md
	SysCallGasLimit = uint64(30_000_000)
)

type RejectedTx struct {
	Index int    `json:"index"    gencodec:"required"`
	Err   string `json:"error"    gencodec:"required"`
}

type RejectedTxs []*RejectedTx

type EphemeralExecResult struct {
	StateRoot        libcommon.Hash        `json:"stateRoot"`
	TxRoot           libcommon.Hash        `json:"txRoot"`
	ReceiptRoot      libcommon.Hash        `json:"receiptsRoot"`
	LogsHash         libcommon.Hash        `json:"logsHash"`
	Bloom            types.Bloom           `json:"logsBloom"        gencodec:"required"`
	Receipts         types.Receipts        `json:"receipts"`
	Rejected         RejectedTxs           `json:"rejected,omitempty"`
	Difficulty       *math.HexOrDecimal256 `json:"currentDifficulty" gencodec:"required"`
	GasUsed          math.HexOrDecimal64   `json:"gasUsed"`
	StateSyncReceipt *types.Receipt        `json:"-"`
}

func ExecuteBlockEphemerallyForBSC(
	chainConfig *chain.Config,
	vmConfig *vm.Config,
	blockHashFunc func(n uint64) libcommon.Hash,
	engine consensus.Engine, block *types.Block,
	stateReader state.StateReader, stateWriter state.WriterWithChangeSets,
	chainReader consensus.ChainReader, getTracer func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error),
	logger log.Logger,
) (*EphemeralExecResult, error) {
	defer blockExecutionTimer.ObserveDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()

	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(block.Time()))

	var (
		rejectedTxs []*RejectedTx
		includedTxs types.Transactions
		receipts    types.Receipts
	)

	if err := InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, ibs, nil, logger, nil); err != nil {
		return nil, err
	}

	noop := state.NewNoopWriter()
	posa, isPoSA := engine.(consensus.PoSA)
	//fmt.Printf("====txs processing start: %d====\n", block.NumberU64())

	for i, tx := range block.Transactions() {
		if isPoSA {
			if isSystemTx, err := posa.IsSystemTransaction(tx, block.Header()); err != nil {
				return nil, err
			} else if isSystemTx {
				continue
			}
		}
		ibs.SetTxContext(i, block.NumberU64())
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			tracer, err := getTracer(i, tx.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}

		receipt, _, err := ApplyTransaction(chainConfig, blockHashFunc, engine, nil, gp, ibs, noop, header, tx, usedGas, usedBlobGas, *vmConfig)
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(tx)
			}

			vmConfig.Tracer = nil
		}
		if err != nil {
			if !vmConfig.StatelessExec {
				return nil, fmt.Errorf("could not apply tx %d from block %d [%v]: %w", i, block.NumberU64(), tx.Hash().Hex(), err)
			}
			rejectedTxs = append(rejectedTxs, &RejectedTx{i, err.Error()})
		} else {
			includedTxs = append(includedTxs, tx)
			if !vmConfig.NoReceipts {
				receipts = append(receipts, receipt)
			}
		}
	}

	var newBlock *types.Block
	var receiptSha libcommon.Hash
	if !vmConfig.ReadOnly {
		// We're doing this hack for BSC to avoid changing consensus interfaces a lot. BSC modifies txs and receipts by appending
		// system transactions, and they increase used gas and write cumulative gas to system receipts, that's why we need
		// to deduct system gas before. This line is equal to "blockGas-systemGas", but since we don't know how much gas is
		// used by system transactions we just override. Of course, we write used by block gas back. It also always true
		// that used gas by block is always equal to original's block header gas, and it's checked by receipts root verification
		// otherwise it causes block verification error.
		header.GasUsed = *usedGas
		syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
			return SysCallContract(contract, data, chainConfig, ibs, header, engine, false /* constCall */)
		}
		outTxs, outReceipts, _, err := engine.Finalize(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), chainReader, syscall, false, nil, 0, logger)
		if err != nil {
			return nil, err
		}
		*usedGas = header.GasUsed

		// We need repack this block because transactions and receipts might be changed by consensus, and
		// it won't pass receipts hash or bloom verification
		newBlock = types.NewBlock(block.Header(), outTxs, block.Uncles(), outReceipts, block.Withdrawals())
		// Update receipts
		if !vmConfig.NoReceipts {
			receipts = outReceipts
		}
		receiptSha = newBlock.ReceiptHash()
	} else {
		newBlock = block
		receiptSha = types.DeriveSha(receipts)
	}

	if chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts {
		if !vmConfig.StatelessExec && receiptSha != block.ReceiptHash() {
			return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
		}
	}
	if !vmConfig.StatelessExec && newBlock.GasUsed() != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d, in new Block: %v", *usedGas, header.GasUsed, newBlock.GasUsed())
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = newBlock.Bloom()
		if !vmConfig.StatelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}

	if err := ibs.CommitBlock(chainConfig.Rules(header.Number.Uint64(), header.Time), stateWriter); err != nil {
		return nil, fmt.Errorf("committing block %d failed: %w", header.Number.Uint64(), err)
	} else if err := stateWriter.WriteChangeSets(); err != nil {
		return nil, fmt.Errorf("writing changesets for block %d failed: %w", header.Number.Uint64(), err)
	}

	execRs := &EphemeralExecResult{
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		Receipts:    receipts,
		Difficulty:  (*math.HexOrDecimal256)(block.Header().Difficulty),
		GasUsed:     math.HexOrDecimal64(*usedGas),
		Rejected:    rejectedTxs,
	}

	return execRs, nil
}

// ExecuteBlockEphemerally runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerally(
	chainConfig *chain.Config, vmConfig *vm.Config,
	blockHashFunc func(n uint64) libcommon.Hash,
	engine consensus.Engine, block *types.Block,
	stateReader state.StateReader, stateWriter state.WriterWithChangeSets,
	chainReader consensus.ChainReader, getTracer func(txIndex int, txHash libcommon.Hash) (vm.EVMLogger, error),
	logger log.Logger,
) (*EphemeralExecResult, error) {

	defer blockExecutionTimer.ObserveDuration(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()

	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(GasPool)
	gp.AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(block.Time()))

	// TODO: send the new tracer once we switch to the tracing.Hook
	if err := InitializeBlockExecution(engine, chainReader, block.Header(), chainConfig, ibs, stateWriter, logger, nil); err != nil {
		return nil, err
	}

	var rejectedTxs []*RejectedTx
	includedTxs := make(types.Transactions, 0, block.Transactions().Len())
	receipts := make(types.Receipts, 0, block.Transactions().Len())
	// noop := state.NewNoopWriter()
	for i, txn := range block.Transactions() {
		ibs.SetTxContext(i, block.NumberU64())
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			tracer, err := getTracer(i, txn.Hash())
			if err != nil {
				return nil, fmt.Errorf("could not obtain tracer: %w", err)
			}
			vmConfig.Tracer = tracer
			writeTrace = true
		}
		receipt, _, err := ApplyTransaction(chainConfig, blockHashFunc, engine, nil, gp, ibs, stateWriter, header, txn, usedGas, usedBlobGas, *vmConfig)
		if writeTrace {
			if ftracer, ok := vmConfig.Tracer.(vm.FlushableTracer); ok {
				ftracer.Flush(txn)
			}

			vmConfig.Tracer = nil
		}
		if err != nil {
			if !vmConfig.StatelessExec {
				return nil, fmt.Errorf("could not apply txn %d from block %d [%v]: %w", i, block.NumberU64(), txn.Hash().Hex(), err)
			}
			rejectedTxs = append(rejectedTxs, &RejectedTx{i, err.Error()})
		} else {
			includedTxs = append(includedTxs, txn)
			if !vmConfig.NoReceipts {
				receipts = append(receipts, receipt)
			}
		}
	}

	receiptSha := types.DeriveSha(receipts)
	if !vmConfig.StatelessExec && chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		if dbg.LogHashMismatchReason() {
			logReceipts(receipts, includedTxs, chainConfig, header, logger)
		}

		return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	if !vmConfig.StatelessExec && *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}

	if header.BlobGasUsed != nil && *usedBlobGas != *header.BlobGasUsed {
		return nil, fmt.Errorf("blob gas used by execution: %d, in header: %d", *usedBlobGas, *header.BlobGasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !vmConfig.StatelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}
	var newBlock *types.Block
	var err error
	if !vmConfig.ReadOnly {
		txs := block.Transactions()
		newBlock, _, _, _, err = FinalizeBlockExecution(engine, stateReader, block.Header(), txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, true, logger)
		if err != nil {
			return nil, err
		}
	}
	blockLogs := ibs.Logs()
	newRoot := newBlock.Root()
	execRs := &EphemeralExecResult{
		StateRoot:   newRoot,
		TxRoot:      types.DeriveSha(includedTxs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		LogsHash:    rlpHash(blockLogs),
		Receipts:    receipts,
		Difficulty:  (*math.HexOrDecimal256)(header.Difficulty),
		GasUsed:     math.HexOrDecimal64(*usedGas),
		Rejected:    rejectedTxs,
	}

	if chainConfig.Bor != nil {
		var logs []*types.Log
		for _, receipt := range receipts {
			logs = append(logs, receipt.Logs...)
		}

		stateSyncReceipt := &types.Receipt{}
		if chainConfig.Consensus == chain.BorConsensus && len(blockLogs) > 0 {
			slices.SortStableFunc(blockLogs, func(i, j *types.Log) int { return cmp.Compare(i.Index, j.Index) })

			if len(blockLogs) > len(logs) {
				stateSyncReceipt.Logs = blockLogs[len(logs):] // get state-sync logs from `state.Logs()`

				// fill the state sync with the correct information
				bortypes.DeriveFieldsForBorReceipt(stateSyncReceipt, block.Hash(), block.NumberU64(), receipts)
				stateSyncReceipt.Status = types.ReceiptStatusSuccessful
			}
		}

		execRs.StateSyncReceipt = stateSyncReceipt
	}

	return execRs, nil
}

func logReceipts(receipts types.Receipts, txns types.Transactions, cc *chain.Config, header *types.Header, logger log.Logger) {
	if len(receipts) == 0 {
		// no-op, can happen if vmConfig.NoReceipts=true or vmConfig.StatelessExec=true
		return
	}

	// note we do not return errors from this func since this is a debug-only
	// informative feature that is best-effort and should not interfere with execution
	if len(receipts) != len(txns) {
		logger.Error("receipts and txns sizes differ", "receiptsLen", receipts.Len(), "txnsLen", txns.Len())
		return
	}

	marshalled := make([]map[string]interface{}, 0, len(receipts))
	for i, receipt := range receipts {
		txn := txns[i]
		marshalled = append(marshalled, ethutils.MarshalReceipt(receipt, txn, cc, header, txn.Hash(), true))
	}

	result, err := json.Marshal(marshalled)
	if err != nil {
		logger.Error("marshalling error when logging receipts", "err", err)
		return
	}

	logger.Info("marshalled receipts", "result", string(result))
}

func rlpHash(x interface{}) (h libcommon.Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, x) //nolint:errcheck
	hw.Sum(h[:0])
	return h
}

func SysCallContract(contract libcommon.Address, data []byte, chainConfig *chain.Config, ibs evmtypes.IntraBlockState, header *types.Header, engine consensus.EngineReader, constCall bool) (result []byte, err error) {
	isBor := chainConfig.Bor != nil
	var author *libcommon.Address
	if isBor {
		author = &header.Coinbase
	} else {
		author = &state.SystemAddress
	}
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), engine, author, chainConfig)
	return SysCallContractWithBlockContext(contract, data, chainConfig, ibs, blockContext, constCall)
}

func SysCallContractWithBlockContext(contract libcommon.Address, data []byte, chainConfig *chain.Config, ibs evmtypes.IntraBlockState, blockContext evmtypes.BlockContext, constCall bool) (result []byte, err error) {
	msg := types.NewMessage(
		state.SystemAddress,
		&contract,
		0, u256.Num0,
		SysCallGasLimit,
		u256.Num0,
		nil, nil,
		data, nil, false,
		true, // isFree
		nil,  // maxFeePerBlobGas
	)
	vmConfig := vm.Config{NoReceipts: true, RestoreState: constCall}
	// Create a new context to be used in the EVM environment
	isBor := chainConfig.Bor != nil
	var txContext evmtypes.TxContext
	if isBor {
		txContext = evmtypes.TxContext{}
	} else {
		txContext = NewEVMTxContext(msg)
	}
	evm := vm.NewEVM(blockContext, txContext, ibs, chainConfig, vmConfig)

	ret, _, err := evm.Call(
		vm.AccountRef(msg.From()),
		*msg.To(),
		msg.Data(),
		msg.Gas(),
		msg.Value(),
		false,
	)
	if isBor && err != nil {
		return nil, nil
	}
	return ret, err
}

// SysCreate is a special (system) contract creation methods for genesis constructors.
func SysCreate(contract libcommon.Address, data []byte, chainConfig *chain.Config, ibs *state.IntraBlockState, header *types.Header) (result []byte, err error) {
	msg := types.NewMessage(
		contract,
		nil, // to
		0, u256.Num0,
		SysCallGasLimit,
		u256.Num0,
		nil, nil,
		data, nil, false,
		true, // isFree
		nil,  // maxFeePerBlobGas
	)
	vmConfig := vm.Config{NoReceipts: true}
	// Create a new context to be used in the EVM environment
	author := &contract
	txContext := NewEVMTxContext(msg)
	blockContext := NewEVMBlockContext(header, GetHashFn(header, nil), nil, author, chainConfig)
	evm := vm.NewEVM(blockContext, txContext, ibs, chainConfig, vmConfig)

	ret, _, err := evm.SysCreate(
		vm.AccountRef(msg.From()),
		msg.Data(),
		msg.Gas(),
		msg.Value(),
		contract,
	)
	return ret, err
}

func FinalizeBlockExecution(
	engine consensus.Engine, stateReader state.StateReader,
	header *types.Header, txs types.Transactions, uncles []*types.Header,
	stateWriter state.StateWriter, cc *chain.Config,
	ibs *state.IntraBlockState, receipts types.Receipts,
	withdrawals []*types.Withdrawal, chainReader consensus.ChainReader,
	isMining bool,
	logger log.Logger,
) (newBlock *types.Block, newTxs types.Transactions, newReceipt types.Receipts, retRequests types.FlatRequests, err error) {
	syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
		return SysCallContract(contract, data, cc, ibs, header, engine, false /* constCall */)
	}

	if isMining {
		newBlock, newTxs, newReceipt, retRequests, err = engine.FinalizeAndAssemble(cc, header, ibs, txs, uncles, receipts, withdrawals, chainReader, syscall, nil, logger)
	} else {
		newTxs, newReceipt, retRequests, err = engine.Finalize(cc, header, ibs, txs, uncles, receipts, withdrawals, chainReader, syscall, false, nil, 0, logger)
	}
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if err := ibs.CommitBlock(cc.Rules(header.Number.Uint64(), header.Time), stateWriter); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("committing block %d failed: %w", header.Number.Uint64(), err)
	}

	if casted, ok := stateWriter.(state.WriterWithChangeSets); ok {
		if err := casted.WriteChangeSets(); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("writing changesets for block %d failed: %w", header.Number.Uint64(), err)
		}
	}
	return newBlock, newTxs, newReceipt, retRequests, nil
}

func InitializeBlockExecution(engine consensus.Engine, chain consensus.ChainHeaderReader, header *types.Header,
	cc *chain.Config, ibs *state.IntraBlockState, stateWriter state.StateWriter, logger log.Logger, tracer *tracing.Hooks,
) error {
	// skip this for bsc
	if cc.Parlia != nil {
		return nil
	}
	engine.Initialize(cc, chain, header, ibs, func(contract libcommon.Address, data []byte, ibState *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		return SysCallContract(contract, data, cc, ibState, header, engine, constCall)
	}, logger, tracer)
	if stateWriter == nil {
		stateWriter = state.NewNoopWriter()
	}
	ibs.FinalizeTx(cc.Rules(header.Number.Uint64(), header.Time), stateWriter)
	return nil
}

func BlockPostValidation(gasUsed, blobGasUsed uint64, checkReceipts bool, receipts types.Receipts, h *types.Header, isMining bool, txns types.Transactions, chainConfig *chain.Config, logger log.Logger) error {
	if gasUsed != h.GasUsed {
		return fmt.Errorf("gas used by execution: %d, in header: %d, headerNum=%d, %x",
			gasUsed, h.GasUsed, h.Number.Uint64(), h.Hash())
	}

	if h.BlobGasUsed != nil && blobGasUsed != *h.BlobGasUsed {
		return fmt.Errorf("blobGasUsed by execution: %d, in header: %d, headerNum=%d, %x",
			blobGasUsed, *h.BlobGasUsed, h.Number.Uint64(), h.Hash())
	}
	if checkReceipts {
		for _, r := range receipts {
			r.Bloom = types.CreateBloom(types.Receipts{r})
		}
		receiptHash := types.DeriveSha(receipts)
		if receiptHash != h.ReceiptHash {
			if isMining {
				h.ReceiptHash = receiptHash
				return nil
			}
			if dbg.LogHashMismatchReason() {
				logReceipts(receipts, txns, chainConfig, h, logger)
			}
			return fmt.Errorf("receiptHash mismatch: %x != %x, headerNum=%d, %x",
				receiptHash, h.ReceiptHash, h.Number.Uint64(), h.Hash())
		}

		lbloom := types.CreateBloom(receipts)
		if lbloom != h.Bloom {
			return fmt.Errorf("invalid bloom (remote: %x  local: %x)", h.Bloom, lbloom)
		}
	}
	return nil
}

var systemContracts = map[libcommon.Address]struct{}{
	systemcontracts.ValidatorContract:          {},
	systemcontracts.SlashContract:              {},
	systemcontracts.SystemRewardContract:       {},
	systemcontracts.LightClientContract:        {},
	systemcontracts.RelayerHubContract:         {},
	systemcontracts.GovHubContract:             {},
	systemcontracts.TokenHubContract:           {},
	systemcontracts.RelayerIncentivizeContract: {},
	systemcontracts.CrossChainContract:         {},
	systemcontracts.StakeHubContract:           {},
	systemcontracts.GovernorContract:           {},
	systemcontracts.GovTokenContract:           {},
	systemcontracts.TimelockContract:           {},
	systemcontracts.TokenRecoverPortalContract: {},
}

func IsToSystemContract(to libcommon.Address) bool {
	_, ok := systemContracts[to]
	return ok
}
