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

package exec3

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon/consensus/misc"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/core/systemcontracts"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/turbo/services"
)

type HistoricalTraceWorker struct {
	consumer TraceConsumer
	in       *state.QueueWithRetry
	out      *state.ResultsQueue

	stateReader *state.HistoryReaderV3
	ibs         *state.IntraBlockState
	evm         *vm.EVM

	chainTx     kv.TemporalTx
	background  bool
	ctx         context.Context
	stateWriter state.StateWriter
	chain       consensus.ChainReader
	logger      log.Logger

	execArgs *ExecArgs

	taskGasPool *core.GasPool

	// calculated by .changeBlock()
	blockHash common.Hash
	blockNum  uint64
	header    *types.Header
	blockCtx  *evmtypes.BlockContext
	rules     *chain.Rules
	signer    *types.Signer
	vmCfg     *vm.Config
}

type TraceConsumer struct {
	//Reduce receiving results of execution. They are sorted and have no gaps.
	Reduce func(task *state.TxTask, tx kv.Tx) error
}

func NewHistoricalTraceWorker(
	consumer TraceConsumer,
	in *state.QueueWithRetry,
	out *state.ResultsQueue,
	background bool,

	ctx context.Context,
	execArgs *ExecArgs,
	logger log.Logger,
) *HistoricalTraceWorker {
	ie := &HistoricalTraceWorker{
		consumer: consumer,
		in:       in,
		out:      out,

		logger:   logger,
		ctx:      ctx,
		execArgs: execArgs,

		background:  background,
		stateReader: state.NewHistoryReaderV3(),

		taskGasPool: new(core.GasPool),
		vmCfg:       &vm.Config{},
	}
	ie.evm = vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, execArgs.ChainConfig, *ie.vmCfg)
	ie.taskGasPool.AddBlobGas(execArgs.ChainConfig.GetMaxBlobGasPerBlock(0))
	ie.ibs = state.New(ie.stateReader)
	return ie
}

func (rw *HistoricalTraceWorker) LogStats() { rw.evm.JumpDestCache.LogStats() }

func (rw *HistoricalTraceWorker) Run() (err error) {
	defer func() { // convert panic to err - because it's background workers
		if rec := recover(); rec != nil {
			err = fmt.Errorf("HistoricalTraceWorker panic: %s, %s", rec, dbg.Stack())
		}
	}()
	defer rw.LogStats()
	for txTask, ok := rw.in.Next(rw.ctx); ok; txTask, ok = rw.in.Next(rw.ctx) {
		rw.RunTxTaskNoLock(txTask)
		if err := rw.out.Add(rw.ctx, txTask); err != nil {
			return err
		}
	}
	return nil
}

func (rw *HistoricalTraceWorker) RunTxTaskNoLock(txTask *state.TxTask) {
	if rw.background && rw.chainTx == nil {
		var err error
		if rw.chainTx, err = rw.execArgs.ChainDB.BeginTemporalRo(rw.ctx); err != nil {
			panic(fmt.Errorf("BeginRo: %w", err))
		}
		rw.stateReader.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.execArgs.ChainConfig, rw.chainTx, rw.execArgs.BlockReader, rw.logger)
	}
	txTask.Error = nil

	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateReader.ResetReadSet()
	rw.vmCfg.Debug = true
	tracer := NewCallTracer()
	rw.vmCfg.Tracer = tracer

	rw.ibs.Reset()
	ibs, cc := rw.ibs, rw.execArgs.ChainConfig

	var err error
	rules, header := txTask.Rules, txTask.Header
	_, isPoSA := rw.execArgs.Engine.(consensus.PoSA)
	switch {
	case txTask.TxIndex == -1:
		if txTask.BlockNum == 0 {
			_, ibs, err = core.GenesisToBlock(rw.execArgs.Genesis, rw.execArgs.Dirs, rw.logger)
			if err != nil {
				panic(fmt.Errorf("GenesisToBlock: %w", err))
			}
			// For Genesis, rules should be empty, so that empty accounts can be included
			rules = &chain.Rules{} //nolint
			break
		}

		// Block initialisation
		syscall := func(contract common.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
			return core.SysCallContract(contract, data, cc, ibs, header, rw.execArgs.Engine, constCall /* constCall */)
		}
		if isPoSA {
			if !rw.execArgs.ChainConfig.IsFeynman(header.Number.Uint64(), header.Time) {
				systemcontracts.UpgradeBuildInSystemContract(rw.execArgs.ChainConfig, header.Number, txTask.LastBlockTime, header.Time, ibs, rw.logger)
			}
			// HistoryStorageAddress is a special system contract in bsc, which can't be upgraded
			if rw.execArgs.ChainConfig.IsOnPrague(header.Number, txTask.LastBlockTime, header.Time) {
				misc.InitializeBlockHashesEip2935(ibs)
				log.Info("Set code for HistoryStorageAddress", "blockNumber", header.Number.Uint64(), "blockTime", header.Time)
			}
		}
		if err := rw.execArgs.Engine.Initialize(cc, rw.chain, header, ibs, syscall, rw.logger, nil); err != nil {
			txTask.Error = err
		} else {
			txTask.Error = ibs.FinalizeTx(rules, noop)
		}
	case txTask.Final:
		if txTask.BlockNum == 0 {
			break
		}

		if _, isPoSa := rw.execArgs.Engine.(consensus.PoSA); isPoSa {
			// Is an empty block
			if rw.execArgs.ChainConfig.IsFeynman(header.Number.Uint64(), header.Time) && txTask.TxIndex == 0 {
				systemcontracts.UpgradeBuildInSystemContract(rw.execArgs.ChainConfig, header.Number, txTask.LastBlockTime, header.Time, ibs, rw.logger)
			}
			break
		}
		if rw.background { // `Final` system txn must be executed in reducer, because `consensus.Finalize` requires "all receipts of block" to be available
			break
		}

		// End of block transaction in a block
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			res, err := core.SysCallContract(contract, data, cc, ibs, header, rw.execArgs.Engine, false /* constCall */)
			if err != nil {
				return nil, err
			}
			txTask.Logs = append(txTask.Logs, ibs.GetRawLogs(txTask.TxIndex)...)
			return res, nil
		}

		_, _, _, err := rw.execArgs.Engine.Finalize(cc, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, false, nil, txTask.TxIndex, rw.logger)
		if err != nil {
			txTask.Error = err
		}
	case txTask.SystemTxIndex > 0:
		systemCall := func(ibs *state.IntraBlockState) ([]byte, bool, error) {
			rw.taskGasPool.Reset(txTask.Tx.GetGas(), cc.GetMaxBlobGasPerBlock(header.Time))
			rw.vmCfg.SkipAnalysis = txTask.SkipAnalysis
			ibs.SetTxContext(txTask.TxIndex, txTask.BlockNum)
			msg := txTask.TxAsMessage
			if rw.execArgs.ChainConfig.IsCancun(header.Number.Uint64(), header.Time) {
				rules := rw.execArgs.ChainConfig.Rules(header.Number.Uint64(), header.Time)
				ibs.Prepare(rules, msg.From(), txTask.EvmBlockContext.Coinbase, msg.To(), vm.ActivePrecompiles(rules), msg.AccessList(), nil)
			}
			txContext := core.NewEVMTxContext(msg)
			if rw.vmCfg.TraceJumpDest {
				txContext.TxHash = txTask.Tx.Hash()
			}
			rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, txContext, ibs, *rw.vmCfg, rules)
			// Increment the nonce for the next transaction
			nonce, err := ibs.GetNonce(msg.From())
			if err != nil {
				return nil, false, fmt.Errorf("%w: %w", core.ErrStateTransitionFailed, err)
			}
			if err = ibs.SetNonce(msg.From(), nonce+1); err != nil {
				return nil, false, fmt.Errorf("%w: %w", core.ErrStateTransitionFailed, err)
			}
			ret, leftOverGas, err := rw.evm.Call(
				vm.AccountRef(msg.From()),
				*msg.To(),
				msg.Data(),
				msg.Gas(),
				msg.Value(),
				false,
			)
			if err != nil {
				txTask.Error = err
			} else {
				txTask.Failed = false
				txTask.UsedGas = msg.Gas() - leftOverGas
				// Update the state with pending changes
				ibs.SoftFinalise()
				txTask.Logs = ibs.GetLogs(txTask.TxIndex, txTask.Tx.Hash(), txTask.BlockNum, txTask.BlockHash)
				txTask.TraceFroms = tracer.Froms()
				txTask.TraceTos = tracer.Tos()
			}
			return ret, true, nil
		}

		_, _, _, err := rw.execArgs.Engine.Finalize(rw.execArgs.ChainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, nil, false, systemCall, txTask.TxIndex, rw.logger)
		if err != nil {
			txTask.Error = err
		}
	default:
		rw.taskGasPool.Reset(txTask.Tx.GetGas(), cc.GetMaxBlobGasPerBlock(header.Time))
		rw.vmCfg.SkipAnalysis = txTask.SkipAnalysis
		ibs.SetTxContext(txTask.TxIndex, txTask.BlockNum)
		msg := txTask.TxAsMessage
		//msg.SetCheckNonce(!rw.vmConfig.StatelessExec)
		txn := txTask.Tx

		txContext := core.NewEVMTxContext(msg)
		if rw.vmCfg.TraceJumpDest {
			txContext.TxHash = txn.Hash()
		}
		rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, txContext, ibs, *rw.vmCfg, rules)

		// MA applytx
		applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */, rw.execArgs.Engine)
		if err != nil {
			txTask.Error = err
		} else {
			txTask.Failed = applyRes.Failed()
			txTask.UsedGas = applyRes.UsedGas
			// Update the state with pending changes
			ibs.SoftFinalise()

			txTask.Logs = ibs.GetLogs(txTask.TxIndex, txn.Hash(), txTask.BlockNum, txTask.BlockHash)
			txTask.TraceFroms = tracer.Froms()
			txTask.TraceTos = tracer.Tos()
		}
	}
}
func (rw *HistoricalTraceWorker) ResetTx(chainTx kv.TemporalTx) {
	if rw.background && rw.chainTx != nil {
		rw.chainTx.Rollback()
		rw.chainTx = nil
	}
	if chainTx != nil {
		rw.chainTx = chainTx
		rw.stateReader.SetTx(rw.chainTx)
		//rw.stateWriter.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.execArgs.ChainConfig, rw.chainTx, rw.execArgs.BlockReader, rw.logger)
	}
}

// immutable (aka. global) params required for block execution. can instantiate once at app-start
type ExecArgs struct {
	ChainDB     kv.TemporalRoDB
	Genesis     *types.Genesis
	BlockReader services.FullBlockReader
	Engine      consensus.Engine
	Dirs        datadir.Dirs
	ChainConfig *chain.Config
	Workers     int
}

func NewHistoricalTraceWorkers(consumer TraceConsumer, cfg *ExecArgs, ctx context.Context, toTxNum uint64, in *state.QueueWithRetry, workerCount int, outputTxNum *atomic.Uint64, logger log.Logger) *errgroup.Group {
	g, ctx := errgroup.WithContext(ctx)

	// can afford big limits - because historical execution doesn't need conflicts-resolution
	resultChannelLimit := workerCount * 128
	heapLimit := workerCount * 128
	out := state.NewResultsQueue(resultChannelLimit, heapLimit) // mapGroup owns (and closing) it

	g.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("'reduce worker' paniced: %s, %s", rec, dbg.Stack())
			}
		}()
		defer out.Close()
		return doHistoryMap(consumer, cfg, ctx, in, workerCount, out, logger)
	})
	g.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("'reduce worker' paniced: %s, %s", rec, dbg.Stack())
			}
		}()
		return doHistoryReduce(consumer, cfg, ctx, toTxNum, outputTxNum, out, logger)
	})
	return g
}

func doHistoryReduce(consumer TraceConsumer, cfg *ExecArgs, ctx context.Context, toTxNum uint64, outputTxNum *atomic.Uint64, out *state.ResultsQueue, logger log.Logger) error {
	tx, err := cfg.ChainDB.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	applyWorker := NewHistoricalTraceWorker(consumer, nil, nil, false, ctx, cfg, logger)
	defer applyWorker.LogStats()
	applyWorker.ResetTx(tx)

	//logEvery := time.NewTicker(1 * time.Second)
	//defer logEvery.Stop()

	for outputTxNum.Load() <= toTxNum {
		//select {
		//case <-logEvery.C:
		//	log.Info("[dbg] out", "chanLen", out.ChanLen(), "chanCapacity", out.ChanCapacity(), "heapLen", out.Len(), "heapCapacity", out.Capacity())
		//default:
		//}

		err = out.DrainNonBlocking(ctx)
		if err != nil {
			return err
		}

		processedTxNum, _, err := processResultQueueHistorical(consumer, out, outputTxNum.Load(), tx, true, applyWorker)
		if err != nil {
			return fmt.Errorf("processResultQueueHistorical: %w", err)
		}
		if processedTxNum > 0 {
			outputTxNum.Store(processedTxNum)
		}

	}
	//if outputTxNum.Load() != toTxNum {
	//	return fmt.Errorf("not all txnums proceeded: toTxNum=%d, outputTxNum=%d", toTxNum, outputTxNum.Load())
	//}
	return nil
}
func doHistoryMap(consumer TraceConsumer, cfg *ExecArgs, ctx context.Context, in *state.QueueWithRetry, workerCount int, out *state.ResultsQueue, logger log.Logger) error {
	workers := make([]*HistoricalTraceWorker, workerCount)
	mapGroup, ctx := errgroup.WithContext(ctx)
	// we all errors in background workers (except ctx.Cancel), because applyLoop will detect this error anyway.
	// and in applyLoop all errors are critical
	for i := 0; i < workerCount; i++ {
		i := i
		workers[i] = NewHistoricalTraceWorker(consumer, in, out, true, ctx, cfg, logger)
		mapGroup.Go(func() error {
			return workers[i].Run()
		})
	}
	defer func() {
		mapGroup.Wait()
		for _, w := range workers {
			w.ResetTx(nil)
		}
	}()
	return mapGroup.Wait()
}

func processResultQueueHistorical(consumer TraceConsumer, rws *state.ResultsQueue, outputTxNumIn uint64, tx kv.TemporalTx, forceStopAtBlockEnd bool, applyWorker *HistoricalTraceWorker) (outputTxNum uint64, stopedAtBlockEnd bool, err error) {
	rwsIt := rws.Iter()
	defer rwsIt.Close()

	outputTxNum = outputTxNumIn
	for rwsIt.HasNext(outputTxNum) {
		txTask := rwsIt.PopNext()
		outputTxNum++
		stopedAtBlockEnd = txTask.Final

		if txTask.Final { // `Final` system txn must be executed in reducer, because `consensus.Finalize` requires "all receipts of block" to be available
			applyWorker.RunTxTaskNoLock(txTask)
		}
		if txTask.Error != nil {
			return outputTxNum, false, txTask.Error
		}

		txTask.CreateReceipt(tx)
		if err := consumer.Reduce(txTask, tx); err != nil {
			return outputTxNum, false, err
		}

		if forceStopAtBlockEnd && txTask.Final {
			break
		}
	}
	return
}

func CustomTraceMapReduce(fromBlock, toBlock uint64, consumer TraceConsumer, ctx context.Context, tx kv.TemporalTx, cfg *ExecArgs, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("'CustomTraceMapReduce' paniced: %s, %s", rec, dbg.Stack())
			log.Warn("[StageCustomTrace]", "err", err)
		}
	}()

	br := cfg.BlockReader
	chainConfig := cfg.ChainConfig

	txNumsReader := cfg.BlockReader.TxnumReader(ctx)

	fromTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}
	toTxNum, err := txNumsReader.Max(tx, toBlock)
	if err != nil {
		return err
	}

	// "Map-Reduce on history" is conflict-free - means we don't need "Retry" feature.
	// But still can use this data-type as simple queue.
	in := state.NewQueueWithRetry(10_000)
	defer in.Close()

	var WorkerCount = estimate.AlmostAllCPUs()
	if cfg.Workers > 0 {
		WorkerCount = cfg.Workers
	}

	{
		fromStep, toStep, err := BlkRangeToSteps(tx, fromBlock, toBlock, txNumsReader)
		if err != nil {
			return err
		}
		log.Info("[custom_trace] batch start", "blocks", fmt.Sprintf("%.1fm-%.1fm", float64(fromBlock)/1_000_000, float64(toBlock)/1_000_000), "steps", fmt.Sprintf("%.2f-%.2f", fromStep, toStep), "workers", cfg.Workers)
	}

	getHeaderFunc := func(hash common.Hash, number uint64) (h *types.Header) {
		if tx != nil && WorkerCount == 1 {
			h, _ = cfg.BlockReader.Header(ctx, tx, hash, number)
		} else {
			cfg.ChainDB.View(ctx, func(tx kv.Tx) error {
				h, _ = cfg.BlockReader.Header(ctx, tx, hash, number)
				return nil
			})
		}
		return h
	}

	outTxNum := &atomic.Uint64{}
	outTxNum.Store(fromTxNum)

	ctx, cancleCtx := context.WithCancel(ctx)
	workers := NewHistoricalTraceWorkers(consumer, cfg, ctx, toTxNum, in, WorkerCount, outTxNum, logger)
	defer workers.Wait()

	workersExited := &atomic.Bool{}
	go func() {
		if err := workers.Wait(); err != nil {
			cancleCtx()
		}
		workersExited.Store(true)
	}()

	// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
	// can't use OS-level ReadAhead - because Data >> RAM
	// it also warmsup state a bit - by touching senders/coninbase accounts and code
	readAhead, clean := BlocksReadAhead(ctx, 2, cfg.ChainDB, cfg.Engine, cfg.BlockReader)
	defer clean()

	inputTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	for blockNum := fromBlock; blockNum <= toBlock && !workersExited.Load(); blockNum++ {
		select {
		case readAhead <- blockNum:
		default:
		}

		var b *types.Block
		b, err = blockWithSenders(ctx, nil, tx, br, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			// TODO: panic here and see that overall process deadlock
			return fmt.Errorf("nil block %d", blockNum)
		}
		var lastBlockTime uint64
		var parent *types.Block
		if blockNum > 0 {
			parent, err = blockWithSenders(ctx, nil, tx, br, blockNum-1)
			if err != nil {
				return err
			}
			if parent == nil {
				return fmt.Errorf("nil parent block %d", blockNum-1)
			}
			lastBlockTime = parent.Time()
		}
		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, blockNum)
		signer := *types.MakeSigner(chainConfig, blockNum, header.Time)

		f := core.GetHashFn(header, getHeaderFunc)
		getHashFnMute := &sync.Mutex{}
		getHashFn := func(n uint64) common.Hash {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return f(n)
		}
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.Engine, nil /* author */, chainConfig)
		blockReceipts := make(types.Receipts, len(txs))
		var systemTxIndex int
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			txTask := &state.TxTask{
				BlockNum:        blockNum,
				Header:          header,
				Coinbase:        b.Coinbase(),
				Uncles:          b.Uncles(),
				Rules:           chainConfig.Rules(blockNum, b.Time()),
				Txs:             txs,
				TxNum:           inputTxNum,
				TxIndex:         txIndex,
				BlockHash:       b.Hash(),
				SkipAnalysis:    skipAnalysis,
				Final:           txIndex == len(txs),
				GetHashFn:       getHashFn,
				EvmBlockContext: blockContext,
				Withdrawals:     b.Withdrawals(),
				LastBlockTime:   lastBlockTime,

				// use history reader instead of state reader to catch up to the tx where we left off
				HistoryExecution: true,
				BlockReceipts:    blockReceipts,
			}

			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxAsMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
				if err != nil {
					return err
				}
			}
			posa, isPoSa := cfg.Engine.(consensus.PoSA)
			if txIndex >= 0 && !txTask.Final && isPoSa {
				if isSystemTx, err := posa.IsSystemTransaction(txs[txIndex], header); err != nil {
					return err
				} else if isSystemTx {
					systemTxIndex++
					txTask.SystemTxIndex = systemTxIndex
				}
			}
			in.Add(ctx, txTask)
			inputTxNum++
		}

		// run heavy computation in current goroutine - because it's not a bottleneck
		// it will speed up `processResultQueueHistorical` goroutine
		for _, t := range b.Transactions() {
			t.Hash()
		}
	}
	in.Close() //no more work. no retries in map-reduce. means can close here.

	if err := workers.Wait(); err != nil {
		return fmt.Errorf("WorkersPool: %w", err)
	}

	return nil
}

func blockWithSenders(ctx context.Context, db kv.RoDB, tx kv.Tx, blockReader services.BlockReader, blockNum uint64) (b *types.Block, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if tx == nil {
		tx, err = db.BeginRo(context.Background())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}
	b, err = blockReader.BlockByNumber(context.Background(), tx, blockNum)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	return b, err
}
func BlkRangeToSteps(tx kv.Tx, fromBlock, toBlock uint64, txNumsReader rawdbv3.TxNumsReader) (float64, float64, error) {
	fromTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return 0, 0, err
	}
	toTxNum, err := txNumsReader.Min(tx, toBlock)
	if err != nil {
		return 0, 0, err
	}

	stepSize := libstate.AggTx(tx).StepSize()
	return float64(fromTxNum) / float64(stepSize), float64(toTxNum) / float64(stepSize), nil
}

func BlkRangeToStepsOnDB(db kv.RoDB, fromBlock, toBlock uint64, txNumsReader rawdbv3.TxNumsReader) (float64, float64, error) {
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	return BlkRangeToSteps(tx, fromBlock, toBlock, txNumsReader)
}
