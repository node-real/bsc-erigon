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
	"sync"

	"github.com/erigontech/erigon/consensus/misc"

	"github.com/erigontech/erigon/core/systemcontracts"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common/dbg"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon/eth/consensuschain"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

var noop = state.NewNoopWriter()

type Worker struct {
	lock        sync.Locker
	logger      log.Logger
	chainDb     kv.RoDB
	chainTx     kv.TemporalTx
	background  bool // if true - worker does manage RoTx (begin/rollback) in .ResetTx()
	blockReader services.FullBlockReader
	in          *state.QueueWithRetry
	rs          *state.StateV3
	stateWriter *state.StateWriterV3
	stateReader state.ResettableStateReader
	historyMode bool // if true - stateReader is HistoryReaderV3, otherwise it's state reader
	chainConfig *chain.Config

	ctx      context.Context
	engine   consensus.Engine
	genesis  *types.Genesis
	resultCh *state.ResultsQueue
	chain    consensus.ChainReader
	isPoSA   bool
	posa     consensus.PoSA

	callTracer  *CallTracer
	taskGasPool *core.GasPool

	evm   *vm.EVM
	ibs   *state.IntraBlockState
	vmCfg vm.Config

	dirs datadir.Dirs

	isMining bool
}

func NewWorker(lock sync.Locker, logger log.Logger, ctx context.Context, background bool, chainDb kv.RoDB, in *state.QueueWithRetry, blockReader services.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis, results *state.ResultsQueue, engine consensus.Engine, dirs datadir.Dirs, isMining bool) *Worker {
	w := &Worker{
		lock:        lock,
		logger:      logger,
		chainDb:     chainDb,
		in:          in,
		background:  background,
		blockReader: blockReader,
		chainConfig: chainConfig,

		ctx:      ctx,
		genesis:  genesis,
		resultCh: results,
		engine:   engine,

		evm:         vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{}),
		callTracer:  NewCallTracer(),
		taskGasPool: new(core.GasPool),

		dirs: dirs,

		isMining: isMining,
	}
	w.taskGasPool.AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(0))
	w.vmCfg = vm.Config{Debug: true, Tracer: w.callTracer}
	w.ibs = state.New(w.stateReader)
	w.posa, w.isPoSA = engine.(consensus.PoSA)
	return w
}

func (rw *Worker) LogLRUStats() { rw.evm.JumpDestCache.LogStats() }

func (rw *Worker) ResetState(rs *state.StateV3, accumulator *shards.Accumulator) {
	rw.rs = rs
	if rw.background {
		rw.SetReader(state.NewReaderParallelV3(rs.Domains()))
	} else {
		rw.SetReader(state.NewReaderV3(rs.Domains()))
	}
	rw.stateWriter = state.NewStateWriterV3(rs, accumulator)
}

func (rw *Worker) SetGaspool(gp *core.GasPool) {
	rw.taskGasPool = gp
}

func (rw *Worker) Tx() kv.TemporalTx { return rw.chainTx }
func (rw *Worker) DiscardReadList()  { rw.stateReader.DiscardReadList() }
func (rw *Worker) ResetTx(chainTx kv.Tx) {
	if rw.background && rw.chainTx != nil {
		rw.chainTx.Rollback()
		rw.chainTx = nil
	}
	if chainTx != nil {
		rw.chainTx = chainTx.(kv.TemporalTx)
		rw.stateReader.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.chainConfig, rw.chainTx, rw.blockReader, rw.logger)
	}
}

func (rw *Worker) Run() (err error) {
	defer func() { // convert panic to err - because it's background workers
		if rec := recover(); rec != nil {
			err = fmt.Errorf("exec3.Worker panic: %s, %s", rec, dbg.Stack())
		}
	}()

	for txTask, ok := rw.in.Next(rw.ctx); ok; txTask, ok = rw.in.Next(rw.ctx) {
		//fmt.Println("RTX", txTask.BlockNum, txTask.TxIndex, txTask.TxNum, txTask.Final)
		rw.RunTxTask(txTask, rw.isMining)
		if err := rw.resultCh.Add(rw.ctx, txTask); err != nil {
			return err
		}
	}
	return nil
}

func (rw *Worker) RunTxTask(txTask *state.TxTask, isMining bool) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.RunTxTaskNoLock(txTask, isMining, false)
}

// Needed to set history reader when need to offset few txs from block beginning and does not break processing,
// like compute gas used for block and then to set state reader to continue processing on latest data.
func (rw *Worker) SetReader(reader state.ResettableStateReader) {
	rw.stateReader = reader
	rw.stateReader.SetTx(rw.Tx())
	rw.ibs.Reset()
	rw.ibs = state.New(rw.stateReader)

	switch reader.(type) {
	case *state.HistoryReaderV3:
		rw.historyMode = true
	case *state.ReaderV3:
		rw.historyMode = false
	default:
		rw.historyMode = false
		//fmt.Printf("[worker] unknown reader %T: historyMode is set to disabled\n", reader)
	}
}

func (rw *Worker) RunTxTaskNoLock(txTask *state.TxTask, isMining, skipPostEvaluaion bool) {
	if txTask.HistoryExecution && !rw.historyMode {
		// in case if we cancelled execution and commitment happened in the middle of the block, we have to process block
		// from the beginning until committed txNum and only then disable history mode.
		// Needed to correctly evaluate spent gas and other things.
		rw.SetReader(state.NewHistoryReaderV3())
	} else if !txTask.HistoryExecution && rw.historyMode {
		if rw.background {
			rw.SetReader(state.NewReaderParallelV3(rw.rs.Domains()))
		} else {
			rw.SetReader(state.NewReaderV3(rw.rs.Domains()))
		}
	}
	if rw.background && rw.chainTx == nil {
		var err error
		if rw.chainTx, err = rw.chainDb.(kv.TemporalRoDB).BeginTemporalRo(rw.ctx); err != nil {
			panic(err)
		}
		rw.stateReader.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.chainConfig, rw.chainTx, rw.blockReader, rw.logger)
	}
	txTask.Error = nil

	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.rs.Domains().SetTxNum(txTask.TxNum)
	rw.stateReader.ResetReadSet()
	rw.stateWriter.ResetWriteSet()

	rw.ibs.Reset()
	ibs := rw.ibs
	//ibs.SetTrace(true)

	rules := txTask.Rules
	var err error
	header := txTask.Header
	//fmt.Printf("txNum=%d blockNum=%d history=%t\n", txTask.TxNum, txTask.BlockNum, txTask.HistoryExecution)

	switch {
	case txTask.TxIndex == -1:
		if txTask.BlockNum == 0 {

			//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
			_, ibs, err = core.GenesisToBlock(rw.genesis, rw.dirs, rw.logger)
			if err != nil {
				panic(err)
			}
			// For Genesis, rules should be empty, so that empty accounts can be included
			rules = &chain.Rules{}
			break
		}

		// Block initialisation
		//fmt.Printf("txNum=%d, blockNum=%d, initialisation of the block\n", txTask.TxNum, txTask.BlockNum)
		syscall := func(contract libcommon.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
			return core.SysCallContract(contract, data, rw.chainConfig, ibs, header, rw.engine, constCall /* constCall */)
		}
		if rw.isPoSA {
			if !rw.chainConfig.IsFeynman(header.Number.Uint64(), header.Time) {
				systemcontracts.UpgradeBuildInSystemContract(rw.chainConfig, header.Number, txTask.LastBlockTime, header.Time, ibs, rw.logger)
			}
			// HistoryStorageAddress is a special system contract in bsc, which can't be upgraded
			if rw.chainConfig.IsOnPrague(header.Number, txTask.LastBlockTime, header.Time) {
				misc.InitializeBlockHashesEip2935(ibs)
				log.Info("Set code for HistoryStorageAddress", "blockNumber", header.Number.Uint64(), "blockTime", header.Time)
			}
		}
		if err := rw.engine.Initialize(rw.chainConfig, rw.chain, header, ibs, syscall, rw.logger, nil); err != nil {
			txTask.Error = err
		} else {
			txTask.Error = ibs.FinalizeTx(rules, noop)
		}
	case txTask.Final:
		if txTask.BlockNum == 0 {
			break
		}

		if _, isPoSa := rw.engine.(consensus.PoSA); isPoSa {
			// Is an empty block
			if rw.chainConfig.IsFeynman(header.Number.Uint64(), header.Time) && txTask.TxIndex == 0 {
				systemcontracts.UpgradeBuildInSystemContract(rw.chainConfig, header.Number, txTask.LastBlockTime, header.Time, ibs, rw.logger)
			}
			break
		}

		// End of block transaction in a block
		syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
			res, err := core.SysCallContract(contract, data, rw.chainConfig, ibs, header, rw.engine, false /* constCall */)
			txTask.Logs = append(txTask.Logs, ibs.GetRawLogs(txTask.TxIndex)...)
			return res, err
		}

		if isMining {
			_, txTask.Txs, txTask.BlockReceipts, _, err = rw.engine.FinalizeAndAssemble(rw.chainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, nil, rw.logger)
		} else {
			_, _, _, err = rw.engine.Finalize(rw.chainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, skipPostEvaluaion, nil, txTask.TxIndex, rw.logger)
		}
		if err != nil {
			txTask.Error = err
		} else {
			txTask.TraceTos = map[libcommon.Address]struct{}{}
			txTask.TraceTos[txTask.Coinbase] = struct{}{}
			for _, uncle := range txTask.Uncles {
				txTask.TraceTos[uncle.Coinbase] = struct{}{}
			}
		}
	case txTask.SystemTxIndex > 0:
		systemCall := func(ibs *state.IntraBlockState) ([]byte, bool, error) {
			rw.callTracer.Reset()
			rw.vmCfg.SkipAnalysis = txTask.SkipAnalysis
			msg := txTask.TxAsMessage
			ibs.SetTxContext(txTask.TxIndex, txTask.BlockNum)
			if rw.chainConfig.IsCancun(header.Number.Uint64(), header.Time) {
				rules := rw.chainConfig.Rules(header.Number.Uint64(), header.Time)
				ibs.Prepare(rules, msg.From(), txTask.EvmBlockContext.Coinbase, msg.To(), vm.ActivePrecompiles(rules), msg.AccessList(), nil)
			}
			rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, rw.vmCfg, rules)
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
				//txTask.Error = ibs.FinalizeTx(rules, noop)
				txTask.Logs = ibs.GetLogs(txTask.TxIndex, txTask.Tx.Hash(), txTask.BlockNum, txTask.BlockHash)
				txTask.TraceFroms = rw.callTracer.Froms()
				txTask.TraceTos = rw.callTracer.Tos()

				txTask.CreateReceipt(rw.Tx())
			}
			return ret, true, nil
		}

		_, _, _, err := rw.engine.Finalize(rw.chainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, nil, skipPostEvaluaion, systemCall, txTask.TxIndex, rw.logger)
		if err != nil {
			txTask.Error = err
		}
	default:
		// This doesn't make sense, but I am not sure if this wrong behaviour is needed somewhere else:
		// rw.taskGasPool.Reset(txTask.Tx.GetGasLimit(), rw.chainConfig.GetMaxBlobGasPerBlock(header.Time))
		rw.callTracer.Reset()
		rw.vmCfg.SkipAnalysis = txTask.SkipAnalysis
		ibs.SetTxContext(txTask.TxIndex, txTask.BlockNum)
		msg := txTask.TxAsMessage

		rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, rw.vmCfg, rules)

		// MA applytx
		applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */, rw.engine)
		if err != nil {
			txTask.Error = err
		} else {
			txTask.Failed = applyRes.Failed()
			txTask.UsedGas = applyRes.UsedGas
			// Update the state with pending changes
			ibs.SoftFinalise()
			//txTask.Error = ibs.FinalizeTx(rules, noop)
			txTask.Logs = ibs.GetLogs(txTask.TxIndex, txTask.Tx.Hash(), txTask.BlockNum, txTask.BlockHash)
			txTask.TraceFroms = rw.callTracer.Froms()
			txTask.TraceTos = rw.callTracer.Tos()

			txTask.CreateReceipt(rw.Tx())
		}

	}
	// Prepare read set, write set and balanceIncrease set and send for serialisation
	if txTask.Error == nil {
		txTask.BalanceIncreaseSet = ibs.BalanceIncreaseSet()
		//for addr, bal := range txTask.BalanceIncreaseSet {
		//	fmt.Printf("BalanceIncreaseSet [%x]=>[%d]\n", addr, &bal)
		//}
		if err = ibs.MakeWriteSet(rules, rw.stateWriter); err != nil {
			panic(err)
		}
		txTask.ReadLists = rw.stateReader.ReadSet()
		txTask.WriteLists = rw.stateWriter.WriteSet()
		txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = rw.stateWriter.PrevAndDels()
	}
}

func NewWorkersPool(lock sync.Locker, accumulator *shards.Accumulator, logger log.Logger, ctx context.Context, background bool, chainDb kv.RoDB, rs *state.StateV3, in *state.QueueWithRetry, blockReader services.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis, engine consensus.Engine, workerCount int, dirs datadir.Dirs, isMining bool) (reconWorkers []*Worker, applyWorker *Worker, rws *state.ResultsQueue, clear func(), wait func()) {
	reconWorkers = make([]*Worker, workerCount)

	resultChSize := workerCount * 8
	rws = state.NewResultsQueue(resultChSize, workerCount) // workerCount * 4
	{
		// we all errors in background workers (except ctx.Cancel), because applyLoop will detect this error anyway.
		// and in applyLoop all errors are critical
		ctx, cancel := context.WithCancel(ctx)
		g, ctx := errgroup.WithContext(ctx)
		for i := 0; i < workerCount; i++ {
			reconWorkers[i] = NewWorker(lock, logger, ctx, background, chainDb, in, blockReader, chainConfig, genesis, rws, engine, dirs, isMining)
			reconWorkers[i].ResetState(rs, accumulator)
		}
		if background {
			for i := 0; i < workerCount; i++ {
				i := i
				g.Go(func() error {
					return reconWorkers[i].Run()
				})
			}
			wait = func() { g.Wait() }
		}

		var clearDone bool
		clear = func() {
			if clearDone {
				return
			}
			clearDone = true
			cancel()
			g.Wait()
			for _, w := range reconWorkers {
				w.ResetTx(nil)
			}
			//applyWorker.ResetTx(nil)
		}
	}
	applyWorker = NewWorker(lock, logger, ctx, false, chainDb, in, blockReader, chainConfig, genesis, rws, engine, dirs, isMining)

	return reconWorkers, applyWorker, rws, clear, wait
}
