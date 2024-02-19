package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/log/v3"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

// TraceBlockByNumber implements debug_traceBlockByNumber. Returns Geth style block traces.
func (api *PrivateDebugAPIImpl) TraceBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	return api.traceBlock(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), config, stream)
}

// TraceBlockByHash implements debug_traceBlockByHash. Returns Geth style block traces.
func (api *PrivateDebugAPIImpl) TraceBlockByHash(ctx context.Context, hash common.Hash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	return api.traceBlock(ctx, rpc.BlockNumberOrHashWithHash(hash, true), config, stream)
}

func (api *PrivateDebugAPIImpl) traceBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()
	var (
		block    *types.Block
		number   rpc.BlockNumber
		numberOk bool
		hash     common.Hash
		hashOk   bool
	)
	if number, numberOk = blockNrOrHash.Number(); numberOk {
		block, err = api.blockByRPCNumber(number, tx)
	} else if hash, hashOk = blockNrOrHash.Hash(); hashOk {
		block, err = api.blockByHashWithSenders(tx, hash)
	} else {
		return fmt.Errorf("invalid arguments; neither block nor hash specified")
	}

	if err != nil {
		stream.WriteNil()
		return err
	}

	if block == nil {
		if numberOk {
			return fmt.Errorf("invalid arguments; block with number %d not found", number)
		}
		return fmt.Errorf("invalid arguments; block with hash %x not found", hash)
	}

	// if we've pruned this history away for this block then just return early
	// to save any red herring errors
	err = api.BaseAPI.checkPruneHistory(tx, block.NumberU64())
	if err != nil {
		stream.WriteNil()
		return err
	}

	if config == nil {
		config = &tracers.TraceConfig{}
	}

	if config.BorTraceEnabled == nil {
		config.BorTraceEnabled = newBoolPtr(false)
	}

	var excessDataGas *big.Int
	parentBlock, err := api.blockByHashWithSenders(tx, block.ParentHash())
	if err != nil {
		stream.WriteNil()
		return err
	}
	if parentBlock != nil {
		excessDataGas = parentBlock.ExcessDataGas()
	}
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	engine := api.engine()

	_, blockCtx, _, ibs, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, 0, api.historyV3(tx))
	if err != nil {
		stream.WriteNil()
		return err
	}

	signer := types.MakeSigner(chainConfig, block.NumberU64())
	rules := chainConfig.Rules(block.NumberU64(), block.Time())
	stream.WriteArrayStart()

	borTx, _, _, _ := rawdb.ReadBorTransactionForBlock(tx, block)
	txns := block.Transactions()
	if borTx != nil && *config.BorTraceEnabled {
		txns = append(txns, borTx)
	}

	for idx, txn := range txns {
		stream.WriteObjectStart()
		stream.WriteObjectField("result")
		select {
		default:
		case <-ctx.Done():
			stream.WriteNil()
			return ctx.Err()
		}
		ibs.Prepare(txn.Hash(), block.Hash(), idx)
		msg, _ := txn.AsMessage(*signer, block.BaseFee(), rules)

		if msg.FeeCap().IsZero() && engine != nil {
			syscall := func(contract common.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *chainConfig, ibs, block.Header(), engine, true /* constCall */, excessDataGas)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		txCtx := evmtypes.TxContext{
			TxHash:   txn.Hash(),
			Origin:   msg.From(),
			GasPrice: msg.GasPrice(),
		}

		if borTx != nil && idx == len(txns)-1 {
			if *config.BorTraceEnabled {
				config.BorTx = newBoolPtr(true)
			}
		}

		err = transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream, api.evmCallTimeout)
		if err == nil {
			err = ibs.FinalizeTx(rules, state.NewNoopWriter())
		}
		stream.WriteObjectEnd()

		// if we have an error we want to output valid json for it before continuing after clearing down potential writes to the stream
		if err != nil {
			stream.WriteMore()
			stream.WriteObjectStart()
			err = rpc.HandleError(err, stream)
			stream.WriteObjectEnd()
			if err != nil {
				return err
			}
		}
		if idx != len(txns)-1 {
			stream.WriteMore()
		}
		stream.Flush()
	}
	stream.WriteArrayEnd()
	stream.Flush()
	return nil
}

// TraceTransaction implements debug_traceTransaction. Returns Geth style transaction traces.
func (api *PrivateDebugAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	// Retrieve the transaction and assemble its EVM context
	blockNum, ok, err := api.txnLookup(ctx, tx, hash)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if !ok {
		stream.WriteNil()
		return nil
	}

	// check pruning to ensure we have history at this block level
	err = api.BaseAPI.checkPruneHistory(tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}

	// Private API returns 0 if transaction is not found.
	if blockNum == 0 && chainConfig.Bor != nil {
		blockNumPtr, err := rawdb.ReadBorTxLookupEntry(tx, hash)
		if err != nil {
			stream.WriteNil()
			return err
		}
		if blockNumPtr == nil {
			stream.WriteNil()
			return nil
		}
		blockNum = *blockNumPtr
	}
	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if block == nil {
		stream.WriteNil()
		return nil
	}
	var txnIndex uint64
	var txn types.Transaction
	for i, transaction := range block.Transactions() {
		if transaction.Hash() == hash {
			txnIndex = uint64(i)
			txn = transaction
			break
		}
	}
	if txn == nil {
		var borTx types.Transaction
		borTx, _, _, _, err = rawdb.ReadBorTransaction(tx, hash)
		if err != nil {
			return err
		}

		if borTx != nil {
			stream.WriteNil()
			return nil
		}
		stream.WriteNil()
		return fmt.Errorf("transaction %#x not found", hash)
	}
	engine := api.engine()

	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, int(txnIndex), api.historyV3(tx))
	if err != nil {
		stream.WriteNil()
		return err
	}
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream, api.evmCallTimeout)
}

func (api *PrivateDebugAPIImpl) TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return fmt.Errorf("create ro transaction: %v", err)
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return fmt.Errorf("read chain config: %v", err)
	}
	engine := api.engine()

	blockNumber, hash, _, err := rpchelper.GetBlockNumber(blockNrOrHash, dbtx, api.filters)
	if err != nil {
		return fmt.Errorf("get block number: %v", err)
	}

	err = api.BaseAPI.checkPruneHistory(dbtx, blockNumber)
	if err != nil {
		return err
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, dbtx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(dbtx), api._blockReader, chainConfig.ChainName)
	if err != nil {
		return fmt.Errorf("create state reader: %v", err)
	}
	header, err := api._blockReader.Header(context.Background(), dbtx, hash, blockNumber)
	if err != nil {
		return fmt.Errorf("could not fetch header %d(%x): %v", blockNumber, hash, err)
	}
	if header == nil {
		return fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}
	ibs := state.New(stateReader)

	if config != nil && config.StateOverrides != nil {
		if err := config.StateOverrides.Override(ibs); err != nil {
			return fmt.Errorf("override state: %v", err)
		}
	}

	var baseFee *uint256.Int
	if header != nil && header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return fmt.Errorf("header.BaseFee uint256 overflow")
		}
	}
	msg, err := args.ToMessage(api.GasCap, baseFee)
	if err != nil {
		return fmt.Errorf("convert args to msg: %v", err)
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, blockNrOrHash.RequireCanonical, dbtx, api._blockReader)
	txCtx := core.NewEVMTxContext(msg)
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream, api.evmCallTimeout)
}

func (api *PrivateDebugAPIImpl) TraceCallMany(ctx context.Context, bundles []Bundle, simulateContext StateContext, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	var (
		hash               common.Hash
		replayTransactions types.Transactions
		evm                *vm.EVM
		blockCtx           evmtypes.BlockContext
		txCtx              evmtypes.TxContext
		overrideBlockHash  map[uint64]common.Hash
		baseFee            uint256.Int
	)

	if config == nil {
		config = &tracers.TraceConfig{}
	}

	overrideBlockHash = make(map[uint64]common.Hash)
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if len(bundles) == 0 {
		stream.WriteNil()
		return fmt.Errorf("empty bundles")
	}
	empty := true
	for _, bundle := range bundles {
		if len(bundle.Transactions) != 0 {
			empty = false
		}
	}

	if empty {
		stream.WriteNil()
		return fmt.Errorf("empty bundles")
	}

	defer func(start time.Time) { log.Trace("Tracing CallMany finished", "runtime", time.Since(start)) }(time.Now())

	blockNum, hash, _, err := rpchelper.GetBlockNumber(simulateContext.BlockNumber, tx, api.filters)
	if err != nil {
		stream.WriteNil()
		return err
	}

	err = api.BaseAPI.checkPruneHistory(tx, blockNum)
	if err != nil {
		return err
	}

	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}

	// -1 is a default value for transaction index.
	// If it's -1, we will try to replay every single transaction in that block
	transactionIndex := -1

	if simulateContext.TransactionIndex != nil {
		transactionIndex = *simulateContext.TransactionIndex
	}

	if transactionIndex == -1 {
		transactionIndex = len(block.Transactions())
	}

	replayTransactions = block.Transactions()[:transactionIndex]

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum-1)), 0, api.filters, api.stateCache, api.historyV3(tx), api._blockReader, chainConfig.ChainName)
	if err != nil {
		stream.WriteNil()
		return err
	}

	st := state.New(stateReader)

	parent := block.Header()

	if parent == nil {
		stream.WriteNil()
		return fmt.Errorf("block %d(%x) not found", blockNum, hash)
	}

	getHash := func(i uint64) common.Hash {
		if hash, ok := overrideBlockHash[i]; ok {
			return hash
		}
		hash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			log.Debug("Can't get block hash by number", "number", i, "only-canonical", true)
		}
		return hash
	}

	if parent.BaseFee != nil {
		baseFee.SetFromBig(parent.BaseFee)
	}

	blockCtx = evmtypes.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     getHash,
		Coinbase:    parent.Coinbase,
		BlockNumber: parent.Number.Uint64(),
		Time:        parent.Time,
		Difficulty:  new(big.Int).Set(parent.Difficulty),
		GasLimit:    parent.GasLimit,
		BaseFee:     &baseFee,
	}

	// Get a new instance of the EVM
	evm = vm.NewEVM(blockCtx, txCtx, st, chainConfig, vm.Config{Debug: false})
	signer := types.MakeSigner(chainConfig, blockNum)
	rules := chainConfig.Rules(blockNum, blockCtx.Time)

	// Setup the gas pool (also for unmetered requests)
	// and apply the message.
	gp := new(core.GasPool).AddGas(math.MaxUint64)
	for idx, txn := range replayTransactions {
		st.Prepare(txn.Hash(), block.Hash(), idx)
		msg, err := txn.AsMessage(*signer, block.BaseFee(), rules)
		if err != nil {
			stream.WriteNil()
			return err
		}
		txCtx = core.NewEVMTxContext(msg)
		evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{Debug: false})
		// Execute the transaction message
		_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			stream.WriteNil()
			return err
		}
		_ = st.FinalizeTx(rules, state.NewNoopWriter())

	}

	// after replaying the txns, we want to overload the state
	if config.StateOverrides != nil {
		err = config.StateOverrides.Override(evm.IntraBlockState().(*state.IntraBlockState))
		if err != nil {
			stream.WriteNil()
			return err
		}
	}

	stream.WriteArrayStart()
	for bundle_index, bundle := range bundles {
		stream.WriteArrayStart()
		// first change blockContext
		blockHeaderOverride(&blockCtx, bundle.BlockOverride, overrideBlockHash)
		for txn_index, txn := range bundle.Transactions {
			if txn.Gas == nil || *(txn.Gas) == 0 {
				txn.Gas = (*hexutil.Uint64)(&api.GasCap)
			}
			msg, err := txn.ToMessage(api.GasCap, blockCtx.BaseFee)
			if err != nil {
				stream.WriteNil()
				return err
			}
			txCtx = core.NewEVMTxContext(msg)
			ibs := evm.IntraBlockState().(*state.IntraBlockState)
			ibs.Prepare(common.Hash{}, parent.Hash(), txn_index)
			err = transactions.TraceTx(ctx, msg, blockCtx, txCtx, evm.IntraBlockState(), config, chainConfig, stream, api.evmCallTimeout)

			if err != nil {
				stream.WriteNil()
				return err
			}

			_ = ibs.FinalizeTx(rules, state.NewNoopWriter())

			if txn_index < len(bundle.Transactions)-1 {
				stream.WriteMore()
			}
		}
		stream.WriteArrayEnd()

		if bundle_index < len(bundles)-1 {
			stream.WriteMore()
		}
		blockCtx.BlockNumber++
		blockCtx.Time++
	}
	stream.WriteArrayEnd()
	return nil
}

func newBoolPtr(bb bool) *bool {
	b := bb
	return &b
}

// TraceBlockDiffByHash implements debug_traceBlockDiffByHash. Returns Geth style block diff layer.
func (api *PrivateDebugAPIImpl) TraceBlockDiffByHash(ctx context.Context, hash common.Hash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	return api.traceBlockDiff(ctx, rpc.BlockNumberOrHashWithHash(hash, true), config, stream)
}

// TraceBlockDiffByNumber implements debug_traceBlockDiffByNumber. Returns Geth style block diff layer.
func (api *PrivateDebugAPIImpl) TraceBlockDiffByNumber(ctx context.Context, blockNum rpc.BlockNumber, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	return api.traceBlockDiff(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), config, stream)
}

func (api *PrivateDebugAPIImpl) traceBlockDiff(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	roTx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer roTx.Rollback()
	noOpWriter := state.NewNoopWriter()
	var (
		b        *types.Block
		number   rpc.BlockNumber
		numberOk bool
		hash     common.Hash
		hashOk   bool
	)
	if number, numberOk = blockNrOrHash.Number(); numberOk {
		b, err = api.blockByRPCNumber(number, roTx)
	} else if hash, hashOk = blockNrOrHash.Hash(); hashOk {
		b, err = api.blockByHashWithSenders(roTx, hash)
	} else {
		return fmt.Errorf("invalid arguments; neither block nor hash specified")
	}

	if err != nil {
		stream.WriteNil()
		return err
	}

	if b == nil {
		if numberOk {
			return fmt.Errorf("invalid arguments; block with number %d not found", number)
		}
		return fmt.Errorf("invalid arguments; block with hash %x not found", hash)
	}

	// if we've pruned this history away for this block then just return early
	// to save any red herring errors
	err = api.BaseAPI.checkPruneHistory(roTx, b.NumberU64())
	if err != nil {
		stream.WriteNil()
		return err
	}

	if config == nil {
		config = &tracers.TraceConfig{}
	}

	if config.BorTraceEnabled == nil {
		config.BorTraceEnabled = newBoolPtr(false)
	}

	chainConfig, err := api.chainConfig(roTx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	engine := api.engine()
	// do not use state.NewPlainState, use this method, will cause different code in BSC: Cross Chain contract at 4369997
	//reader := state.NewPlainState(roTx, b.NumberU64(), nil)
	reader, err := rpchelper.CreateHistoryStateReader(roTx, b.NumberU64(), 0, api.historyV3(roTx), b.Time(), chainConfig.ChainName)
	if err != nil {
		return err
	}

	// Create the parent state database
	intraBlockState := state.New(reader)
	blockWriter := NewDiffLayerWriter()
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := api._blockReader.Header(ctx, roTx, hash, number)
		if e != nil {
			panic(e)
		}
		return h
	}
	_, err1 := api.runBlockDiff(roTx, engine.(consensus.Engine), intraBlockState, noOpWriter, blockWriter, chainConfig, getHeader, b, vm.Config{}, false)
	if err1 != nil {
		panic(fmt.Errorf("run block failed: %v, numer: %v", err1, b.NumberU64()))
	}
	//if chainConfig.IsByzantium(b.NumberU64()) {
	//	receiptSha := types.DeriveSha(receipts)
	//	if receiptSha != b.ReceiptHash() {
	//		panic(fmt.Errorf("mismatched receipt headers for block: %v", b.NumberU64()))
	//	}
	//}

	stream.WriteObjectStart()
	stream.WriteObjectField("diff_rlp")
	dataLen, err := stream.Write(blockWriter.GetData())
	if err != nil {
		log.Info("stream writ data", "data", blockWriter.GetData(), "wlen", dataLen, "err", err.Error())
		return err
	}

	stream.WriteObjectEnd()
	stream.Flush()
	return nil
}

func (api *PrivateDebugAPIImpl) runBlockDiff(dbtx kv.Tx, engine consensus.Engine, ibs *state.IntraBlockState, txnWriter state.StateWriter, blockWriter state.StateWriter,
	chainConfig *chain2.Config, getHeader func(hash common.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config, trace bool) (types.Receipts, error) {
	header := block.Header()
	parent := getHeader(header.ParentHash, header.Number.Uint64()-1)
	excessDataGas := header.ParentExcessDataGas(getHeader)
	vmConfig.TraceJumpDest = true
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	if chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	if !chainConfig.IsFeynman(header.Number.Uint64(), header.Time) {
		systemcontracts.UpgradeBuildInSystemContract(chainConfig, header.Number, parent.Time, header.Time, ibs)
	}
	rules := chainConfig.Rules(block.NumberU64(), block.Time())
	posa, okPosa := engine.(consensus.PoSA)
	//balanceDiff, balanceResult := *ibs.GetBalance(consensus.SystemAddress), new(uint256.Int)
	for i, tx := range block.Transactions() {
		if okPosa {
			if isSystemTx, err := posa.IsSystemTransaction(tx, block.Header()); err != nil {
				return nil, err
			} else if isSystemTx {
				continue
			}
		}
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		//gasUsedTmp1 := uint256.NewInt(*usedGas)
		//log.Info("ApplyTransaction txHash", "hash", tx.Hash().Hex())
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, txnWriter, header, tx, usedGas, vmConfig, excessDataGas)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		//gasUsedTmp2 := uint256.NewInt(*usedGas)
		//log.Info("ApplyTransaction ret", "txIndex", i, "usedGas", gasUsedTmp2.Sub(gasUsedTmp2, gasUsedTmp1).Hex(), "systemAddr Balance",
		//	ibs.GetBalance(consensus.SystemAddress).Hex(), "systemAddr Balance diff", balanceResult.Sub(ibs.GetBalance(consensus.SystemAddress), &balanceDiff).Hex())
		//balanceDiff = *ibs.GetBalance(consensus.SystemAddress)
		if trace {
			log.Info("tx idx %d, gas used %d", i, receipt.GasUsed)
		}
		receipts = append(receipts, receipt)
	}

	if !vmConfig.ReadOnly {
		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		tx := block.Transactions()
		if _, _, _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, tx, block.Uncles(), receipts, block.Withdrawals(), stagedsync.NewChainReaderImpl(chainConfig, dbtx, api._blockReader), nil, nil); err != nil {
			return nil, fmt.Errorf("finalize of block %d failed: %w", block.NumberU64(), err)
		}
		if err := ibs.CommitBlock(rules, blockWriter); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
		}
	}

	return receipts, nil
}
