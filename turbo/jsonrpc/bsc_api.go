package jsonrpc

import (
	"context"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/parlia"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
)

// BscAPI is a collection of functions that are exposed in the
type BscAPI interface {
	Etherbase(ctx context.Context) (libcommon.Address, error)
	FillTransaction(ctx context.Context, args map[string]interface{}) (map[string]interface{}, error)
	GetDiffAccounts(ctx context.Context, blockNr rpc.BlockNumber) ([]libcommon.Address, error)
	GetDiffAccountsWithScope(ctx context.Context, blockNr rpc.BlockNumber, accounts []libcommon.Address) (*types.DiffAccountsInBlock, error)
	GetFilterLogs(ctx context.Context, id rpc.ID) ([]*types.Log, error)
	GetHashrate(ctx context.Context) (uint64, error)
	GetHeaderByHash(ctx context.Context, hash libcommon.Hash) (map[string]interface{}, error)
	GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error)
	GetTransactionDataAndReceipt(ctx context.Context, hash libcommon.Hash) (map[string]interface{}, error)
	GetTransactionReceiptsByBlockNumber(ctx context.Context, number rpc.BlockNumberOrHash) ([]map[string]interface{}, error)
	Health(ctx context.Context) bool
	Resend(ctx context.Context, sendArgs map[string]interface{}, gasPrice *hexutil.Big, gasLimit *hexutil.Uint64) (libcommon.Hash, error)
	GetTransactionsByBlockNumber(ctx context.Context, blockNr rpc.BlockNumber) ([]*RPCTransaction, error)
	GetVerifyResult(ctx context.Context, blockNr rpc.BlockNumber, blockHash libcommon.Hash, diffHash libcommon.Hash) ([]map[string]interface{}, error)
	PendingTransactions() ([]*RPCTransaction, error)
	GetBlobSidecars(ctx context.Context, numberOrHash rpc.BlockNumberOrHash, fullBlob *bool) ([]map[string]interface{}, error)
	GetBlobSidecarByTxHash(ctx context.Context, hash libcommon.Hash, fullBlob *bool) (map[string]interface{}, error)
	GetFinalizedHeader(ctx context.Context, verifiedValidatorNum int64) (map[string]interface{}, error)
	GetFinalizedBlock(ctx context.Context, verifiedValidatorNum int64, fullTx bool) (map[string]interface{}, error)
}

type BscImpl struct {
	ethApi *APIImpl
}

// NewBscAPI returns BscAPIImpl instance.
func NewBscAPI(eth *APIImpl) *BscImpl {
	return &BscImpl{
		ethApi: eth,
	}
}

func (api *BscImpl) parlia() (*parlia.Parlia, error) {
	type lazy interface {
		HasEngine() bool
		Engine() consensus.EngineReader
	}

	switch engine := api.ethApi.engine().(type) {
	case *parlia.Parlia:
		return engine, nil
	case lazy:
		if engine.HasEngine() {
			if parlia, ok := engine.Engine().(*parlia.Parlia); ok {
				return parlia, nil
			}
		}
	}

	return nil, fmt.Errorf("unknown or invalid consensus engine: %T", api.ethApi.engine())
}

// Etherbase is the address that mining rewards will be send to
func (api *BscImpl) Etherbase(ctx context.Context) (libcommon.Address, error) {
	return api.ethApi.ethBackend.Etherbase(ctx)
}

// FillTransaction fills the defaults (nonce, gas, gasPrice or 1559 fields)
// on a given unsigned transaction, and returns it to the caller for further
// processing (signing + broadcast).
func (api *BscImpl) FillTransaction(ctx context.Context, args map[string]interface{}) (map[string]interface{}, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_fillTransaction")
}

// GetDiffAccountsWithScope returns detailed changes of some interested accounts in a specific block number.
func (api *BscImpl) GetDiffAccountsWithScope(ctx context.Context, blockNr rpc.BlockNumber, accounts []libcommon.Address) (*types.DiffAccountsInBlock, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getDiffAccountsWithScope")
}

// GetDiffAccounts returns changed accounts in a specific block number.
func (api *BscImpl) GetDiffAccounts(ctx context.Context, blockNr rpc.BlockNumber) ([]libcommon.Address, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getDiffAccounts")
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
//
// https://eth.wiki/json-rpc/API#eth_getfilterlogs
func (api *BscImpl) GetFilterLogs(ctx context.Context, id rpc.ID) ([]*types.Log, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getFilterLogs")
}

// GetHashrate returns the current hashrate for local CPU miner and remote miner.
func (api *BscImpl) GetHashrate(ctx context.Context) (uint64, error) {
	return api.ethApi.Hashrate(ctx)
}

// GetHeaderByHash returns the requested header by hash
func (api *BscImpl) GetHeaderByHash(ctx context.Context, hash libcommon.Hash) (map[string]interface{}, error) {
	tx, beginErr := api.ethApi.db.BeginRo(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer tx.Rollback()
	header, err := api.ethApi._blockReader.HeaderByHash(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	fields := ethapi.RPCMarshalHeader(header)
	td, err := rawdb.ReadTd(tx, header.Hash(), header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	fields["totalDifficulty"] = (*hexutil.Big)(td)
	return fields, nil
}

// GetHeaderByNumber returns the requested canonical block header.
func (api *BscImpl) GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error) {
	tx, beginErr := api.ethApi.db.BeginRo(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer tx.Rollback()
	header, err := api.ethApi.headerByRPCNumber(ctx, number, tx)
	if err != nil {
		return nil, err
	}
	fields := ethapi.RPCMarshalHeader(header)
	td, err := rawdb.ReadTd(tx, header.Hash(), header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	fields["totalDifficulty"] = (*hexutil.Big)(td)
	return fields, nil
}

// GetTransactionDataAndReceipt returns the original transaction data and transaction receipt for the given transaction hash.
func (api *BscImpl) GetTransactionDataAndReceipt(ctx context.Context, hash libcommon.Hash) (map[string]interface{}, error) {
	rpcTransaction, err := api.ethApi.GetTransactionByHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	receipt, err := api.ethApi.GetTransactionReceipt(ctx, hash)
	if err != nil {
		return nil, err
	}

	txData := map[string]interface{}{
		"blockHash":        rpcTransaction.BlockHash.String(),
		"blockNumber":      rpcTransaction.BlockNumber.String(),
		"from":             rpcTransaction.From.String(),
		"gas":              rpcTransaction.Gas.String(),
		"gasPrice":         rpcTransaction.GasPrice.String(),
		"hash":             rpcTransaction.Hash.String(),
		"input":            rpcTransaction.Input.String(),
		"nonce":            rpcTransaction.Nonce.String(),
		"to":               rpcTransaction.To.String(),
		"transactionIndex": rpcTransaction.TransactionIndex.String(),
		"value":            rpcTransaction.Value.String(),
		"v":                rpcTransaction.V.String(),
		"r":                rpcTransaction.R.String(),
		"s":                rpcTransaction.S.String(),
	}

	result := map[string]interface{}{
		"txData":  txData,
		"receipt": receipt,
	}
	return result, nil
}

// Health returns true if more than 75% of calls are executed faster than 5 secs
func (api *BscImpl) Health(ctx context.Context) bool {
	return true
}

// Resend accepts an existing transaction and a new gas price and limit. It will remove
// the given transaction from the pool and reinsert it with the new gas price and limit.
func (api *BscImpl) Resend(ctx context.Context, sendArgs map[string]interface{}, gasPrice *hexutil.Big, gasLimit *hexutil.Uint64) (libcommon.Hash, error) {
	return libcommon.Hash{}, fmt.Errorf(NotImplemented, "eth_resend")
}

// GetTransactionsByBlockNumber returns all the transactions for the given block number.
func (api *BscImpl) GetTransactionsByBlockNumber(ctx context.Context, blockNr rpc.BlockNumber) ([]*RPCTransaction, error) {
	tx, beginErr := api.ethApi.db.BeginRo(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer tx.Rollback()
	block, err := api.ethApi.blockByNumber(ctx, blockNr, tx)
	if err != nil {
		return nil, err
	}
	txes := block.Transactions()
	result := make([]*RPCTransaction, 0, len(txes))
	for idx, tx := range txes {
		result = append(result, NewRPCTransaction(tx, block.Hash(), block.NumberU64(), uint64(idx), block.BaseFee()))
	}
	return result, nil
}

func (api *BscImpl) GetTransactionReceiptsByBlockNumber(ctx context.Context, blockNr rpc.BlockNumberOrHash) ([]map[string]interface{}, error) {
	return api.ethApi.GetBlockReceipts(ctx, blockNr)
}

func (api *BscImpl) GetVerifyResult(ctx context.Context, blockNr rpc.BlockNumber, blockHash libcommon.Hash, diffHash libcommon.Hash) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getVerifyResult")
}

// PendingTransactions returns the transactions that are in the transaction pool
// and have a from address that is one of the accounts this node manages.
func (api *BscImpl) PendingTransactions() ([]*RPCTransaction, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_pendingTransactions")
}

func (api *BscImpl) GetBlobSidecars(ctx context.Context, numberOrHash rpc.BlockNumberOrHash, fullBlob *bool) ([]map[string]interface{}, error) {
	showBlob := true
	if fullBlob != nil {
		showBlob = *fullBlob
	}
	tx, err := api.ethApi.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockNumber, _, _, err := rpchelper.GetBlockNumber(numberOrHash, tx, api.ethApi.filters)
	if err != nil {
		return nil, err
	}
	blobSidecars, found, err := api.ethApi._blockReader.ReadBlobByNumber(ctx, tx, blockNumber)
	if err != nil || !found {
		return nil, err
	}
	result := make([]map[string]interface{}, len(blobSidecars))
	for i, sidecar := range blobSidecars {
		result[i] = marshalBlobSidecar(sidecar, showBlob)
	}
	return result, nil
}

func (api *BscImpl) GetBlobSidecarByTxHash(ctx context.Context, hash libcommon.Hash, fullBlob *bool) (map[string]interface{}, error) {
	showBlob := true
	if fullBlob != nil {
		showBlob = *fullBlob
	}
	roTx, err := api.ethApi.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer roTx.Rollback()
	tx, err := api.ethApi.GetTransactionByHash(ctx, hash)
	if err != nil {
		return nil, err
	}
	if tx == nil || tx.BlockNumber == nil || tx.BlockHash == nil || tx.TransactionIndex == nil {
		return nil, nil
	}

	blobSidecars, found, err := api.ethApi._blockReader.ReadBlobByNumber(ctx, roTx, tx.BlockNumber.Uint64())
	if err != nil || !found {
		return nil, err
	}
	for _, sidecar := range blobSidecars {
		if sidecar.TxIndex == uint64(*tx.TransactionIndex) {
			return marshalBlobSidecar(sidecar, showBlob), nil
		}
	}
	return nil, nil
}

func (api *BscImpl) GetFinalizedHeader(ctx context.Context, verifiedValidatorNum int64) (map[string]interface{}, error) {
	finalizedBlockNumber, err := api.getFinalizedNumber(ctx, verifiedValidatorNum)
	if err != nil { // impossible
		return nil, err
	}
	return api.GetHeaderByNumber(ctx, rpc.BlockNumber(finalizedBlockNumber))
}

func (api *BscImpl) GetFinalizedBlock(ctx context.Context, verifiedValidatorNum int64, fullTx bool) (map[string]interface{}, error) {
	finalizedBlockNumber, err := api.getFinalizedNumber(ctx, verifiedValidatorNum)
	if err != nil { // impossible
		return nil, err
	}
	return api.ethApi.GetBlockByNumber(ctx, rpc.BlockNumber(finalizedBlockNumber), fullTx)
}

func (api *BscImpl) getFinalizedNumber(ctx context.Context, verifiedValidatorNum int64) (int64, error) {
	tx, err := api.ethApi.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	chainConfig, err := api.ethApi.chainConfig(ctx, tx)
	if err != nil {
		return 0, err
	}
	// init consensus db
	bsc, err := api.parlia()
	if err != nil {
		return 0, err
	}
	chain := stagedsync.NewChainReaderImpl(chainConfig, tx, api.ethApi._blockReader, nil)

	// get validator set
	service := bsc.APIs(chain)[0].Service
	currentHead := rpc.LatestBlockNumber
	curValidators, err := service.(*parlia.API).GetValidators(&currentHead)
	if err != nil { // impossible
		return 0, err
	}
	valLen := int64(len(curValidators))
	if verifiedValidatorNum < 1 || verifiedValidatorNum > valLen {
		return 0, fmt.Errorf("%d out of range [1,%d]", verifiedValidatorNum, valLen)
	}

	fastFinalizedHeader, err := api.ethApi.headerByRPCNumber(ctx, rpc.FinalizedBlockNumber, tx)
	if err != nil { // impossible
		return 0, err
	}
	latestHeader, err := api.ethApi.headerByRPCNumber(ctx, rpc.LatestBlockNumber, tx)
	if err != nil { // impossible
		return 0, err
	}
	lastHeader := latestHeader
	confirmedValSet := make(map[libcommon.Address]struct{}, valLen)
	confirmedValSet[lastHeader.Coinbase] = struct{}{}
	for count := 1; int64(len(confirmedValSet)) < verifiedValidatorNum && count <= int(chainConfig.Parlia.Epoch) && lastHeader.Number.Int64() > max(fastFinalizedHeader.Number.Int64(), 1); count++ {
		lastHeader, err = api.ethApi._blockReader.HeaderByHash(ctx, tx, lastHeader.ParentHash)
		if err != nil { // impossible
			return 0, err
		}
		confirmedValSet[lastHeader.Coinbase] = struct{}{}
	}
	finalizedBlockNumber := max(fastFinalizedHeader.Number.Int64(), lastHeader.Number.Int64())
	log.Debug("getFinalizedNumber", "LatestBlockNumber", latestHeader.Number.Int64(), "fastFinalizedHeight", fastFinalizedHeader.Number.Int64(),
		"lastHeader", lastHeader.Number.Int64(), "finalizedBlockNumber", finalizedBlockNumber, "len(confirmedValSet)", len(confirmedValSet))

	return finalizedBlockNumber, nil
}

func marshalBlobSidecar(sidecar *types.BlobSidecar, fullBlob bool) map[string]interface{} {
	fields := map[string]interface{}{
		"blockHash":   sidecar.BlockHash,
		"blockNumber": hexutil.EncodeUint64(sidecar.BlockNumber.Uint64()),
		"txHash":      sidecar.TxHash,
		"txIndex":     hexutil.EncodeUint64(sidecar.TxIndex),
	}
	fields["blobSidecar"] = marshalBlob(sidecar.BlobTxSidecar, fullBlob)
	return fields
}

func marshalBlob(blobTxSidecar types.BlobTxSidecar, fullBlob bool) map[string]interface{} {
	fields := map[string]interface{}{
		"blobs":       blobTxSidecar.Blobs,
		"commitments": blobTxSidecar.Commitments,
		"proofs":      blobTxSidecar.Proofs,
	}
	if !fullBlob {
		var blobs []libcommon.Hash
		for _, blob := range blobTxSidecar.Blobs {
			var value libcommon.Hash
			copy(value[:], blob[:32])
			blobs = append(blobs, value)
		}
		fields["blobs"] = blobs
	}
	return fields
}
