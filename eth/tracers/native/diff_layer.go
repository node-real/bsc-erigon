package native

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/tracers"
	_ "github.com/ledgerwatch/erigon/params"
	_ "github.com/ledgerwatch/erigon/params/networkname"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
)

func init() {
	register("diffLayer", newDiffLayerTracer)
}

type DiffLayerStateLayer struct {
	Accounts  map[libcommon.Address]LayerAccount
	Destructs map[libcommon.Address]bool
	Storages  map[libcommon.Address]map[libcommon.Hash]libcommon.Hash `json:"storages"`
}

type LayerAccount struct {
	Balance  string         `json:"balance"`
	Nonce    uint64         `json:"nonce"`
	CodeHash libcommon.Hash `json:"code"`
	exists   bool           // exsits only used in VM.CREATE and VM.CREATE2 to check whether the addr is in stateDb or not
}

type DiffLayer struct {
	PreStateLayer   DiffLayerStateLayer `json:"preStateLayer"`
	AfterStateLayer DiffLayerStateLayer `json:"afterStateLayer"`
}

type diffLayerTracer struct {
	env             vm.VMInterface
	preStateLayer   DiffLayerStateLayer
	afterStateLayer DiffLayerStateLayer
	create          bool
	to              libcommon.Address
	gasLimit        uint64 // Amount of gas bought for the whole tx
	interrupt       uint32 // Atomic flag to signal execution interruption
	reason          error  // Textual reason for the interruption
	deleteReCreated map[libcommon.Address]bool
	destructAddress map[libcommon.Address]bool
	created         map[libcommon.Address]bool
	rwMutex         sync.RWMutex
}

type DiffLayerTracerConfig struct {
	BlockNumber uint64         `json:"blockNumber"`
	BlockHash   libcommon.Hash `json:"blockHash"`
	ChainId     uint64         `json:"chainId"`
}

func newDiffLayerTracer(ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	return &diffLayerTracer{
		preStateLayer: DiffLayerStateLayer{
			Accounts:  map[libcommon.Address]LayerAccount{},
			Destructs: map[libcommon.Address]bool{},
			Storages:  map[libcommon.Address]map[libcommon.Hash]libcommon.Hash{},
		},
		afterStateLayer: DiffLayerStateLayer{
			Accounts:  map[libcommon.Address]LayerAccount{},
			Destructs: map[libcommon.Address]bool{},
			Storages:  map[libcommon.Address]map[libcommon.Hash]libcommon.Hash{},
		},
		deleteReCreated: map[libcommon.Address]bool{},
		destructAddress: map[libcommon.Address]bool{},
		rwMutex:         sync.RWMutex{},
		created:         map[libcommon.Address]bool{},
	}, nil
}

func (t *diffLayerTracer) systemContractsUpgrade() {
	if t.env.Context().BlockNumber == 0 {
		return
	}
	chainConfig := t.env.ChainConfig()
	blockNumber := new(big.Int)
	blockNumber.SetUint64(t.env.Context().BlockNumber)
	if chainConfig.IsOnRamanujan(blockNumber) {
		t.RecordSystemContractUpgrade(systemcontracts.RamanujanUpgrade[chainConfig.ChainName], blockNumber)
	}

	if chainConfig.IsOnNiels(blockNumber) {
		t.RecordSystemContractUpgrade(systemcontracts.NielsUpgrade[chainConfig.ChainName], blockNumber)
	}

	if chainConfig.IsOnMirrorSync(blockNumber) {
		t.RecordSystemContractUpgrade(systemcontracts.MirrorUpgrade[chainConfig.ChainName], blockNumber)
	}

	if chainConfig.IsOnBruno(blockNumber) {
		t.RecordSystemContractUpgrade(systemcontracts.BrunoUpgrade[chainConfig.ChainName], blockNumber)
	}

	if chainConfig.IsOnEuler(blockNumber) {
		t.RecordSystemContractUpgrade(systemcontracts.EulerUpgrade[chainConfig.ChainName], blockNumber)
	}

	if chainConfig.IsOnMoran(blockNumber) {
		t.RecordSystemContractUpgrade(systemcontracts.MoranUpgrade[chainConfig.ChainName], blockNumber)
	}

	if chainConfig.IsOnGibbs(blockNumber) {
		t.RecordSystemContractUpgrade(systemcontracts.GibbsUpgrade[chainConfig.ChainName], blockNumber)
	}
}

func (t *diffLayerTracer) RecordSystemContractUpgrade(upgrade *systemcontracts.Upgrade, blockNumber *big.Int) {
	if upgrade == nil {
		return
	}

	//log.Infof(fmt.Sprintf("Apply upgrade %s at height %d", upgrade.UpgradeName, blockNumber.Int64()))
	for _, cfg := range upgrade.Configs {
		//log.Infof(fmt.Sprintf("Upgrade contract %s to commit %s", cfg.ContractAddr.String(), cfg.CommitUrl))

		//t.lookupAccount(cfg.ContractAddr)
		diffAccount := LayerAccount{}
		if t.env.IntraBlockState().Exist(cfg.ContractAddr) {
			diffAccount.exists = true
		}
		newContractCode, err := hex.DecodeString(cfg.Code)
		if err != nil {
			panic(fmt.Errorf("failed to decode new contract code: %s", err.Error()))
		}
		diffAccount.Balance, diffAccount.Nonce, diffAccount.CodeHash = BigToHex(t.env.IntraBlockState().GetBalance(cfg.ContractAddr)), t.env.IntraBlockState().GetNonce(cfg.ContractAddr),
			crypto.Keccak256Hash(newContractCode)
		t.afterStateLayer.Accounts[cfg.ContractAddr] = diffAccount
		//if cfg.BeforeUpgrade != nil {
		//	err := cfg.BeforeUpgrade(blockNumber, cfg.ContractAddr, t.env.IntraBlockState())
		//	if err != nil {
		//		panic(fmt.Errorf("contract address: %s, execute beforeUpgrade error: %s", cfg.ContractAddr.String(), err.Error()))
		//	}
		//}

		//newContractCode, err := hex.DecodeString(cfg.Code)
		//if err != nil {
		//	panic(fmt.Errorf("failed to decode new contract code: %s", err.Error()))
		//}

		//prevContractCode := t.env.StateDB.GetCode(cfg.ContractAddr)
		//if len(prevContractCode) == 0 && len(newContractCode) > 0 {
		//	// system contracts defined after genesis need to be explicitly created
		//	t.env.StateDB.CreateAccount(cfg.ContractAddr)
		//}
		//
		//t.env.StateDB.SetCode(cfg.ContractAddr, newContractCode)
		//
		//if cfg.AfterUpgrade != nil {
		//	err := cfg.AfterUpgrade(blockNumber, cfg.ContractAddr, t.env.StateDB)
		//	if err != nil {
		//		panic(fmt.Errorf("contract address: %s, execute afterUpgrade error: %s", cfg.ContractAddr.String(), err.Error()))
		//	}
		//}
	}
}

// CaptureStart implements the EVMLogger interface to initialize the tracing operation.
func (t *diffLayerTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precomplile, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.env = env
	t.create = create
	t.to = to

	t.lookupAccount(from)
	t.lookupAccount(to)
	t.lookupAccount(t.env.Context().Coinbase)
	t.lookupAccount(consensus.SystemAddress)
	// 22107423 block number, lookup systemcontracts do not work, need special logic
	t.systemContractsUpgrade()

	// The recipient balance includes the value transferred.
	toAccount := t.preStateLayer.Accounts[to]
	toBal := hexutil.MustDecodeBig(toAccount.Balance)
	toBal = new(big.Int).Sub(toBal, value.ToBig())
	toAccount.Balance = hexutil.EncodeBig(toBal)
	t.preStateLayer.Accounts[to] = toAccount

	// The sender balance is after reducing: value and gasLimit.
	// We need to re-add them to get the pre-tx balance.
	fromAccount := t.preStateLayer.Accounts[from]
	fromBal := hexutil.MustDecodeBig(fromAccount.Balance)
	gasPrice := env.TxContext().GasPrice
	consumedGas := new(big.Int).Mul(gasPrice.ToBig(), new(big.Int).SetUint64(t.gasLimit))
	fromBal.Add(fromBal, new(big.Int).Add(value.ToBig(), consumedGas))
	fromAccount.Balance = hexutil.EncodeBig(fromBal)
	fromAccount.Nonce--
	t.preStateLayer.Accounts[from] = fromAccount
	if create {
		t.created[to] = true
	}
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (t *diffLayerTracer) CaptureEnd(output []byte, gasUsed uint64, err error) {
	if t.create {
		// Exclude created contract.
		addr := t.to
		if t.env.IntraBlockState().HasSelfdestructed(addr) || t.env.IntraBlockState().Empty(addr) {
			// when t.create, tracer's logic is captureStart -> captureEnd, the create address no need to add in afterStateLayer.Destructs
			// as it doesn't have any other branch logic
			t.destructAddress[addr] = true
		} else {
			// if err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas), account will revert
			if err != nil && (err == vm.ErrCodeStoreOutOfGas && !t.env.ChainRules().IsHomestead) {
				//delete(t.preStateLayer.Accounts, t.to)
				//delete(t.preStateLayer.Storages, t.to)
				return
			}
			preLayerAccount := t.preStateLayer.Accounts[addr]
			afterLayerAccount := LayerAccount{
				Nonce:    t.env.IntraBlockState().GetNonce(addr),
				Balance:  BigToHex(t.env.IntraBlockState().GetBalance(addr)),
				CodeHash: t.env.IntraBlockState().GetCodeHash(addr),
			}
			if preLayerAccount.Nonce != afterLayerAccount.Nonce || preLayerAccount.CodeHash.Hex() != afterLayerAccount.CodeHash.Hex() ||
				preLayerAccount.Balance != afterLayerAccount.Balance {
				t.afterStateLayer.Accounts[addr] = afterLayerAccount
				t.afterStateLayer.Storages[addr] = make(map[libcommon.Hash]libcommon.Hash)
				for key := range t.preStateLayer.Storages[addr] {
					// getCommittedState or getState?
					var v uint256.Int
					t.env.IntraBlockState().GetState(addr, &key, &v)
					if libcommon.BytesToHash(v.Bytes()) != t.preStateLayer.Storages[addr][key] {
						if _, oka := t.afterStateLayer.Accounts[addr]; !oka {
							afterLayerAccountT := LayerAccount{
								Nonce:    t.env.IntraBlockState().GetNonce(addr),
								Balance:  BigToHex(t.env.IntraBlockState().GetBalance(addr)),
								CodeHash: t.env.IntraBlockState().GetCodeHash(addr),
							}
							t.afterStateLayer.Accounts[addr] = afterLayerAccountT
						}
						_, ok := t.afterStateLayer.Storages[addr]
						if ok {
							t.afterStateLayer.Storages[addr][key] = libcommon.BytesToHash(v.Bytes())
						} else {
							t.afterStateLayer.Storages[addr] = make(map[libcommon.Hash]libcommon.Hash)
							t.afterStateLayer.Storages[addr][key] = libcommon.BytesToHash(v.Bytes())
						}
					}
				}
			}
			if preLayerAccount.exists {
				t.deleteReCreated[addr] = true
			}
		}
		//delete(t.preStateLayer.Accounts, t.to)
		//delete(t.preStateLayer.Storages, t.to)
	}
}

// CaptureState implements the EVMLogger interface to trace a single step of VM execution.
func (t *diffLayerTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	stack := scope.Stack
	stackData := stack.Data
	stackLen := len(stackData)
	caller := scope.Contract.Address()
	switch {
	case stackLen >= 1 && (op == vm.SLOAD || op == vm.SSTORE):
		//case stackLen >= 1 && op == vm.SSTORE:
		slot := libcommon.Hash(stackData[stackLen-1].Bytes32())
		t.lookupStorage(caller, slot)
	case stackLen >= 1 && (op == vm.EXTCODECOPY || op == vm.EXTCODEHASH || op == vm.EXTCODESIZE || op == vm.BALANCE || op == vm.SELFDESTRUCT):
		addr := libcommon.Address(stackData[stackLen-1].Bytes20())
		t.lookupAccount(addr)
		//if op == vm.SELFDESTRUCT {
		// t.destructAddress[addr] = true
		//}
	case stackLen >= 5 && (op == vm.DELEGATECALL || op == vm.CALL || op == vm.STATICCALL || op == vm.CALLCODE):
		addr := libcommon.Address(stackData[stackLen-2].Bytes20())
		t.lookupAccount(addr)
	case op == vm.CREATE:
		nonce := t.env.IntraBlockState().GetNonce(caller)
		createAddr := crypto.CreateAddress(caller, nonce)
		t.lookupAccount(createAddr)
		// deal with the situation when account being contract
		prestateAccount, ok := t.preStateLayer.Accounts[createAddr]
		if ok {
			bi := new(big.Int)
			bi.SetString(prestateAccount.Balance[2:], 16)
			if (bi.Sign() != 0 || prestateAccount.Nonce != 0) && strings.ToLower(t.preStateLayer.Accounts[createAddr].CodeHash.Hex()) == "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470" {
				t.deleteReCreated[createAddr] = true
			}
		}
	case stackLen >= 4 && op == vm.CREATE2:
		offset := stackData[stackLen-2]
		size := stackData[stackLen-3]
		init := scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
		inithash := crypto.Keccak256(init)
		salt := stackData[stackLen-4]
		createAddr := crypto.CreateAddress2(caller, salt.Bytes32(), inithash)
		t.lookupAccount(createAddr)
		// deal with the situation when account being contract
		prestateAccount, ok := t.preStateLayer.Accounts[createAddr]
		if ok {
			bi := new(big.Int)
			bi.SetString(prestateAccount.Balance[2:], 16)
			if (bi.Sign() != 0 || prestateAccount.Nonce != 0) && strings.ToLower(t.preStateLayer.Accounts[createAddr].CodeHash.Hex()) == "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470" {
				t.deleteReCreated[createAddr] = true
			}
		}
	}
}

// CaptureFault implements the EVMLogger interface to trace an execution fault.
func (t *diffLayerTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, _ *vm.ScopeContext, depth int, err error) {
}

// CaptureEnter is called when EVM enters a new scope (via call, create or selfdestruct).
func (t *diffLayerTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

// CaptureExit is called when EVM exits a scope, even if the scope didn't
// execute any code.
func (t *diffLayerTracer) CaptureExit(output []byte, gasUsed uint64, err error) {
}

func (t *diffLayerTracer) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

func (t *diffLayerTracer) CaptureTxEnd(restGas uint64) {
	for addr := range t.preStateLayer.Accounts {
		if t.created[addr] == true {
			continue
		}
		if t.env.IntraBlockState().HasSelfdestructed(addr) || t.env.IntraBlockState().Empty(addr) {
			prestateAccount, ok := t.preStateLayer.Accounts[addr]
			if ok {
				bi := new(big.Int)
				bi.SetString(prestateAccount.Balance[2:], 16)
				//if prestateAccount.Nonce == 0 && bi.Sign() == 0 && strings.ToLower(t.preStateLayer.Accounts[addr].CodeHash.Hex()) == "0x0000000000000000000000000000000000000000000000000000000000000000" {
				// t.afterStateLayer.Destructs[addr] = true
				//} else
				if prestateAccount.Nonce == 0 && bi.Sign() == 0 && strings.ToLower(t.preStateLayer.Accounts[addr].CodeHash.Hex()) == "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470" {
					t.afterStateLayer.Destructs[addr] = true
				} else if bi.Sign() != 0 || prestateAccount.Nonce != 0 {
					t.afterStateLayer.Destructs[addr] = true
				} else if strings.ToLower(t.preStateLayer.Accounts[addr].CodeHash.Hex()) != "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470" {
					if strings.ToLower(t.preStateLayer.Accounts[addr].CodeHash.Hex()) == "0x0000000000000000000000000000000000000000000000000000000000000000" {
						if prestateAccount.exists {
							t.afterStateLayer.Destructs[addr] = true
						}
					} else {
						t.afterStateLayer.Destructs[addr] = true
					}
				}
				t.destructAddress[addr] = true
			} else {
				t.destructAddress[addr] = true
			}
		} else {
			preLayerAccount := t.preStateLayer.Accounts[addr]
			afterLayerAccount := LayerAccount{
				Nonce:    t.env.IntraBlockState().GetNonce(addr),
				Balance:  BigToHex(t.env.IntraBlockState().GetBalance(addr)),
				CodeHash: t.env.IntraBlockState().GetCodeHash(addr),
			}
			//log.Infof("addr:%s, pre: %d, %s, %s. after:%d, %s, %s", addr.Hex(), preLayerAccount.Nonce, preLayerAccount.CodeHash.Hex(),
			// preLayerAccount.Balance, afterLayerAccount.Nonce, afterLayerAccount.CodeHash.Hex(), afterLayerAccount.Balance)
			if preLayerAccount.Nonce != afterLayerAccount.Nonce || preLayerAccount.CodeHash.Hex() != afterLayerAccount.CodeHash.Hex() ||
				preLayerAccount.Balance != afterLayerAccount.Balance {
				t.afterStateLayer.Accounts[addr] = afterLayerAccount
			}
		}
	}
	for addr := range t.preStateLayer.Storages {
		//storageChange := false
		for key := range t.preStateLayer.Storages[addr] {
			// getCommittedState or getState?
			var v uint256.Int
			k := key
			t.env.IntraBlockState().GetState(addr, &k, &v)
			if libcommon.BytesToHash(v.Bytes()) != t.preStateLayer.Storages[addr][key] {
				//storageChange = true
				if _, oka := t.afterStateLayer.Accounts[addr]; !oka {
					afterLayerAccount := LayerAccount{
						Nonce:    t.env.IntraBlockState().GetNonce(addr),
						Balance:  BigToHex(t.env.IntraBlockState().GetBalance(addr)),
						CodeHash: t.env.IntraBlockState().GetCodeHash(addr),
					}
					t.afterStateLayer.Accounts[addr] = afterLayerAccount
				}
				_, ok := t.afterStateLayer.Storages[addr]
				if ok {
					t.afterStateLayer.Storages[addr][key] = libcommon.BytesToHash(v.Bytes())
				} else {
					t.afterStateLayer.Storages[addr] = make(map[libcommon.Hash]libcommon.Hash)
					t.afterStateLayer.Storages[addr][key] = libcommon.BytesToHash(v.Bytes())
				}
			}
		}
		//if storageChange == true && t.destructAddress[addr] == true {
		// t.destructAddress[addr] = false
		//}
	}
	for addr := range t.afterStateLayer.Destructs {
		if t.afterStateLayer.Destructs[addr] == true {
			delete(t.afterStateLayer.Accounts, addr)
			delete(t.afterStateLayer.Storages, addr)
		}
	}
	for addr := range t.deleteReCreated {
		prestateAccount, ok := t.preStateLayer.Accounts[addr]
		if ok {
			bi := new(big.Int)
			bi.SetString(prestateAccount.Balance[2:], 16)
			afterLayerAccount := LayerAccount{
				Nonce:    t.env.IntraBlockState().GetNonce(addr),
				Balance:  BigToHex(t.env.IntraBlockState().GetBalance(addr)),
				CodeHash: t.env.IntraBlockState().GetCodeHash(addr),
			}
			// if prestateAccount equal to afterAccount, may be the contract is being reverted
			if prestateAccount.Nonce == afterLayerAccount.Nonce && prestateAccount.CodeHash.Hex() == afterLayerAccount.CodeHash.Hex() &&
				prestateAccount.Balance == afterLayerAccount.Balance {
				continue
			}
		}
		t.afterStateLayer.Destructs[addr] = true
	}
	for addr := range t.destructAddress {
		delete(t.afterStateLayer.Accounts, addr)
		delete(t.afterStateLayer.Storages, addr)
	}
}

// GetResult returns the json-encoded nested list of call traces, and any
// error arising from the encoding or forceful termination (via `Stop`).
func (t *diffLayerTracer) GetResult() (json.RawMessage, error) {
	diffLayerData := DiffLayer{
		PreStateLayer:   t.preStateLayer,
		AfterStateLayer: t.afterStateLayer,
	}
	//log.Infof("prestate account len:%d, after State account len:%d", len(t.preStateLayer.Accounts), len(t.afterStateLayer.Accounts))
	res, err := json.Marshal(diffLayerData)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(res), t.reason
}

// Stop terminates execution of the tracer at the first opportune moment.
func (t *diffLayerTracer) Stop(err error) {
	t.reason = err
	atomic.StoreUint32(&t.interrupt, 1)
}

// lookupAccount fetches details of an account and adds it to the prestate
// if it doesn't exist there.
func (t *diffLayerTracer) lookupAccount(addr libcommon.Address) {
	t.rwMutex.RLock()
	if _, ok := t.preStateLayer.Accounts[addr]; ok {
		t.rwMutex.RUnlock()
		return
	}
	t.rwMutex.RUnlock()
	t.rwMutex.Lock()
	diffAccount := LayerAccount{}
	if t.env.IntraBlockState().Exist(addr) {
		diffAccount.exists = true
	}
	diffAccount.Balance, diffAccount.Nonce, diffAccount.CodeHash = BigToHex(t.env.IntraBlockState().GetBalance(addr)), t.env.IntraBlockState().GetNonce(addr),
		t.env.IntraBlockState().GetCodeHash(addr)
	//diffAccount := LayerAccount{
	// Nonce:    t.env.StateDB.GetNonce(addr),
	// Balance:  BigToHex(t.env.StateDB.GetBalance(addr)),
	// CodeHash: t.env.StateDB.GetCodeHash(addr),
	//}
	//diffAccount.Storage = make(map[libcommon.Hash]libcommon.Hash)
	t.preStateLayer.Accounts[addr] = diffAccount
	//t.preStateLayer.Storages[addr] = make(map[libcommon.Hash]libcommon.Hash)
	t.rwMutex.Unlock()
	return
}

// lookupStorage fetches the requested storage slot and adds
// it to the prestate of the given contract. It assumes `lookupAccount`
// has been performed on the contract before.
func (t *diffLayerTracer) lookupStorage(addr libcommon.Address, key libcommon.Hash) {
	t.rwMutex.RLock()
	_, ok := t.preStateLayer.Storages[addr]
	if ok {
		//if _, ok1 := diffAccount.Storage[key]; ok1 {
		// t.rwMutex.RUnlock()
		// return
		//}
		if _, ok1 := t.preStateLayer.Storages[addr][key]; ok1 {
			t.rwMutex.RUnlock()
			return
		}
	}
	t.rwMutex.RUnlock()

	t.rwMutex.Lock()
	var v uint256.Int
	t.env.IntraBlockState().GetState(addr, &key, &v)
	if ok {
		//diffAccount.Storage[key] = libcommon.Hash{}
		t.preStateLayer.Storages[addr][key] = libcommon.BytesToHash(v.Bytes())
	} else {
		//diffAccount = DiffAccount{}
		//diffAccount.Storage = make(map[libcommon.Hash]libcommon.Hash)
		//diffAccount.Storage[key] = libcommon.Hash{}
		t.preStateLayer.Storages[addr] = make(map[libcommon.Hash]libcommon.Hash)
		t.preStateLayer.Storages[addr][key] = libcommon.BytesToHash(v.Bytes())
	}
	//t.stateDiff.Accounts[addr] = diffAccount
	t.rwMutex.Unlock()
	return
}

func BigToHex(n *uint256.Int) string {
	if n == nil {
		return ""
	}
	return "0x" + n.ToBig().Text(16)
}
