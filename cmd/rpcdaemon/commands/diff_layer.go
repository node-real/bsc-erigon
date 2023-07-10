package commands

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"io"
	"math/big"
)

type DiffLayer struct {
	Accounts  []DiffAccount
	Codes     []DiffCode
	Destructs []libcommon.Address
	Storages  []DiffStorage
}

type DiffCode struct {
	Hash libcommon.Hash
	Code []byte
}

type DiffAccount struct {
	Account libcommon.Address
	Blob    []byte
}

type DiffStorage struct {
	Account libcommon.Address
	Keys    []string
	Vals    [][]byte
}

type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     []byte
	CodeHash []byte
}

type accountRLPShadow Account

var (
	// EmptyRoot is the known root hash of an empty trie.
	EmptyRoot = libcommon.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// EmptyCodeHash is the known hash of the empty EVM bytecode.
	EmptyCodeHash = crypto.Keccak256Hash(nil)
)

func (a *Account) EncodeRLP(w io.Writer) error {
	a1 := accountRLPShadow(*a)
	if bytes.Equal(a1.Root, EmptyRoot.Bytes()) {
		a1.Root = nil
	}
	if bytes.Equal(a1.CodeHash, EmptyCodeHash.Bytes()) {
		a1.CodeHash = nil
	}

	return rlp.Encode(w, a1)
}

// Implements core/state/StateWriter to provide state diffs
type DiffLayerWriter struct {
	layer DiffLayer

	dirtyCodeAddress map[libcommon.Address]struct{}
	storageSlot      map[libcommon.Address]int
}

func NewDiffLayerWriter() *DiffLayerWriter {
	return &DiffLayerWriter{
		dirtyCodeAddress: map[libcommon.Address]struct{}{},
		storageSlot:      map[libcommon.Address]int{},
	}
}

func AccountEqual(original, account *accounts.Account) bool {
	return original.Nonce == account.Nonce && original.CodeHash == account.CodeHash &&
		original.Balance.Cmp(&account.Balance) == 0 && original.Root == account.Root
	//original.Balance == account.Balance && original.Root == account.Root
	//&& original.Incarnation == account.Incarnation &&
	//	original.Root == account.Root
}

func (dlw *DiffLayerWriter) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	// this may have bugs, warn
	//if address == systemcontracts.RelayerHubContract || address == systemcontracts.CrossChainContract {
	//	log.Info("updateAccountData", "addr", address.Hex(), "n1", original.Nonce, "n2", account.Nonce, "codeHash1",
	//		original.CodeHash.Hex(), "codeHash2", account.CodeHash.Hex(), "b1", original.Balance.Hex(), "b2", account.Balance.Hex(),
	//		"r1", original.Root.Hex(), "r2", account.Root.Hex())
	//}
	if _, ok := dlw.storageSlot[address]; AccountEqual(original, account) && !ok {
		//_, dirtyCodeOK := dlw.dirtyCodeAddress[address]
		//_, systemContractOK := diffSystemContracts[address]
		//if !dirtyCodeOK && !systemContractOK {
		//	return nil
		//}
		return nil
	}

	acc := &Account{
		Nonce:    account.Nonce,
		Balance:  account.Balance.ToBig(),
		Root:     account.Root.Bytes(),
		CodeHash: account.CodeHash.Bytes(),
	}

	accData, err := rlp.EncodeToBytes(acc)
	if err != nil || len(accData) == 0 {
		panic(fmt.Errorf("encode failed: %v", err))
	}
	dlw.layer.Accounts = append(dlw.layer.Accounts, DiffAccount{
		Account: address,
		Blob:    accData,
	})
	return nil
}

func (dlw *DiffLayerWriter) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	dlw.layer.Codes = append(dlw.layer.Codes, DiffCode{
		Hash: codeHash,
		Code: code,
	})
	dlw.dirtyCodeAddress[address] = struct{}{}
	return nil
}

func (dlw *DiffLayerWriter) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	if original == nil || !original.Initialised {
		return nil
	}

	dlw.layer.Destructs = append(dlw.layer.Destructs, address)
	//delete(dlw.dirtyCodeAddress, address)
	return nil
}

func (dlw *DiffLayerWriter) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}

	idx, ok := dlw.storageSlot[address]
	if !ok {
		idx = len(dlw.layer.Storages)
		dlw.storageSlot[address] = idx
		dlw.layer.Storages = append(dlw.layer.Storages, DiffStorage{
			Account: address,
			Keys:    nil,
			Vals:    nil,
		})
	}

	if address != dlw.layer.Storages[idx].Account {
		panic("account mismatch")
	}

	dlw.layer.Storages[idx].Keys = append(dlw.layer.Storages[idx].Keys, string(key.Bytes()))
	//dlw.layer.Storages[idx].Keys = append(dlw.layer.Storages[idx].Keys, key.Hex())
	val := value.Bytes()
	if len(val) > 0 {
		val, _ = rlp.EncodeToBytes(val)
	}
	dlw.layer.Storages[idx].Vals = append(dlw.layer.Storages[idx].Vals, val)
	return nil
}

func (dlw *DiffLayerWriter) CreateContract(address libcommon.Address) error {
	dlw.dirtyCodeAddress[address] = struct{}{}
	return nil
}

func (dlw *DiffLayerWriter) GetData() json.RawMessage {
	diffAccountMap := make(map[libcommon.Address]struct{})
	for _, acc := range dlw.layer.Accounts {
		delete(dlw.dirtyCodeAddress, acc.Account)
		diffAccountMap[acc.Account] = struct{}{}
	}

	if len(dlw.dirtyCodeAddress) != 0 {
		addrList := ""
		for k := range dlw.dirtyCodeAddress {
			addrList += k.Hex() + " "
			delete(dlw.dirtyCodeAddress, k)
		}
		log.Info("diff layer dirtyCode", "addrList", addrList)
		panic("diff account missing")
	}

	for _, stor := range dlw.layer.Storages {
		if _, ok := diffAccountMap[stor.Account]; !ok {
			panic("diff account x missing")
		}
	}
	buf := &bytes.Buffer{}
	jsonEncoder := jsoniter.NewEncoder(buf)
	jsonEncoder.SetEscapeHTML(false)
	_ = jsonEncoder.Encode(&dlw.layer)
	//data, _ := json.Marshal(&dlw.layer)
	return json.RawMessage(buf.Bytes())
}
