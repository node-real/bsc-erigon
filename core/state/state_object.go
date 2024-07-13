// Copyright 2019 The go-ethereum Authors
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

package state

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"math/big"

	"github.com/holiman/uint256"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/core/tracing"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

var emptyCodeHash = crypto.Keccak256(nil)
var emptyCodeHashH = libcommon.BytesToHash(emptyCodeHash)

type Code []byte

func (c Code) String() string {
	return string(c) //strings.Join(Disassemble(c), " ")
}

type Storage map[libcommon.Hash]uint256.Int

func (s Storage) String() (str string) {
	for key, value := range s {
		str += fmt.Sprintf("%X : %X\n", key, value)
	}

	return
}

func (s Storage) Copy() Storage {
	return maps.Clone(s)
}

// stateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// First you need to obtain a state object.
// Account values can be accessed and modified through the object.
type stateObject struct {
	address  libcommon.Address
	data     accounts.Account
	original accounts.Account
	db       *IntraBlockState

	// Write caches.
	//trie Trie // storage trie, which becomes non-nil on first access
	code Code // contract bytecode, which gets set when code is loaded

	originStorage Storage // Storage cache of original entries to dedup rewrites
	// blockOriginStorage keeps the values of storage items at the beginning of the block
	// Used to make decision on whether to make a write to the
	// database (value != origin) or not (value == origin)
	blockOriginStorage Storage
	dirtyStorage       Storage // Storage entries that need to be flushed to disk
	fakeStorage        Storage // Fake storage which constructed by caller for debugging purpose.

	// Cache flags.
	// When an object is marked selfdestructed it will be delete from the trie
	// during the "update" phase of the state transition.
	dirtyCode       bool // true if the code was updated
	selfdestructed  bool
	deleted         bool // true if account was deleted during the lifetime of this object
	newlyCreated    bool // true if this object was created in the current transaction
	createdContract bool // true if this object represents a newly created contract
}

// empty returns whether the account is considered empty.
func (so *stateObject) empty() bool {
	return so.data.Nonce == 0 && so.data.Balance.IsZero() && bytes.Equal(so.data.CodeHash[:], emptyCodeHash)
}

// newObject creates a state object.
func newObject(db *IntraBlockState, address libcommon.Address, data, original *accounts.Account) *stateObject {
	var so = stateObject{
		db:                 db,
		address:            address,
		originStorage:      make(Storage),
		blockOriginStorage: make(Storage),
		dirtyStorage:       make(Storage),
	}
	so.data.Copy(data)
	if !so.data.Initialised {
		so.data.Balance.SetUint64(0)
		so.data.Initialised = true
	}
	if so.data.CodeHash == (libcommon.Hash{}) {
		so.data.CodeHash = emptyCodeHashH
	}
	if so.data.Root == (libcommon.Hash{}) {
		so.data.Root = trie.EmptyRoot
	}
	so.original.Copy(original)

	return &so
}

// EncodeRLP implements rlp.Encoder.
func (so *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, so.data)
}

// setError remembers the first non-nil error it is called with.
func (so *stateObject) setError(err error) {
	if so.db.savedErr == nil {
		so.db.savedErr = err
	}
}

func (so *stateObject) markSelfdestructed() {
	so.selfdestructed = true
}

func (so *stateObject) touch() {
	so.db.journal.append(touchChange{
		account: &so.address,
	})
	if so.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		so.db.journal.dirty(so.address)
	}
}

// GetState returns a value from account storage.
func (so *stateObject) GetState(key *libcommon.Hash, out *uint256.Int) {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if so.fakeStorage != nil {
		*out = so.fakeStorage[*key]
		return
	}
	value, dirty := so.dirtyStorage[*key]
	if dirty {
		*out = value
		return
	}
	// Otherwise return the entry's original value
	so.GetCommittedState(key, out)
}

type HotFixPattern struct {
	txHash libcommon.Hash
	addr   libcommon.Address
	kvList Storage
}

func (so *stateObject) patchGethHotFixMainnet1() {
	var patchBlockHash libcommon.Hash = libcommon.HexToHash("0x022296e50021d7225b75f3873e7bc5a2bf6376a08079b4368f9dee81946d623b")
	if so.db.bhash != patchBlockHash {
		return
	}

	totalPatches := []HotFixPattern{}
	// patch 1: BlockNum 33851236, txIndex 89
	patch1 := HotFixPattern{
		txHash: libcommon.HexToHash("0x7eba4edc7c1806d6ee1691d43513838931de5c94f9da56ec865721b402f775b0"),
		addr:   libcommon.HexToAddress("0x00000000001f8b68515EfB546542397d3293CCfd"),
		kvList: make(Storage),
	}
	patch1KVs := map[string]string{
		"0x0000000000000000000000000000000000000000000000000000000000000001": "0x00000000000000000000000052db206170b430da8223651d28830e56ba3cdc04",
		"0x0000000000000000000000000000000000000000000000000000000000000002": "0x000000000000000000000000bb45f138499734bf5c0948d490c65903676ea1de",
		"0x65c95177950b486c2071bf2304da1427b9136564150fb97266ffb318b03a71cc": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x245e58a02bec784ccbdb9e022a84af83227a4125a22a5e68fcc596c7e436434e": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x1c4534c86090a60a9120f34c7b15254913c00bda3d4b276d6edb65c9f48a913f": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x0000000000000000000000000000000000000000000000000000000000000004": "0x0000000000000000000000000000000000000000000000000000000000000019",
		"0x8a35acfbc15ff81a39ae7d344fd709f28e8600b4aa8c65c6b64bfe7fe36bd1b4": "0x0000000000000000000000000000000000000000000000000000000000000000",
		"0x8a35acfbc15ff81a39ae7d344fd709f28e8600b4aa8c65c6b64bfe7fe36bd1b5": "0x0000000000000000000000000000000000000000000000000000000000000000",
		"0x8a35acfbc15ff81a39ae7d344fd709f28e8600b4aa8c65c6b64bfe7fe36bd1b6": "0x0000000000000000000000000000000000000000000000000000000000000000",
		"0x0000000000000000000000000000000000000000000000000000000000000005": "0x00000000000000000000000000000000000000000000000000000000000fc248",
		"0x0000000000000000000000000000000000000000000000000000000000000006": "0x00000000000000000000000000000000000000000000000000000000000fc132",
	}
	for k, v := range patch1KVs {
		patch1.kvList[libcommon.HexToHash(k)] = *new(uint256.Int).SetBytes(hexutil.MustDecode(v))
	}
	totalPatches = append(totalPatches, patch1)

	// patch 2: BlockNum 33851236, txIndex 90
	patch2 := HotFixPattern{
		txHash: libcommon.HexToHash("0x5217324f0711af744fe8e12d73f13fdb11805c8e29c0c095ac747b7e4563e935"),
		addr:   libcommon.HexToAddress("0x00000000001f8b68515EfB546542397d3293CCfd"),
		kvList: make(Storage),
	}
	patch2KVs := map[string]string{
		"0xbcfc62ca570bdb58cf9828ac51ae8d7e063a1cc0fa1aee57691220a7cd78b1c8": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x30dce49ce1a4014301bf21aad0ee16893e4dcc4a4e4be8aa10e442dd13259837": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xc0582628d787ee16fe03c8e5b5f5644d3b81989686f8312280b7a1f733145525": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xfca5cf22ff2e8d58aece8e4370cce33cd0144d48d00f40a5841df4a42527694b": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xb189302b37865d2ae522a492ff1f61a5addc1db44acbdcc4b6814c312c815f46": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xfe1f1986775fc2ac905aeaecc7b1aa8b0d6722b852c90e26edacd2dac7382489": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x36052a8ddb27fecd20e2e09da15494a0f2186bf8db36deebbbe701993f8c4aae": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x4959a566d8396b889ff4bc20e18d2497602e01e5c6013af5af7a7c4657ece3e2": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xe0b5aeb100569add952966f803cb67aca86dc6ec8b638f5a49f9e0760efa9a7a": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x632467ad388b91583f956f76488afc42846e283c962cbb215d288033ffc4fb71": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x9ad4e69f52519f7b7b8ee5ae3326d57061b429428ea0c056dd32e7a7102e79a7": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x35e130c7071699eae5288b12374ef157a15e4294e2b3a352160b7c1cd4641d82": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xa0d8279f845f63979dc292228adfa0bda117de27e44d90ac2adcd44465b225e7": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x9a100b70ffda9ed9769becdadca2b2936b217e3da4c9b9817bad30d85eab25ff": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x28d67156746295d901005e2d95ce589e7093decb638f8c132d9971fd0a37e176": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x297c4e115b5df76bcd5a1654b8032661680a1803e30a0774cb42bb01891e6d97": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x5f71b88f1032d27d8866948fc9c49525f3e584bdd52a66de6060a7b1f767326f": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xe6d8ddf6a0bbeb4840f48f0c4ffda9affa4675354bdb7d721235297f5a094f54": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x30ba10aef6238bf19667aaa988b18b72adb4724c016e19eb64bbb52808d1a842": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0x9c6806a4d6a99e4869b9a4aaf80b0a3bf5f5240a1d6032ed82edf0e86f2a2467": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xe8480d613bbf3b979aee2de4487496167735bb73df024d988e1795b3c7fa559a": "0x0000000000000000000000000000000000000000000000000000000000000001",
		"0xebfaec01f898f7f0e2abdb4b0aee3dfbf5ec2b287b1e92f9b62940f85d5f5bac": "0x0000000000000000000000000000000000000000000000000000000000000001",
	}
	for k, v := range patch2KVs {
		patch2.kvList[libcommon.HexToHash(k)] = *new(uint256.Int).SetBytes(hexutil.MustDecode(v))
	}
	totalPatches = append(totalPatches, patch2)

	// apply the patches
	for _, patch := range totalPatches {
		if so.db.thash != patch.txHash {
			continue
		}
		if so.address != patch.addr {
			continue
		}
		for k, v := range patch.kvList {
			so.originStorage[k] = v
		}
	}
}

func (so *stateObject) patchGethHotFixChapel1() {
	var patchBlockHash libcommon.Hash = libcommon.HexToHash("0x1237cb09a7d08c187a78e777853b70be28a41bb188c5341987408623c1a4f4aa")
	if so.db.bhash != patchBlockHash {
		return
	}

	totalPatches := []HotFixPattern{}
	// patch 1: BlockNum 35547779, txIndex 196
	patch1 := HotFixPattern{
		txHash: libcommon.HexToHash("0x7ce9a3cf77108fcc85c1e84e88e363e3335eca515dfcf2feb2011729878b13a7"),
		addr:   libcommon.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"),
		kvList: make(Storage),
	}
	patch1KVs := map[string]string{
		"0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0": "0x0000000000000000000000000000000000000000000000f6a7831804efd2cd0a",
	}
	for k, v := range patch1KVs {
		patch1.kvList[libcommon.HexToHash(k)] = *new(uint256.Int).SetBytes(hexutil.MustDecode(v))
	}
	totalPatches = append(totalPatches, patch1)

	// apply the patches
	for _, patch := range totalPatches {
		if so.db.thash != patch.txHash {
			continue
		}
		if so.address != patch.addr {
			continue
		}
		for k, v := range patch.kvList {
			so.originStorage[k] = v
		}
	}
}

func (so *stateObject) patchGethHotFixChapel2() {
	var patchBlockHash libcommon.Hash = libcommon.HexToHash("0xcdd38b3681c8f3f1da5569a893231466ab35f47d58ba85dbd7d9217f304983bf")
	if so.db.bhash != patchBlockHash {
		return
	}

	totalPatches := []HotFixPattern{}
	// patch 1: BlockNum 35548081, txIndex 486
	patch1 := HotFixPattern{
		txHash: libcommon.HexToHash("0xe3895eb95605d6b43ceec7876e6ff5d1c903e572bf83a08675cb684c047a695c"),
		addr:   libcommon.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"),
		kvList: make(Storage),
	}
	patch1KVs := map[string]string{
		"0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0": "0x0000000000000000000000000000000000000000000000114be8ecea72b64003",
	}
	for k, v := range patch1KVs {
		patch1.kvList[libcommon.HexToHash(k)] = *new(uint256.Int).SetBytes(hexutil.MustDecode(v))
	}
	totalPatches = append(totalPatches, patch1)

	// apply the patches
	for _, patch := range totalPatches {
		if so.db.thash != patch.txHash {
			continue
		}
		if so.address != patch.addr {
			continue
		}
		for k, v := range patch.kvList {
			so.originStorage[k] = v
		}
	}
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (so *stateObject) GetCommittedState(key *libcommon.Hash, out *uint256.Int) {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if so.fakeStorage != nil {
		*out = so.fakeStorage[*key]
		return
	}
	so.patchGethHotFixMainnet1()
	so.patchGethHotFixChapel1()
	so.patchGethHotFixChapel2()
	// If we have the original value cached, return that
	{
		value, cached := so.originStorage[*key]
		if cached {
			*out = value
			return
		}
	}
	if so.createdContract {
		out.Clear()
		return
	}
	// Load from DB in case it is missing.
	enc, err := so.db.StateReader.ReadAccountStorage(so.address, so.data.GetIncarnation(), key)
	if err != nil {
		so.setError(err)
		out.Clear()
		return
	}
	if enc != nil {
		out.SetBytes(enc)
	} else {
		out.Clear()
	}
	so.originStorage[*key] = *out
	so.blockOriginStorage[*key] = *out
}

// SetState updates a value in account storage.
func (so *stateObject) SetState(key *libcommon.Hash, value uint256.Int) {
	// If the fake storage is set, put the temporary state update here.
	if so.fakeStorage != nil {
		so.db.journal.append(fakeStorageChange{
			account:  &so.address,
			key:      *key,
			prevalue: so.fakeStorage[*key],
		})
		so.fakeStorage[*key] = value
		return
	}
	// If the new value is the same as old, don't set
	var prev uint256.Int
	so.GetState(key, &prev)
	if prev == value {
		return
	}
	// New value is different, update and journal the change
	so.db.journal.append(storageChange{
		account:  &so.address,
		key:      *key,
		prevalue: prev,
	})
	so.setState(key, value)
}

// SetStorage replaces the entire state storage with the given one.
//
// After this function is called, all original state will be ignored and state
// lookup only happens in the fake state storage.
//
// Note this function should only be used for debugging purpose.
func (so *stateObject) SetStorage(storage Storage) {
	// Allocate fake storage if it's nil.
	if so.fakeStorage == nil {
		so.fakeStorage = make(Storage)
	}
	for key, value := range storage {
		so.fakeStorage[key] = value
	}
	// Don't bother journal since this function should only be used for
	// debugging and the `fake` storage won't be committed to database.
}

func (so *stateObject) setState(key *libcommon.Hash, value uint256.Int) {
	so.dirtyStorage[*key] = value
}

// updateTrie writes cached storage modifications into the object's storage trie.
func (so *stateObject) updateTrie(stateWriter StateWriter) error {
	for key, value := range so.dirtyStorage {
		value := value
		original := so.blockOriginStorage[key]
		so.originStorage[key] = value
		if err := stateWriter.WriteAccountStorage(so.address, so.data.GetIncarnation(), &key, &original, &value); err != nil {
			return err
		}
	}
	return nil
}
func (so *stateObject) printTrie() {
	for key, value := range so.dirtyStorage {
		fmt.Printf("UpdateStorage: %x,%x,%s\n", so.address, key, value.Hex())
	}
}

// AddBalance adds amount to so's balance.
// It is used to add funds to the destination account of a transfer.
func (so *stateObject) AddBalance(amount *uint256.Int, reason tracing.BalanceChangeReason) {
	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.IsZero() {
		if so.empty() {
			so.touch()
		}

		return
	}

	so.SetBalance(new(uint256.Int).Add(so.Balance(), amount), reason)
}

// SubBalance removes amount from so's balance.
// It is used to remove funds from the origin account of a transfer.
func (so *stateObject) SubBalance(amount *uint256.Int, reason tracing.BalanceChangeReason) {
	if amount.IsZero() {
		return
	}
	so.SetBalance(new(uint256.Int).Sub(so.Balance(), amount), reason)
}

func (so *stateObject) SetBalance(amount *uint256.Int, reason tracing.BalanceChangeReason) {
	so.db.journal.append(balanceChange{
		account: &so.address,
		prev:    so.data.Balance,
	})
	so.setBalance(amount)
}

func (so *stateObject) setBalance(amount *uint256.Int) {
	so.data.Balance.Set(amount)
	so.data.Initialised = true
}

// Return the gas back to the origin. Used by the Virtual machine or Closures
func (so *stateObject) ReturnGas(gas *big.Int) {}

func (so *stateObject) setIncarnation(incarnation uint64) {
	so.data.SetIncarnation(incarnation)
}

//
// Attribute accessors
//

// Returns the address of the contract/account
func (so *stateObject) Address() libcommon.Address {
	return so.address
}

// Code returns the contract code associated with this object, if any.
func (so *stateObject) Code() []byte {
	if so.code != nil {
		return so.code
	}
	if bytes.Equal(so.CodeHash(), emptyCodeHash) {
		return nil
	}
	code, err := so.db.StateReader.ReadAccountCode(so.Address(), so.data.Incarnation, libcommon.BytesToHash(so.CodeHash()))
	if err != nil {
		so.setError(fmt.Errorf("can't load code hash %x: %w", so.CodeHash(), err))
	}
	so.code = code
	return code
}

func (so *stateObject) SetCode(codeHash libcommon.Hash, code []byte) {
	prevcode := so.Code()
	so.db.journal.append(codeChange{
		account:  &so.address,
		prevhash: so.data.CodeHash,
		prevcode: prevcode,
	})
	so.setCode(codeHash, code)
}

func (so *stateObject) setCode(codeHash libcommon.Hash, code []byte) {
	so.code = code
	so.data.CodeHash = codeHash
	so.dirtyCode = true
}

func (so *stateObject) SetNonce(nonce uint64) {
	so.db.journal.append(nonceChange{
		account: &so.address,
		prev:    so.data.Nonce,
	})
	so.setNonce(nonce)
}

func (so *stateObject) setNonce(nonce uint64) {
	so.data.Nonce = nonce
}

func (so *stateObject) CodeHash() []byte {
	return so.data.CodeHash[:]
}

func (so *stateObject) Balance() *uint256.Int {
	return &so.data.Balance
}

func (so *stateObject) Nonce() uint64 {
	return so.data.Nonce
}

// Never called, but must be present to allow stateObject to be used
// as a vm.Account interface that also satisfies the vm.ContractRef
// interface. Interfaces are awesome.
func (so *stateObject) Value() *big.Int {
	panic("Value on stateObject should never be called")
}
