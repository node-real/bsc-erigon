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

package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/common/cryptozerocopy"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/trie"
	"github.com/erigontech/erigon-lib/types/accounts"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
)

var ErrBehindCommitment = errors.New("behind commitment")

// KvList sort.Interface to sort write list by keys
type KvList struct {
	Keys []string
	Vals [][]byte
}

func (l *KvList) Push(key string, val []byte) {
	l.Keys = append(l.Keys, key)
	l.Vals = append(l.Vals, val)
}

func (l *KvList) Len() int {
	return len(l.Keys)
}

func (l *KvList) Less(i, j int) bool {
	return l.Keys[i] < l.Keys[j]
}

func (l *KvList) Swap(i, j int) {
	l.Keys[i], l.Keys[j] = l.Keys[j], l.Keys[i]
	l.Vals[i], l.Vals[j] = l.Vals[j], l.Vals[i]
}

type dataWithPrevStep struct {
	data     []byte
	prevStep uint64
}

type SharedDomains struct {
	aggTx  *AggregatorRoTx
	sdCtx  *SharedDomainsCommitmentContext
	roTx   kv.Tx
	logger log.Logger

	txNum    uint64
	blockNum atomic.Uint64
	estSize  int
	trace    bool //nolint
	//muMaps   sync.RWMutex
	//walLock sync.RWMutex

	domains [kv.DomainLen]map[string]dataWithPrevStep
	storage *btree2.Map[string, dataWithPrevStep]

	domainWriters [kv.DomainLen]*domainBufferedWriter
	iiWriters     []*invertedIndexBufferedWriter

	currentChangesAccumulator *StateChangeSet
	pastChangesAccumulator    map[string]*StateChangeSet
}

type HasAggTx interface {
	AggTx() any
}
type HasAgg interface {
	Agg() any
}

func NewSharedDomains(tx kv.Tx, logger log.Logger) (*SharedDomains, error) {

	sd := &SharedDomains{
		logger:  logger,
		storage: btree2.NewMap[string, dataWithPrevStep](128),
		//trace:   true,
	}
	sd.SetTx(tx)
	sd.iiWriters = make([]*invertedIndexBufferedWriter, len(sd.aggTx.iis))

	for id, ii := range sd.aggTx.iis {
		sd.iiWriters[id] = ii.NewWriter()
	}

	for id, d := range sd.aggTx.d {
		sd.domains[id] = map[string]dataWithPrevStep{}
		sd.domainWriters[id] = d.NewWriter()
	}

	sd.SetTxNum(0)
	sd.sdCtx = NewSharedDomainsCommitmentContext(sd, commitment.ModeDirect, commitment.VariantHexPatriciaTrie)

	if _, err := sd.SeekCommitment(context.Background(), tx); err != nil {
		return nil, err
	}
	return sd, nil
}

func (sd *SharedDomains) SetChangesetAccumulator(acc *StateChangeSet) {
	sd.currentChangesAccumulator = acc
	for idx := range sd.domainWriters {
		if sd.currentChangesAccumulator == nil {
			sd.domainWriters[idx].diff = nil
		} else {
			sd.domainWriters[idx].diff = &sd.currentChangesAccumulator.Diffs[idx]
		}
	}
}

func (sd *SharedDomains) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *StateChangeSet) {
	if sd.pastChangesAccumulator == nil {
		sd.pastChangesAccumulator = make(map[string]*StateChangeSet)
	}
	key := make([]byte, 40)
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	sd.pastChangesAccumulator[toStringZeroCopy(key)] = acc
}

func (sd *SharedDomains) GetCommitmentContext() *SharedDomainsCommitmentContext {
	return sd.sdCtx
}

func (sd *SharedDomains) GetDiffset(tx kv.RwTx, blockHash common.Hash, blockNumber uint64) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
	var key [40]byte
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], blockHash[:])
	if changeset, ok := sd.pastChangesAccumulator[toStringZeroCopy(key[:])]; ok {
		return [kv.DomainLen][]kv.DomainEntryDiff{
			changeset.Diffs[kv.AccountsDomain].GetDiffSet(),
			changeset.Diffs[kv.StorageDomain].GetDiffSet(),
			changeset.Diffs[kv.CodeDomain].GetDiffSet(),
			changeset.Diffs[kv.CommitmentDomain].GetDiffSet(),
		}, true, nil
	}
	return ReadDiffSet(tx, blockNumber, blockHash)
}

func (sd *SharedDomains) AggTx() any { return sd.aggTx }

// aggregator context should call aggTx.Unwind before this one.
func (sd *SharedDomains) Unwind(ctx context.Context, rwTx kv.RwTx, blockUnwindTo, txUnwindTo uint64, changeset *[kv.DomainLen][]kv.DomainEntryDiff) error {
	step := txUnwindTo / sd.aggTx.a.StepSize()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	sd.aggTx.a.logger.Info("aggregator unwind", "step", step,
		"txUnwindTo", txUnwindTo, "stepsRangeInDB", sd.aggTx.a.StepsRangeInDBAsStr(rwTx))
	//fmt.Printf("aggregator unwind step %d txUnwindTo %d stepsRangeInDB %s\n", step, txUnwindTo, sd.aggTx.a.StepsRangeInDBAsStr(rwTx))
	sf := time.Now()
	defer mxUnwindSharedTook.ObserveDuration(sf)

	if err := sd.Flush(ctx, rwTx); err != nil {
		return err
	}

	for idx, d := range sd.aggTx.d {
		if err := d.unwind(ctx, rwTx, step, txUnwindTo, changeset[idx]); err != nil {
			return err
		}
	}
	for _, ii := range sd.aggTx.iis {
		if err := ii.unwind(ctx, rwTx, txUnwindTo, math.MaxUint64, math.MaxUint64, logEvery, true, nil); err != nil {
			return err
		}
	}

	sd.ClearRam(true)
	sd.SetTxNum(txUnwindTo)
	sd.SetBlockNum(blockUnwindTo)
	return sd.Flush(ctx, rwTx)
}

func (sd *SharedDomains) rebuildCommitment(ctx context.Context, roTx kv.Tx, blockNum uint64) ([]byte, error) {
	it, err := sd.aggTx.HistoryRange(kv.StorageDomain, int(sd.TxNum()), math.MaxInt64, order.Asc, -1, roTx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		sd.sdCtx.TouchKey(kv.AccountsDomain, string(k), nil)
	}

	it, err = sd.aggTx.HistoryRange(kv.StorageDomain, int(sd.TxNum()), math.MaxInt64, order.Asc, -1, roTx)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		sd.sdCtx.TouchKey(kv.StorageDomain, string(k), nil)
	}

	sd.sdCtx.Reset()
	return sd.ComputeCommitment(ctx, true, blockNum, "rebuild commit")
}

// DiscardWrites disables updates collection for further flushing into db.
// Instead, it keeps them temporarily available until .ClearRam/.Close will make them unavailable.
func (sd *SharedDomains) DiscardWrites(d kv.Domain) {
	if d >= kv.DomainLen {
		return
	}
	sd.domainWriters[d].discard = true
	sd.domainWriters[d].h.discard = true
}

func (sd *SharedDomains) RebuildCommitmentShard(ctx context.Context, next func() (bool, []byte), cfg *RebuiltCommitment) (*RebuiltCommitment, error) {
	sd.DiscardWrites(kv.AccountsDomain)
	sd.DiscardWrites(kv.StorageDomain)
	sd.DiscardWrites(kv.CodeDomain)

	visComFiles := sd.aggTx.d[kv.CommitmentDomain].Files()
	sd.logger.Info("starting commitment", "shard", fmt.Sprintf("%d-%d", cfg.StepFrom, cfg.StepTo),
		"totalKeys", common.PrettyCounter(cfg.Keys), "block", sd.BlockNum(),
		"commitment files before dump step", cfg.StepTo,
		"files", fmt.Sprintf("%d %v", len(visComFiles), visComFiles))

	sf := time.Now()
	var processed uint64
	for ok, key := next(); ; ok, key = next() {
		sd.sdCtx.TouchKey(kv.AccountsDomain, string(key), nil)
		processed++
		if !ok {
			break
		}
	}
	collectionSpent := time.Since(sf)
	rh, err := sd.sdCtx.ComputeCommitment(ctx, true, sd.BlockNum(), fmt.Sprintf("%d-%d", cfg.StepFrom, cfg.StepTo))
	if err != nil {
		return nil, err
	}
	sd.logger.Info("sealing", "shard", fmt.Sprintf("%d-%d", cfg.StepFrom, cfg.StepTo),
		"root", hex.EncodeToString(rh), "commitment", time.Since(sf).String(),
		"collection", collectionSpent.String())

	sb := time.Now()

	// rng := MergeRange{from: cfg.TxnFrom, to: cfg.TxnTo}
	// vt, err := sd.aggTx.d[kv.CommitmentDomain].commitmentValTransformDomain(rng, sd.aggTx.d[kv.AccountsDomain], sd.aggTx.d[kv.StorageDomain], nil, nil)
	// if err != nil {
	// 	return nil, err
	// }
	err = sd.aggTx.d[kv.CommitmentDomain].d.DumpStepRangeOnDisk(ctx, cfg.StepFrom, cfg.StepTo, cfg.TxnFrom, cfg.TxnTo, sd.domainWriters[kv.CommitmentDomain], nil)
	if err != nil {
		return nil, err
	}

	sd.logger.Info("shard built", "shard", fmt.Sprintf("%d-%d", cfg.StepFrom, cfg.StepTo), "root", hex.EncodeToString(rh), "ETA", time.Since(sf).String(), "file dump", time.Since(sb).String())

	return &RebuiltCommitment{
		RootHash: rh,
		StepFrom: cfg.StepFrom,
		StepTo:   cfg.StepTo,
		TxnFrom:  cfg.TxnFrom,
		TxnTo:    cfg.TxnTo,
		Keys:     processed,
	}, nil
}

type RebuiltCommitment struct {
	RootHash []byte
	StepFrom uint64
	StepTo   uint64
	TxnFrom  uint64
	TxnTo    uint64
	Keys     uint64
}

// SeekCommitment lookups latest available commitment and sets it as current
func (sd *SharedDomains) SeekCommitment(ctx context.Context, tx kv.Tx) (txsFromBlockBeginning uint64, err error) {
	bn, txn, ok, err := sd.sdCtx.SeekCommitment(tx, sd.aggTx.d[kv.CommitmentDomain], 0, math.MaxUint64)
	if err != nil {
		return 0, err
	}
	if dbg.DiscardCommitment() && bn == 0 {
		txn = sd.aggTx.EndTxNumNoCommitment() - 1
		sd.SetBlockNum(bn)
		sd.SetTxNum(txn)
		return 0, nil
	}
	if ok {
		if bn > 0 {
			lastBn, _, err := rawdbv3.TxNums.Last(tx)
			if err != nil {
				return 0, err
			}
			if lastBn < bn {
				return 0, errors.WithMessage(ErrBehindCommitment, fmt.Sprintf("TxNums index is at block %d and behind commitment %d", lastBn, bn))
			}
		}
		sd.SetBlockNum(bn)
		sd.SetTxNum(txn)
		return 0, nil
	}
	// handle case when we have no commitment, but have executed blocks
	bnBytes, err := tx.GetOne(kv.SyncStageProgress, []byte("Execution")) //TODO: move stages to erigon-lib
	if err != nil {
		return 0, err
	}
	if len(bnBytes) == 8 {
		bn = binary.BigEndian.Uint64(bnBytes)
		txn, err = rawdbv3.TxNums.Max(tx, bn)
		if err != nil {
			return 0, err
		}
	}
	if bn == 0 && txn == 0 {
		sd.SetBlockNum(0)
		sd.SetTxNum(0)
		return 0, nil
	}
	sd.SetBlockNum(bn)
	sd.SetTxNum(txn)
	newRh, err := sd.rebuildCommitment(ctx, tx, bn)
	if err != nil {
		return 0, err
	}
	if bytes.Equal(newRh, commitment.EmptyRootHash) {
		sd.SetBlockNum(0)
		sd.SetTxNum(0)
		return 0, nil
	}
	if sd.trace {
		fmt.Printf("rebuilt commitment %x %d %d\n", newRh, sd.TxNum(), sd.BlockNum())
	}
	sd.SetBlockNum(bn)
	sd.SetTxNum(txn)
	return 0, nil
}

func (sd *SharedDomains) ClearRam(resetCommitment bool) {
	//sd.muMaps.Lock()
	//defer sd.muMaps.Unlock()
	for i := range sd.domains {
		sd.domains[i] = map[string]dataWithPrevStep{}
	}
	if resetCommitment {
		sd.sdCtx.updates.Reset()
		sd.sdCtx.Reset()
	}

	sd.storage = btree2.NewMap[string, dataWithPrevStep](128)
	sd.estSize = 0
}

func (sd *SharedDomains) ResetCommitment() {
	sd.sdCtx.updates.Reset()
}

func (sd *SharedDomains) SaveCommitment(blockNum uint64, rootHash []byte) error {
	return sd.sdCtx.storeCommitmentState(blockNum, rootHash)
}

func (sd *SharedDomains) put(domain kv.Domain, key string, val []byte) {
	// disable mutex - because work on parallel execution postponed after E3 release.
	//sd.muMaps.Lock()
	valWithPrevStep := dataWithPrevStep{data: val, prevStep: sd.txNum / sd.aggTx.a.StepSize()}
	if domain == kv.StorageDomain {
		if old, ok := sd.storage.Set(key, valWithPrevStep); ok {
			sd.estSize += len(val) - len(old.data)
		} else {
			sd.estSize += len(key) + len(val)
		}
		return
	}

	if old, ok := sd.domains[domain][key]; ok {
		sd.estSize += len(val) - len(old.data)
	} else {
		sd.estSize += len(key) + len(val)
	}
	sd.domains[domain][key] = valWithPrevStep
	//sd.muMaps.Unlock()
}

// get returns cached value by key. Cache is invalidated when associated WAL is flushed
func (sd *SharedDomains) get(table kv.Domain, key []byte) (v []byte, prevStep uint64, ok bool) {
	//sd.muMaps.RLock()
	keyS := toStringZeroCopy(key)
	var dataWithPrevStep dataWithPrevStep
	if table == kv.StorageDomain {
		dataWithPrevStep, ok = sd.storage.Get(keyS)
		return dataWithPrevStep.data, dataWithPrevStep.prevStep, ok

	}
	dataWithPrevStep, ok = sd.domains[table][keyS]
	return dataWithPrevStep.data, dataWithPrevStep.prevStep, ok
	//sd.muMaps.RUnlock()
}

func (sd *SharedDomains) SizeEstimate() uint64 {
	//sd.muMaps.RLock()
	//defer sd.muMaps.RUnlock()

	// multiply 2: to cover data-structures overhead (and keep accounting cheap)
	// and muliply 2 more: for Commitment calculation when batch is full
	return uint64(sd.estSize) * 4
}

func (sd *SharedDomains) LatestCommitment(prefix []byte) ([]byte, uint64, error) {
	if v, prevStep, ok := sd.get(kv.CommitmentDomain, prefix); ok {
		// sd cache values as is (without transformation) so safe to return
		return v, prevStep, nil
	}
	v, step, found, err := sd.aggTx.d[kv.CommitmentDomain].getLatestFromDB(prefix, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}
	if found {
		// db store values as is (without transformation) so safe to return
		return v, step, nil
	}

	// getFromFiles doesn't provide same semantics as getLatestFromDB - it returns start/end tx
	// of file where the value is stored (not exact step when kv has been set)
	v, _, startTx, endTx, err := sd.aggTx.d[kv.CommitmentDomain].getLatestFromFiles(prefix, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}

	if !sd.aggTx.a.commitmentValuesTransform || bytes.Equal(prefix, keyCommitmentState) {
		return v, endTx / sd.aggTx.a.StepSize(), nil
	}

	// replace shortened keys in the branch with full keys to allow HPH work seamlessly
	rv, err := sd.replaceShortenedKeysInBranch(prefix, commitment.BranchData(v), startTx, endTx)
	if err != nil {
		return nil, 0, err
	}
	return rv, endTx / sd.aggTx.a.StepSize(), nil
}

// replaceShortenedKeysInBranch replaces shortened keys in the branch with full keys
func (sd *SharedDomains) replaceShortenedKeysInBranch(prefix []byte, branch commitment.BranchData, fStartTxNum uint64, fEndTxNum uint64) (commitment.BranchData, error) {
	if !sd.aggTx.d[kv.CommitmentDomain].d.replaceKeysInValues && sd.aggTx.a.commitmentValuesTransform {
		panic("domain.replaceKeysInValues is disabled, but agg.commitmentValuesTransform is enabled")
	}

	if !sd.aggTx.a.commitmentValuesTransform ||
		len(branch) == 0 ||
		sd.aggTx.TxNumsInFiles(kv.StateDomains...) == 0 ||
		bytes.Equal(prefix, keyCommitmentState) ||
		((fEndTxNum-fStartTxNum)/sd.aggTx.a.StepSize())%2 != 0 { // this checks if file has even number of steps, singular files does not transform values.

		return branch, nil // do not transform, return as is
	}

	sto := sd.aggTx.d[kv.StorageDomain]
	acc := sd.aggTx.d[kv.AccountsDomain]
	storageItem, err := sto.rawLookupFileByRange(fStartTxNum, fEndTxNum)
	if err != nil {
		sd.logger.Crit("dereference key during commitment read", "failed", err.Error())
		return nil, err
	}
	accountItem, err := acc.rawLookupFileByRange(fStartTxNum, fEndTxNum)
	if err != nil {
		sd.logger.Crit("dereference key during commitment read", "failed", err.Error())
		return nil, err
	}
	storageGetter := seg.NewReader(storageItem.decompressor.MakeGetter(), sto.d.Compression)
	accountGetter := seg.NewReader(accountItem.decompressor.MakeGetter(), acc.d.Compression)
	metricI := 0
	for i, f := range sd.aggTx.d[kv.CommitmentDomain].files {
		if i > 5 {
			metricI = 5
			break
		}
		if f.startTxNum == fStartTxNum && f.endTxNum == fEndTxNum {
			metricI = i
		}
	}

	aux := make([]byte, 0, 256)
	return branch.ReplacePlainKeys(aux, func(key []byte, isStorage bool) ([]byte, error) {
		if isStorage {
			if len(key) == length.Addr+length.Hash {
				return nil, nil // save storage key as is
			}
			if dbg.KVReadLevelledMetrics {
				defer branchKeyDerefSpent[metricI].ObserveDuration(time.Now())
			}
			// Optimised key referencing a state file record (file number and offset within the file)
			storagePlainKey, found := sto.lookupByShortenedKey(key, storageGetter)
			if !found {
				s0, s1 := fStartTxNum/sd.aggTx.a.StepSize(), fEndTxNum/sd.aggTx.a.StepSize()
				sd.logger.Crit("replace back lost storage full key", "shortened", fmt.Sprintf("%x", key),
					"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, decodeShorterKey(key)))
				return nil, fmt.Errorf("replace back lost storage full key: %x", key)
			}
			return storagePlainKey, nil
		}

		if len(key) == length.Addr {
			return nil, nil // save account key as is
		}

		if dbg.KVReadLevelledMetrics {
			defer branchKeyDerefSpent[metricI].ObserveDuration(time.Now())
		}
		apkBuf, found := acc.lookupByShortenedKey(key, accountGetter)
		if !found {
			s0, s1 := fStartTxNum/sd.aggTx.a.StepSize(), fEndTxNum/sd.aggTx.a.StepSize()
			sd.logger.Crit("replace back lost account full key", "shortened", fmt.Sprintf("%x", key),
				"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, decodeShorterKey(key)))
			return nil, fmt.Errorf("replace back lost account full key: %x", key)
		}
		return apkBuf, nil
	})
}

const CodeSizeTableFake = "CodeSize"

func (sd *SharedDomains) ReadsValid(readLists map[string]*KvList) bool {
	//sd.muMaps.RLock()
	//defer sd.muMaps.RUnlock()

	for table, list := range readLists {
		switch table {
		case kv.AccountsDomain.String():
			m := sd.domains[kv.AccountsDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case kv.CodeDomain.String():
			m := sd.domains[kv.CodeDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case kv.StorageDomain.String():
			m := sd.storage
			for i, key := range list.Keys {
				if val, ok := m.Get(key); ok {
					if !bytes.Equal(list.Vals[i], val.data) {
						return false
					}
				}
			}
		case CodeSizeTableFake:
			m := sd.domains[kv.CodeDomain]
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if binary.BigEndian.Uint64(list.Vals[i]) != uint64(len(val.data)) {
						return false
					}
				}
			}
		default:
			panic(table)
		}
	}

	return true
}

func (sd *SharedDomains) updateAccountData(addr []byte, account, prevAccount []byte, prevStep uint64) error {
	addrS := string(addr)
	sd.sdCtx.TouchKey(kv.AccountsDomain, addrS, account)
	sd.put(kv.AccountsDomain, addrS, account)
	return sd.domainWriters[kv.AccountsDomain].PutWithPrev(addr, nil, account, prevAccount, prevStep)
}

func (sd *SharedDomains) updateAccountCode(addr, code, prevCode []byte, prevStep uint64) error {
	addrS := string(addr)
	sd.sdCtx.TouchKey(kv.CodeDomain, addrS, code)
	sd.put(kv.CodeDomain, addrS, code)
	if len(code) == 0 {
		return sd.domainWriters[kv.CodeDomain].DeleteWithPrev(addr, nil, prevCode, prevStep)
	}
	return sd.domainWriters[kv.CodeDomain].PutWithPrev(addr, nil, code, prevCode, prevStep)
}

func (sd *SharedDomains) updateCommitmentData(prefix string, data, prev []byte, prevStep uint64) error {
	sd.put(kv.CommitmentDomain, prefix, data)
	return sd.domainWriters[kv.CommitmentDomain].PutWithPrev(toBytesZeroCopy(prefix), nil, data, prev, prevStep)
}

func (sd *SharedDomains) deleteAccount(addr, prev []byte, prevStep uint64) error {
	addrS := string(addr)
	if err := sd.DomainDelPrefix(kv.StorageDomain, addr); err != nil {
		return err
	}

	// commitment delete already has been applied via account
	if err := sd.DomainDel(kv.CodeDomain, addr, nil, nil, prevStep); err != nil {
		return err
	}

	sd.sdCtx.TouchKey(kv.AccountsDomain, addrS, nil)
	sd.put(kv.AccountsDomain, addrS, nil)
	if err := sd.domainWriters[kv.AccountsDomain].DeleteWithPrev(addr, nil, prev, prevStep); err != nil {
		return err
	}

	return nil
}

func (sd *SharedDomains) writeAccountStorage(addr, loc []byte, value, preVal []byte, prevStep uint64) error {
	composite := addr
	if loc != nil { // if caller passed already `composite` key, then just use it. otherwise join parts
		composite = make([]byte, 0, len(addr)+len(loc))
		composite = append(append(composite, addr...), loc...)
	}
	compositeS := string(composite)
	sd.sdCtx.TouchKey(kv.StorageDomain, compositeS, value)
	sd.put(kv.StorageDomain, compositeS, value)
	return sd.domainWriters[kv.StorageDomain].PutWithPrev(composite, nil, value, preVal, prevStep)
}

func (sd *SharedDomains) delAccountStorage(addr, loc []byte, preVal []byte, prevStep uint64) error {
	composite := addr
	if loc != nil { // if caller passed already `composite` key, then just use it. otherwise join parts
		composite = make([]byte, 0, len(addr)+len(loc))
		composite = append(append(composite, addr...), loc...)
	}
	compositeS := string(composite)
	sd.sdCtx.TouchKey(kv.StorageDomain, compositeS, nil)
	sd.put(kv.StorageDomain, compositeS, nil)
	return sd.domainWriters[kv.StorageDomain].DeleteWithPrev(composite, nil, preVal, prevStep)
}

func (sd *SharedDomains) IndexAdd(table kv.InvertedIdx, key []byte) (err error) {
	for _, writer := range sd.iiWriters {
		if writer.name == table {
			return writer.Add(key)
		}
	}
	panic(fmt.Errorf("unknown index %s", table))
}

func (sd *SharedDomains) SetTx(tx kv.Tx) {
	if tx == nil {
		panic("tx is nil")
	}
	sd.roTx = tx

	casted, ok := tx.(HasAggTx)
	if !ok {
		panic(fmt.Errorf("type %T need AggTx method", tx))
	}

	sd.aggTx = casted.AggTx().(*AggregatorRoTx)
	if sd.aggTx == nil {
		panic(errors.New("aggtx is nil"))
	}
}

func (sd *SharedDomains) StepSize() uint64 { return sd.aggTx.a.StepSize() }

// SetTxNum sets txNum for all domains as well as common txNum for all domains
// Requires for sd.rwTx because of commitment evaluation in shared domains if aggregationStep is reached
func (sd *SharedDomains) SetTxNum(txNum uint64) {
	sd.txNum = txNum
	for _, d := range sd.domainWriters {
		if d != nil {
			d.SetTxNum(txNum)
		}
	}
	for _, iiWriter := range sd.iiWriters {
		if iiWriter != nil {
			iiWriter.SetTxNum(txNum)
		}
	}
}

func (sd *SharedDomains) TxNum() uint64 { return sd.txNum }

func (sd *SharedDomains) BlockNum() uint64 { return sd.blockNum.Load() }

func (sd *SharedDomains) SetBlockNum(blockNum uint64) {
	sd.blockNum.Store(blockNum)
}

func (sd *SharedDomains) SetTrace(b bool) {
	sd.trace = b
}

func (sd *SharedDomains) ComputeCommitment(ctx context.Context, saveStateAfter bool, blockNum uint64, logPrefix string) (rootHash []byte, err error) {
	rootHash, err = sd.sdCtx.ComputeCommitment(ctx, saveStateAfter, blockNum, logPrefix)
	return
}

// IterateStoragePrefix iterates over key-value pairs of the storage domain that start with given prefix
// Such iteration is not intended to be used in public API, therefore it uses read-write transaction
// inside the domain. Another version of this for public API use needs to be created, that uses
// roTx instead and supports ending the iterations before it reaches the end.
//
// k and v lifetime is bounded by the lifetime of the iterator
func (sd *SharedDomains) IterateStoragePrefix(prefix []byte, it func(k []byte, v []byte, step uint64) error) error {
	haveRamUpdates := sd.storage.Len() > 0
	return sd.aggTx.d[kv.StorageDomain].debugIteratePrefix(prefix, haveRamUpdates, sd.storage.Iter(), it, sd.txNum, sd.StepSize(), sd.roTx)
}

func (sd *SharedDomains) Close() {
	sd.SetBlockNum(0)
	if sd.aggTx != nil {
		sd.SetTxNum(0)

		//sd.walLock.Lock()
		//defer sd.walLock.Unlock()
		for _, d := range sd.domainWriters {
			d.close()
		}
		for _, iiWriter := range sd.iiWriters {
			iiWriter.close()
		}
	}

	if sd.sdCtx != nil {
		sd.sdCtx.Close()
	}
}

func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	for key, changeset := range sd.pastChangesAccumulator {
		blockNum := binary.BigEndian.Uint64(toBytesZeroCopy(key[:8]))
		blockHash := common.BytesToHash(toBytesZeroCopy(key[8:]))
		if err := WriteDiffSet(tx, blockNum, blockHash, changeset); err != nil {
			return err
		}
	}
	sd.pastChangesAccumulator = make(map[string]*StateChangeSet)

	defer mxFlushTook.ObserveDuration(time.Now())
	var err error
	if !dbg.DiscardCommitment() {
		fh, err := sd.ComputeCommitment(ctx, true, sd.BlockNum(), "flush-commitment")
		if err != nil {
			return err
		}
		if sd.trace {
			_, f, l, _ := runtime.Caller(1)
			fmt.Printf("[SD aggTx=%d] FLUSHING at tx %d [%x], caller %s:%d\n", sd.aggTx.id, sd.TxNum(), fh, filepath.Base(f), l)
		}
	}
	for di, w := range sd.domainWriters {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
		sd.aggTx.d[di].closeValsCursor()
	}
	for _, w := range sd.iiWriters {
		if w == nil {
			continue
		}
		if err := w.Flush(ctx, tx); err != nil {
			return err
		}
	}
	if dbg.PruneOnFlushTimeout != 0 {
		_, err = sd.aggTx.PruneSmallBatches(ctx, dbg.PruneOnFlushTimeout, tx)
		if err != nil {
			return err
		}
	}

	for _, w := range sd.domainWriters {
		if w == nil {
			continue
		}
		w.close()
	}
	for _, w := range sd.iiWriters {
		if w == nil {
			continue
		}
		w.close()
	}
	return nil
}

// TemporalDomain satisfaction
func (sd *SharedDomains) GetLatest(domain kv.Domain, k []byte) (v []byte, step uint64, err error) {
	if domain == kv.CommitmentDomain {
		return sd.LatestCommitment(k)
	}
	if v, prevStep, ok := sd.get(domain, k); ok {
		return v, prevStep, nil
	}
	v, step, _, err = sd.aggTx.GetLatest(domain, k, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}
	return v, step, nil
}

// GetAsOfFile returns value from domain with respect to limit ofMaxTxnum
func (sd *SharedDomains) getAsOfFile(domain kv.Domain, k, k2 []byte, ofMaxTxnum uint64) (v []byte, step uint64, err error) {
	if domain == kv.CommitmentDomain {
		return sd.LatestCommitment(k)
	}
	if k2 != nil {
		k = append(k, k2...)
	}

	v, ok, _, _, err := sd.aggTx.DebugGetLatestFromFiles(domain, k, ofMaxTxnum)
	if err != nil {
		return nil, 0, fmt.Errorf("domain '%s' %x txn=%d read error: %w", domain, k, ofMaxTxnum, err)
	}
	if !ok {
		return nil, 0, nil
	}
	return v, step, nil
}

// DomainPut
// Optimizations:
//   - user can provide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainPut(domain kv.Domain, k1, k2 []byte, val, prevVal []byte, prevStep uint64) error {
	if val == nil {
		return fmt.Errorf("DomainPut: %s, trying to put nil value. not allowed", domain)
	}
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, k1)
		if err != nil {
			return err
		}
	}

	switch domain {
	case kv.AccountsDomain:
		return sd.updateAccountData(k1, val, prevVal, prevStep)
	case kv.StorageDomain:
		return sd.writeAccountStorage(k1, k2, val, prevVal, prevStep)
	case kv.CodeDomain:
		if bytes.Equal(prevVal, val) {
			return nil
		}
		return sd.updateAccountCode(k1, val, prevVal, prevStep)
	case kv.CommitmentDomain, kv.RCacheDomain:
		sd.put(domain, toStringZeroCopy(append(k1, k2...)), val)
		return sd.domainWriters[domain].PutWithPrev(k1, k2, val, prevVal, prevStep)
	default:
		if bytes.Equal(prevVal, val) {
			return nil
		}
		sd.put(domain, toStringZeroCopy(append(k1, k2...)), val)
		return sd.domainWriters[domain].PutWithPrev(k1, k2, val, prevVal, prevStep)
	}
}

// DomainDel
// Optimizations:
//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainDel(domain kv.Domain, k1, k2 []byte, prevVal []byte, prevStep uint64) error {
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, k1)
		if err != nil {
			return err
		}
	}

	switch domain {
	case kv.AccountsDomain:
		return sd.deleteAccount(k1, prevVal, prevStep)
	case kv.StorageDomain:
		return sd.delAccountStorage(k1, k2, prevVal, prevStep)
	case kv.CodeDomain:
		if prevVal == nil {
			return nil
		}
		return sd.updateAccountCode(k1, nil, prevVal, prevStep)
	case kv.CommitmentDomain:
		return sd.updateCommitmentData(toStringZeroCopy(k1), nil, prevVal, prevStep)
	default:
		sd.put(domain, toStringZeroCopy(append(k1, k2...)), nil)
		return sd.domainWriters[domain].DeleteWithPrev(k1, k2, prevVal, prevStep)
	}
}

func (sd *SharedDomains) DomainDelPrefix(domain kv.Domain, prefix []byte) error {
	if domain != kv.StorageDomain {
		return errors.New("DomainDelPrefix: not supported")
	}

	type tuple struct {
		k, v []byte
		step uint64
	}
	tombs := make([]tuple, 0, 8)
	if err := sd.IterateStoragePrefix(prefix, func(k, v []byte, step uint64) error {
		tombs = append(tombs, tuple{k, v, step})
		return nil
	}); err != nil {
		return err
	}
	for _, tomb := range tombs {
		if err := sd.DomainDel(kv.StorageDomain, tomb.k, nil, tomb.v, tomb.step); err != nil {
			return err
		}
	}

	if assert.Enable {
		forgotten := 0
		if err := sd.IterateStoragePrefix(prefix, func(k, v []byte, step uint64) error {
			forgotten++
			return nil
		}); err != nil {
			return err
		}
		if forgotten > 0 {
			panic(fmt.Errorf("DomainDelPrefix: %d forgotten keys after '%x' prefix removal", forgotten, prefix))
		}
	}
	return nil
}
func (sd *SharedDomains) Tx() kv.Tx { return sd.roTx }

type SharedDomainsCommitmentContext struct {
	sharedDomains *SharedDomains
	keccak        cryptozerocopy.KeccakState
	updates       *commitment.Updates
	patriciaTrie  commitment.Trie
	justRestored  atomic.Bool

	limitReadAsOfTxNum uint64
	domainsOnly        bool // if true, do not use history reader and limit to domain files only
}

// Limits max txNum for read operations. If set to 0, all read operations return latest value.
// If domainOnly=true and txNum > 0, then read operations will be limited to domain files only.
// If domainOnly=false and txNum > 0, then read operations will be limited to history files only.
func (sdc *SharedDomainsCommitmentContext) SetLimitReadAsOfTxNum(txNum uint64, domainOnly bool) {
	sdc.limitReadAsOfTxNum = txNum
	sdc.domainsOnly = domainOnly
}

func NewSharedDomainsCommitmentContext(sd *SharedDomains, mode commitment.Mode, trieVariant commitment.TrieVariant) *SharedDomainsCommitmentContext {
	ctx := &SharedDomainsCommitmentContext{
		sharedDomains: sd,
		keccak:        sha3.NewLegacyKeccak256().(cryptozerocopy.KeccakState),
	}

	ctx.patriciaTrie, ctx.updates = commitment.InitializeTrieAndUpdates(trieVariant, mode, sd.aggTx.a.tmpdir)
	ctx.patriciaTrie.ResetContext(ctx)
	return ctx
}

func (sdc *SharedDomainsCommitmentContext) Close() {
	sdc.updates.Close()
}

func (sdc *SharedDomainsCommitmentContext) Branch(pref []byte) ([]byte, uint64, error) {
	if !sdc.domainsOnly && sdc.limitReadAsOfTxNum > 0 {
		branch, _, err := sdc.sharedDomains.aggTx.GetAsOf(sdc.sharedDomains.roTx, kv.CommitmentDomain, pref, sdc.limitReadAsOfTxNum)
		if sdc.sharedDomains.trace {
			fmt.Printf("[SDC] Branch @%d: %x: %x\n%s\n", sdc.limitReadAsOfTxNum, pref, branch, commitment.BranchData(branch).String())
		}
		if err != nil {
			return nil, 0, fmt.Errorf("branch history read failed: %w", err)
		}
		return branch, sdc.limitReadAsOfTxNum / sdc.sharedDomains.StepSize(), nil
	}

	// Trie reads prefix during unfold and after everything is ready reads it again to Merge update.
	// Dereferenced branch is kept inside sharedDomains commitment domain map (but not written into buffer so not flushed into db, unless updated)
	v, step, err := sdc.sharedDomains.LatestCommitment(pref)
	if err != nil {
		return nil, 0, fmt.Errorf("branch failed: %w", err)
	}
	if sdc.sharedDomains.trace {
		fmt.Printf("[SDC] Branch: %x: %x\n", pref, v)
	}
	if len(v) == 0 {
		return nil, 0, nil
	}
	return v, step, nil
}

func (sdc *SharedDomainsCommitmentContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep uint64) error {
	if sdc.limitReadAsOfTxNum > 0 && !sdc.domainsOnly { // do not store branches if explicitly operate on history
		return nil
	}
	prefixS := toStringZeroCopy(prefix)
	if sdc.sharedDomains.trace {
		fmt.Printf("[SDC] PutBranch: %x: %x\n", prefix, data)
	}
	return sdc.sharedDomains.updateCommitmentData(prefixS, data, prevData, prevStep)
}

func (sdc *SharedDomainsCommitmentContext) readAccount(plainKey []byte) (encAccount []byte, err error) {
	if sdc.limitReadAsOfTxNum > 0 { // read not from latest
		if sdc.domainsOnly { // read from previous files
			encAccount, _, err = sdc.sharedDomains.getAsOfFile(kv.AccountsDomain, plainKey, nil, sdc.limitReadAsOfTxNum)
		} else { // read from history
			encAccount, _, err = sdc.sharedDomains.aggTx.GetAsOf(sdc.sharedDomains.roTx, kv.AccountsDomain, plainKey, sdc.limitReadAsOfTxNum)
		}
	} else { // read latest value from domain
		encAccount, _, err = sdc.sharedDomains.GetLatest(kv.AccountsDomain, plainKey)
	}
	if err != nil {
		return nil, fmt.Errorf("GetAccount failed (latest=%t): %w", sdc.limitReadAsOfTxNum == 0, err)
	}
	return encAccount, nil
}

// func (sdc *SharedDomainsCommitmentContext) readCode(plainKey []byte) (code []byte, err error) {
// 	if sdc.limitReadAsOfTxNum > 0 {
// 		if sdc.domainsOnly { // read from domain file
// 			code, _, err = sdc.sharedDomains.getAsOfFile(kv.CodeDomain, plainKey, nil, sdc.limitReadAsOfTxNum)
// 		} else { // read from history
// 			code, _, err = sdc.sharedDomains.aggTx.GetAsOf(sdc.sharedDomains.roTx, kv.CodeDomain, plainKey, sdc.limitReadAsOfTxNum)
// 		}
// 	} else { // read latest value
// 		code, _, err = sdc.sharedDomains.GetLatest(kv.CodeDomain, plainKey)
// 	}

// 	if err != nil {
// 		return nil, fmt.Errorf("GetAccount/Code: failed to read code (latest=%t): %w", sdc.limitReadAsOfTxNum == 0, err)
// 	}
// 	return code, nil
// }

func (sdc *SharedDomainsCommitmentContext) readStorage(plainKey []byte) (enc []byte, err error) {
	if sdc.limitReadAsOfTxNum > 0 {
		if sdc.domainsOnly { // read from domain file
			enc, _, err = sdc.sharedDomains.getAsOfFile(kv.StorageDomain, plainKey, nil, sdc.limitReadAsOfTxNum)
		} else { // read from history
			enc, _, err = sdc.sharedDomains.aggTx.GetAsOf(sdc.sharedDomains.roTx, kv.StorageDomain, plainKey, sdc.limitReadAsOfTxNum)
		}
	} else { // get latest value
		enc, _, err = sdc.sharedDomains.GetLatest(kv.StorageDomain, plainKey)
	}

	if err != nil {
		return nil, fmt.Errorf("GetStorage: failed to read latest storage (latest=%t): %w", sdc.limitReadAsOfTxNum == 0, err)
	}
	return enc, nil
}

func (sdc *SharedDomainsCommitmentContext) Account(plainKey []byte) (u *commitment.Update, err error) {
	encAccount, err := sdc.readAccount(plainKey)
	if err != nil {
		return nil, err
	}

	u = &commitment.Update{CodeHash: commitment.EmptyCodeHashArray}
	if len(encAccount) == 0 {
		u.Flags = commitment.DeleteUpdate
		return u, nil
	}

	acc := new(accounts.Account)
	if err = accounts.DeserialiseV3(acc, encAccount); err != nil {
		return nil, err
	}

	u.Flags |= commitment.NonceUpdate
	u.Nonce = acc.Nonce

	u.Flags |= commitment.BalanceUpdate
	u.Balance.Set(&acc.Balance)

	if ch := acc.CodeHash.Bytes(); len(ch) > 0 { // if code hash is not empty
		u.Flags |= commitment.CodeUpdate
		copy(u.CodeHash[:], ch)
	}
	// if u.CodeHash != commitment.EmptyCodeHashArray {
	// 	// todo do we really need to read code and then hash it again once we have hash in account?)))

	// 	code, err := sdc.readCode(plainKey)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	if len(code) > 0 {
	// 		copy(u.CodeHash[:], crypto.Keccak256(code))
	// 		u.Flags |= commitment.CodeUpdate
	// 	} else {
	// 		u.CodeHash = commitment.EmptyCodeHashArray
	// 	}
	// }
	return u, nil
}

func (sdc *SharedDomainsCommitmentContext) Storage(plainKey []byte) (u *commitment.Update, err error) {
	enc, err := sdc.readStorage(plainKey)
	if err != nil {
		return nil, err
	}
	u = &commitment.Update{
		Flags:      commitment.DeleteUpdate,
		StorageLen: len(enc),
	}

	if u.StorageLen > 0 {
		u.Flags = commitment.StorageUpdate
		copy(u.Storage[:u.StorageLen], enc)
	}
	return u, nil
}

func (sdc *SharedDomainsCommitmentContext) Reset() {
	if !sdc.justRestored.Load() {
		sdc.patriciaTrie.Reset()
	}
}

func (sdc *SharedDomainsCommitmentContext) TempDir() string {
	return sdc.sharedDomains.aggTx.a.dirs.Tmp
}

func (sdc *SharedDomainsCommitmentContext) KeysCount() uint64 {
	return sdc.updates.Size()
}

func (sdc *SharedDomainsCommitmentContext) Trie() commitment.Trie {
	return sdc.patriciaTrie
}

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (sdc *SharedDomainsCommitmentContext) TouchKey(d kv.Domain, key string, val []byte) {
	if sdc.updates.Mode() == commitment.ModeDisabled {
		return
	}

	switch d {
	case kv.AccountsDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.TouchAccount)
	case kv.CodeDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.TouchCode)
	case kv.StorageDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.TouchStorage)
	default:
		panic(fmt.Errorf("TouchKey: unknown domain %s", d))
	}
}

func (sdc *SharedDomainsCommitmentContext) Witness(ctx context.Context, expectedRoot []byte, logPrefix string) (proofTrie *trie.Trie, rootHash []byte, err error) {
	hexPatriciaHashed, ok := sdc.Trie().(*commitment.HexPatriciaHashed)
	if ok {
		return hexPatriciaHashed.GenerateWitness(ctx, sdc.updates, nil, expectedRoot, logPrefix)
	}

	return nil, nil, errors.New("shared domains commitment context doesn't have HexPatriciaHashed")
}

// Evaluates commitment for processed state.
func (sdc *SharedDomainsCommitmentContext) ComputeCommitment(ctx context.Context, saveState bool, blockNum uint64, logPrefix string) (rootHash []byte, err error) {
	mxCommitmentRunning.Inc()
	defer mxCommitmentRunning.Dec()
	defer func(s time.Time) { mxCommitmentTook.ObserveDuration(s) }(time.Now())

	updateCount := sdc.updates.Size()
	if sdc.sharedDomains.trace {
		defer sdc.sharedDomains.logger.Trace("ComputeCommitment", "block", blockNum, "keys", updateCount, "mode", sdc.updates.Mode())
	}
	if updateCount == 0 {
		rootHash, err = sdc.patriciaTrie.RootHash()
		return rootHash, err
	}

	// data accessing functions should be set when domain is opened/shared context updated
	sdc.patriciaTrie.SetTrace(sdc.sharedDomains.trace)
	sdc.Reset()

	rootHash, err = sdc.patriciaTrie.Process(ctx, sdc.updates, logPrefix)
	if err != nil {
		return nil, err
	}
	sdc.justRestored.Store(false)

	if saveState {
		if err := sdc.storeCommitmentState(blockNum, rootHash); err != nil {
			return nil, err
		}
	}

	return rootHash, err
}

func (sdc *SharedDomainsCommitmentContext) storeCommitmentState(blockNum uint64, rootHash []byte) error {
	if sdc.sharedDomains.aggTx == nil {
		return fmt.Errorf("store commitment state: AggregatorContext is not initialized")
	}
	encodedState, err := sdc.encodeCommitmentState(blockNum, sdc.sharedDomains.txNum)
	if err != nil {
		return err
	}
	prevState, prevStep, err := sdc.Branch(keyCommitmentState)
	if err != nil {
		return err
	}
	if len(prevState) == 0 && prevState != nil {
		prevState = nil
	}
	// state could be equal but txnum/blocknum could be different.
	// We do skip only full matches
	if bytes.Equal(prevState, encodedState) {
		//fmt.Printf("[commitment] skip store txn %d block %d (prev b=%d t=%d) rh %x\n",
		//	binary.BigEndian.Uint64(prevState[8:16]), binary.BigEndian.Uint64(prevState[:8]), dc.ht.iit.txNum, blockNum, rh)
		return nil
	}
	if sdc.sharedDomains.trace {
		fmt.Printf("[commitment] store txn %d block %d rootHash %x\n", sdc.sharedDomains.txNum, blockNum, rootHash)
	}
	sdc.sharedDomains.put(kv.CommitmentDomain, keyCommitmentStateS, encodedState)
	return sdc.sharedDomains.domainWriters[kv.CommitmentDomain].PutWithPrev(keyCommitmentState, nil, encodedState, prevState, prevStep)
}

func (sdc *SharedDomainsCommitmentContext) encodeCommitmentState(blockNum, txNum uint64) ([]byte, error) {
	var state []byte
	var err error

	switch trie := (sdc.patriciaTrie).(type) {
	case *commitment.HexPatriciaHashed:
		state, err = trie.EncodeCurrentState(nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported state storing for patricia trie type: %T", sdc.patriciaTrie)
	}

	cs := &commitmentState{trieState: state, blockNum: blockNum, txNum: txNum}
	encoded, err := cs.Encode()
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// by that key stored latest root hash and tree state
const keyCommitmentStateS = "state"

var keyCommitmentState = []byte(keyCommitmentStateS)

func (sd *SharedDomains) LatestCommitmentState(tx kv.Tx, sinceTx, untilTx uint64) (blockNum, txNum uint64, state []byte, err error) {
	return sd.sdCtx.LatestCommitmentState()
}

func _decodeTxBlockNums(v []byte) (txNum, blockNum uint64) {
	return binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:16])
}

// LatestCommitmentState searches for last encoded state for CommitmentContext.
// Found value does not become current state.
func (sdc *SharedDomainsCommitmentContext) LatestCommitmentState() (blockNum, txNum uint64, state []byte, err error) {
	if sdc.patriciaTrie.Variant() != commitment.VariantHexPatriciaTrie {
		return 0, 0, nil, fmt.Errorf("state storing is only supported hex patricia trie")
	}
	state, _, err = sdc.Branch(keyCommitmentState)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(state) < 16 {
		return 0, 0, nil, nil
	}

	txNum, blockNum = _decodeTxBlockNums(state)
	return blockNum, txNum, state, nil
}

// SeekCommitment [sinceTx, untilTx] searches for last encoded state from DomainCommitted
// and if state found, sets it up to current domain
func (sdc *SharedDomainsCommitmentContext) SeekCommitment(tx kv.Tx, cd *DomainRoTx, sinceTx, untilTx uint64) (blockNum, txNum uint64, ok bool, err error) {
	_, _, state, err := sdc.LatestCommitmentState()
	if err != nil {
		return 0, 0, false, err
	}
	blockNum, txNum, err = sdc.restorePatriciaState(state)
	return blockNum, txNum, true, err
}

// After commitment state is retored, method .Reset() should NOT be called until new updates.
// Otherwise state should be restorePatriciaState()d again.

func (sdc *SharedDomainsCommitmentContext) restorePatriciaState(value []byte) (uint64, uint64, error) {
	cs := new(commitmentState)
	if err := cs.Decode(value); err != nil {
		if len(value) > 0 {
			return 0, 0, fmt.Errorf("failed to decode previous stored commitment state: %w", err)
		}
		// nil value is acceptable for SetState and will reset trie
	}
	if hext, ok := sdc.patriciaTrie.(*commitment.HexPatriciaHashed); ok {
		if err := hext.SetState(cs.trieState); err != nil {
			return 0, 0, fmt.Errorf("failed restore state : %w", err)
		}
		sdc.justRestored.Store(true) // to prevent double reset
		if sdc.sharedDomains.trace {
			rootHash, err := hext.RootHash()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to get root hash after state restore: %w", err)
			}
			fmt.Printf("[commitment] restored state: block=%d txn=%d rootHash=%x\n", cs.blockNum, cs.txNum, rootHash)
		}
	} else {
		return 0, 0, errors.New("state storing is only supported hex patricia trie")
	}
	return cs.blockNum, cs.txNum, nil
}

func toStringZeroCopy(v []byte) string { return unsafe.String(&v[0], len(v)) }
func toBytesZeroCopy(s string) []byte  { return unsafe.Slice(unsafe.StringData(s), len(s)) }

func AggTx(tx kv.Tx) *AggregatorRoTx {
	if withAggTx, ok := tx.(interface{ AggTx() any }); ok {
		return withAggTx.AggTx().(*AggregatorRoTx)
	}

	return nil
}
