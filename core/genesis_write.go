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

package core

import (
	"context"
	"crypto/ecdsa"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

//go:embed allocs
var allocs embed.FS

// CommitGenesisBlock writes or updates the genesis block in db.
// The block that will be used is:
//
//	                     genesis == nil       genesis != nil
//	                  +------------------------------------------
//	db has no genesis |  main-net          |  genesis
//	db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration is never nil.
func CommitGenesisBlock(db kv.RwDB, genesis *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	return CommitGenesisBlockWithOverride(db, genesis, nil, dirs, logger)
}

func CommitGenesisBlockWithOverride(db kv.RwDB, genesis *types.Genesis, overridePragueTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	c, b, err := WriteGenesisBlock(tx, genesis, overridePragueTime, dirs, logger)
	if err != nil {
		return c, b, err
	}
	err = tx.Commit()
	if err != nil {
		return c, b, err
	}
	return c, b, nil
}

func WriteGenesisBlock(tx kv.RwTx, genesis *types.Genesis, overridePragueTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	if err := rawdb.WriteGenesisIfNotExist(tx, genesis); err != nil {
		return nil, nil, err
	}

	var storedBlock *types.Block
	if genesis != nil && genesis.Config == nil {
		return params.AllProtocolChanges, nil, types.ErrGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	storedHash, storedErr := rawdb.ReadCanonicalHash(tx, 0)
	if storedErr != nil {
		return nil, nil, storedErr
	}

	applyOverrides := func(config *chain.Config) {
		if overridePragueTime != nil {
			config.PragueTime = overridePragueTime
		}
	}

	if (storedHash == libcommon.Hash{}) {
		custom := true
		if genesis == nil {
			logger.Info("Writing main-net genesis block")
			genesis = MainnetGenesisBlock()
			custom = false
		}
		applyOverrides(genesis.Config)
		block, _, err1 := write(tx, genesis, dirs, logger)
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		if custom {
			logger.Info("Writing custom genesis block", "hash", block.Hash().String())
		}
		return genesis.Config, block, nil
	}

	// Check whether the genesis block is already written.
	if genesis != nil {
		block, _, err1 := GenesisToBlock(genesis, dirs, logger)
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		hash := block.Hash()
		if hash != storedHash {
			return genesis.Config, block, &types.GenesisMismatchError{Stored: storedHash, New: hash}
		}
	}
	number := rawdb.ReadHeaderNumber(tx, storedHash)
	if number != nil {
		var err error
		storedBlock, _, err = rawdb.ReadBlockWithSenders(tx, storedHash, *number)
		if err != nil {
			return genesis.Config, nil, err
		}
	}
	// Get the existing chain configuration.
	newCfg := genesis.ConfigOrDefault(storedHash)
	applyOverrides(newCfg)
	if err := newCfg.CheckConfigForkOrder(); err != nil {
		return newCfg, nil, err
	}
	storedCfg, storedErr := rawdb.ReadChainConfig(tx, storedHash)
	if storedErr != nil && newCfg.Bor == nil {
		return newCfg, nil, storedErr
	}
	if storedCfg == nil {
		logger.Warn("Found genesis block without chain config")
		err1 := rawdb.WriteChainConfig(tx, storedHash, newCfg)
		if err1 != nil {
			return newCfg, nil, err1
		}
		return newCfg, storedBlock, nil
	}
	// Special case: don't change the existing config of a private chain if no new
	// config is supplied. This is useful, for example, to preserve DB config created by erigon init.
	// In that case, only apply the overrides.
	if genesis == nil && params.ChainConfigByGenesisHash(storedHash) == nil {
		newCfg = storedCfg
		applyOverrides(newCfg)
	}
	// Check config compatibility and write the config. Compatibility errors
	// are returned to the caller unless we're already at block zero.
	height := rawdb.ReadHeaderNumber(tx, rawdb.ReadHeadHeaderHash(tx))
	if height != nil {
		compatibilityErr := storedCfg.CheckCompatible(newCfg, *height)
		if compatibilityErr != nil && *height != 0 && compatibilityErr.RewindTo != 0 {
			return newCfg, storedBlock, compatibilityErr
		}
	}
	if err := rawdb.WriteChainConfig(tx, storedHash, newCfg); err != nil {
		return newCfg, nil, err
	}
	return newCfg, storedBlock, nil
}

func WriteGenesisState(g *types.Genesis, tx kv.RwTx, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err := GenesisToBlock(g, dirs, logger)
	if err != nil {
		return nil, nil, err
	}

	var stateWriter state.StateWriter
	stateWriter = state.NewNoopWriter()

	if block.Number().Sign() != 0 {
		return nil, statedb, errors.New("can't commit genesis block with number > 0")
	}
	if err := statedb.CommitBlock(&chain.Rules{}, stateWriter); err != nil {
		return nil, statedb, fmt.Errorf("cannot write state: %w", err)
	}

	return block, statedb, nil
}

func MustCommitGenesis(g *types.Genesis, db kv.RwDB, dirs datadir.Dirs, logger log.Logger) *types.Block {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	block, _, err := write(tx, g, dirs, logger)
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	return block
}

// Write writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func write(tx kv.RwTx, g *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err2 := WriteGenesisState(g, tx, dirs, logger)
	if err2 != nil {
		return block, statedb, err2
	}
	config := g.Config
	if config == nil {
		config = params.AllProtocolChanges
	}
	if err := config.CheckConfigForkOrder(); err != nil {
		return nil, nil, err
	}

	if err := rawdb.WriteBlock(tx, block); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteTd(tx, block.Hash(), block.NumberU64(), g.Difficulty); err != nil {
		return nil, nil, err
	}
	if err := rawdbv3.TxNums.Append(tx, 0, uint64(block.Transactions().Len()+1)); err != nil {
		return nil, nil, err
	}

	if err := rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64()); err != nil {
		return nil, nil, err
	}

	rawdb.WriteHeadBlockHash(tx, block.Hash())
	if err := rawdb.WriteHeadHeaderHash(tx, block.Hash()); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteChainConfig(tx, block.Hash(), config); err != nil {
		return nil, nil, err
	}

	return block, statedb, nil
}

// GenesisBlockForTesting creates and writes a block in which addr has the given wei balance.
func GenesisBlockForTesting(db kv.RwDB, addr libcommon.Address, balance *big.Int, dirs datadir.Dirs, logger log.Logger) *types.Block {
	g := types.Genesis{Alloc: types.GenesisAlloc{addr: {Balance: balance}}, Config: params.TestChainConfig}
	block := MustCommitGenesis(&g, db, dirs, logger)
	return block
}

type GenAccount struct {
	Addr    libcommon.Address
	Balance *big.Int
}

func GenesisWithAccounts(db kv.RwDB, accs []GenAccount, dirs datadir.Dirs, logger log.Logger) *types.Block {
	g := types.Genesis{Config: params.TestChainConfig}
	allocs := make(map[libcommon.Address]types.GenesisAccount)
	for _, acc := range accs {
		allocs[acc.Addr] = types.GenesisAccount{Balance: acc.Balance}
	}
	g.Alloc = allocs
	block := MustCommitGenesis(&g, db, dirs, logger)
	return block
}

// MainnetGenesisBlock returns the Ethereum main net genesis block.
func MainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.MainnetChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa"),
		GasLimit:   5000,
		Difficulty: big.NewInt(17179869184),
		Alloc:      readPrealloc("allocs/mainnet.json"),
	}
}

// HoleskyGenesisBlock returns the Holesky main net genesis block.
func HoleskyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HoleskyChainConfig,
		Nonce:      4660,
		GasLimit:   25000000,
		Difficulty: big.NewInt(1),
		Timestamp:  1695902100,
		Alloc:      readPrealloc("allocs/holesky.json"),
	}
}

// SepoliaGenesisBlock returns the Sepolia network genesis block.
func SepoliaGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.SepoliaChainConfig,
		Nonce:      0,
		ExtraData:  []byte("Sepolia, Athens, Attica, Greece!"),
		GasLimit:   30000000,
		Difficulty: big.NewInt(131072),
		Timestamp:  1633267481,
		Alloc:      readPrealloc("allocs/sepolia.json"),
	}
}

func BSCGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.BSCChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000002a7cdd959bfe8d9487b2a43b33565295a698f7e26488aa4d1955ee33403f8ccb1d4de5fb97c7ade29ef9f4360c606c7ab4db26b016007d3ad0ab86a0ee01c3b1283aa067c58eab4709f85e99d46de5fe685b1ded8013785d6623cc18d214320b6bb6475978f3adfc719c99674c072166708589033e2d9afec2be4ec20253b8642161bc3f444f53679c1f3d472f7be8361c80a4c1e7e9aaf001d0877f1cfde218ce2fd7544e0b2cc94692d4a704debef7bcb61328b8f7166496996a7da21cf1f1b04d9b3e26a3d0772d4c407bbe49438ed859fe965b140dcf1aab71a96bbad7cf34b5fa511d8e963dbba288b1960e75d64430b3230294d12c6ab2aac5c2cd68e80b16b581ea0a6e3c511bbd10f4519ece37dc24887e11b55d7ae2f5b9e386cd1b50a4550696d957cb4900f03a82012708dafc9e1b880fd083b32182b869be8e0922b81f8e175ffde54d797fe11eb03f9e3bf75f1d68bf0b8b6fb4e317a0f9d6f03eaf8ce6675bc60d8c4d90829ce8f72d0163c1d5cf348a862d55063035e7a025f4da968de7e4d7e4004197917f4070f1d6caa02bbebaebb5d7e581e4b66559e635f805ff0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      readPrealloc("allocs/bsc.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

func ChapelGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.ChapelChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001284214b9b9c85549ab3d2b972df0deef66ac2c9b71b214cb885500844365e95cd9942c7276e7fd8a2959d3f95eae5dc7d70144ce1b73b403b7eb6e0980a75ecd1309ea12fa2ed87a8744fbfc9b863d535552c16704d214347f29fa77f77da6d75d7c752f474cf03cceff28abc65c9cbae594f725c80e12d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      readPrealloc("allocs/chapel.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

func RialtoGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.RialtoChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001284214b9b9c85549ab3d2b972df0deef66ac2c9b71b214cb885500844365e95cd9942c7276e7fd8a2959d3f95eae5dc7d70144ce1b73b403b7eb6e0980a75ecd1309ea12fa2ed87a8744fbfc9b863d535552c16704d214347f29fa77f77da6d75d7c752f474cf03cceff28abc65c9cbae594f725c80e12d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      readPrealloc("allocs/bsc.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

// HoodiGenesisBlock returns the Hoodi network genesis block.
func HoodiGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.HoodiChainConfig,
		Nonce:      0x1234,
		ExtraData:  []byte(""),
		GasLimit:   0x2255100, // 36M
		Difficulty: big.NewInt(1),
		Timestamp:  1742212800,
		Alloc:      readPrealloc("allocs/hoodi.json"),
	}
}

// AmoyGenesisBlock returns the Amoy network genesis block.
func AmoyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.AmoyChainConfig,
		Nonce:      0,
		Timestamp:  1700225065,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/amoy.json"),
	}
}

// BorMainnetGenesisBlock returns the Bor Mainnet network genesis block.
func BorMainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.BorMainnetChainConfig,
		Nonce:      0,
		Timestamp:  1590824836,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_mainnet.json"),
	}
}

func BorDevnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.BorDevnetChainConfig,
		Nonce:      0,
		Timestamp:  1558348305,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_devnet.json"),
	}
}

func GnosisGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.GnosisChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, libcommon.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/gnosis.json"),
	}
}

func ChiadoGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.ChiadoChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, libcommon.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/chiado.json"),
	}
}
func TestGenesisBlock() *types.Genesis {
	return &types.Genesis{Config: params.TestChainConfig}
}

// Pre-calculated version of:
//
//	DevnetSignPrivateKey = crypto.HexToECDSA(sha256.Sum256([]byte("erigon devnet key")))
//	DevnetEtherbase=crypto.PubkeyToAddress(DevnetSignPrivateKey.PublicKey)
var DevnetSignPrivateKey, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")
var DevnetEtherbase = libcommon.HexToAddress("67b1d87101671b127f5f8714789c7192f7ad340e")

// DevnetSignKey is defined like this to allow the devnet process to pre-allocate keys
// for nodes and then pass the address via --miner.etherbase - the function will be called
// to retieve the mining key
var DevnetSignKey = func(address libcommon.Address) *ecdsa.PrivateKey {
	return DevnetSignPrivateKey
}

// DeveloperGenesisBlock returns the 'geth --dev' genesis block.
func DeveloperGenesisBlock(period uint64, faucet libcommon.Address) *types.Genesis {
	// Override the default period to the user requested one
	config := *params.AllCliqueProtocolChanges
	config.Clique.Period = period

	// Assemble and return the genesis with the precompiles and faucet pre-funded
	return &types.Genesis{
		Config:     &config,
		ExtraData:  append(append(make([]byte, 32), faucet[:]...), make([]byte, crypto.SignatureLength)...),
		GasLimit:   11500000,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/dev.json"),
	}
}

// ToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func GenesisToBlock(g *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	if dirs.SnapDomain == "" {
		panic("empty `dirs` variable")
	}
	_ = g.Alloc //nil-check

	head := &types.Header{
		Number:        new(big.Int).SetUint64(g.Number),
		Nonce:         types.EncodeNonce(g.Nonce),
		Time:          g.Timestamp,
		ParentHash:    g.ParentHash,
		Extra:         g.ExtraData,
		GasLimit:      g.GasLimit,
		GasUsed:       g.GasUsed,
		Difficulty:    g.Difficulty,
		MixDigest:     g.Mixhash,
		Coinbase:      g.Coinbase,
		BaseFee:       g.BaseFee,
		BlobGasUsed:   g.BlobGasUsed,
		ExcessBlobGas: g.ExcessBlobGas,
		RequestsHash:  g.RequestsHash,
	}
	if g.AuRaSeal != nil && len(g.AuRaSeal.AuthorityRound.Signature) > 0 {
		head.AuRaSeal = g.AuRaSeal.AuthorityRound.Signature
		head.AuRaStep = uint64(g.AuRaSeal.AuthorityRound.Step)
	}
	if g.GasLimit == 0 {
		head.GasLimit = params.GenesisGasLimit
	}
	if g.Difficulty == nil {
		head.Difficulty = params.GenesisDifficulty
	}
	if g.Config != nil && g.Config.IsLondon(0) {
		if g.BaseFee != nil {
			head.BaseFee = g.BaseFee
		} else {
			if g.Config.Parlia != nil {
				head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFeeForBSC)
			} else {
				head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
			}
		}
	}

	var withdrawals []*types.Withdrawal
	if g.Config != nil && g.Config.IsShanghai(g.Number, g.Timestamp) {
		withdrawals = []*types.Withdrawal{}
	}

	if g.Config != nil && g.Config.IsCancun(g.Number, g.Timestamp) {
		if g.BlobGasUsed != nil {
			head.BlobGasUsed = g.BlobGasUsed
		} else {
			head.BlobGasUsed = new(uint64)
		}
		if g.ExcessBlobGas != nil {
			head.ExcessBlobGas = g.ExcessBlobGas
		} else {
			head.ExcessBlobGas = new(uint64)
		}
		if g.ParentBeaconBlockRoot != nil && !g.Config.IsBohr(g.Number, g.Timestamp) {
			head.ParentBeaconBlockRoot = g.ParentBeaconBlockRoot
		} else {
			head.ParentBeaconBlockRoot = &libcommon.Hash{}
		}
	}

	if g.Config != nil && g.Config.IsPrague(g.Timestamp) {
		if g.RequestsHash != nil {
			head.RequestsHash = g.RequestsHash
		} else {
			head.RequestsHash = &types.EmptyRequestsHash
		}
	}

	var root libcommon.Hash
	var statedb *state.IntraBlockState // reader behind this statedb is dead at the moment of return, tx is rolled back

	ctx := context.Background()
	wg, ctx := errgroup.WithContext(ctx)
	// we may run inside write tx, can't open 2nd write tx in same goroutine
	wg.Go(func() error {
		// some users creaing > 1Gb custome genesis by `erigon init`
		genesisTmpDB := mdbx.New(kv.TemporaryDB, logger).InMem(dirs.DataDir).MapSize(2 * datasize.GB).GrowthStep(1 * datasize.MB).MustOpen()
		defer genesisTmpDB.Close()

		agg, err := state2.NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, genesisTmpDB, logger)
		if err != nil {
			return err
		}
		defer agg.Close()

		tdb := temporal.New(genesisTmpDB, agg)
		defer tdb.Close()

		tx, err := tdb.BeginTemporalRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		sd, err := state2.NewSharedDomains(tx, logger)
		if err != nil {
			return err
		}
		defer sd.Close()

		//r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)
		r, w := state.NewReaderV3(sd), state.NewWriterV4(sd)
		statedb = state.New(r)
		statedb.SetTrace(false)

		hasConstructorAllocation := false
		for _, account := range g.Alloc {
			if len(account.Constructor) > 0 {
				hasConstructorAllocation = true
				break
			}
		}
		// See https://github.com/NethermindEth/nethermind/blob/master/src/Nethermind/Nethermind.Consensus.AuRa/InitializationSteps/LoadGenesisBlockAuRa.cs
		if hasConstructorAllocation && g.Config.Aura != nil {
			statedb.CreateAccount(libcommon.Address{}, false)
		}

		keys := sortedAllocKeys(g.Alloc)
		for _, key := range keys {
			addr := libcommon.BytesToAddress([]byte(key))
			account := g.Alloc[addr]

			balance, overflow := uint256.FromBig(account.Balance)
			if overflow {
				panic("overflow at genesis allocs")
			}
			statedb.AddBalance(addr, balance, tracing.BalanceIncreaseGenesisBalance)
			statedb.SetCode(addr, account.Code)
			statedb.SetNonce(addr, account.Nonce)
			for key, value := range account.Storage {
				key := key
				val := uint256.NewInt(0).SetBytes(value.Bytes())
				statedb.SetState(addr, &key, *val)
			}

			if len(account.Constructor) > 0 {
				if _, err = SysCreate(addr, account.Constructor, g.Config, statedb, head); err != nil {
					return err
				}
			}

			if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
				statedb.SetIncarnation(addr, state.FirstContractIncarnation)
			}
		}
		if err = statedb.FinalizeTx(&chain.Rules{}, w); err != nil {
			return err
		}

		rh, err := sd.ComputeCommitment(context.Background(), true, 0, "genesis")
		if err != nil {
			return err
		}
		root = libcommon.BytesToHash(rh)
		return nil
	})

	if err := wg.Wait(); err != nil {
		return nil, nil, err
	}

	head.Root = root

	return types.NewBlock(head, nil, nil, nil, withdrawals), statedb, nil
}

func sortedAllocKeys(m types.GenesisAlloc) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = string(k.Bytes())
		i++
	}
	slices.Sort(keys)
	return keys
}

func readPrealloc(filename string) types.GenesisAlloc {
	f, err := allocs.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Could not open genesis preallocation for %s: %v", filename, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	ga := make(types.GenesisAlloc)
	err = decoder.Decode(&ga)
	if err != nil {
		panic(fmt.Sprintf("Could not parse genesis preallocation for %s: %v", filename, err))
	}
	return ga
}

func GenesisBlockByChainName(chain string) *types.Genesis {
	switch chain {
	case networkname.Mainnet:
		return MainnetGenesisBlock()
	case networkname.Holesky:
		return HoleskyGenesisBlock()
	case networkname.Sepolia:
		return SepoliaGenesisBlock()
	case networkname.BSC:
		return BSCGenesisBlock()
	case networkname.Chapel:
		return ChapelGenesisBlock()
	case networkname.Rialto:
		return RialtoGenesisBlock()
	case networkname.Hoodi:
		return HoodiGenesisBlock()
	case networkname.Amoy:
		return AmoyGenesisBlock()
	case networkname.BorMainnet:
		return BorMainnetGenesisBlock()
	case networkname.BorDevnet:
		return BorDevnetGenesisBlock()
	case networkname.Gnosis:
		return GnosisGenesisBlock()
	case networkname.Chiado:
		return ChiadoGenesisBlock()
	case networkname.Test:
		return TestGenesisBlock()
	default:
		return nil
	}
}
