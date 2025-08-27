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

package chainspec

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"math/big"

	"github.com/jinzhu/copier"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

//go:embed allocs
var allocs embed.FS

func ReadPrealloc(fileSys fs.FS, filename string) types.GenesisAlloc {
	f, err := fileSys.Open(filename)
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

// MainnetGenesisBlock returns the Ethereum main net genesis block.
func MainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     MainnetChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa"),
		GasLimit:   5000,
		Difficulty: big.NewInt(17179869184),
		Alloc:      ReadPrealloc(allocs, "allocs/mainnet.json"),
	}
}

// HoleskyGenesisBlock returns the Holesky main net genesis block.
func HoleskyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     HoleskyChainConfig,
		Nonce:      4660,
		GasLimit:   25000000,
		Difficulty: big.NewInt(1),
		Timestamp:  1695902100,
		Alloc:      ReadPrealloc(allocs, "allocs/holesky.json"),
	}
}

// SepoliaGenesisBlock returns the Sepolia network genesis block.
func SepoliaGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     SepoliaChainConfig,
		Nonce:      0,
		ExtraData:  []byte("Sepolia, Athens, Attica, Greece!"),
		GasLimit:   30000000,
		Difficulty: big.NewInt(131072),
		Timestamp:  1633267481,
		Alloc:      ReadPrealloc(allocs, "allocs/sepolia.json"),
	}
}

// HoodiGenesisBlock returns the Hoodi network genesis block.
func HoodiGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     HoodiChainConfig,
		Nonce:      0x1234,
		ExtraData:  []byte(""),
		GasLimit:   0x2255100, // 36M
		Difficulty: big.NewInt(1),
		Timestamp:  1742212800,
		Alloc:      ReadPrealloc(allocs, "allocs/hoodi.json"),
	}
}

func GnosisGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     GnosisChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      ReadPrealloc(allocs, "allocs/gnosis.json"),
	}
}

func ChiadoGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     ChiadoChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      ReadPrealloc(allocs, "allocs/chiado.json"),
	}
}

func TestGenesisBlock() *types.Genesis {
	return &types.Genesis{Config: chain.TestChainConfig}
}

// DeveloperGenesisBlock returns the 'geth --dev' genesis block.
func DeveloperGenesisBlock(period uint64, faucet common.Address) *types.Genesis {
	// Override the default period to the user requested one
	var config chain.Config
	copier.Copy(&config, AllCliqueProtocolChanges)
	config.Clique.Period = period

	// Assemble and return the genesis with the precompiles and faucet pre-funded
	return &types.Genesis{
		Config:     &config,
		ExtraData:  append(append(make([]byte, 32), faucet[:]...), make([]byte, crypto.SignatureLength)...),
		GasLimit:   11500000,
		Difficulty: big.NewInt(1),
		Alloc:      ReadPrealloc(allocs, "allocs/dev.json"),
	}
}

func BSCGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     BSCChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000002a7cdd959bfe8d9487b2a43b33565295a698f7e26488aa4d1955ee33403f8ccb1d4de5fb97c7ade29ef9f4360c606c7ab4db26b016007d3ad0ab86a0ee01c3b1283aa067c58eab4709f85e99d46de5fe685b1ded8013785d6623cc18d214320b6bb6475978f3adfc719c99674c072166708589033e2d9afec2be4ec20253b8642161bc3f444f53679c1f3d472f7be8361c80a4c1e7e9aaf001d0877f1cfde218ce2fd7544e0b2cc94692d4a704debef7bcb61328b8f7166496996a7da21cf1f1b04d9b3e26a3d0772d4c407bbe49438ed859fe965b140dcf1aab71a96bbad7cf34b5fa511d8e963dbba288b1960e75d64430b3230294d12c6ab2aac5c2cd68e80b16b581ea0a6e3c511bbd10f4519ece37dc24887e11b55d7ae2f5b9e386cd1b50a4550696d957cb4900f03a82012708dafc9e1b880fd083b32182b869be8e0922b81f8e175ffde54d797fe11eb03f9e3bf75f1d68bf0b8b6fb4e317a0f9d6f03eaf8ce6675bc60d8c4d90829ce8f72d0163c1d5cf348a862d55063035e7a025f4da968de7e4d7e4004197917f4070f1d6caa02bbebaebb5d7e581e4b66559e635f805ff0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      ReadPrealloc(allocs, "allocs/bsc.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

func ChapelGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     ChapelChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001284214b9b9c85549ab3d2b972df0deef66ac2c9b71b214cb885500844365e95cd9942c7276e7fd8a2959d3f95eae5dc7d70144ce1b73b403b7eb6e0980a75ecd1309ea12fa2ed87a8744fbfc9b863d535552c16704d214347f29fa77f77da6d75d7c752f474cf03cceff28abc65c9cbae594f725c80e12d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      ReadPrealloc(allocs, "allocs/chapel.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

func RialtoGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     RialtoChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001284214b9b9c85549ab3d2b972df0deef66ac2c9b71b214cb885500844365e95cd9942c7276e7fd8a2959d3f95eae5dc7d70144ce1b73b403b7eb6e0980a75ecd1309ea12fa2ed87a8744fbfc9b863d535552c16704d214347f29fa77f77da6d75d7c752f474cf03cceff28abc65c9cbae594f725c80e12d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      ReadPrealloc(allocs, "allocs/bsc.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

var genesisBlockByChainName = make(map[string]*types.Genesis)

func GenesisBlockByChainName(chain string) *types.Genesis {
	return genesisBlockByChainName[chain]
}
