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

package core

import (
	"fmt"
	"strconv"

	"github.com/erigontech/erigon-lib/chain/networkname"
	libcommon "github.com/erigontech/erigon-lib/common"
	_ "github.com/erigontech/erigon-lib/common/hexutility"
	_ "github.com/erigontech/erigon/polygon/bor/borcfg"

	"github.com/erigontech/erigon/core/systemcontracts"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

func init() {
	// Initialise SystemContractCodeLookup
	for _, chainName := range []string{networkname.BSCChainName, networkname.ChapelChainName, networkname.RialtoChainName, networkname.BorMainnetChainName, networkname.MumbaiChainName, networkname.BorDevnetChainName} {
		byChain := map[libcommon.Address][]libcommon.CodeRecord{}
		systemcontracts.SystemContractCodeLookup[chainName] = byChain
		// Apply genesis with the block number 0
		genesisBlock := GenesisBlockByChainName(chainName)
		allocToCodeRecords(genesisBlock.Alloc, byChain, 0, 0)
		// Process upgrades
		chainConfig := params.ChainConfigByChainName(chainName)
		parliaConfig := chainConfig.Parlia
		if parliaConfig == nil || parliaConfig.BlockAlloc == nil {
			return
		}
		for blockNumOrTime, genesisAlloc := range parliaConfig.BlockAlloc {
			numOrTime, err := strconv.ParseUint(blockNumOrTime, 10, 64)
			if err != nil {
				panic(fmt.Errorf("failed to parse block number in BlockAlloc: %s", err.Error()))
			}
			alloc, err := types.DecodeGenesisAlloc(genesisAlloc)
			if err != nil {
				panic(fmt.Errorf("failed to decode block alloc: %v", err))
			}
			var blockNum, blockTime uint64
			if numOrTime >= chainConfig.ShanghaiTime.Uint64() {
				blockTime = numOrTime
			} else {
				blockNum = numOrTime
			}
			allocToCodeRecords(alloc, byChain, blockNum, blockTime)
		}
	}

}

func allocToCodeRecords(alloc types.GenesisAlloc, byChain map[libcommon.Address][]libcommon.CodeRecord, blockNum, blockTime uint64) {
	for addr, account := range alloc {
		if len(account.Code) > 0 {
			list := byChain[addr]
			codeHash, err := libcommon.HashData(account.Code)
			if err != nil {
				panic(fmt.Errorf("failed to hash system contract code: %s", err.Error()))
			}
			if blockTime == 0 {
				list = append(list, libcommon.CodeRecord{BlockNumber: blockNum, CodeHash: codeHash})
			} else {
				list = append(list, libcommon.CodeRecord{BlockTime: blockTime, CodeHash: codeHash})
			}
			byChain[addr] = list
		}
	}
}
