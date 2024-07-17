package core

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func init() {
	// Initialise systemContractCodeLookup
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
		blockAlloc, err := parliaConfig.SortedBlockAlloc()
		if err != nil {
			panic(fmt.Errorf("failed to sort block alloc: %v", err))
		}

		for _, pair := range blockAlloc {
			alloc, err := types.DecodeGenesisAlloc(pair.BlockAlloc)
			if err != nil {
				panic(fmt.Errorf("failed to decode block alloc: %v", err))
			}
			var blockNum, blockTime uint64
			if pair.NumOrTime >= chainConfig.ShanghaiTime.Uint64() {
				blockTime = pair.NumOrTime
			} else {
				blockNum = pair.NumOrTime
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
