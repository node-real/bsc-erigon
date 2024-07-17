package systemcontracts

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type UpgradeConfig struct {
	BeforeUpgrade upgradeHook
	AfterUpgrade  upgradeHook
	ContractAddr  libcommon.Address
	CommitUrl     string
	Code          string
}

type Upgrade struct {
	UpgradeName string
	Configs     []*UpgradeConfig
}

type upgradeHook func(blockNumber *big.Int, contractAddr libcommon.Address, statedb *state.IntraBlockState) error

var (
	// SystemContractCodeLookup is used to address a flaw in the upgrade logic of the system contracts. Since they are updated directly, without first being self-destructed
	// and then re-created, the usual incarnation logic does not get activated, and all historical records of the code of these contracts are retrieved as the most
	// recent version. This problem will not exist in erigon3, but until then, a workaround will be used to access code of such contracts through this structure
	// Lookup is performed first by chain name, then by contract address. The value in the map is the list of CodeRecords, with increasing block numbers,
	// to be used in binary search to determine correct historical code
	SystemContractCodeLookup = map[string]map[libcommon.Address][]libcommon.CodeRecord{}
)

func UpgradeBuildInSystemContract(config *chain.Config, blockNumber *big.Int, lastBlockTime uint64, blockTime uint64, state *state.IntraBlockState, logger log.Logger) {
	if config == nil || blockNumber == nil || state == nil {
		return
	}

	if config.Parlia == nil || config.Parlia.BlockAlloc == nil {
		return
	}

	for blockNumberOrTime, genesisAlloc := range config.Parlia.BlockAlloc {
		numOrTime, err := strconv.ParseUint(blockNumberOrTime, 10, 64)
		if err != nil {
			panic(fmt.Errorf("failed to parse block number in BlockAlloc: %s", err.Error()))
		}
		if numOrTime == blockNumber.Uint64() || (lastBlockTime < numOrTime && blockTime >= numOrTime) {
			allocs, err := types.DecodeGenesisAlloc(genesisAlloc)
			if err != nil {
				panic(fmt.Errorf("failed to decode genesis alloc: %v", err))
			}
			for addr, account := range allocs {
				logger.Debug("[parlia] upgrade System Contract code", "blockNumber", blockNumber, "blockTime", blockTime, "targetNumberOrTime", numOrTime, "address", addr)
				prevContractCode := state.GetCode(addr)
				if len(prevContractCode) == 0 && len(account.Code) > 0 {
					// system contracts defined after genesis need to be explicitly created
					state.CreateAccount(addr, true)
				}
				state.SetCode(addr, account.Code)
			}
		}
	}
}
