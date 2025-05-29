package systemcontracts

import (
	"github.com/erigontech/erigon-lib/common"
)

var (
	// genesis contracts
	ValidatorContract          = common.HexToAddress("0x0000000000000000000000000000000000001000")
	SlashContract              = common.HexToAddress("0x0000000000000000000000000000000000001001")
	SystemRewardContract       = common.HexToAddress("0x0000000000000000000000000000000000001002")
	LightClientContract        = common.HexToAddress("0x0000000000000000000000000000000000001003")
	TokenHubContract           = common.HexToAddress("0x0000000000000000000000000000000000001004")
	RelayerIncentivizeContract = common.HexToAddress("0x0000000000000000000000000000000000001005")
	RelayerHubContract         = common.HexToAddress("0x0000000000000000000000000000000000001006")
	GovHubContract             = common.HexToAddress("0x0000000000000000000000000000000000001007")
	TokenManagerContract       = common.HexToAddress("0x0000000000000000000000000000000000001008")
	MaticTokenContract         = common.HexToAddress("0x0000000000000000000000000000000000001010")
	CrossChainContract         = common.HexToAddress("0x0000000000000000000000000000000000002000")
	StakingContract            = common.HexToAddress("0x0000000000000000000000000000000000002001")
	StakeHubContract           = common.HexToAddress("0x0000000000000000000000000000000000002002")
	StakeCreditContract        = common.HexToAddress("0x0000000000000000000000000000000000002003")
	GovernorContract           = common.HexToAddress("0x0000000000000000000000000000000000002004")
	GovTokenContract           = common.HexToAddress("0x0000000000000000000000000000000000002005")
	TimelockContract           = common.HexToAddress("0x0000000000000000000000000000000000002006")
	TokenRecoverPortalContract = common.HexToAddress("0x0000000000000000000000000000000000003000")
)
