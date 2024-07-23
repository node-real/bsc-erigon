package systemcontracts

import (
	libcommon "github.com/erigontech/erigon-lib/common"
)

var (
	// genesis contracts
	ValidatorContract          = libcommon.HexToAddress("0x0000000000000000000000000000000000001000")
	SlashContract              = libcommon.HexToAddress("0x0000000000000000000000000000000000001001")
	SystemRewardContract       = libcommon.HexToAddress("0x0000000000000000000000000000000000001002")
	LightClientContract        = libcommon.HexToAddress("0x0000000000000000000000000000000000001003")
	TokenHubContract           = libcommon.HexToAddress("0x0000000000000000000000000000000000001004")
	RelayerIncentivizeContract = libcommon.HexToAddress("0x0000000000000000000000000000000000001005")
	RelayerHubContract         = libcommon.HexToAddress("0x0000000000000000000000000000000000001006")
	GovHubContract             = libcommon.HexToAddress("0x0000000000000000000000000000000000001007")
	TokenManagerContract       = libcommon.HexToAddress("0x0000000000000000000000000000000000001008")
	MaticTokenContract         = libcommon.HexToAddress("0x0000000000000000000000000000000000001010")
	CrossChainContract         = libcommon.HexToAddress("0x0000000000000000000000000000000000002000")
	StakingContract            = libcommon.HexToAddress("0x0000000000000000000000000000000000002001")
	StakeHubContract           = libcommon.HexToAddress("0x0000000000000000000000000000000000002002")
	StakeCreditContract        = libcommon.HexToAddress("0x0000000000000000000000000000000000002003")
	GovernorContract           = libcommon.HexToAddress("0x0000000000000000000000000000000000002004")
	GovTokenContract           = libcommon.HexToAddress("0x0000000000000000000000000000000000002005")
	TimelockContract           = libcommon.HexToAddress("0x0000000000000000000000000000000000002006")
	TokenRecoverPortalContract = libcommon.HexToAddress("0x0000000000000000000000000000000000003000")
)
