package parlia

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/systemcontracts"
)

type Bytes []byte

func (p *Parlia) getCurrentValidatorsBeforeLuban(header *types.Header, ibs *state.IntraBlockState) ([]common.Address, error) {

	// prepare different method
	method := "getValidators"
	if p.chainConfig.IsEuler(header.Number) {
		method = "getMiningValidators"
	}

	data, err := p.validatorSetABIBeforeLuban.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for getValidators", "error", err)
		return nil, err
	}

	// do smart contract call
	msgData := Bytes(data)
	_, returnData, err := p.systemCall(header.Coinbase, systemcontracts.ValidatorContract, msgData[:], ibs, header, u256.Num0)
	if err != nil {
		return nil, err
	}

	var valSet []common.Address
	err = p.validatorSetABIBeforeLuban.UnpackIntoInterface(&valSet, method, returnData)
	return valSet, err
}
