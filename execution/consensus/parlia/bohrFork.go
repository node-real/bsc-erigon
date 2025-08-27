package parlia

import (
	"errors"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/systemcontracts"
	params2 "github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
	"math/big"
	mrand "math/rand"
)

func (p *Parlia) getTurnLength(chain consensus.ChainHeaderReader, header *types.Header, ibs *state.IntraBlockState) (*uint8, error) {
	parent := chain.GetHeaderByHash(header.ParentHash)
	if parent == nil {
		return nil, errors.New("parent not found")
	}

	var turnLength uint8
	if p.chainConfig.IsBohr(parent.Number.Uint64(), parent.Time) {
		turnLengthFromContract, err := p.getTurnLengthFromContract(parent, ibs)
		if err != nil {
			return nil, err
		}
		if turnLengthFromContract == nil {
			return nil, errors.New("unexpected error when getTurnLengthFromContract")
		}
		turnLength = uint8(turnLengthFromContract.Int64())
	} else {
		turnLength = defaultTurnLength
	}
	log.Trace("getTurnLength", "turnLength", turnLength)

	return &turnLength, nil
}

func (p *Parlia) getTurnLengthFromContract(header *types.Header, ibs *state.IntraBlockState) (turnLength *big.Int, err error) {
	// mock to get turnLength from the contract
	if params2.FixedTurnLength >= 1 && params2.FixedTurnLength <= 9 {
		if params2.FixedTurnLength == 2 {
			return p.getRandTurnLength(header)
		}
		return big.NewInt(int64(params2.FixedTurnLength)), nil
	}
	method := "getTurnLength"
	data, err := p.validatorSetABI.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for getTurnLength", "error", err)
		return nil, err
	}

	// do smart contract call
	msgData := Bytes(data)
	_, returnData, err := p.systemCall(header.Coinbase, systemcontracts.ValidatorContract, msgData[:], ibs, header, u256.Num0)
	if err != nil {
		return nil, err
	}

	err = p.validatorSetABI.UnpackIntoInterface(&turnLength, method, returnData)
	return turnLength, err
}

// getRandTurnLength returns a random valid value, used to test switching turn length
func (p *Parlia) getRandTurnLength(header *types.Header) (turnLength *big.Int, err error) {
	turnLengths := [8]uint8{1, 3, 4, 5, 6, 7, 8, 9}
	r := mrand.New(mrand.NewSource(int64(header.Time)))
	lengthIndex := int(r.Int31n(int32(len(turnLengths))))
	return big.NewInt(int64(turnLengths[lengthIndex])), nil
}
