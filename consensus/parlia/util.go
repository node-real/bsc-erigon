package parlia

import (
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/params"
	"math/rand"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
)

func backOffTime(snap *Snapshot, parent, header *types.Header, val libcommon.Address, chainConfig *chain.Config) uint64 {
	if snap.inturn(val) {
		log.Trace("backOffTime", "blockNumber", header.Number, "in turn validator", val)
		return 0
	} else {
		delay := defaultInitialBackOffTime
		// When mining blocks, `header.Time` is temporarily set to time.Now() + 1.
		// Therefore, using `header.Time` to determine whether a hard fork has occurred is incorrect.
		// As a result, during the Bohr and Lorentz hard forks, the network may experience some instability,
		// So use `parent.Time` instead.
		isParentLorentz := chainConfig.IsLorentz(parent.Number.Uint64(), parent.Time)
		if isParentLorentz {
			// If the in-turn validator has not signed recently, the expected backoff times are [2, 3, 4, ...].
			delay = lorentzInitialBackOffTime
		}
		validators := snap.validators()
		if chainConfig.IsPlanck(header.Number.Uint64()) {
			// reverse the key/value of snap.Recents to get recentsMap
			counts := snap.countRecents()
			for addr, seenTimes := range counts {
				log.Trace("backOffTime", "blockNumber", header.Number, "validator", addr, "seenTimes", seenTimes)
			}

			// The backOffTime does not matter when a validator has signed recently.
			if snap.signRecentlyByCounts(val, counts) {
				return 0
			}

			inTurnAddr := snap.inturnValidator()
			if snap.signRecentlyByCounts(inTurnAddr, counts) {
				log.Trace("in turn validator has recently signed, skip initialBackOffTime",
					"inTurnAddr", inTurnAddr)
				delay = 0
			}

			// Exclude the recently signed validators and the in turn validator
			temp := make([]libcommon.Address, 0, len(validators))
			for _, addr := range validators {
				if snap.signRecentlyByCounts(addr, counts) {
					continue
				}
				if chainConfig.IsBohr(header.Number.Uint64(), header.Time) {
					if addr == inTurnAddr {
						continue
					}
				}
				temp = append(temp, addr)
			}
			validators = temp
		}

		// get the index of current validator and its shuffled backoff time.
		idx := -1
		for index, itemAddr := range validators {
			if val == itemAddr {
				idx = index
			}
		}
		if idx < 0 {
			log.Debug("The validator is not authorized", "addr", val)
			return 0
		}

		randSeed := snap.Number
		if chainConfig.IsBohr(header.Number.Uint64(), header.Time) {
			randSeed = header.Number.Uint64() / uint64(snap.TurnLength)
		}
		s := rand.NewSource(int64(randSeed))
		r := rand.New(s)
		n := len(validators)
		backOffSteps := make([]uint64, 0, n)

		for i := uint64(0); i < uint64(n); i++ {
			backOffSteps = append(backOffSteps, i)
		}

		r.Shuffle(n, func(i, j int) {
			backOffSteps[i], backOffSteps[j] = backOffSteps[j], backOffSteps[i]
		})

		if delay == 0 && isParentLorentz {
			// If the in-turn validator has signed recently, the expected backoff times are [0, 2, 3, ...].
			if backOffSteps[idx] == 0 {
				return 0
			}
			return lorentzInitialBackOffTime + (backOffSteps[idx]-1)*wiggleTime
		}
		delay += backOffSteps[idx] * wiggleTime
		return delay
	}
}

// BlockInterval returns number of blocks in one epoch for the given header
func (p *Parlia) epochLength(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) (uint64, error) {
	if header == nil {
		return params.DefaultEpochLength, errUnknownBlock
	}
	if header.Number.Uint64() == 0 {
		return params.DefaultEpochLength, nil
	}
	snap, err := p.snapshot(chain, header.Number.Uint64()-1, header.ParentHash, parents, false)
	if err != nil {
		return params.DefaultEpochLength, err
	}
	return snap.EpochLength, nil
}

// BlockInterval returns the block interval in milliseconds for the given header
func (p *Parlia) BlockInterval(chain consensus.ChainHeaderReader, header *types.Header) (uint64, error) {
	if header == nil {
		return params.DefaultBlockInterval, errUnknownBlock
	}
	if header.Number.Uint64() == 0 {
		return params.DefaultBlockInterval, nil
	}
	snap, err := p.snapshot(chain, header.Number.Uint64()-1, header.ParentHash, nil, false)
	if err != nil {
		return params.DefaultBlockInterval, err
	}
	return snap.BlockInterval, nil
}
