package parlia

import (
	"github.com/erigontech/erigon-lib/log/v3"
	"math/rand"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
)

func backOffTime(snap *Snapshot, header *types.Header, val libcommon.Address, chainConfig *chain.Config) uint64 {
	if snap.inturn(val) {
		log.Trace("backOffTime", "blockNumber", header.Number, "in turn validator", val)
		return 0
	} else {
		delay := initialBackOffTime
		validators := snap.validators()
		if chainConfig.IsPlanck(header.Number.Uint64()) {
			// reverse the key/value of snap.Recents to get recentsMap
			counts := snap.countRecents()
			for addr, seenTimes := range counts {
				log.Debug("backOffTime", "blockNumber", header.Number, "validator", addr, "seenTimes", seenTimes)
			}

			// The backOffTime does not matter when a validator has signed recently.
			if snap.signRecentlyByCounts(val, counts) {
				return 0
			}

			inTurnAddr := snap.inturnValidator()
			if snap.signRecentlyByCounts(inTurnAddr, counts) {
				log.Debug("in turn validator has recently signed, skip initialBackOffTime",
					"inTurnAddr", inTurnAddr)
				delay = 0
			}

			// Exclude the recently signed validators
			temp := make([]libcommon.Address, 0, len(validators))
			for _, addr := range validators {
				if snap.signRecentlyByCounts(addr, counts) {
					continue
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

		delay += backOffSteps[idx] * wiggleTime
		return delay
	}
}
