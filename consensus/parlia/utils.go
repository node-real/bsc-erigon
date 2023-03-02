package parlia

import (
	"math/rand"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
)

func backOffTime(snap *Snapshot, header *types.Header, val libcommon.Address, chainConfig *chain.Config ) uint64 {
	if snap.inturn(val) {
		return 0
	} else {
		idx := snap.indexOfVal(val)
		if idx < 0 {
			// The backOffTime does not matter when a validator is not authorized.
			return 0
		}
		s := rand.NewSource(int64(snap.Number))
		r := rand.New(s) // nolint: gosec
		n := len(snap.Validators)
		backOffSteps := make([]uint64, 0, n)

		if !chainConfig.IsBohr(header.Number) {
			for i := uint64(0); i < uint64(n); i++ {
				backOffSteps = append(backOffSteps, i)
			}
			r.Shuffle(n, func(i, j int) {
				backOffSteps[i], backOffSteps[j] = backOffSteps[j], backOffSteps[i]
			})
			delay := initialBackOffTime + backOffSteps[idx]*wiggleTime
			return delay
		}

		// Exclude the recently signed validators first, and then compute the backOffTime.
		recentVals := make(map[libcommon.Address]bool, len(snap.Recents))
		for _, recent := range snap.Recents {
			if val == recent {
				// The backOffTime does not matter when a validator has signed recently.
				return 0
			}
			recentVals[recent] = true
		}

		backOffIndex := idx
		validators := snap.validators()
		for i := 0; i < n; i++ {
			if isRecent, ok := recentVals[validators[i]]; ok && isRecent {
				if i < idx {
					backOffIndex--
				}
				continue
			}
			backOffSteps = append(backOffSteps, uint64(len(backOffSteps)))
		}
		r.Shuffle(len(backOffSteps), func(i, j int) {
			backOffSteps[i], backOffSteps[j] = backOffSteps[j], backOffSteps[i]
		})
		delay := initialBackOffTime + backOffSteps[backOffIndex]*wiggleTime

		// If the in turn validator has recently signed, no initial delay.
		inTurnVal := validators[(snap.Number+1)%uint64(len(validators))]
		if isRecent, ok := recentVals[inTurnVal]; ok && isRecent {
			delay -= initialBackOffTime
		}
		return delay
	}
}
