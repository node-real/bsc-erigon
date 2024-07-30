package parlia

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
)

func (p *Parlia) blockTimeForRamanujanFork(snap *Snapshot, header, parent *types.Header) uint64 {
	blockTime := parent.Time + p.config.Period
	if p.chainConfig.IsRamanujan(header.Number.Uint64()) {
		blockTime = blockTime + backOffTime(snap, header, p.val, p.chainConfig)
	}
	return blockTime
}

func (p *Parlia) blockTimeVerifyForRamanujanFork(snap *Snapshot, header, parent *types.Header) error {
	if p.chainConfig.IsRamanujan(header.Number.Uint64()) {
		if header.Time < parent.Time+p.config.Period+backOffTime(snap, header, header.Coinbase, p.chainConfig) {
			return fmt.Errorf("header %d, time %d, now %d, period: %d, backof: %d, %w", header.Number.Uint64(), header.Time, time.Now().Unix(), p.config.Period, backOffTime(snap, header, header.Coinbase, p.chainConfig), consensus.ErrFutureBlock)
		}
	}
	return nil
}
