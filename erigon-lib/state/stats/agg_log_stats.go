package stats

import (
	"fmt"
	"runtime"
	"strings"

	common2 "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
)

func LogStats(tx kv.TemporalTx, logger log.Logger, tx2block func(endTxNumMinimax uint64) (uint64, error)) {
	at := tx.(state.HasAggTx).AggTx().(*state.AggregatorRoTx)
	maxTxNum := at.TxNumsInFiles(kv.StateDomains...)
	if maxTxNum == 0 {
		return
	}

	domainBlockNumProgress, err := tx2block(maxTxNum)
	if err != nil {
		logger.Warn("[snapshots:history] Stat", "err", err)
		return
	}
	accFiles := at.DomainFiles(kv.AccountsDomain)
	str := make([]string, 0, len(accFiles))
	for _, item := range accFiles {
		if !strings.HasSuffix(item.Filename(), ".kv") {
			continue
		}
		bn, err := tx2block(item.EndRootNum())
		if err != nil {
			logger.Warn("[snapshots:history] Stat", "err", err)
			return
		}
		str = append(str, fmt.Sprintf("%d=%dK", item.EndRootNum()/at.StepSize(), bn/1_000))
	}
	firstHistoryIndexBlockInDB, err := tx2block(at.MinStepInDb(tx, kv.AccountsDomain) * at.StepSize())
	if err != nil {
		logger.Warn("[snapshots:history] Stat", "err", err)
		return
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info("[snapshots:history] Stat",
		"blocks", common2.PrettyCounter(domainBlockNumProgress+1),
		"txNum2blockNum", strings.Join(str, ","),
		"txs", common2.PrettyCounter(at.Agg().EndTxNumMinimax()),
		"first_history_idx_in_db", firstHistoryIndexBlockInDB,
		"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))

}
