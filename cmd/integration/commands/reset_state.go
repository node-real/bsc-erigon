// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commands

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/backup"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/rawdb/rawdbhelpers"
	reset2 "github.com/erigontech/erigon/core/rawdb/rawdbreset"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var cmdResetState = &cobra.Command{
	Use:   "reset_state",
	Short: "Reset StateStages (5,6,7,8,9,10) and buckets",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		ctx, _ := common.RootContext()
		defer db.Close()
		sn, borSn, agg, _, bscSn, _, _, err := allSnapshots(ctx, db, logger)
		if err != nil {
			logger.Error("Opening snapshots", "error", err)
			return
		}
		defer sn.Close()
		defer borSn.Close()
		defer bscSn.Close()
		defer agg.Close()

		if err := db.View(ctx, func(tx kv.Tx) error { return printStages(tx, sn, borSn, bscSn) }); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}

		if err = reset2.ResetState(db, agg, ctx, chain, "", log.Root()); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}

		// set genesis after reset all buckets
		fmt.Printf("After reset: \n")
		if err := db.View(ctx, func(tx kv.Tx) error { return printStages(tx, sn, borSn, bscSn) }); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdClearBadBlocks = &cobra.Command{
	Use:   "clear_bad_blocks",
	Short: "Clear table with bad block hashes to allow to process this blocks one more time",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return err
		}
		defer db.Close()

		return db.Update(ctx, func(tx kv.RwTx) error {
			return backup.ClearTables(ctx, tx, "BadHeaderNumber")
		})
	},
}

func init() {
	withConfig(cmdResetState)
	withDataDir(cmdResetState)
	withChain(cmdResetState)
	rootCmd.AddCommand(cmdResetState)

	withDataDir(cmdClearBadBlocks)
	rootCmd.AddCommand(cmdClearBadBlocks)
}

func printStages(tx kv.Tx, snapshots *freezeblocks.RoSnapshots, borSn *heimdall.RoSnapshots, bscSn *freezeblocks.BscRoSnapshots) error {
	var err error
	var progress uint64
	w := new(tabwriter.Writer)
	defer w.Flush()
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "Note: prune_at doesn't mean 'all data before were deleted' - it just mean stage.Prune function were run to this block. Because 1 stage may prune multiple data types to different prune distance.\n")
	fmt.Fprint(w, "\n \t\t stage_at \t prune_at\n")

	for _, stage := range stages.AllStages {
		if progress, err = stages.GetStageProgress(tx, stage); err != nil {
			return err
		}
		prunedTo, err := stages.GetStagePruneProgress(tx, stage)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s \t\t %d \t %d\n", string(stage), progress, prunedTo)
	}
	pm, err := prune.Get(tx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "--\n")
	fmt.Fprintf(w, "prune distance: %s\n\n", pm.String())
	if snapshots != nil {
		fmt.Fprintf(w, "blocks: segments=%d, indices=%d\n", snapshots.SegmentsMax(), snapshots.IndicesMax())
	} else {
		fmt.Fprintf(w, "blocks: segments=0, indices=0; failed to open snapshots\n")
	}
	if borSn != nil {
		fmt.Fprintf(w, "blocks.bor: segments=%d, indices=%d\n", borSn.SegmentsMax(), borSn.IndicesMax())
	} else {
		fmt.Fprintf(w, "blocks.bor: segments=0, indices=0; failed to open bor snapshots\n")
	}
	if bscSn != nil {
		fmt.Fprintf(w, "blocks.bsc: segments=%d, indices=%d\n\n", bscSn.SegmentsMax(), bscSn.IndicesMax())
	} else {
		fmt.Fprintf(w, "blocks.bsc: segments=0, indices=0; failed to open bsc snapshots\n")
	}

	_lb, _lt, _ := rawdbv3.TxNums.Last(tx)

	fmt.Fprintf(w, "state.history: idx steps: %.02f, TxNums_Index(%d,%d)\n\n", rawdbhelpers.IdxStepsCountV3(tx), _lb, _lt)
	ethTxSequence, err := tx.ReadSequence(kv.EthTx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "sequence: EthTx=%d\n\n", ethTxSequence)

	{
		firstNonGenesisHeader, err := rawdbv3.SecondKey(tx, kv.Headers)
		if err != nil {
			return err
		}
		lastHeaders, err := rawdbv3.LastKey(tx, kv.Headers)
		if err != nil {
			return err
		}
		firstNonGenesisBody, err := rawdbv3.SecondKey(tx, kv.BlockBody)
		if err != nil {
			return err
		}
		lastBody, err := rawdbv3.LastKey(tx, kv.BlockBody)
		if err != nil {
			return err
		}
		fstHeader := u64or0(firstNonGenesisHeader)
		lstHeader := u64or0(lastHeaders)
		fstBody := u64or0(firstNonGenesisBody)
		lstBody := u64or0(lastBody)
		fmt.Fprintf(w, "in db: first header %d, last header %d, first body %d, last body %d\n", fstHeader, lstHeader, fstBody, lstBody)
	}

	fmt.Fprintf(w, "--\n")

	//fmt.Printf("==== state =====\n")
	//db.ForEach(kv.PlainState, nil, func(k, v []byte) error {
	//	fmt.Printf("st: %x, %x\n", k, v)
	//	return nil
	//})
	//fmt.Printf("====  code =====\n")
	//db.ForEach(kv.Code, nil, func(k, v []byte) error {
	//	fmt.Printf("code: %x, %x\n", k, v)
	//	return nil
	//})
	//fmt.Printf("==== PlainContractCode =====\n")
	//db.ForEach(kv.PlainContractCode, nil, func(k, v []byte) error {
	//	fmt.Printf("code2: %x, %x\n", k, v)
	//	return nil
	//})
	//fmt.Printf("====  IncarnationMap =====\n")
	//db.ForEach(kv.IncarnationMap, nil, func(k, v []byte) error {
	//	fmt.Printf("IncarnationMap: %x, %x\n", k, v)
	//	return nil
	//})
	return nil
}
func u64or0(in []byte) (v uint64) {
	if len(in) > 0 {
		v = binary.BigEndian.Uint64(in)
	}
	return v
}
