package commands

import (
	"context"
	"errors"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	log "github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"
)

var (
	endblock uint64
	concur   uint64
)

func init() {
	withChain(diffLayerCmd)
	withBlock(diffLayerCmd)
	withDataDir(diffLayerCmd)
	withSnapshotBlocks(diffLayerCmd)
	diffLayerCmd.Flags().Uint64Var(&endblock, "endblock", 0, "")
	diffLayerCmd.Flags().Uint64Var(&concur, "concur", 16, "")
	rootCmd.AddCommand(diffLayerCmd)
}

var diffLayerCmd = &cobra.Command{
	Use:   "diffLayer",
	Short: "Re-executes historical transactions in read-only mode and generate diff layer",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return GenDiffLayers(genesis, logger, block, chaindata)
	},
}

// CheckChangeSets re-executes historical transactions in read-only mode
// and checks that their outputs match the database ChangeSets.
func GenDiffLayers(genesis *types.Genesis, logger log.Logger, blockNum uint64, chaindata string) error {
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	db, err := kv2.NewMDBX(logger).Path(chaindata).Open()
	if err != nil {
		return err
	}
	var blockReader services.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	useSnapshots := ethconfig.UseSnapshotsByChainName(chainConfig.ChainName) && snapshotsCli
	if useSnapshots {
		allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadirCli, "snapshots"))
		defer allSnapshots.Close()
		if err := allSnapshots.ReopenFolder(); err != nil {
			return fmt.Errorf("reopen snapshot segments: %w", err)
		}
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots, ethconfig.Defaults.TransactionsV3)
	} else {
		//blockReader = snapshotsync.NewBlockReader()
		return errors.New("use snapshot")
	}
	chainDb := db
	defer chainDb.Close()
	ctx := context.Background()

	chainConfig := genesis.Config
	vmConfig := vm.Config{}

	roTx, err := chainDb.BeginRo(ctx)
	if err != nil {
		return err
	}

	execAt, err1 := stages.GetStageProgress(roTx, stages.Execution)
	if err1 != nil {
		return err1
	}
	historyAt, err1 := stages.GetStageProgress(roTx, stages.StorageHistoryIndex)
	if err1 != nil {
		return err1
	}
	roTx.Rollback()

	log.Info("begin", "begin", blockNum, "end", endblock, "execAt", execAt, "historyAt", historyAt)

	if endblock > execAt {
		log.Warn(fmt.Sprintf("Force stop: because trying to check blockNumber=%d higher than Exec stage=%d", blockNum, execAt))
		return nil
	}
	if endblock > historyAt {
		log.Warn(fmt.Sprintf("Force stop: because trying to check blockNumber=%d higher than History stage=%d", blockNum, historyAt))
		return nil
	}

	engine := initConsensusEngine(chainConfig, allSnapshots)

	limit := make(chan struct{}, concur)
	var wg sync.WaitGroup
	for n := blockNum; n <= endblock; n++ {
		limit <- struct{}{}
		wg.Add(1)
		go func(blockNum uint64) {
			log.Info("start", "number", blockNum)

			historyTx, err1 := chainDb.BeginRo(ctx)
			if err1 != nil {
				panic(fmt.Errorf("begin history tx failed: %v", err1))
			}
			defer historyTx.Rollback()

			noOpWriter := state.NewNoopWriter()

			defer func(start time.Time) {
				<-limit
				wg.Done()
				log.Info("generated", "number", blockNum, "cost", time.Since(start).String())
			}(time.Now())

			blockHash, err := blockReader.CanonicalHash(ctx, historyTx, blockNum)
			if err != nil {
				panic(fmt.Errorf("get canonical hash failed: %v, number: %v", err, blockNum))
			}
			var b *types.Block
			b, _, err = blockReader.BlockWithSenders(ctx, historyTx, blockHash, blockNum)
			if err != nil {
				panic(fmt.Errorf("get block failed: %v, number: %v", err, blockNum))
			}
			if b == nil {
				panic(fmt.Errorf("block missing, number: %v", blockNum))
			}
			reader := state.NewPlainState(historyTx, blockNum, nil)
			intraBlockState := state.New(reader)
			blockWriter := NewDiffLayerWriter()

			getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
				h, e := blockReader.Header(ctx, historyTx, hash, number)
				if e != nil {
					panic(e)
				}
				return h
			}
			//cr := stagedsync.NewChainReaderImpl(chainConfig, historyTx, blockReader)
			//receipts, err1 := runBlock(engine, intraBlockState, noOpWriter, blockWriter, chainConfig, getHeader, b, vmConfig, false, cr)
			receipts, err1 := runBlock(engine, intraBlockState, noOpWriter, blockWriter, chainConfig, getHeader, b, vmConfig, false)
			if err1 != nil {
				panic(fmt.Errorf("run block failed: %v, numer: %v", err, blockNum))
			}
			if chainConfig.IsByzantium(blockNum) {
				receiptSha := types.DeriveSha(receipts)
				if receiptSha != b.ReceiptHash() {
					panic(fmt.Errorf("mismatched receipt headers for block: %v", err, blockNum))
				}
			}

			filename := fmt.Sprintf("diff_%010d_%s", b.NumberU64(), b.Hash())
			if err := ioutil.WriteFile(filename, blockWriter.GetData(), 0664); err != nil {
				panic(fmt.Errorf("write diff failed: %v", err))
			}
		}(n)
	}

	wg.Wait()
	log.Info("Generated", "start", blockNum, "end", endblock, "duration", time.Since(startTime))
	return nil
}
