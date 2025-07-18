package stages

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
)

// shouldProcessBlobs checks if any block in the given list of blocks
// has a version greater than or equal to DenebVersion and contains BlobKzgCommitments.
func shouldProcessBlobs(blocks []*cltypes.SignedBeaconBlock, cfg *Cfg) bool {
	if !cfg.caplinConfig.ArchiveBlobs && !cfg.caplinConfig.ImmediateBlobsBackfilling {
		return false
	}
	blobsExist := false
	highestSlot := blocks[0].Block.Slot
	for _, block := range blocks {
		// Check if block version is greater than or equal to DenebVersion and contains BlobKzgCommitments
		if block.Version() >= clparams.DenebVersion && block.Block.Body.BlobKzgCommitments.Len() > 0 {
			blobsExist = true
		}
		if block.Block.Slot > highestSlot {
			highestSlot = block.Block.Slot
		}
	}
	// Check if the requested blocks are too old to request blobs
	// https://github.com/ethereum/consensus-specs/blob/dev/specs/deneb/p2p-interface.md#the-reqresp-domain

	// this is bad
	// highestEpoch := highestSlot / cfg.beaconCfg.SlotsPerEpoch
	// currentEpoch := cfg.ethClock.GetCurrentEpoch()
	// minEpochDist := uint64(0)
	// if currentEpoch > cfg.beaconCfg.MinEpochsForBlobSidecarsRequests {
	// 	minEpochDist = currentEpoch - cfg.beaconCfg.MinEpochsForBlobSidecarsRequests
	// }
	// finalizedEpoch := currentEpoch - 2
	// if highestEpoch < max(cfg.beaconCfg.DenebForkEpoch, minEpochDist, finalizedEpoch) {
	// 	return false
	// }

	return blobsExist
}

// downloadAndProcessEip4844DA handles downloading and processing of EIP-4844 data availability blobs.
// It takes highest slot processed, and a list of signed beacon blocks as input.
// It returns the highest blob slot processed and an error if any.
func downloadAndProcessEip4844DA(ctx context.Context, logger log.Logger, cfg *Cfg, highestSlotProcessed uint64, blocks []*cltypes.SignedBeaconBlock) (highestBlobSlotProcessed uint64, err error) {
	var (
		ids   *solid.ListSSZ[*cltypes.BlobIdentifier]
		blobs *network2.PeerAndSidecars
	)

	// Retrieve blob identifiers from the given blocks
	ids, err = network2.BlobsIdentifiersFromBlocks(blocks, cfg.beaconCfg)
	if err != nil {
		// Return an error if blob identifiers could not be retrieved
		err = fmt.Errorf("failed to get blob identifiers: %w", err)
		return
	}

	// If there are no blobs to retrieve, return the highest slot processed
	if ids.Len() == 0 {
		return highestSlotProcessed, nil
	}

	// Request blobs from the network
	blobs, err = network2.RequestBlobsFrantically(ctx, cfg.rpc, ids)
	if err != nil {
		// Return an error if blobs could not be retrieved
		err = fmt.Errorf("failed to get blobs: %w", err)
		return
	}

	var highestProcessed, inserted uint64
	// Verify and insert blobs into the blob store
	if highestProcessed, inserted, err = blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStore, ids, blobs.Responses, nil); err != nil {
		// Ban the peer if verification fails
		cfg.rpc.BanPeer(blobs.Peer)
		// Return an error if blobs could not be verified
		err = fmt.Errorf("failed to verify blobs: %w", err)
		return
	}
	// If all blobs were inserted successfully, return the highest processed slot
	if inserted == uint64(ids.Len()) {
		return highestProcessed, nil
	}

	// If not all blobs were inserted, return the highest processed slot minus one
	return highestProcessed - 1, err
}

// processDownloadedBlockBatches processes a batch of downloaded blocks.
// It takes the highest block processed, a flag to determine if insertion is needed, and a list of signed beacon blocks as input.
// It returns the new highest block processed and an error if any.
func processDownloadedBlockBatches(ctx context.Context, logger log.Logger, cfg *Cfg, highestBlockProcessed uint64, shouldInsert bool, blocks []*cltypes.SignedBeaconBlock) (newHighestBlockProcessed uint64, err error) {
	// Pre-process the block batch to ensure that the blocks are sorted by slot in ascending order
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Block.Slot < blocks[j].Block.Slot
	})

	var (
		blockRoot common.Hash
		st        *state.CachingBeaconState
	)
	newHighestBlockProcessed = highestBlockProcessed
	if shouldProcessBlobs(blocks, cfg) {
		_, err = downloadAndProcessEip4844DA(ctx, logger, cfg, highestBlockProcessed, blocks)
		if err != nil {
			logger.Trace("[Caplin] Failed to process blobs", "err", err)
			return highestBlockProcessed, nil
		}
	}
	// Iterate over each block in the sorted list
	for _, block := range blocks {
		// Compute the hash of the current block
		blockRoot, err = block.Block.HashSSZ()
		if err != nil {
			// Return an error if block hashing fails
			err = fmt.Errorf("failed to hash block: %w", err)
			return
		}

		var hasSignedHeaderInDB bool

		if err = cfg.indiciesDB.View(ctx, func(tx kv.Tx) error {
			_, hasSignedHeaderInDB, err = beacon_indicies.ReadSignedHeaderByBlockRoot(ctx, tx, blockRoot)
			return err
		}); err != nil {
			err = fmt.Errorf("failed to read signed header: %w", err)
			return
		}

		checkDataAvaiability := cfg.caplinConfig.ArchiveBlobs || cfg.caplinConfig.ImmediateBlobsBackfilling
		// Process the block
		if err = processBlock(ctx, cfg, cfg.indiciesDB, block, false, true, checkDataAvaiability); err != nil {
			if errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) {
				// Return an error if EIP-4844 data is not available
				logger.Trace("[Caplin] forward sync EIP-4844 data not available", "blockSlot", block.Block.Slot)
				if newHighestBlockProcessed == 0 {
					return 0, nil
				}
				return newHighestBlockProcessed - 1, nil
			}
			// Return an error if block processing fails
			err = fmt.Errorf("bad blocks segment received: %w", err)
			return
		}

		if !hasSignedHeaderInDB && block.Block.Slot%(cfg.beaconCfg.SlotsPerEpoch*2) == 0 {
			// Perform post-processing on the block
			st, err = cfg.forkChoice.GetStateAtBlockRoot(blockRoot, false)
			if err == nil && st != nil {
				// Dump the beacon state on disk if conditions are met
				if err = cfg.forkChoice.DumpBeaconStateOnDisk(st); err != nil {
					// Return an error if dumping the state fails
					err = fmt.Errorf("failed to dump state: %w", err)
					return
				}
				if err = saveHeadStateOnDiskIfNeeded(cfg, st); err != nil {
					// Return an error if saving the head state fails
					err = fmt.Errorf("failed to save head state: %w", err)
					return
				}
			}
		}

		// Update the highest block processed if the current block's slot is higher
		if newHighestBlockProcessed < block.Block.Slot {
			newHighestBlockProcessed = block.Block.Slot
		}

		// If block version is less than BellatrixVersion or shouldInsert is false, skip insertion
		if block.Version() < clparams.BellatrixVersion || !shouldInsert {
			continue
		}
		// Add the block to the block collector
		if err = cfg.blockCollector.AddBlock(block.Block); err != nil {
			// Return an error if adding the block to the collector fails
			err = fmt.Errorf("failed to add block to collector: %w", err)
			return
		}
	}
	return
}

// forwardSync (MAIN ROUTINE FOR ForwardSync) performs the forward synchronization of beacon blocks.
func forwardSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	var (
		shouldInsert = cfg.executionClient != nil && cfg.executionClient.SupportInsertion() // Check if the execution client supports insertion
		startSlot    = cfg.forkChoice.HighestSeen() - 8                                     // Start forwardsync a little bit behind the highest seen slot (account for potential reorgs)
		secsPerLog   = 30                                                                   // Interval in seconds for logging progress
		logTicker    = time.NewTicker(time.Duration(secsPerLog) * time.Second)              // Ticker for logging progress
		downloader   = network2.NewForwardBeaconDownloader(ctx, cfg.rpc)                    // Initialize a new forward beacon downloader
		currentSlot  atomic.Uint64                                                          // Atomic variable to track the current slot
	)

	// Initialize the slot to download from the finalized checkpoint
	currentSlot.Store(startSlot)

	// Always start from the current finalized checkpoint
	downloader.SetHighestProcessedSlot(currentSlot.Load())

	// Set the function to process downloaded blocks
	downloader.SetProcessFunction(func(initialHighestSlotProcessed uint64, blocks []*cltypes.SignedBeaconBlock) (newHighestSlotProcessed uint64, err error) {
		highestSlotProcessed, err := processDownloadedBlockBatches(ctx, logger, cfg, initialHighestSlotProcessed, shouldInsert, blocks)
		if err != nil {
			logger.Warn("[Caplin] Failed to process block batch", "err", err)
			return initialHighestSlotProcessed, err
		}
		currentSlot.Store(highestSlotProcessed)
		return highestSlotProcessed, nil
	})

	// Get the current slot of the chain tip
	chainTipSlot := cfg.ethClock.GetCurrentSlot()
	logger.Info("[Caplin] Forward Sync", "from", currentSlot.Load(), "to", chainTipSlot)
	prevProgress := currentSlot.Load()

	// Run the log loop until the highest processed slot reaches the chain tip slot
	for downloader.GetHighestProcessedSlot() < chainTipSlot {
		downloader.RequestMore(ctx)
		select {
		case <-ctx.Done():
			// Return if the context is done
			return ctx.Err()
		case <-logTicker.C:
			// Log progress at regular intervals
			progressMade := chainTipSlot - currentSlot.Load()
			distFromChainTip := time.Duration(progressMade*cfg.beaconCfg.SecondsPerSlot) * time.Second
			timeProgress := currentSlot.Load() - prevProgress
			estimatedTimeRemaining := 999 * time.Hour
			if timeProgress > 0 {
				estimatedTimeRemaining = time.Duration(float64(progressMade)/(float64(currentSlot.Load()-prevProgress)/float64(secsPerLog))) * time.Second
			}
			if distFromChainTip < 0 || estimatedTimeRemaining < 0 {
				continue
			}
			prevProgress = currentSlot.Load()
			logger.Info("[Caplin] Forward Sync", "progress", currentSlot.Load(), "distance-from-chain-tip", distFromChainTip, "estimated-time-remaining", estimatedTimeRemaining)
		default:
		}
	}

	return nil
}
