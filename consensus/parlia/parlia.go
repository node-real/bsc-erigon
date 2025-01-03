package parlia

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Giulio2002/bls"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/holiman/uint256"
	"github.com/willf/bitset"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/crypto/cryptopool"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/consensus/parlia/finality"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/forkid"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/systemcontracts"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/services"
)

const (
	inMemorySnapshots  = 256  // Number of recent snapshots to keep in memory
	inMemorySignatures = 4096 // Number of recent block signatures to keep in memory

	CheckpointInterval = 1024        // Number of blocks after which to save the snapshot to the database
	defaultEpochLength = uint64(100) // Default number of blocks of checkpoint to update validatorSet from contract
	defaultTurnLength  = uint8(1)    // Default consecutive number of blocks a validator receives priority for block production
	extraVanity        = 32          // Fixed number of extra-data prefix bytes reserved for signer vanity
	extraSeal          = 65          // Fixed number of extra-data suffix bytes reserved for signer seal
	nextForkHashSize   = 4           // Fixed number of extra-data suffix bytes reserved for nextForkHash.

	validatorBytesLengthBeforeLuban = length.Addr
	validatorBytesLength            = length.Addr + types.BLSPublicKeyLength
	validatorNumberSize             = 1 // Fixed number of extra prefix bytes reserved for validator number after Luban
	turnLengthSize                  = 1 // Fixed number of extra-data suffix bytes reserved for turnLength

	wiggleTime         = uint64(1) // second, Random delay (per signer) to allow concurrent signers
	initialBackOffTime = uint64(1) // second
	processBackOffTime = uint64(1) // second

	systemRewardPercent = 4 // it means 1/2^4 = 1/16 percentage of gas fee incoming will be distributed to system

	collectAdditionalVotesRewardRatio = float64(1) // ratio of additional reward for collecting more votes than needed
)

var (
	uncleHash  = types.CalcUncleHash(nil) // Always Keccak256(RLP([])) as uncles are meaningless outside of PoW.
	diffInTurn = big.NewInt(2)            // Block difficulty for in-turn signatures
	diffNoTurn = big.NewInt(1)            // Block difficulty for out-of-turn signatures
	// 100 native token
	maxSystemBalance = new(uint256.Int).Mul(uint256.NewInt(100), uint256.NewInt(params.Ether))

	validatorItemsCache       []ValidatorItem
	maxElectedValidatorsCache = big.NewInt(0)
)

// Various error messages to mark blocks invalid. These should be private to
// prevent engine specific errors from being referenced in the remainder of the
// codebase, inherently breaking if the engine is swapped out. Please put common
// error types into the consensus package.
var (
	// errUnknownBlock is returned when the list of validators is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")

	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte signature suffix missing")

	// errExtraValidators is returned if non-sprint-end block contain validator data in
	// their extra-data fields.
	errExtraValidators = errors.New("non-sprint-end block contains extra validator list")

	// errInvalidSpanValidators is returned if a block contains an
	// invalid list of validators (i.e. non divisible by 20 bytes).
	errInvalidSpanValidators = errors.New("invalid validator list on sprint end block")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")

	// errInvalidTurnLength is returned if a block contains an
	// invalid length of turn (i.e. no data left after parsing validators).
	errInvalidTurnLength = errors.New("invalid turnLength")

	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")

	// errMismatchingEpochValidators is returned if a sprint block contains a
	// list of validators different than the one the local node calculated.
	errMismatchingEpochValidators = errors.New("mismatching validator list on epoch block")

	// errInvalidDifficulty is returned if the difficulty of a block is missing.
	errInvalidDifficulty = errors.New("invalid difficulty")

	// errMismatchingEpochTurnLength is returned if a sprint block contains a
	// turn length different than the one the local node calculated.
	errMismatchingEpochTurnLength = errors.New("mismatching turn length on epoch block")

	// errWrongDifficulty is returned if the difficulty of a block doesn't match the
	// turn of the signer.
	errWrongDifficulty = errors.New("wrong difficulty")

	// errOutOfRangeChain is returned if an authorization list is attempted to
	// be modified via out-of-range or non-contiguous headers.
	errOutOfRangeChain = errors.New("out of range or non-contiguous chain")

	// errBlockHashInconsistent is returned if an authorization list is attempted to
	// insert an inconsistent block.
	errBlockHashInconsistent = errors.New("the block hash is inconsistent")

	// errUnauthorizedValidator is returned if a header is signed by a non-authorized entity.
	errUnauthorizedValidator = errors.New("unauthorized validator")

	// errCoinBaseMisMatch is returned if a header's coinbase do not match with signature
	errCoinBaseMisMatch = errors.New("coinbase do not match with signature")

	// errRecentlySigned is returned if a header is signed by an authorized entity
	// that already signed a header recently, thus is temporarily not allowed to.
	errRecentlySigned = errors.New("recently signed")
)

// SignFn is a signer callback function to request a header to be signed by a
// backing account.
type SignFn func(validator libcommon.Address, payload []byte, chainId *big.Int) ([]byte, error)

// ecrecover extracts the Ethereum account address from a signed header.
func ecrecover(header *types.Header, sigCache *lru.ARCCache[libcommon.Hash, libcommon.Address], chainId *big.Int) (libcommon.Address, error) {
	// If the signature's already cached, return that
	hash := header.Hash()
	if address, known := sigCache.Get(hash); known {
		return address, nil
	}
	// Retrieve the signature from the header extra-data
	if len(header.Extra) < extraSeal {
		return libcommon.Address{}, errMissingSignature
	}
	signature := header.Extra[len(header.Extra)-extraSeal:]

	// Recover the public key and the Ethereum address
	pubkey, err := crypto.Ecrecover(types.SealHash(header, chainId).Bytes(), signature)
	if err != nil {
		return libcommon.Address{}, err
	}
	var signer libcommon.Address
	copy(signer[:], crypto.Keccak256(pubkey[1:])[12:])

	sigCache.Add(hash, signer)
	return signer, nil
}

// parliaRLP returns the rlp bytes which needs to be signed for the parlia
// sealing. The RLP to sign consists of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func parliaRLP(header *types.Header, chainId *big.Int) []byte {
	b := new(bytes.Buffer)
	types.EncodeSigHeader(b, header, chainId)
	return b.Bytes()
}

type Parlia struct {
	chainConfig *chain.Config       // Chain config
	config      *chain.ParliaConfig // Consensus engine configuration parameters for parlia consensus
	genesisHash libcommon.Hash
	db          kv.RwDB // Database to store and retrieve snapshot checkpoints
	BlobStore   services.BlobStorage

	recentSnaps *lru.ARCCache[libcommon.Hash, *Snapshot]         // Snapshots for recent block to speed up
	signatures  *lru.ARCCache[libcommon.Hash, libcommon.Address] // Signatures of recent blocks to speed up mining

	signer *types.Signer

	val    libcommon.Address // Ethereum address of the signing key
	signFn SignFn            // Signer function to authorize hashes with

	signerLock sync.RWMutex // Protects the signer fields

	validatorSetABIBeforeLuban abi.ABI
	validatorSetABI            abi.ABI
	slashABI                   abi.ABI
	stakeHubABI                abi.ABI

	// The fields below are for testing only
	fakeDiff               bool     // Skip difficulty verifications
	heightForks, timeForks []uint64 // Forks extracted from the chainConfig
	blockReader            services.FullBlockReader
	logger                 log.Logger
}

// New creates a Parlia consensus engine.
func New(
	chainConfig *chain.Config,
	db kv.RwDB,
	blobStore services.BlobStorage,
	blockReader services.FullBlockReader,
	logger log.Logger,
) *Parlia {
	// get parlia config
	parliaConfig := chainConfig.Parlia

	// Set any missing consensus parameters to their defaults
	if parliaConfig != nil && parliaConfig.Epoch == 0 {
		parliaConfig.Epoch = defaultEpochLength
	}

	// Allocate the snapshot caches and create the engine
	recentSnaps, err := lru.NewARC[libcommon.Hash, *Snapshot](inMemorySnapshots)
	if err != nil {
		panic(err)
	}
	signatures, err := lru.NewARC[libcommon.Hash, libcommon.Address](inMemorySignatures)
	if err != nil {
		panic(err)
	}
	vABIBeforeLuban, err := abi.JSON(strings.NewReader(validatorSetABIBeforeLuban))
	if err != nil {
		panic(err)
	}
	vABI, err := abi.JSON(strings.NewReader(validatorSetABI))
	if err != nil {
		panic(err)
	}
	sABI, err := abi.JSON(strings.NewReader(slashABI))
	if err != nil {
		panic(err)
	}
	stABI, err := abi.JSON(strings.NewReader(stakeABI))
	if err != nil {
		panic(err)
	}
	c := &Parlia{
		chainConfig:                chainConfig,
		config:                     parliaConfig,
		db:                         db,
		BlobStore:                  blobStore,
		recentSnaps:                recentSnaps,
		signatures:                 signatures,
		validatorSetABIBeforeLuban: vABIBeforeLuban,
		validatorSetABI:            vABI,
		slashABI:                   sABI,
		stakeHubABI:                stABI,
		signer:                     types.LatestSigner(chainConfig),
		blockReader:                blockReader,
		logger:                     logger,
	}
	genesisTime := uint64(0)
	c.heightForks, c.timeForks = forkid.GatherForks(chainConfig, genesisTime)

	return c
}

// Type returns underlying consensus engine
func (p *Parlia) Type() chain.ConsensusName {
	return chain.ParliaConsensus
}

// Author retrieves the Ethereum address of the account that minted the given
// block, which may be different from the header's coinbase if a consensus
// engine is based on signatures.
// This is thread-safe (only access the header.Coinbase)
func (p *Parlia) Author(header *types.Header) (libcommon.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of a
// given engine. Verifying the seal may be done optionally here, or explicitly
// via the VerifySeal method.
func (p *Parlia) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	return p.verifyHeader(chain, header, nil)
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers
// concurrently. The method returns a quit channel to abort the operations and
// a results channel to retrieve the async verifications (the order is that of
// the input slice).
func (p *Parlia) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) error {
	for i, header := range headers {
		err := p.verifyHeader(chain, header, headers[:i])
		if err != nil {
			return err
		}
	}
	return nil
}

// getValidatorBytesFromHeader returns the validators bytes extracted from the header's extra field if exists.
// The validators bytes would be contained only in the epoch block's header, and its each validator bytes length is fixed.
// On Luban fork, we introduce vote attestation into the header's extra field, so extra format is different from before.
// Before Luban fork: |---Extra Vanity---|---Validators Bytes (or Empty)---|---Extra Seal---|
// After Luban fork:  |---Extra Vanity---|---Validators Number and Validators Bytes (or Empty)---|---Vote Attestation (or Empty)---|---Extra Seal---|
// After bohr fork:   |---Extra Vanity---|---Validators Number and Validators Bytes (or Empty)---|---Turn Length (or Empty)---|---Vote Attestation (or Empty)---|---Extra Seal---|
func getValidatorBytesFromHeader(header *types.Header, chainConfig *chain.Config, parliaConfig *chain.ParliaConfig) []byte {
	if len(header.Extra) <= extraVanity+extraSeal {
		return nil
	}

	if !chainConfig.IsLuban(header.Number.Uint64()) {
		if header.Number.Uint64()%parliaConfig.Epoch == 0 && (len(header.Extra)-extraSeal-extraVanity)%validatorBytesLengthBeforeLuban != 0 {
			return nil
		}
		return header.Extra[extraVanity : len(header.Extra)-extraSeal]
	}

	if header.Number.Uint64()%parliaConfig.Epoch != 0 {
		return nil
	}
	num := int(header.Extra[extraVanity])
	start := extraVanity + validatorNumberSize
	end := start + num*validatorBytesLength
	extraMinLen := end + extraSeal
	if chainConfig.IsBohr(header.Number.Uint64(), header.Time) {
		extraMinLen += turnLengthSize
	}
	if num == 0 || len(header.Extra) < extraMinLen {
		return nil
	}
	return header.Extra[start:end]
}

// getVoteAttestationFromHeader returns the vote attestation extracted from the header's extra field if exists.
func getVoteAttestationFromHeader(header *types.Header, chainConfig *chain.Config, parliaConfig *chain.ParliaConfig) (*types.VoteAttestation, error) {
	if len(header.Extra) <= extraVanity+extraSeal {
		return nil, nil
	}

	if !chainConfig.IsLuban(header.Number.Uint64()) {
		return nil, nil
	}

	var attestationBytes []byte
	if header.Number.Uint64()%parliaConfig.Epoch != 0 {
		attestationBytes = header.Extra[extraVanity : len(header.Extra)-extraSeal]
	} else {
		num := int(header.Extra[extraVanity])
		start := extraVanity + validatorNumberSize + num*validatorBytesLength
		if chainConfig.IsBohr(header.Number.Uint64(), header.Time) {
			start += turnLengthSize
		}
		end := len(header.Extra) - extraSeal
		if end <= start {
			return nil, nil
		}
		attestationBytes = header.Extra[start:end]
	}

	var attestation types.VoteAttestation
	if err := rlp.Decode(bytes.NewReader(attestationBytes), &attestation); err != nil {
		return nil, fmt.Errorf("block %d has vote attestation info, decode err: %s", header.Number.Uint64(), err)
	}
	return &attestation, nil
}

// getParent returns the parent of a given block.
func (p *Parlia) getParent(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) (*types.Header, error) {
	var parent *types.Header
	number := header.Number.Uint64()
	if len(parents) > 0 {
		parent = parents[len(parents)-1]
	} else {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}

	if parent == nil || parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return nil, fmt.Errorf("number = %v, hash = %v, err = %v", number-1, header.ParentHash, consensus.ErrUnknownAncestor)
	}
	return parent, nil
}

// verifyVoteAttestation checks whether the vote attestation in the header is valid.
func (p *Parlia) verifyVoteAttestation(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	attestation, err := getVoteAttestationFromHeader(header, p.chainConfig, p.config)
	if err != nil {
		return err
	}
	if attestation == nil {
		return nil
	}
	if attestation.Data == nil {
		return errors.New("invalid attestation, vote data is nil")
	}
	if len(attestation.Extra) > types.MaxAttestationExtraLength {
		return fmt.Errorf("invalid attestation, too large extra length: %d", len(attestation.Extra))
	}

	parent, err := p.getParent(chain, header, parents)
	if err != nil {
		return err
	}

	// The target block should be direct parent.
	targetNumber := attestation.Data.TargetNumber
	targetHash := attestation.Data.TargetHash
	if targetNumber != parent.Number.Uint64() || targetHash != parent.Hash() {
		return fmt.Errorf("invalid attestation, target mismatch, expected block: %d, hash: %s; real block: %d, hash: %s",
			parent.Number.Uint64(), parent.Hash(), targetNumber, targetHash)
	}

	// The source block should be the highest justified block.
	sourceNumber := attestation.Data.SourceNumber
	sourceHash := attestation.Data.SourceHash
	justifiedBlockNumber, justifiedBlockHash, err := p.GetJustifiedNumberAndHash(chain, parent)
	if err != nil {
		return errors.New("unexpected error when getting the highest justified number and hash")
	}
	if sourceNumber != justifiedBlockNumber || sourceHash != justifiedBlockHash {
		return fmt.Errorf("invalid attestation, source mismatch, expected block: %d, hash: %s; real block: %d, hash: %s",
			justifiedBlockNumber, justifiedBlockHash, sourceNumber, sourceHash)
	}

	// The snapshot should be the targetNumber-1 block's snapshot.
	if len(parents) > 1 {
		parents = parents[:len(parents)-1]
	} else {
		parents = nil
	}
	snap, err := p.snapshot(chain, parent.Number.Uint64()-1, parent.ParentHash, parents, true)
	if err != nil {
		return err
	}

	// Filter out valid validator from attestation.
	validators := snap.validators()
	validatorsBitSet := bitset.From([]uint64{uint64(attestation.VoteAddressSet)})
	if validatorsBitSet.Count() > uint(len(validators)) {
		return errors.New("invalid attestation, vote number larger than validators number")
	}
	votedAddrs := make([]bls.PublicKey, 0, validatorsBitSet.Count())
	for index, val := range validators {
		if !validatorsBitSet.Test(uint(index)) {
			continue
		}

		voteAddr, err := bls.NewPublicKeyFromBytes(snap.Validators[val].VoteAddress[:])
		if err != nil {
			return fmt.Errorf("BLS public key converts failed: %v", err)
		}
		votedAddrs = append(votedAddrs, voteAddr)
	}

	// The valid voted validators should be no less than 2/3 validators.
	if len(votedAddrs) < math.CeilDiv(len(snap.Validators)*2, 3) {
		return errors.New("invalid attestation, not enough validators voted")
	}

	aggSig, err := bls.NewSignatureFromBytes(attestation.AggSignature[:])
	if err != nil {
		return fmt.Errorf("BLS signature converts failed: %v", err)
	}
	if !aggSig.VerifyAggregate(attestation.Data.Hash().Bytes(), votedAddrs) {
		return errors.New("invalid attestation, signature verify failed")
	}

	return nil
}

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (p *Parlia) verifyHeader(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	if header.Number == nil {
		return errUnknownBlock
	}

	// Don't waste time checking blocks from the future
	if header.Time > uint64(time.Now().Unix()) {
		return fmt.Errorf("header %d, time %d, now %d, %w", header.Number.Uint64(), header.Time, time.Now().Unix(), consensus.ErrFutureBlock)
	}

	if err := ValidateHeaderUnusedFields(header); err != nil {
		return err
	}

	// Check that the extra-data contains the vanity, validators and signature.
	if len(header.Extra) < extraVanity {
		return errMissingVanity
	}
	if len(header.Extra) < extraVanity+extraSeal {
		return errMissingSignature
	}

	// check extra data
	number := header.Number.Uint64()
	isEpoch := number%p.config.Epoch == 0

	// Ensure that the extra-data contains a signer list on checkpoint, but none otherwise
	signersBytes := getValidatorBytesFromHeader(header, p.chainConfig, p.config)
	if !isEpoch && len(signersBytes) != 0 {
		return errExtraValidators
	}

	if isEpoch && len(signersBytes) == 0 {
		return errInvalidSpanValidators
	}

	// Ensure that the mix digest is zero as we don't have fork protection currently
	if header.MixDigest != (libcommon.Hash{}) {
		return errInvalidMixDigest
	}
	// Ensure that the block doesn't contain any uncles which are meaningless in PoA
	if header.UncleHash != uncleHash {
		return errInvalidUncleHash
	}
	// Ensure that the block's difficulty is meaningful (may not be correct at this point)
	if number > 0 {
		if header.Difficulty == nil {
			return errInvalidDifficulty
		}
	}

	parent, err := p.getParent(chain, header, parents)
	if err != nil {
		return err
	}

	// Verify the block's gas usage and (if applicable) verify the base fee.
	if !chain.Config().IsLondon(header.Number.Uint64()) {
		// Verify BaseFee not present before EIP-1559 fork.
		if header.BaseFee != nil {
			return fmt.Errorf("invalid baseFee before fork: have %d, expected 'nil'", header.BaseFee)
		}
	} else if err := misc.VerifyEip1559Header(chain.Config(), parent, header, false); err != nil {
		// Verify the header's EIP-1559 attributes.
		return err
	}

	// Verify the existence / non-existence of excessBlobGas
	cancun := chain.Config().IsCancun(header.Number.Uint64(), header.Time)
	if !cancun {
		if err := misc.VerifyBscAbsenceOfCancunHeaderFields(header); err != nil {
			return err
		}
	} else {
		bohr := chain.Config().IsBohr(header.Number.Uint64(), header.Time)
		if bohr {
			if err := misc.VerifyPresenceOfBohrHeaderFields(header); err != nil {
				return err
			}
		} else {
			if err := misc.VerifyBscPresenceOfCancunHeaderFields(header); err != nil {
				return err
			}
		}
	}

	prague := chain.Config().IsPrague(header.Time)
	if !prague {
		if header.RequestsHash != nil {
			return fmt.Errorf("invalid RequestsHash, have %#x, expected nil", header.RequestsHash)
		}
	} else {
		if header.RequestsHash == nil {
			return errors.New("header has nil RequestsHash after Prague")
		}
	}

	// All basic checks passed, verify cascading fields
	return p.verifyCascadingFields(chain, header, parents)
}

// ValidateHeaderUnusedFields validates that unused fields are empty.
func ValidateHeaderUnusedFields(header *types.Header) error {
	// Ensure that the mix digest is zero as we don't have fork protection currently
	if header.MixDigest != (libcommon.Hash{}) {
		return errInvalidMixDigest
	}

	// Ensure that the block doesn't contain any uncles which are meaningless in PoA
	if header.UncleHash != uncleHash {
		return errInvalidUncleHash
	}

	return nil
}

// verifyCascadingFields verifies all the header fields that are not standalone,
// rather depend on a batch of previous headers. The caller may optionally pass
// in a batch of parents (ascending order) to avoid looking those up from the
// database. This is useful for concurrently verifying a batch of new headers.
func (p *Parlia) verifyCascadingFields(chain consensus.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	// The genesis block is the always valid dead-end
	number := header.Number.Uint64()
	if number == 0 {
		return nil
	}

	parent, err := p.getParent(chain, header, parents)
	if err != nil {
		return err
	}

	snap, err := p.snapshot(chain, number-1, header.ParentHash, parents, true /* verify */)
	if err != nil {
		return err
	}

	err = p.blockTimeVerifyForRamanujanFork(snap, header, parent)
	if err != nil {
		return err
	}

	// Verify that the gas limit is <= 2^63-1
	capacity := uint64(0x7fffffffffffffff)
	if header.GasLimit > capacity {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, capacity)
	}
	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}

	// Verify that the gas limit remains within allowed bounds
	diff := int64(parent.GasLimit) - int64(header.GasLimit)
	if diff < 0 {
		diff *= -1
	}
	limit := parent.GasLimit / params.GasLimitBoundDivisor

	if uint64(diff) >= limit || header.GasLimit < params.MinGasLimit {
		return fmt.Errorf("invalid gas limit: have %d, want %d += %d", header.GasLimit, parent.GasLimit, limit)
	}

	// Verify vote attestation for fast finality.
	if err := p.verifyVoteAttestation(chain, header, parents); err != nil {
		p.logger.Warn("Verify vote attestation failed", "error", err, "hash", header.Hash(), "number", header.Number,
			"parent", header.ParentHash, "coinbase", header.Coinbase, "extra", libcommon.Bytes2Hex(header.Extra))
		if chain.Config().IsPlato(header.Number.Uint64()) {
			return err
		}
	}

	// All basic checks passed, verify the seal and return
	return p.verifySeal(header, snap)
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (p *Parlia) verifySeal(header *types.Header, snap *Snapshot) error {
	// Verifying the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}

	// Resolve the authorization key and check against validators
	signer, err := ecrecover(header, p.signatures, p.chainConfig.ChainID)
	if err != nil {
		return err
	}

	if signer != header.Coinbase {
		return errCoinBaseMisMatch
	}

	if _, ok := snap.Validators[signer]; !ok {
		return fmt.Errorf("parlia.verifySeal: headerNum=%d, validator=%x, %w", header.Number.Uint64(), signer.Bytes(), errUnauthorizedValidator)
	}

	if snap.SignRecently(signer) {
		return errRecentlySigned
	}

	// Ensure that the difficulty corresponds to the turn-ness of the signer
	if !p.fakeDiff {
		inturn := snap.inturn(signer)
		if inturn && header.Difficulty.Cmp(diffInTurn) != 0 {
			return errWrongDifficulty
		}
		if !inturn && header.Difficulty.Cmp(diffNoTurn) != 0 {
			return errWrongDifficulty
		}
	}

	return nil
}

// snapshot retrieves the authorization snapshot at a given point in time.
// !!! be careful
// the block with `number` and `hash` is just the last element of `parents`,
// unlike other interfaces such as verifyCascadingFields, `parents` are real parents
func (p *Parlia) snapshot(chain consensus.ChainHeaderReader, number uint64, hash libcommon.Hash, parents []*types.Header, verify bool) (*Snapshot, error) {
	// Search for a snapshot in memory or on disk for checkpoints
	var (
		headers []*types.Header
		snap    *Snapshot
	)

	for snap == nil {
		// If an in-memory snapshot was found, use that
		if s, ok := p.recentSnaps.Get(hash); ok {
			snap = s
			break
		}

		// If an on-disk checkpoint snapshot can be found, use that
		if number%CheckpointInterval == 0 {
			if s, err := loadSnapshot(p.config, p.signatures, p.db, number, hash); err == nil {
				p.logger.Trace("Loaded snapshot from disk", "number", number, "hash", hash)
				snap = s
				if !verify || snap != nil {
					break
				}
			}
		}
		// If we're at the genesis, snapshot the initial state.
		if number == 0 {
			// Headers included into the snapshots have to be trusted as checkpoints
			checkpoint := chain.GetHeader(hash, number)
			if checkpoint != nil {
				// get validators from headers
				validators, voteAddrs, err := parseValidators(checkpoint, p.chainConfig, p.config)
				if err != nil {
					return nil, err
				}
				// new snapshot
				snap = newSnapshot(p.config, p.signatures, number, hash, validators, voteAddrs)
				if err := snap.store(p.db); err != nil {
					return nil, err
				}
				p.logger.Info("Stored checkpoint snapshot to disk", "number", number, "hash", hash)
				break
			}
		}

		// No snapshot for this header, gather the header and move backward
		var header *types.Header
		if len(parents) > 0 {
			// If we have explicit parents, pick from there (enforced)
			header = parents[len(parents)-1]
			if header.Hash() != hash || header.Number.Uint64() != number {
				return nil, consensus.ErrUnknownAncestor
			}
			parents = parents[:len(parents)-1]
		} else {
			header = chain.GetHeader(hash, number)
			if header == nil {
				return nil, consensus.ErrUnknownAncestor
			}
		}
		headers = append(headers, header)
		number, hash = number-1, header.ParentHash
	}

	// check if snapshot is nil
	if snap == nil {
		return nil, fmt.Errorf("unknown error while retrieving snapshot at block number %v", number)
	}

	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	snap, err := snap.apply(headers, chain, parents, p.chainConfig, p.recentSnaps, false)
	if err != nil {
		return nil, err
	}

	// If we've generated a new checkpoint snapshot, save to disk
	if verify && snap.Number%CheckpointInterval == 0 && len(headers) > 0 {
		if err = snap.store(p.db); err != nil {
			return nil, err
		}
		p.logger.Trace("Stored snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}
	return snap, err
}

// VerifyUncles verifies that the given block's uncles conform to the consensus
// rules of a given engine.
func (p *Parlia) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	if len(uncles) > 0 {
		return errors.New("uncles not allowed")
	}
	return nil
}

// Prepare initializes the consensus fields of a block header according to the
// rules of a particular engine. The changes are executed inline.
func (p *Parlia) Prepare(chain consensus.ChainHeaderReader, header *types.Header, ibs *state.IntraBlockState) error {
	return nil
}

func (p *Parlia) verifyValidators(header, parentHeader *types.Header, state *state.IntraBlockState) error {
	if (header.Number.Uint64())%p.config.Epoch != 0 {
		return nil
	}

	newValidators, voteAddressMap, err := p.getCurrentValidators(parentHeader, state)
	if err != nil {
		return nil
	}

	// sort validator by address
	sort.Sort(validatorsAscending(newValidators))
	var validatorsBytes []byte
	validatorsNumber := len(newValidators)

	if !p.chainConfig.IsLuban(header.Number.Uint64()) {
		validatorsBytes = make([]byte, validatorsNumber*validatorBytesLengthBeforeLuban)
		for i, validator := range newValidators {
			copy(validatorsBytes[i*validatorBytesLengthBeforeLuban:], validator.Bytes())
		}
	} else {
		if uint8(validatorsNumber) != header.Extra[extraVanity] {
			p.logger.Error("verifyValidators failed", "len(validatorsNumber)", validatorsNumber, "header.Extra[extraVanity]", header.Extra[extraVanity])
			return errMismatchingEpochValidators
		}
		validatorsBytes = make([]byte, validatorsNumber*validatorBytesLength)
		if p.chainConfig.IsOnLuban(header.Number) {
			voteAddressMap = make(map[libcommon.Address]*types.BLSPublicKey, len(newValidators))
			var zeroBlsKey types.BLSPublicKey
			for _, validator := range newValidators {
				voteAddressMap[validator] = &zeroBlsKey
			}
		}
		for i, validator := range newValidators {
			copy(validatorsBytes[i*validatorBytesLength:], validator.Bytes())
			copy(validatorsBytes[i*validatorBytesLength+length.Addr:], voteAddressMap[validator].Bytes())
		}
	}
	if !bytes.Equal(getValidatorBytesFromHeader(header, p.chainConfig, p.config), validatorsBytes) {
		return errMismatchingEpochValidators
	}
	return nil
}

func (p *Parlia) verifyTurnLength(chain consensus.ChainHeaderReader, header *types.Header, ibs *state.IntraBlockState) error {
	if header.Number.Uint64()%p.config.Epoch != 0 ||
		!p.chainConfig.IsBohr(header.Number.Uint64(), header.Time) {
		return nil
	}

	turnLengthFromHeader, err := parseTurnLength(header, p.chainConfig, p.config)
	if err != nil {
		return err
	}
	if turnLengthFromHeader != nil {
		turnLength, err := p.getTurnLength(chain, header, ibs)
		if err != nil {
			return err
		}
		if turnLength != nil && *turnLength == *turnLengthFromHeader {
			log.Trace("verifyTurnLength", "turnLength", *turnLength)
			return nil
		}
	}

	return errMismatchingEpochTurnLength
}

// Initialize runs any pre-transaction state modifications (e.g. epoch start)
func (p *Parlia) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header,
	state *state.IntraBlockState, syscall consensus.SysCallCustom, logger log.Logger, tracer *tracing.Hooks) error {
	var err error
	parentHeader := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	if err = p.verifyValidators(header, parentHeader, state); err != nil {
		return err
	}
	if err = p.verifyTurnLength(chain, header, state); err != nil {
		return err
	}

	// store block hashes for EIP-2935 (BEP440) upgrade
	if config.IsPrague(header.Time) {
		misc.StoreBlockHashesEip2935(header, state)
	}

	// update validators every day
	if p.chainConfig.IsFeynman(header.Number.Uint64(), header.Time) && isBreatheBlock(parentHeader.Time, header.Time) {
		// we should avoid update validators in the Feynman upgrade block
		if !p.chainConfig.IsOnFeynman(header.Number, parentHeader.Time, header.Time) {
			validatorItemsCache, err = p.getValidatorElectionInfo(parentHeader, state)
			if err != nil {
				return err
			}
			maxElectedValidatorsCache, err = p.getMaxElectedValidators(parentHeader, state)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Parlia) splitTxs(txs types.Transactions, header *types.Header) (userTxs types.Transactions, systemTxs types.Transactions, err error) {
	userTxs = types.Transactions{}
	systemTxs = types.Transactions{}
	for _, tx := range txs {
		isSystemTx, err2 := p.IsSystemTransaction(tx, header)
		if err2 != nil {
			err = err2
			return
		}
		if isSystemTx {
			systemTxs = append(systemTxs, tx)
		} else {
			userTxs = append(userTxs, tx)
		}
	}
	return
}

// Finalize runs any post-transaction state modifications (e.g. block rewards)
// but does not assemble the block.
//
// Note: The block header and state database might be updated to reflect any
// consensus rules that happen at finalization (e.g. block rewards).
func (p *Parlia) Finalize(_ *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, _ []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, systemTxCall consensus.SystemTxCall, txIndex int,
	logger log.Logger) (types.Transactions, types.Receipts, types.FlatRequests, error) {
	return p.finalize(header, state, txs, receipts, chain, false, systemTxCall, txIndex, logger)
}

func (p *Parlia) finalize(header *types.Header, ibs *state.IntraBlockState, txs types.Transactions,
	receipts types.Receipts, chain consensus.ChainHeaderReader, mining bool, systemTxCall consensus.SystemTxCall,
	txIndex int, logger log.Logger) (types.Transactions, types.Receipts, types.FlatRequests, error) {
	userTxs, systemTxs, err := p.splitTxs(txs, header)
	if err != nil {
		return nil, nil, nil, err
	}
	curIndex := userTxs.Len()
	// warn if not in majority fork
	number := header.Number.Uint64()
	snap, err := p.snapshot(chain, number-1, header.ParentHash, nil, false /* verify */)
	if err != nil {
		return nil, nil, nil, err
	}
	// If the block is an epoch end block, verify the validator list
	// The verification can only be done when the state is ready, it can't be done in VerifyHeader.
	parentHeader := chain.GetHeader(header.ParentHash, number-1)

	var finish bool
	defer func() {
		if txIndex == len(txs)-1 && finish {
			if fs := finality.GetFinalizationService(); fs != nil {
				curSnap, _ := p.snapshot(chain, number, header.Hash(), nil, true)
				if curSnap != nil && curSnap.Attestation != nil {
					fs.UpdateFinality(curSnap.Attestation.SourceHash, curSnap.Attestation.TargetHash)
				}
			}
		}
	}()

	if curIndex == txIndex {
		if p.chainConfig.IsFeynman(header.Number.Uint64(), header.Time) {
			systemcontracts.UpgradeBuildInSystemContract(p.chainConfig, header.Number, parentHeader.Time, header.Time, ibs, logger)
		}
	}

	if p.chainConfig.IsOnFeynman(header.Number, parentHeader.Time, header.Time) {
		finish, err = p.initializeFeynmanContract(ibs, header, &txs, &receipts, &systemTxs, &header.GasUsed, false, systemTxCall, &curIndex, &txIndex)
		if err != nil {
			log.Error("init feynman contract failed", "error", err)
			return nil, nil, nil, fmt.Errorf("init feynman contract failed: %v", err)
		} else if finish {
			return nil, nil, nil, nil
		}
	}
	// No block rewards in PoA, so the state remains as is and uncles are dropped
	if number == 1 {
		finish, err = p.initContract(ibs, header, &txs, &receipts, &systemTxs, &header.GasUsed, mining, systemTxCall, &curIndex, &txIndex)
		if err != nil {
			p.logger.Error("[parlia] init contract failed", "err", err)
			return nil, nil, nil, fmt.Errorf("init contract failed: %v", err)
		} else if finish {
			return nil, nil, nil, nil
		}
	}
	if header.Difficulty.Cmp(diffInTurn) != 0 {
		spoiledVal := snap.inturnValidator()
		signedRecently := false
		if p.chainConfig.IsPlato(header.Number.Uint64()) {
			if snap.SignRecently(spoiledVal) {
				signedRecently = true
			}
		} else {
			for _, recent := range snap.Recents {
				if recent == spoiledVal {
					signedRecently = true
					break
				}
			}
		}
		if !signedRecently {
			//log.Trace("slash validator", "block hash", header.Hash(), "address", spoiledVal)
			finish, err = p.slash(spoiledVal, ibs, header, &txs, &receipts, &systemTxs, &header.GasUsed, mining, systemTxCall, &curIndex, &txIndex)
			if err != nil {
				// it is possible that slash validator failed because of the slash channel is disabled.
				p.logger.Error("slash validator failed", "block hash", header.Hash(), "address", spoiledVal, "error", err)
			} else if finish {
				return nil, nil, nil, nil
			}
		}
	}
	if !p.chainConfig.IsKepler(header.Number.Uint64(), header.Time) {
		finish, err = p.distributeToSystem(header.Coinbase, ibs, header, &txs, &receipts, &systemTxs, &header.GasUsed, mining, systemTxCall, &curIndex, &txIndex)
		if err != nil || finish {
			//log.Error("distributeIncoming", "block hash", header.Hash(), "error", err, "systemTxs", len(systemTxs))
			return nil, nil, nil, err
		}
	}

	finish, err = p.distributeToValidator(header.Coinbase, ibs, header, &txs, &receipts, &systemTxs, &header.GasUsed, mining, systemTxCall, &curIndex, &txIndex)
	if err != nil || finish {
		//log.Error("distributeIncoming", "block hash", header.Hash(), "error", err, "systemTxs", len(systemTxs))
		return nil, nil, nil, err
	}

	if p.chainConfig.IsPlato(header.Number.Uint64()) {
		finish, err = p.distributeFinalityReward(chain, ibs, header, &txs, &receipts, &systemTxs, &header.GasUsed, false, systemTxCall, &curIndex, &txIndex)
		if err != nil {
			return nil, nil, nil, err
		} else if finish {
			return nil, nil, nil, nil
		}
	}
	// update validators every day
	if p.chainConfig.IsFeynman(header.Number.Uint64(), header.Time) && isBreatheBlock(parentHeader.Time, header.Time) {
		// we should avoid update validators in the Feynman upgrade block
		if !p.chainConfig.IsOnFeynman(header.Number, parentHeader.Time, header.Time) {
			finish, err = p.updateValidatorSetV2(chain, ibs, header, &txs, &receipts, &systemTxs, &header.GasUsed, false, systemTxCall, &curIndex, &txIndex)
			if err != nil {
				return nil, nil, nil, err
			} else if finish {
				return nil, nil, nil, nil
			}
		}
	}
	return nil, nil, nil, nil
}

func (p *Parlia) distributeFinalityReward(chain consensus.ChainHeaderReader, state *state.IntraBlockState, header *types.Header,
	txs *types.Transactions, receipts *types.Receipts, systemTxs *types.Transactions,
	usedGas *uint64, mining bool, systemTxCall consensus.SystemTxCall, curIndex *int, txIndex *int) (bool, error) {
	currentHeight := header.Number.Uint64()
	epoch := p.config.Epoch
	chainConfig := chain.Config()
	if currentHeight%epoch != 0 {
		return false, nil
	}

	head := header
	accumulatedWeights := make(map[libcommon.Address]uint64)
	for height := currentHeight - 1; height+epoch >= currentHeight && height >= 1; height-- {
		head = chain.GetHeaderByHash(head.ParentHash)
		if head == nil {
			return true, fmt.Errorf("header is nil at height %d", height)
		}
		voteAttestation, err := getVoteAttestationFromHeader(head, chainConfig, p.config)
		if err != nil {
			return true, err
		}
		if voteAttestation == nil {
			continue
		}
		justifiedBlock := chain.GetHeaderByHash(voteAttestation.Data.TargetHash)
		if justifiedBlock == nil {
			p.logger.Warn("justifiedBlock is nil at height %d", voteAttestation.Data.TargetNumber)
			continue
		}

		snap, err := p.snapshot(chain, justifiedBlock.Number.Uint64()-1, justifiedBlock.ParentHash, nil, true)
		if err != nil {
			return true, err
		}
		validators := snap.validators()
		validatorsBitSet := bitset.From([]uint64{uint64(voteAttestation.VoteAddressSet)})
		if validatorsBitSet.Count() > uint(len(validators)) {
			p.logger.Error("invalid attestation, vote number larger than validators number")
			continue
		}
		validVoteCount := 0
		for index, val := range validators {
			if validatorsBitSet.Test(uint(index)) {
				accumulatedWeights[val] += 1
				validVoteCount += 1
			}
		}
		quorum := math.CeilDiv(len(snap.Validators)*2, 3)
		if validVoteCount > quorum {
			accumulatedWeights[head.Coinbase] += uint64(float64(validVoteCount-quorum) * collectAdditionalVotesRewardRatio)
		}
	}

	validators := make([]libcommon.Address, 0, len(accumulatedWeights))
	weights := make([]*big.Int, 0, len(accumulatedWeights))
	for val := range accumulatedWeights {
		validators = append(validators, val)
	}
	sort.Sort(validatorsAscending(validators))
	for _, val := range validators {
		weights = append(weights, big.NewInt(int64(accumulatedWeights[val])))
	}

	// generate system transaction
	method := "distributeFinalityReward"
	data, err := p.validatorSetABI.Pack(method, validators, weights)
	if err != nil {
		p.logger.Error("Unable to pack tx for distributeFinalityReward", "error", err)
		return true, err
	}
	if *curIndex == *txIndex {
		return p.applyTransaction(header.Coinbase, systemcontracts.ValidatorContract, u256.Num0, data, state, header,
			txs, receipts, systemTxs, usedGas, mining, systemTxCall, curIndex)
	}
	*curIndex++
	return false, err
}

// FinalizeAndAssemble runs any post-transaction state modifications (e.g. block
// rewards) and assembles the final block.
//
// Note: The block header and state database might be updated to reflect any
// consensus rules that happen at finalization (e.g. block rewards).
func (p *Parlia) FinalizeAndAssemble(chainConfig *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger,
) (*types.Block, types.Transactions, types.Receipts, types.FlatRequests, error) {

	outTxs, outReceipts, _, err := p.finalize(header, ibs, txs, receipts, chain, true, nil, 0, logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return types.NewBlock(header, outTxs, nil, outReceipts, withdrawals), outTxs, outReceipts, nil, nil
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (p *Parlia) Authorize(val libcommon.Address, signFn SignFn) {
	p.signerLock.Lock()
	defer p.signerLock.Unlock()

	p.val = val
	p.signFn = signFn
}

// Seal generates a new sealing request for the given input block and pushes
// the result into the given channel.
//
// Note, the method returns immediately and will send the result async. More
// than one result may also be returned depending on the consensus algorithm.
func (p *Parlia) Seal(chain consensus.ChainHeaderReader, blockWithReceipts *types.BlockWithReceipts, results chan<- *types.BlockWithReceipts, stop <-chan struct{}) error {
	return nil
}

func encodeSigHeaderWithoutVoteAttestation(w io.Writer, header *types.Header, chainId *big.Int) {
	err := rlp.Encode(w, []interface{}{
		chainId,
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:extraVanity], // this will panic if extra is too short, should check before calling encodeSigHeaderWithoutVoteAttestation
		header.MixDigest,
		header.Nonce,
	})
	if err != nil {
		panic("can't encode: " + err.Error())
	}
}

// SealHash returns the hash of a block prior to it being sealed.
func (p *Parlia) SealHash(header *types.Header) (hash libcommon.Hash) {
	hasher := cryptopool.NewLegacyKeccak256()
	defer cryptopool.ReturnToPoolKeccak256(hasher)

	encodeSigHeaderWithoutVoteAttestation(hasher, header, p.chainConfig.ChainID)
	hasher.Sum(hash[:0])
	return hash
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have.
func (p *Parlia) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash libcommon.Hash, _ uint64) *big.Int {
	snap, err := p.snapshot(chain, parentNumber, parentHash, nil, false /* verify */)
	if err != nil {
		return nil
	}
	return CalcDifficulty(snap, p.val)
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have based on the previous blocks in the chain and the
// current signer.
func CalcDifficulty(snap *Snapshot, signer libcommon.Address) *big.Int {
	if snap.inturn(signer) {
		return new(big.Int).Set(diffInTurn)
	}
	return new(big.Int).Set(diffNoTurn)
}

func (p *Parlia) GenerateSeal(chain consensus.ChainHeaderReader, current, parent *types.Header, call consensus.Call) []byte {
	return nil
}

// APIs returns the RPC APIs this consensus engine provides.
func (p *Parlia) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{{
		Namespace: "parlia",
		Version:   "1.0",
		Service:   &API{chain: chain, parlia: p},
		Public:    false,
	}}
}

func (p *Parlia) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	return false
}

func (p *Parlia) IsSystemTransaction(tx types.Transaction, header *types.Header) (bool, error) {
	// deploy a contract
	if tx.GetTo() == nil {
		return false, nil
	}
	sender, err := tx.Sender(*p.signer)
	if err != nil {
		return false, errors.New("UnAuthorized transaction")
	}
	if sender == header.Coinbase && core.IsToSystemContract(*tx.GetTo()) && tx.GetPrice().IsZero() {
		return true, nil
	}
	return false, nil
}

func (p *Parlia) IsSystemContract(to *libcommon.Address) bool {
	if to == nil {
		return false
	}
	return core.IsToSystemContract(*to)
}

func (p *Parlia) EnoughDistance(chain consensus.ChainReader, header *types.Header) bool {
	snap, err := p.snapshot(chain, header.Number.Uint64()-1, header.ParentHash, nil, false /* verify */)
	if err != nil {
		return true
	}
	return snap.enoughDistance(p.val, header)
}

func (p *Parlia) IsLocalBlock(header *types.Header) bool {
	return p.val == header.Coinbase
}

func (p *Parlia) AllowLightProcess(chain consensus.ChainReader, currentHeader *types.Header) bool {
	snap, err := p.snapshot(chain, currentHeader.Number.Uint64()-1, currentHeader.ParentHash, nil, false /* verify */)
	if err != nil {
		return true
	}

	idx := snap.indexOfVal(p.val)
	// validator is not allowed to diff sync
	return idx < 0
}

// Close terminates any background threads maintained by the consensus engine.
func (p *Parlia) Close() error {
	return nil
}

// ==========================  interaction with contract/account =========

// getCurrentValidators get current validators
func (p *Parlia) getCurrentValidators(header *types.Header, ibs *state.IntraBlockState) ([]libcommon.Address, map[libcommon.Address]*types.BLSPublicKey, error) {
	// This is actually the parentNumber
	if !p.chainConfig.IsLuban(header.Number.Uint64()) {
		validators, err := p.getCurrentValidatorsBeforeLuban(header, ibs)
		return validators, nil, err
	}

	// method
	method := "getMiningValidators"

	data, err := p.validatorSetABI.Pack(method)
	if err != nil {
		p.logger.Error("Unable to pack tx for getMiningValidators", "err", err)
		return nil, nil, err
	}
	msgData := hexutility.Bytes(data)
	_, returnData, err := p.systemCall(header.Coinbase, systemcontracts.ValidatorContract, msgData[:], ibs, header, u256.Num0)
	if err != nil {
		return nil, nil, err
	}
	var valSet []libcommon.Address
	var voteAddrSet []types.BLSPublicKey

	if err := p.validatorSetABI.UnpackIntoInterface(&[]interface{}{&valSet, &voteAddrSet}, method, returnData); err != nil {
		return nil, nil, err
	}

	voteAddrmap := make(map[libcommon.Address]*types.BLSPublicKey, len(valSet))
	for i := 0; i < len(valSet); i++ {
		voteAddrmap[valSet[i]] = &(voteAddrSet)[i]
	}
	return valSet, voteAddrmap, nil
}

// slash spoiled validators
func (p *Parlia) slash(spoiledVal libcommon.Address, state *state.IntraBlockState, header *types.Header,
	txs *types.Transactions, receipts *types.Receipts, systemTxs *types.Transactions, usedGas *uint64, mining bool,
	systemTxCall consensus.SystemTxCall, curIndex *int, txIndex *int) (bool, error) {
	// method
	method := "slash"

	// get packed data
	data, err := p.slashABI.Pack(method,
		spoiledVal,
	)
	if err != nil {
		p.logger.Error("[parlia] Unable to pack tx for slash", "err", err)
		return false, err
	}
	// apply message
	if *curIndex == *txIndex {
		return p.applyTransaction(header.Coinbase, systemcontracts.SlashContract, u256.Num0, data, state, header, txs, receipts, systemTxs, usedGas, mining, systemTxCall, curIndex)
	}
	*curIndex++
	return false, nil
}

// init contract
func (p *Parlia) initContract(state *state.IntraBlockState, header *types.Header,
	txs *types.Transactions, receipts *types.Receipts, systemTxs *types.Transactions,
	usedGas *uint64, mining bool, systemTxCall consensus.SystemTxCall, curIndex *int, txIndex *int,
) (finish bool, err error) {
	// method
	method := "init"
	// contracts
	contracts := []libcommon.Address{
		systemcontracts.ValidatorContract,
		systemcontracts.SlashContract,
		systemcontracts.LightClientContract,
		systemcontracts.RelayerHubContract,
		systemcontracts.TokenHubContract,
		systemcontracts.RelayerIncentivizeContract,
		systemcontracts.CrossChainContract,
	}
	// get packed data
	data, err := p.validatorSetABI.Pack(method)
	if err != nil {
		p.logger.Error("[parlia] Unable to pack tx for init validator set", "err", err)
		return false, err
	}
	for _, c := range contracts {
		// p.logger.Info("Init contracts", "len(systemTxs)", len(*systemTxs), "len(txs)", len(*txs))
		if *curIndex == *txIndex {
			return p.applyTransaction(header.Coinbase, c, u256.Num0, data, state, header, txs, receipts, systemTxs, usedGas, mining, systemTxCall, curIndex)
		}
		*curIndex++
	}
	return false, nil
}

func (p *Parlia) distributeToSystem(val libcommon.Address, ibs *state.IntraBlockState, header *types.Header,
	txs *types.Transactions, receipts *types.Receipts, systemTxs *types.Transactions,
	usedGas *uint64, mining bool, systemTxCall consensus.SystemTxCall, curIndex, txIndex *int) (bool, error) {
	if *curIndex == *txIndex {
		bal, err := ibs.GetBalance(consensus.SystemAddress)
		if err != nil {
			return false, err
		}
		balance := bal.Clone()
		if balance.Cmp(u256.Num0) <= 0 {
			return false, nil
		}
		systemReward, err := ibs.GetBalance(systemcontracts.SystemRewardContract)
		if err != nil {
			return false, err
		}
		doDistributeSysReward := systemReward.Cmp(maxSystemBalance) < 0
		if doDistributeSysReward {
			rewards := new(uint256.Int)
			rewards = rewards.Rsh(balance, systemRewardPercent)

			ibs.SetBalance(consensus.SystemAddress, balance.Sub(balance, rewards), tracing.BalanceChangeUnspecified)
			ibs.AddBalance(val, rewards, tracing.BalanceChangeUnspecified)
			if rewards.Cmp(u256.Num0) > 0 {
				return p.applyTransaction(val, systemcontracts.SystemRewardContract, rewards, nil, ibs, header,
					txs, receipts, systemTxs, usedGas, mining, systemTxCall, curIndex)
			}
		}
	} else if *(*txs)[*curIndex].GetTo() == systemcontracts.SystemRewardContract {
		*curIndex++
	}
	return false, nil
}

// distributeToValidator deposits validator reward to validator contract
func (p *Parlia) distributeToValidator(val libcommon.Address, ibs *state.IntraBlockState, header *types.Header,
	txs *types.Transactions, receipts *types.Receipts, systemTxs *types.Transactions,
	usedGas *uint64, mining bool, systemTxCall consensus.SystemTxCall, curIndex, txIndex *int) (bool, error) {

	if *curIndex == *txIndex {
		bal, err := ibs.GetBalance(consensus.SystemAddress)
		if err != nil {
			return false, err
		}
		balance := bal.Clone()
		if balance.Cmp(u256.Num0) <= 0 {
			return false, nil
		}
		ibs.SetBalance(consensus.SystemAddress, u256.Num0, tracing.BalanceChangeUnspecified)
		ibs.AddBalance(val, balance, tracing.BalanceChangeUnspecified)
		// method
		method := "deposit"

		// get packed data
		data, err := p.validatorSetABI.Pack(method,
			val,
		)
		if err != nil {
			p.logger.Error("[parlia] Unable to pack tx for deposit", "err", err)
			return true, err
		}
		// apply message
		return p.applyTransaction(val, systemcontracts.ValidatorContract, balance, data, ibs, header, txs, receipts, systemTxs, usedGas, mining, systemTxCall, curIndex)
	}
	*curIndex++
	return false, nil
}

func (p *Parlia) applyTransaction(from libcommon.Address, to libcommon.Address, value *uint256.Int, data []byte,
	ibs *state.IntraBlockState, header *types.Header, txs *types.Transactions, receipts *types.Receipts,
	systemTxs *types.Transactions, usedGas *uint64, mining bool, systemTxCall consensus.SystemTxCall, curIndex *int,
) (bool, error) {
	actualTx := (*txs)[*curIndex]
	expectedTx := types.Transaction(types.NewTransaction(actualTx.GetNonce(), to, value, math.MaxUint64/2, u256.Num0, data))
	expectedHash := expectedTx.SigningHash(p.chainConfig.ChainID)
	if len(*systemTxs) == 0 {
		return false, errors.New("supposed to get a actual transaction, but get none")
	}

	actualHash := actualTx.SigningHash(p.chainConfig.ChainID)
	if !bytes.Equal(actualHash.Bytes(), expectedHash.Bytes()) {
		return false, fmt.Errorf("expected system tx (hash %v, nonce %d, to %s, value %s, gas %d, gasPrice %s, data %s), actual tx (hash %v, nonce %d, to %s, value %s, gas %d, gasPrice %s, data %s)",
			expectedHash.String(),
			expectedTx.GetNonce(),
			expectedTx.GetTo().String(),
			expectedTx.GetValue().String(),
			expectedTx.GetGas(),
			expectedTx.GetPrice().String(),
			hex.EncodeToString(expectedTx.GetData()),
			actualHash.String(),
			actualTx.GetNonce(),
			actualTx.GetTo().String(),
			actualTx.GetValue().String(),
			actualTx.GetGas(),
			actualTx.GetPrice().String(),
			hex.EncodeToString(actualTx.GetData()),
		)
	}
	_, shouldBreak, err := systemTxCall(ibs)
	if err != nil {
		return false, err
	}
	return shouldBreak, nil
}

func (p *Parlia) systemCall(from, contract libcommon.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, value *uint256.Int) (gasUsed uint64, returnData []byte, err error) {
	chainConfig := p.chainConfig
	if chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	msg := types.NewMessage(
		from,
		&contract,
		0, value,
		math.MaxUint64/2, u256.Num0,
		nil, nil,
		data, nil, false,
		true, // isFree
		nil,
	)
	vmConfig := vm.Config{NoReceipts: true}
	// Create a new context to be used in the EVM environment
	blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, nil), p, &from, chainConfig)
	if chainConfig.IsCancun(header.Number.Uint64(), header.Time) {
		rules := chainConfig.Rules(header.Number.Uint64(), header.Time)
		ibs.Prepare(rules, msg.From(), blockContext.Coinbase, msg.To(), vm.ActivePrecompiles(rules), msg.AccessList(), nil)
	}
	evm := vm.NewEVM(blockContext, core.NewEVMTxContext(msg), ibs, chainConfig, vmConfig)

	ret, leftOverGas, err := evm.Call(
		vm.AccountRef(msg.From()),
		*msg.To(),
		msg.Data(),
		msg.Gas(),
		msg.Value(),
		false,
	)
	if err != nil {
		return 0, nil, err
	}
	return msg.Gas() - leftOverGas, ret, nil
}

// GetJustifiedNumberAndHash returns the highest justified block's number and hash on the branch including and before `header`
func (p *Parlia) GetJustifiedNumberAndHash(chain consensus.ChainHeaderReader, header *types.Header) (uint64, libcommon.Hash, error) {
	if chain == nil || header == nil {
		return 0, libcommon.Hash{}, errors.New("illegal chain or header")
	}
	snap, err := p.snapshot(chain, header.Number.Uint64(), header.Hash(), nil, true)
	if err != nil {
		p.logger.Error("GetJustifiedNumberAndHash snapshot",
			"error", err, "blockNumber", header.Number.Uint64(), "blockHash", header.Hash())
		return 0, libcommon.Hash{}, err
	}

	if snap.Attestation == nil {
		if p.chainConfig.IsLuban(header.Number.Uint64()) {
			p.logger.Debug("once one attestation generated, attestation of snap would not be nil forever basically")
		}
		return 0, chain.GetHeaderByNumber(0).Hash(), nil
	}
	return snap.Attestation.TargetNumber, snap.Attestation.TargetHash, nil
}

// GetFinalizedHeader returns highest finalized block header.
func (p *Parlia) GetFinalizedHeader(chain consensus.ChainHeaderReader, header *types.Header) *types.Header {
	if chain == nil || header == nil {
		return nil
	}
	if !chain.Config().IsPlato(header.Number.Uint64()) {
		return chain.GetHeaderByNumber(0)
	}

	snap, err := p.snapshot(chain, header.Number.Uint64(), header.Hash(), nil, true)
	if err != nil {
		p.logger.Error("GetFinalizedHeader snapshot",
			"error", err, "blockNumber", header.Number.Uint64(), "blockHash", header.Hash())
		return nil
	}

	if snap.Attestation != nil {
		return chain.GetHeader(snap.Attestation.SourceHash, snap.Attestation.SourceNumber)
	}
	return nil
}

func (c *Parlia) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall,
) ([]consensus.Reward, error) {
	return []consensus.Reward{}, nil
}

func (c *Parlia) GetTransferFunc() evmtypes.TransferFunc {
	return consensus.Transfer
}

func (c *Parlia) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

func (p *Parlia) blockTimeVerifyForRamanujanFork(snap *Snapshot, header, parent *types.Header) error {
	if p.chainConfig.IsRamanujan(header.Number.Uint64()) {
		if header.Time < parent.Time+p.config.Period+backOffTime(snap, header, header.Coinbase, p.chainConfig) {
			return fmt.Errorf("header %d, time %d, now %d, period: %d, backof: %d, %w", header.Number.Uint64(), header.Time, time.Now().Unix(), p.config.Period, backOffTime(snap, header, header.Coinbase, p.chainConfig), consensus.ErrFutureBlock)
		}
	}
	return nil
}

func (p *Parlia) GetLatestSnapshotHeight() (uint64, error) {
	return getLatestSnapshotHeight(p.db)
}

// ResetSnapshot  Fill consensus db from snapshot
func (p *Parlia) ResetSnapshot(chain consensus.ChainHeaderReader, headers []*types.Header) error {
	// Search for a snapshot in memory or on disk for checkpoints
	var snap *Snapshot

	checkpoint := headers[len(headers)-1]
	start := headers[0]
	// If we're at the genesis, snapshot the initial state.
	if checkpoint.Number.Uint64() == 0 {
		// Headers included into the snapshots have to be trusted as checkpoints get validators from headers
		validators, voteAddrs, err := parseValidators(checkpoint, p.chainConfig, p.config)
		if err != nil {
			return err
		}
		// new snapshot
		snap = newSnapshot(p.config, p.signatures, checkpoint.Number.Uint64(), checkpoint.Hash(), validators, voteAddrs)
		if err := snap.store(p.db); err != nil {
			return err
		}
		p.recentSnaps.Add(checkpoint.Hash(), snap)
		p.logger.Info("Stored checkpoint snapshot to disk", "number", checkpoint.Number.Uint64(), "hash", checkpoint.Hash())
		return nil
	}

	if s, ok := p.recentSnaps.Get(start.ParentHash); ok {
		snap = s
	} else if (start.Number.Uint64()-1)%CheckpointInterval == 0 {
		if s, err := loadSnapshot(p.config, p.signatures, p.db, start.Number.Uint64(), start.Hash()); err == nil {
			p.logger.Debug("Loaded snapshot from disk", "number", start.Number.Uint64(), "hash", start.Hash())
			snap = s
		}
	}

	// check if snapshot is nil
	if snap == nil {
		return fmt.Errorf("unknown error while retrieving snapshot at block number %v", checkpoint.Number.Uint64())
	}

	// Previous snapshot found, apply any pending headers on top of it
	snap, err := snap.apply(headers, chain, nil, p.chainConfig, p.recentSnaps, true)
	if err != nil {
		return err
	}
	if snap.Number%CheckpointInterval == 0 && len(headers) > 0 {
		if err = snap.store(p.db); err != nil {
			return err
		}
		log.Trace("Stored snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}
	return nil
}

type rwWrapper struct {
	kv.RoDB
}

func (w rwWrapper) Update(ctx context.Context, f func(tx kv.RwTx) error) error {
	return errors.New("Update not implemented")
}

func (w rwWrapper) UpdateNosync(ctx context.Context, f func(tx kv.RwTx) error) error {
	return errors.New("UpdateNosync not implemented")
}

func (w rwWrapper) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return nil, errors.New("BeginRw not implemented")
}

func (w rwWrapper) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return nil, errors.New("BeginRwNosync not implemented")
}

// NewRo is used by the rpcdaemon and tests which need read only access to the provided data services
func NewRo(chainConfig *chain.Config, db kv.RoDB, blockReader services.FullBlockReader, logger log.Logger) *Parlia {
	// get bor config
	parliaConfig := chainConfig.Parlia

	// Set any missing consensus parameters to their defaults
	if parliaConfig != nil && parliaConfig.Epoch == 0 {
		parliaConfig.Epoch = defaultEpochLength
	}

	// Allocate the snapshot caches and create the engine
	recentSnaps, err := lru.NewARC[libcommon.Hash, *Snapshot](inMemorySnapshots)
	if err != nil {
		panic(err)
	}
	signatures, err := lru.NewARC[libcommon.Hash, libcommon.Address](inMemorySignatures)
	if err != nil {
		panic(err)
	}
	vABIBeforeLuban, err := abi.JSON(strings.NewReader(validatorSetABIBeforeLuban))
	if err != nil {
		panic(err)
	}
	vABI, err := abi.JSON(strings.NewReader(validatorSetABI))
	if err != nil {
		panic(err)
	}
	sABI, err := abi.JSON(strings.NewReader(slashABI))
	if err != nil {
		panic(err)
	}
	stABI, err := abi.JSON(strings.NewReader(stakeABI))
	if err != nil {
		panic(err)
	}

	return &Parlia{
		chainConfig:                chainConfig,
		config:                     parliaConfig,
		db:                         rwWrapper{db},
		BlobStore:                  nil,
		recentSnaps:                recentSnaps,
		signatures:                 signatures,
		validatorSetABIBeforeLuban: vABIBeforeLuban,
		validatorSetABI:            vABI,
		slashABI:                   sABI,
		stakeHubABI:                stABI,
		signer:                     types.LatestSigner(chainConfig),
		blockReader:                blockReader,
		logger:                     logger,
	}
}
