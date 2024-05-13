package types

import (
	"bytes"
	"math/big"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/stretchr/testify/assert"
)

// Mock data for testing
var (
	blockNumber = big.NewInt(123456)
	blockHash   = libcommon.HexToHash("0x1")
	txHash      = libcommon.HexToHash("0x2")
	txIndex     = uint64(100)
)

// generate newRandBlobTxSidecar
func newRandBlobTxSidecar(l int) BlobTxSidecar {
	return BlobTxSidecar{
		Commitments: newRandCommitments(l),
		Blobs:       newRandBlobs(l),
		Proofs:      newRandProofs(l),
	}
}

// TestNewBlobSidecarFromTx tests the creation of a new BlobSidecar from a transaction
func TestNewBlobSidecarFromTx(t *testing.T) {
	// Setup mock BlobTxWrapper
	tx := newRandBlobWrapper()

	sidecar := NewBlobSidecarFromTx(tx)
	if sidecar == nil {
		t.Errorf("NewBlobSidecarFromTx returned nil")
	}
}

// TestBlobSidecarEncodeDecode tests encoding and decoding of a single BlobSidecar
func TestBlobTxSidecarEncodeDecode(t *testing.T) {
	sidecar := newRandBlobTxSidecar(4)

	var buf bytes.Buffer
	err := sidecar.EncodeRLP(&buf)
	assert.Nil(t, err, "Encoding failed")

	decodedSidecar := new(BlobTxSidecar)
	stream := rlp.NewStream(&buf, 0)
	err = decodedSidecar.DecodeRLP(stream)
	assert.Nil(t, err, "Decoding failed")
	//compare all parts of BlobTxSidecar
	assert.Equal(t, sidecar.Proofs, decodedSidecar.Proofs, "Decoded sidecar proofs does not match original")
	assert.Equal(t, sidecar.Blobs, decodedSidecar.Blobs, "Decoded sidecar blobs does not match original")
	assert.Equal(t, sidecar.Commitments, decodedSidecar.Commitments, "Decoded sidecar commitments does not match original")
}

// TestBlobSidecarEncodeDecode tests encoding and decoding of a single BlobSidecar
func TestBlobSidecarEncodeDecode(t *testing.T) {
	sidecar := &BlobSidecar{
		BlockNumber:   blockNumber,
		BlockHash:     blockHash,
		TxIndex:       txIndex,
		TxHash:        txHash,
		BlobTxSidecar: newRandBlobTxSidecar(5),
	}

	var buf bytes.Buffer
	err := sidecar.EncodeRLP(&buf)
	assert.Nil(t, err, "Encoding failed")

	decodedSidecar := new(BlobSidecar)
	stream := rlp.NewStream(&buf, 0)
	err = decodedSidecar.DecodeRLP(stream)
	assert.Nil(t, err, "Decoding failed")

	assert.Equal(t, sidecar, decodedSidecar, "Decoded sidecar does not match original")
}

// TestBlobSidecarSanityCheck tests the SanityCheck function of a BlobSidecar
func TestBlobSidecarSanityCheck(t *testing.T) {
	sidecar := &BlobSidecar{
		BlockNumber:   blockNumber,
		BlockHash:     blockHash,
		TxIndex:       txIndex,
		TxHash:        txHash,
		BlobTxSidecar: newRandBlobTxSidecar(1),
	}

	err := sidecar.SanityCheck(blockNumber, blockHash)
	assert.Nil(t, err, "Sanity check failed")

	wrongBlockNumber := big.NewInt(654321)
	err = sidecar.SanityCheck(wrongBlockNumber, blockHash)
	assert.NotNil(t, err, "Sanity check should fail with wrong block number")

	wrongBlockHash := libcommon.HexToHash("0x3")
	err = sidecar.SanityCheck(blockNumber, wrongBlockHash)
	assert.NotNil(t, err, "Sanity check should fail with wrong block hash")
}
