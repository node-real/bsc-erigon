package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/rlp"
)

type BlobSidecars []*BlobSidecar

// Len returns the length of s.
func (s BlobSidecars) Len() int { return len(s) }

// EncodeIndex encodes the i'th BlobTxSidecar to w. Note that this does not check for errors
// because we assume that BlobSidecars will only ever contain valid sidecars
func (s BlobSidecars) EncodeIndex(i int, w *bytes.Buffer) {
	rlp.Encode(w, s[i])
}

type BlobSidecar struct {
	BlobTxSidecar
	BlockNumber *big.Int       `json:"blockNumber"`
	BlockHash   libcommon.Hash `json:"blockHash"`
	TxIndex     uint64         `json:"transactionIndex"`
	TxHash      libcommon.Hash `json:"transactionHash"`
}

func NewBlobSidecarFromTx(tx *BlobTxWrapper) *BlobSidecar {
	if len(tx.Blobs) == 0 {
		return nil
	}
	return &BlobSidecar{
		BlobTxSidecar: *tx.BlobTxSidecar(),
		TxHash:        tx.Hash(),
	}
}

func (s *BlobSidecar) SanityCheck(blockNumber *big.Int, blockHash libcommon.Hash) error {
	if s.BlockNumber.Cmp(blockNumber) != 0 {
		return errors.New("BlobSidecar with wrong block number")
	}
	if s.BlockHash != blockHash {
		return errors.New("BlobSidecar with wrong block hash")
	}
	if len(s.Blobs) != len(s.Commitments) {
		return errors.New("BlobSidecar has wrong commitment length")
	}
	if len(s.Blobs) != len(s.Proofs) {
		return errors.New("BlobSidecar has wrong proof length")
	}
	return nil
}

// generate encode and decode rlp for BlobSidecar
func (s *BlobSidecar) EncodeRLP(w io.Writer) error {
	var b [33]byte
	if err := EncodeStructSizePrefix(s.payloadSize(), w, b[:]); err != nil {
		return err
	}
	// blobs
	if err := s.Blobs.encodePayload(w, b[:], s.Blobs.payloadSize()); err != nil {
		return err
	}

	// commitments
	if err := s.Commitments.encodePayload(w, b[:], s.Commitments.payloadSize()); err != nil {
		return err
	}

	// proofs
	if err := s.Proofs.encodePayload(w, b[:], s.Proofs.payloadSize()); err != nil {
		return err
	}

	if err := rlp.Encode(w, s.BlockNumber); err != nil {
		return err
	}
	if err := rlp.Encode(w, s.BlockHash); err != nil {
		return err
	}
	if err := rlp.Encode(w, s.TxIndex); err != nil {
		return err
	}
	if err := rlp.Encode(w, s.TxHash); err != nil {
		return err
	}
	return nil
}

// DecodeRLP decodes a BlobSidecar from an RLP stream.
func (sc *BlobSidecar) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}

	if err := sc.Blobs.DecodeRLP(s); err != nil {
		return fmt.Errorf("decode Blobs: %w", err)
	}

	if err := sc.Commitments.DecodeRLP(s); err != nil {
		return fmt.Errorf("decode Commitments: %w", err)
	}

	if err := sc.Proofs.DecodeRLP(s); err != nil {
		return fmt.Errorf("decode Proofs: %w", err)
	}
	sc.BlockNumber = new(big.Int)
	if err := s.Decode(sc.BlockNumber); err != nil {
		return err
	}
	if err := s.Decode(&sc.BlockHash); err != nil {
		return err
	}
	if err := s.Decode(&sc.TxIndex); err != nil {
		return err
	}
	if err := s.Decode(&sc.TxHash); err != nil {
		return err
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobSidecar: %w", err)
	}
	return nil
}

func (s *BlobSidecar) payloadSize() int {
	size := s.BlobTxSidecar.payloadSize()
	size += 1 + rlp.BigIntLenExcludingHead(s.BlockNumber)
	size += 33
	size += 1 + rlp.IntLenExcludingHead(s.TxIndex)
	size += 33
	return size
}

func (s *BlobSidecar) EncodingSize() int {
	return s.payloadSize()
}
