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
	if err := s.BlobTxSidecar.EncodeRLP(w); err != nil {
		return err
	}
	if err := rlp.EncodeBigInt(s.BlockNumber, w, b[:]); err != nil {
		return err
	}
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(s.BlockHash.Bytes()); err != nil {
		return err
	}
	if err := rlp.EncodeInt(s.TxIndex, w, b[:]); err != nil {
		return err
	}
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(s.TxHash.Bytes()); err != nil {
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
	if err := sc.BlobTxSidecar.DecodeRLP(s); err != nil {
		return err
	}
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read blockNumber: %w", err)
	}
	sc.BlockNumber = new(big.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for BlockHash: %d", len(b))
	}
	copy(sc.BlockHash[:], b)
	if sc.TxIndex, err = s.Uint(); err != nil {
		return err
	}
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for TxHash: %d", len(b))
	}
	copy(sc.TxHash[:], b)

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobSidecar: %w", err)
	}
	return nil
}

func (s *BlobSidecar) payloadSize() int {
	size := s.BlobTxSidecar.payloadSize()
	size++
	size += rlp.BigIntLenExcludingHead(s.BlockNumber)
	size += 33
	size++
	size += rlp.IntLenExcludingHead(s.TxIndex)
	size += 33
	return size
}

func (s *BlobSidecar) EncodingSize() int {
	return s.payloadSize()
}
