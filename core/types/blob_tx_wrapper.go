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

package types

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"io"
	"math/big"
	"math/bits"
	"reflect"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/fixedgas"
	libkzg "github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon-lib/rlp"
)

const (
	LEN_48 = 48 // KZGCommitment & KZGProof sizes
)

type KZGCommitment [LEN_48]byte // Compressed BLS12-381 G1 element
type KZGProof [LEN_48]byte
type Blob [fixedgas.BlobSize]byte

type BlobKzgs []KZGCommitment
type KZGProofs []KZGProof
type Blobs []Blob

var (
	blobT       = reflect.TypeOf(Blob{})
	commitmentT = reflect.TypeOf(KZGCommitment{})
	proofT      = reflect.TypeOf(KZGProof{})
)

// UnmarshalJSON parses a blob in hex syntax.
func (b *Blob) UnmarshalJSON(input []byte) error {
	return hexutility.UnmarshalFixedJSON(blobT, input, b[:])
}

// MarshalText returns the hex representation of b.
func (b *Blob) MarshalText() ([]byte, error) {
	return hexutility.Bytes(b[:]).MarshalText()
}

// UnmarshalJSON parses a commitment in hex syntax.
func (c *KZGCommitment) UnmarshalJSON(input []byte) error {
	return hexutility.UnmarshalFixedJSON(commitmentT, input, c[:])
}

// MarshalText returns the hex representation of c.
func (c KZGCommitment) MarshalText() ([]byte, error) {
	return hexutility.Bytes(c[:]).MarshalText()
}

// UnmarshalJSON parses a proof in hex syntax.
func (p *KZGProof) UnmarshalJSON(input []byte) error {
	return hexutility.UnmarshalFixedJSON(proofT, input, p[:])
}

// MarshalText returns the hex representation of p.
func (p KZGProof) MarshalText() ([]byte, error) {
	return hexutility.Bytes(p[:]).MarshalText()
}

// BlobTxSidecar contains the blobs of a blob transaction.
type BlobTxSidecar struct {
	Blobs       Blobs     `json:"blobs"`       // Blobs needed by the blob pool
	Commitments BlobKzgs  `json:"commitments"` // Commitments needed by the blob pool
	Proofs      KZGProofs `json:"proofs"`      // Proofs needed by the blob pool
}

type BlobTxWrapper struct {
	Tx          BlobTx
	Commitments BlobKzgs
	Blobs       Blobs
	Proofs      KZGProofs
}

/* Blob methods */

func (b *Blob) payloadSize() int {
	size := 1                                                      // 0xb7..0xbf
	size += libcommon.BitLenToByteLen(bits.Len(fixedgas.BlobSize)) // length encoding size
	size += fixedgas.BlobSize                                      // byte_array it self
	return size
}

/* BlobKzgs methods */

func (li BlobKzgs) copy() BlobKzgs {
	cpy := make(BlobKzgs, len(li))
	copy(cpy, li)
	return cpy
}

func (li BlobKzgs) payloadSize() int {
	return 49 * len(li)
}

func (li BlobKzgs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix
	buf := newEncodingBuf()
	l := rlp.EncodeListPrefix(payloadSize, buf[:])
	w.Write(buf[:l])

	for _, cmtmt := range li {
		if err := rlp.EncodeString(cmtmt[:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (li *BlobKzgs) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobKzgs (Commitments): %w", err)
	}
	var b []byte
	cmtmt := KZGCommitment{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == LEN_48 {
			copy((cmtmt)[:], b)
			*li = append(*li, cmtmt)
		} else {
			return fmt.Errorf("wrong size for BlobKzgs (Commitments): %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobKzgs (Commitments): %w", err)
	}

	return nil
}

/* KZGProofs methods */

func (li KZGProofs) copy() KZGProofs {
	cpy := make(KZGProofs, len(li))
	copy(cpy, li)
	return cpy
}

func (li KZGProofs) payloadSize() int {
	return 49 * len(li)
}

func (li KZGProofs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix
	buf := newEncodingBuf()
	l := rlp.EncodeListPrefix(payloadSize, buf[:])
	w.Write(buf[:l])

	for _, proof := range li {
		if err := rlp.EncodeString(proof[:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (li *KZGProofs) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()

	if err != nil {
		return fmt.Errorf("open KZGProofs (Proofs): %w", err)
	}
	var b []byte
	proof := KZGProof{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == LEN_48 {
			copy((proof)[:], b)
			*li = append(*li, proof)
		} else {
			return fmt.Errorf("wrong size for KZGProofs (Proofs): %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close KZGProofs (Proofs): %w", err)
	}

	return nil
}

/* Blobs methods */

func (blobs Blobs) copy() Blobs {
	cpy := make(Blobs, len(blobs))
	copy(cpy, blobs) // each blob element is an array and gets deep-copied
	return cpy
}

func (blobs Blobs) payloadSize() int {
	if len(blobs) > 0 {
		return len(blobs) * blobs[0].payloadSize()
	}
	return 0
}

func (blobs Blobs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix

	buf := newEncodingBuf()
	l := rlp.EncodeListPrefix(payloadSize, buf[:])
	w.Write(buf[:l])
	for _, blob := range blobs {
		if err := rlp.EncodeString(blob[:], w, b); err != nil {
			return err
		}
	}

	return nil
}

func (blobs *Blobs) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open Blobs: %w", err)
	}
	var b []byte
	blob := Blob{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == fixedgas.BlobSize {
			copy((blob)[:], b)
			*blobs = append(*blobs, blob)
		} else {
			return fmt.Errorf("wrong size for Blobs: %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close Blobs: %w", err)
	}

	return nil
}

// Return KZG commitments, versioned hashes and the proofs that correspond to these blobs
func (blobs Blobs) ComputeCommitmentsAndProofs() (commitments []KZGCommitment, versionedHashes []libcommon.Hash, proofs []KZGProof, err error) {
	commitments = make([]KZGCommitment, len(blobs))
	proofs = make([]KZGProof, len(blobs))
	versionedHashes = make([]libcommon.Hash, len(blobs))

	kzgCtx := libkzg.Ctx()
	for i := 0; i < len(blobs); i++ {
		commitment, err := kzgCtx.BlobToKZGCommitment(blobs[i][:], 1 /*numGoRoutines*/)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not convert blob to commitment: %v", err)
		}

		proof, err := kzgCtx.ComputeBlobKZGProof(blobs[i][:], commitment, 1 /*numGoRoutnes*/)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not compute proof for blob: %v", err)
		}
		commitments[i] = KZGCommitment(commitment)
		proofs[i] = KZGProof(proof)
		versionedHashes[i] = libcommon.Hash(libkzg.KZGToVersionedHash(commitment))
	}

	return commitments, versionedHashes, proofs, nil
}

func toBlobs(_blobs Blobs) []gokzg4844.BlobRef {
	blobs := make([]gokzg4844.BlobRef, len(_blobs))
	for i, _blob := range _blobs {
		blobs[i] = _blob[:]
	}
	return blobs
}
func toComms(_comms BlobKzgs) []gokzg4844.KZGCommitment {
	comms := make([]gokzg4844.KZGCommitment, len(_comms))
	for i, _comm := range _comms {
		comms[i] = gokzg4844.KZGCommitment(_comm)
	}
	return comms
}
func toProofs(_proofs KZGProofs) []gokzg4844.KZGProof {
	proofs := make([]gokzg4844.KZGProof, len(_proofs))
	for i, _proof := range _proofs {
		proofs[i] = gokzg4844.KZGProof(_proof)
	}
	return proofs
}

func (c KZGCommitment) ComputeVersionedHash() libcommon.Hash {
	return libcommon.Hash(libkzg.KZGToVersionedHash(gokzg4844.KZGCommitment(c)))
}

// BlobTxSidecar encoderlp
func (sc BlobTxSidecar) EncodeRLP(w io.Writer) error {
	var b [33]byte
	if err := rlp.EncodeStructSizePrefix(sc.payloadSize(), w, b[:]); err != nil {
		return err
	}

	// blobs
	if err := sc.Blobs.encodePayload(w, b[:], sc.Blobs.payloadSize()); err != nil {
		return err
	}

	// commitments
	if err := sc.Commitments.encodePayload(w, b[:], sc.Commitments.payloadSize()); err != nil {
		return err
	}

	// proofs
	if err := sc.Proofs.encodePayload(w, b[:], sc.Proofs.payloadSize()); err != nil {
		return err
	}
	return nil
}

// BlobTxSidecar decoderlp
func (sc *BlobTxSidecar) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobTxSidecar: %w", err)
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

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobTxSidecar: %w", err)
	}

	return nil
}

/* BlobTxSidecar methods */

func (sc BlobTxSidecar) payloadSize() int {
	blobSize := sc.Blobs.payloadSize()
	payloadSize := rlp.ListPrefixLen(blobSize) + blobSize
	commitmentSize := sc.Commitments.payloadSize()
	payloadSize += rlp.ListPrefixLen(commitmentSize) + commitmentSize
	proofSize := sc.Proofs.payloadSize()
	payloadSize += rlp.ListPrefixLen(proofSize) + proofSize
	return payloadSize
}

// ValidateBlobTxSidecar implements validate_blob_tx_sidecar from EIP-4844
func (sc *BlobTxSidecar) ValidateBlobTxSidecar(blobVersionedHashes []libcommon.Hash) error {
	l1 := len(blobVersionedHashes)
	if l1 == 0 {
		return errors.New("a blob tx must contain at least one blob")
	}
	l2 := len(sc.Commitments)
	l3 := len(sc.Blobs)
	l4 := len(sc.Proofs)
	if l1 != l2 || l1 != l3 || l1 != l4 {
		return fmt.Errorf("lengths don't match %v %v %v %v", l1, l2, l3, l4)
	}
	kzgCtx := libkzg.Ctx()
	err := kzgCtx.VerifyBlobKZGProofBatch(toBlobs(sc.Blobs), toComms(sc.Commitments), toProofs(sc.Proofs))
	if err != nil {
		return fmt.Errorf("error during proof verification: %v", err)
	}
	for i, h := range blobVersionedHashes {
		if computed := sc.Commitments[i].ComputeVersionedHash(); computed != h {
			return fmt.Errorf("versioned hash %d supposedly %s but does not match computed %s", i, h, computed)
		}
	}
	return nil
}

/* BlobTxWrapper methods */

// validateBlobTransactionWrapper implements validate_blob_transaction_wrapper from EIP-4844
func (txw *BlobTxWrapper) ValidateBlobTransactionWrapper() error {
	l1 := len(txw.Tx.BlobVersionedHashes)
	if l1 == 0 {
		return errors.New("a blob txn must contain at least one blob")
	}
	l2 := len(txw.Commitments)
	l3 := len(txw.Blobs)
	l4 := len(txw.Proofs)
	if l1 != l2 || l1 != l3 || l1 != l4 {
		return fmt.Errorf("lengths don't match %v %v %v %v", l1, l2, l3, l4)
	}
	kzgCtx := libkzg.Ctx()
	err := kzgCtx.VerifyBlobKZGProofBatch(toBlobs(txw.Blobs), toComms(txw.Commitments), toProofs(txw.Proofs))
	if err != nil {
		return fmt.Errorf("error during proof verification: %v", err)
	}
	for i, h := range txw.Tx.BlobVersionedHashes {
		if computed := txw.Commitments[i].ComputeVersionedHash(); computed != h {
			return fmt.Errorf("versioned hash %d supposedly %s but does not match computed %s", i, h, computed)
		}
	}
	return nil
}

// Implement transaction interface
func (txw *BlobTxWrapper) Type() byte               { return txw.Tx.Type() }
func (txw *BlobTxWrapper) GetChainID() *uint256.Int { return txw.Tx.GetChainID() }
func (txw *BlobTxWrapper) GetNonce() uint64         { return txw.Tx.GetNonce() }
func (txw *BlobTxWrapper) GetPrice() *uint256.Int   { return txw.Tx.GetPrice() }
func (txw *BlobTxWrapper) GetTip() *uint256.Int     { return txw.Tx.GetTip() }
func (txw *BlobTxWrapper) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	return txw.Tx.GetEffectiveGasTip(baseFee)
}
func (txw *BlobTxWrapper) GetFeeCap() *uint256.Int { return txw.Tx.GetFeeCap() }

func (txw *BlobTxWrapper) GetBlobHashes() []libcommon.Hash { return txw.Tx.GetBlobHashes() }

func (txw *BlobTxWrapper) GetGas() uint64            { return txw.Tx.GetGas() }
func (txw *BlobTxWrapper) GetBlobGas() uint64        { return txw.Tx.GetBlobGas() }
func (txw *BlobTxWrapper) GetValue() *uint256.Int    { return txw.Tx.GetValue() }
func (txw *BlobTxWrapper) GetTo() *libcommon.Address { return txw.Tx.GetTo() }

func (txw *BlobTxWrapper) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	return txw.Tx.AsMessage(s, baseFee, rules)
}
func (txw *BlobTxWrapper) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	return txw.Tx.WithSignature(signer, sig)
}

func (txw *BlobTxWrapper) Hash() libcommon.Hash { return txw.Tx.Hash() }

func (txw *BlobTxWrapper) SigningHash(chainID *big.Int) libcommon.Hash {
	return txw.Tx.SigningHash(chainID)
}

func (txw *BlobTxWrapper) GetData() []byte { return txw.Tx.GetData() }

func (txw *BlobTxWrapper) GetAccessList() AccessList { return txw.Tx.GetAccessList() }

func (txw *BlobTxWrapper) Protected() bool { return txw.Tx.Protected() }

func (txw *BlobTxWrapper) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return txw.Tx.RawSignatureValues()
}

func (txw *BlobTxWrapper) cachedSender() (libcommon.Address, bool) { return txw.Tx.cachedSender() }

func (txw *BlobTxWrapper) Sender(s Signer) (libcommon.Address, error) { return txw.Tx.Sender(s) }

func (txw *BlobTxWrapper) GetSender() (libcommon.Address, bool) { return txw.Tx.GetSender() }

func (txw *BlobTxWrapper) SetSender(address libcommon.Address) { txw.Tx.SetSender(address) }

func (txw *BlobTxWrapper) IsContractDeploy() bool { return txw.Tx.IsContractDeploy() }

func (txw *BlobTxWrapper) Unwrap() Transaction { return &txw.Tx }

func (txw *BlobTxWrapper) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}

	if err := txw.Tx.DecodeRLP(s); err != nil {
		return err
	}

	if err := txw.Blobs.DecodeRLP(s); err != nil {
		return err
	}

	if err := txw.Commitments.DecodeRLP(s); err != nil {
		return err
	}

	if err := txw.Proofs.DecodeRLP(s); err != nil {
		return err
	}

	return s.ListEnd()
}

// We deliberately encode only the transaction payload because the only case we need to serialize
// blobs/commitments/proofs is when we reply to GetPooledTransactions (and that's handled by the txpool).
func (txw *BlobTxWrapper) EncodingSize() int {
	return txw.Tx.EncodingSize()
}
func (txw *BlobTxWrapper) payloadSize() (payloadSize int) {
	l, _, _, _, _ := txw.Tx.payloadSize()
	payloadSize += l + rlp.ListPrefixLen(l)
	l = txw.Blobs.payloadSize()
	payloadSize += l + rlp.ListPrefixLen(l)
	l = txw.Commitments.payloadSize()
	payloadSize += l + rlp.ListPrefixLen(l)
	l = txw.Proofs.payloadSize()
	payloadSize += l + rlp.ListPrefixLen(l)
	return
}
func (txw *BlobTxWrapper) MarshalBinaryWrapped(w io.Writer) error {
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	payloadSize := txw.payloadSize()
	l := rlp.EncodeListPrefix(payloadSize, b[1:])
	if _, err := w.Write(b[1 : 1+l]); err != nil {
		return err
	}
	bw := bytes.Buffer{}
	if err := txw.Tx.MarshalBinary(&bw); err != nil {
		return err
	}
	if _, err := w.Write(bw.Bytes()[1:]); err != nil {
		return err
	}

	if err := txw.Blobs.encodePayload(w, b[:], txw.Blobs.payloadSize()); err != nil {
		return err
	}
	if err := txw.Commitments.encodePayload(w, b[:], txw.Commitments.payloadSize()); err != nil {
		return err
	}
	if err := txw.Proofs.encodePayload(w, b[:], txw.Proofs.payloadSize()); err != nil {
		return err
	}
	return nil
}
func (txw *BlobTxWrapper) MarshalBinary(w io.Writer) error {
	return txw.Tx.MarshalBinary(w)
}
func (txw *BlobTxWrapper) EncodeRLP(w io.Writer) error {
	return txw.Tx.EncodeRLP(w)
}

func (txw *BlobTxWrapper) BlobTxSidecar() *BlobTxSidecar {
	return &BlobTxSidecar{
		Blobs:       txw.Blobs.copy(),
		Commitments: txw.Commitments.copy(),
		Proofs:      txw.Proofs.copy(),
	}
}
