// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package vm

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/big"

	"github.com/Giulio2002/bls"

	"github.com/consensys/gnark-crypto/ecc"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fp"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/crypto/blake2b"
	"github.com/erigontech/erigon-lib/crypto/bn256"
	libkzg "github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon-lib/crypto/secp256r1"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/secp256k1"

	//lint:ignore SA1019 Needed for precompile
	"golang.org/x/crypto/ripemd160"
)

// PrecompiledContract is the basic interface for native Go contracts. The implementation
// requires a deterministic gas count based on the input size of the Run method of the
// contract.
type PrecompiledContract interface {
	RequiredGas(input []byte) uint64  // RequiredPrice calculates the contract gas use
	Run(input []byte) ([]byte, error) // Run runs the precompiled contract
}

// PrecompiledContractsHomestead contains the default set of pre-compiled Ethereum
// contracts used in the Frontier and Homestead releases.
var PrecompiledContractsHomestead = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
}

// PrecompiledContractsByzantium contains the default set of pre-compiled Ethereum
// contracts used in the Byzantium release.
var PrecompiledContractsByzantium = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{eip2565: false},
	libcommon.BytesToAddress([]byte{6}): &bn256AddByzantium{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulByzantium{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingByzantium{},
}

// PrecompiledContractsIstanbul contains the default set of pre-compiled Ethereum
// contracts used in the Istanbul release.
var PrecompiledContractsIstanbul = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{eip2565: false},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},
}

var PrecompiledContractsIstanbulForBSC = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidate{},
}

var PrecompiledContractsNano = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidateNano{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidateNano{},
}

var PrecompiledContractsIsMoran = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidateMoran{},
}

// PrecompiledContractsBerlin contains the default set of pre-compiled Ethereum
// contracts used in the Berlin release.
var PrecompiledContractsBerlin = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},
}

var PrecompiledContractsCancun = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{0x01}): &ecrecover{},
	libcommon.BytesToAddress([]byte{0x02}): &sha256hash{},
	libcommon.BytesToAddress([]byte{0x03}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{0x04}): &dataCopy{},
	libcommon.BytesToAddress([]byte{0x05}): &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{0x06}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{0x07}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{0x08}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{0x09}): &blake2F{},
	libcommon.BytesToAddress([]byte{0x0a}): &pointEvaluation{},
}

var PrecompiledContractsCancunForBsc = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{0x01}): &ecrecover{},
	libcommon.BytesToAddress([]byte{0x02}): &sha256hash{},
	libcommon.BytesToAddress([]byte{0x03}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{0x04}): &dataCopy{},
	libcommon.BytesToAddress([]byte{0x05}): &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{0x06}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{0x07}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{0x08}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{0x09}): &blake2F{},
	libcommon.BytesToAddress([]byte{0x0a}): &pointEvaluation{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidatePlato{},
	libcommon.BytesToAddress([]byte{102}): &blsSignatureVerify{},
	libcommon.BytesToAddress([]byte{103}): &cometBFTLightBlockValidateHertz{},
	libcommon.BytesToAddress([]byte{104}): &verifyDoubleSignEvidence{},
	libcommon.BytesToAddress([]byte{105}): &secp256k1SignatureRecover{},
}

var PrecompiledContractsNapoli = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{0x01}):       &ecrecover{},
	libcommon.BytesToAddress([]byte{0x02}):       &sha256hash{},
	libcommon.BytesToAddress([]byte{0x03}):       &ripemd160hash{},
	libcommon.BytesToAddress([]byte{0x04}):       &dataCopy{},
	libcommon.BytesToAddress([]byte{0x05}):       &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{0x06}):       &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{0x07}):       &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{0x08}):       &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{0x09}):       &blake2F{},
	libcommon.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{},
}

var PrecompiledContractsBhilai = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{0x01}):       &ecrecover{},
	libcommon.BytesToAddress([]byte{0x02}):       &sha256hash{},
	libcommon.BytesToAddress([]byte{0x03}):       &ripemd160hash{},
	libcommon.BytesToAddress([]byte{0x04}):       &dataCopy{},
	libcommon.BytesToAddress([]byte{0x05}):       &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{0x06}):       &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{0x07}):       &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{0x08}):       &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{0x09}):       &blake2F{},
	libcommon.BytesToAddress([]byte{0x0b}):       &bls12381G1Add{},
	libcommon.BytesToAddress([]byte{0x0c}):       &bls12381G1MultiExp{},
	libcommon.BytesToAddress([]byte{0x0d}):       &bls12381G2Add{},
	libcommon.BytesToAddress([]byte{0x0e}):       &bls12381G2MultiExp{},
	libcommon.BytesToAddress([]byte{0x0f}):       &bls12381Pairing{},
	libcommon.BytesToAddress([]byte{0x10}):       &bls12381MapFpToG1{},
	libcommon.BytesToAddress([]byte{0x11}):       &bls12381MapFp2ToG2{},
	libcommon.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{},
}

var PrecompiledContractsPrague = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{0x01}): &ecrecover{},
	libcommon.BytesToAddress([]byte{0x02}): &sha256hash{},
	libcommon.BytesToAddress([]byte{0x03}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{0x04}): &dataCopy{},
	libcommon.BytesToAddress([]byte{0x05}): &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{0x06}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{0x07}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{0x08}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{0x09}): &blake2F{},
	libcommon.BytesToAddress([]byte{0x0a}): &pointEvaluation{},
	libcommon.BytesToAddress([]byte{0x0b}): &bls12381G1Add{},
	libcommon.BytesToAddress([]byte{0x0c}): &bls12381G1MultiExp{},
	libcommon.BytesToAddress([]byte{0x0d}): &bls12381G2Add{},
	libcommon.BytesToAddress([]byte{0x0e}): &bls12381G2MultiExp{},
	libcommon.BytesToAddress([]byte{0x0f}): &bls12381Pairing{},
	libcommon.BytesToAddress([]byte{0x10}): &bls12381MapFpToG1{},
	libcommon.BytesToAddress([]byte{0x11}): &bls12381MapFp2ToG2{},
}

var PrecompiledContractsPragueForBSC = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{0x01}): &ecrecover{},
	libcommon.BytesToAddress([]byte{0x02}): &sha256hash{},
	libcommon.BytesToAddress([]byte{0x03}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{0x04}): &dataCopy{},
	libcommon.BytesToAddress([]byte{0x05}): &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{0x06}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{0x07}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{0x08}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{0x09}): &blake2F{},
	libcommon.BytesToAddress([]byte{0x0a}): &pointEvaluation{},
	libcommon.BytesToAddress([]byte{0x0b}): &bls12381G1Add{},
	libcommon.BytesToAddress([]byte{0x0c}): &bls12381G1MultiExp{},
	libcommon.BytesToAddress([]byte{0x0d}): &bls12381G2Add{},
	libcommon.BytesToAddress([]byte{0x0e}): &bls12381G2MultiExp{},
	libcommon.BytesToAddress([]byte{0x0f}): &bls12381Pairing{},
	libcommon.BytesToAddress([]byte{0x10}): &bls12381MapFpToG1{},
	libcommon.BytesToAddress([]byte{0x11}): &bls12381MapFp2ToG2{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidatePlato{},
	libcommon.BytesToAddress([]byte{102}): &blsSignatureVerify{},
	libcommon.BytesToAddress([]byte{103}): &cometBFTLightBlockValidateHertz{},
	libcommon.BytesToAddress([]byte{104}): &verifyDoubleSignEvidence{},
	libcommon.BytesToAddress([]byte{105}): &secp256k1SignatureRecover{},

	libcommon.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{},
}

// PrecompiledContractsHaber contains the default set of pre-compiled Ethereum
// contracts used in the Haber release.
var PrecompiledContractsHaber = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}):    &ecrecover{},
	libcommon.BytesToAddress([]byte{2}):    &sha256hash{},
	libcommon.BytesToAddress([]byte{3}):    &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}):    &dataCopy{},
	libcommon.BytesToAddress([]byte{5}):    &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{6}):    &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}):    &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}):    &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}):    &blake2F{},
	libcommon.BytesToAddress([]byte{0x0a}): &pointEvaluation{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidatePlato{},
	libcommon.BytesToAddress([]byte{102}): &blsSignatureVerify{},
	libcommon.BytesToAddress([]byte{103}): &cometBFTLightBlockValidateHertz{},
	libcommon.BytesToAddress([]byte{104}): &verifyDoubleSignEvidence{},
	libcommon.BytesToAddress([]byte{105}): &secp256k1SignatureRecover{},

	libcommon.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{},
}

var PrecompiledContractsPlanck = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidatePlanck{},
}

// PrecompiledContractsLuban contains the default set of pre-compiled Ethereum
// contracts used in the Luban release.
var PrecompiledContractsLuban = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidatePlanck{},
	libcommon.BytesToAddress([]byte{102}): &blsSignatureVerify{},
	libcommon.BytesToAddress([]byte{103}): &cometBFTLightBlockValidate{},
}

// PrecompiledContractsPlato contains the default set of pre-compiled Ethereum
// contracts used in the Plato release.
var PrecompiledContractsPlato = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidatePlato{},
	libcommon.BytesToAddress([]byte{102}): &blsSignatureVerify{},
	libcommon.BytesToAddress([]byte{103}): &cometBFTLightBlockValidate{},
}

// PrecompiledContractsHertz contains the default set of pre-compiled Ethereum
// contracts used in the Hertz release.
var PrecompiledContractsHertz = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidatePlato{},
	libcommon.BytesToAddress([]byte{102}): &blsSignatureVerify{},
	libcommon.BytesToAddress([]byte{103}): &cometBFTLightBlockValidateHertz{},
}

var PrecompiledContractsFeynman = map[libcommon.Address]PrecompiledContract{
	libcommon.BytesToAddress([]byte{1}): &ecrecover{},
	libcommon.BytesToAddress([]byte{2}): &sha256hash{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy{},
	libcommon.BytesToAddress([]byte{5}): &bigModExp{eip2565: true},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul{},
	libcommon.BytesToAddress([]byte{9}): &blake2F{},

	libcommon.BytesToAddress([]byte{100}): &tmHeaderValidate{},
	libcommon.BytesToAddress([]byte{101}): &iavlMerkleProofValidatePlato{},
	libcommon.BytesToAddress([]byte{102}): &blsSignatureVerify{},
	libcommon.BytesToAddress([]byte{103}): &cometBFTLightBlockValidateHertz{},
	libcommon.BytesToAddress([]byte{104}): &verifyDoubleSignEvidence{},
	libcommon.BytesToAddress([]byte{105}): &secp256k1SignatureRecover{},
}

var (
	PrecompiledAddressesHaber          []libcommon.Address
	PrecompiledAddressesFeynman        []libcommon.Address
	PrecompiledAddressesHertz          []libcommon.Address
	PrecompiledAddressesPlato          []libcommon.Address
	PrecompiledAddressesLuban          []libcommon.Address
	PrecompiledAddressesPlanck         []libcommon.Address
	PrecompiledAddressesMoran          []libcommon.Address
	PrecompiledAddressesNano           []libcommon.Address
	PrecompiledAddressesPrague         []libcommon.Address
	PrecompiledAddressesPragueForBSC   []libcommon.Address
	PrecompiledAddressesNapoli         []libcommon.Address
	PrecompiledAddressesBhilai         []libcommon.Address
	PrecompiledAddressesCancun         []libcommon.Address
	PrecompiledAddressesCancunForBSC   []libcommon.Address
	PrecompiledAddressesBerlin         []libcommon.Address
	PrecompiledAddressesIstanbul       []libcommon.Address
	PrecompiledAddressesIstanbulForBSC []libcommon.Address
	PrecompiledAddressesByzantium      []libcommon.Address
	PrecompiledAddressesHomestead      []libcommon.Address
)

func init() {
	for k := range PrecompiledContractsHomestead {
		PrecompiledAddressesHomestead = append(PrecompiledAddressesHomestead, k)
	}
	for k := range PrecompiledContractsByzantium {
		PrecompiledAddressesByzantium = append(PrecompiledAddressesByzantium, k)
	}
	for k := range PrecompiledContractsIstanbul {
		PrecompiledAddressesIstanbul = append(PrecompiledAddressesIstanbul, k)
	}
	for k := range PrecompiledContractsIstanbulForBSC {
		PrecompiledAddressesIstanbulForBSC = append(PrecompiledAddressesIstanbulForBSC, k)
	}
	for k := range PrecompiledContractsBerlin {
		PrecompiledAddressesBerlin = append(PrecompiledAddressesBerlin, k)
	}
	for k := range PrecompiledContractsNano {
		PrecompiledAddressesNano = append(PrecompiledAddressesNano, k)
	}
	for k := range PrecompiledContractsIsMoran {
		PrecompiledAddressesMoran = append(PrecompiledAddressesMoran, k)
	}
	for k := range PrecompiledContractsPlanck {
		PrecompiledAddressesPlanck = append(PrecompiledAddressesPlanck, k)
	}
	for k := range PrecompiledContractsLuban {
		PrecompiledAddressesLuban = append(PrecompiledAddressesLuban, k)
	}
	for k := range PrecompiledContractsPlato {
		PrecompiledAddressesPlato = append(PrecompiledAddressesPlato, k)
	}
	for k := range PrecompiledContractsHertz {
		PrecompiledAddressesHertz = append(PrecompiledAddressesHertz, k)
	}
	for k := range PrecompiledContractsFeynman {
		PrecompiledAddressesFeynman = append(PrecompiledAddressesFeynman, k)
	}
	for k := range PrecompiledContractsCancun {
		PrecompiledAddressesCancun = append(PrecompiledAddressesCancun, k)
	}
	for k := range PrecompiledContractsCancunForBsc {
		PrecompiledAddressesCancunForBSC = append(PrecompiledAddressesCancunForBSC, k)
	}
	for k := range PrecompiledContractsHaber {
		PrecompiledAddressesHaber = append(PrecompiledAddressesHaber, k)
	}
	for k := range PrecompiledContractsNapoli {
		PrecompiledAddressesNapoli = append(PrecompiledAddressesNapoli, k)
	}
	for k := range PrecompiledContractsBhilai {
		PrecompiledAddressesBhilai = append(PrecompiledAddressesBhilai, k)
	}
	for k := range PrecompiledContractsPrague {
		PrecompiledAddressesPrague = append(PrecompiledAddressesPrague, k)
	}
	for k := range PrecompiledContractsPragueForBSC {
		PrecompiledAddressesPragueForBSC = append(PrecompiledAddressesPragueForBSC, k)
	}
}

// ActivePrecompiles returns the precompiles enabled with the current configuration.
func ActivePrecompiles(rules *chain.Rules) []libcommon.Address {
	switch {
	case rules.IsBhilai:
		return PrecompiledAddressesBhilai
	case rules.IsPrague:
		if rules.IsParlia {
			return PrecompiledAddressesPragueForBSC
		}
		return PrecompiledAddressesPrague
	case rules.IsNapoli:
		return PrecompiledAddressesNapoli
	case rules.IsHaber:
		return PrecompiledAddressesHaber
	case rules.IsCancun:
		if rules.IsParlia {
			return PrecompiledAddressesCancunForBSC
		}
		return PrecompiledAddressesCancun
	case rules.IsFeynman:
		return PrecompiledAddressesFeynman
	case rules.IsHertz:
		return PrecompiledAddressesHertz
	case rules.IsPlato:
		return PrecompiledAddressesPlato
	case rules.IsLuban:
		return PrecompiledAddressesLuban
	case rules.IsPlanck:
		return PrecompiledAddressesPlanck
	case rules.IsMoran:
		return PrecompiledAddressesMoran
	case rules.IsNano:
		return PrecompiledAddressesNano
	case rules.IsBerlin:
		return PrecompiledAddressesBerlin
	case rules.IsIstanbul:
		if rules.IsParlia {
			return PrecompiledAddressesIstanbulForBSC
		}
		return PrecompiledAddressesIstanbul
	case rules.IsByzantium:
		return PrecompiledAddressesByzantium
	default:
		return PrecompiledAddressesHomestead
	}
}

// RunPrecompiledContract runs and evaluates the output of a precompiled contract.
// It returns
// - the returned bytes,
// - the _remaining_ gas,
// - any error that occurred
func RunPrecompiledContract(p PrecompiledContract, input []byte, suppliedGas uint64) (ret []byte, remainingGas uint64, err error) {
	gasCost := p.RequiredGas(input)
	if suppliedGas < gasCost {
		return nil, 0, ErrOutOfGas
	}
	suppliedGas -= gasCost
	output, err := p.Run(input)
	return output, suppliedGas, err
}

// ECRECOVER implemented as a native contract.
type ecrecover struct{}

func (c *ecrecover) RequiredGas(input []byte) uint64 {
	return params.EcrecoverGas
}

func (c *ecrecover) Run(input []byte) ([]byte, error) {
	const ecRecoverInputLength = 128

	input = libcommon.RightPadBytes(input, ecRecoverInputLength)
	// "input" is (hash, v, r, s), each 32 bytes
	// but for ecrecover we want (r, s, v)

	r := new(uint256.Int).SetBytes(input[64:96])
	s := new(uint256.Int).SetBytes(input[96:128])
	v := input[63] - 27

	// tighter sig s values input homestead only apply to txn sigs
	if !allZero(input[32:63]) || !crypto.TransactionSignatureIsValid(v, r, s, true /* allowPreEip2s */) {
		return nil, nil
	}
	// We must make sure not to modify the 'input', so placing the 'v' along with
	// the signature needs to be done on a new allocation
	sig := make([]byte, 65)
	copy(sig, input[64:128])
	sig[64] = v
	// v needs to be at the end for libsecp256k1
	pubKey, err := crypto.Ecrecover(input[:32], sig)
	// make sure the public key is a valid one
	if err != nil {
		return nil, nil
	}

	// the first byte of pubkey is bitcoin heritage
	return libcommon.LeftPadBytes(crypto.Keccak256(pubKey[1:])[12:], 32), nil
}

// SHA256 implemented as a native contract.
type sha256hash struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *sha256hash) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.Sha256PerWordGas + params.Sha256BaseGas
}
func (c *sha256hash) Run(input []byte) ([]byte, error) {
	h := sha256.Sum256(input)
	return h[:], nil
}

// RIPEMD160 implemented as a native contract.
type ripemd160hash struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *ripemd160hash) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.Ripemd160PerWordGas + params.Ripemd160BaseGas
}
func (c *ripemd160hash) Run(input []byte) ([]byte, error) {
	ripemd := ripemd160.New()
	ripemd.Write(input)
	return libcommon.LeftPadBytes(ripemd.Sum(nil), 32), nil
}

// data copy implemented as a native contract.
type dataCopy struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *dataCopy) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.IdentityPerWordGas + params.IdentityBaseGas
}
func (c *dataCopy) Run(in []byte) ([]byte, error) {
	return in, nil
}

// bigModExp implements a native big integer exponential modular operation.
type bigModExp struct {
	eip2565 bool
}

var (
	big0      = big.NewInt(0)
	big1      = big.NewInt(1)
	big3      = big.NewInt(3)
	big4      = big.NewInt(4)
	big7      = big.NewInt(7)
	big8      = big.NewInt(8)
	big16     = big.NewInt(16)
	big20     = big.NewInt(20)
	big32     = big.NewInt(32)
	big64     = big.NewInt(64)
	big96     = big.NewInt(96)
	big480    = big.NewInt(480)
	big1024   = big.NewInt(1024)
	big3072   = big.NewInt(3072)
	big199680 = big.NewInt(199680)
)

// modexpMultComplexity implements bigModexp multComplexity formula, as defined in EIP-198
//
// def mult_complexity(x):
//
//	if x <= 64: return x ** 2
//	elif x <= 1024: return x ** 2 // 4 + 96 * x - 3072
//	else: return x ** 2 // 16 + 480 * x - 199680
//
// where is x is max(length_of_MODULUS, length_of_BASE)
func modexpMultComplexity(x *big.Int) *big.Int {
	switch {
	case x.Cmp(big64) <= 0:
		x.Mul(x, x) // x ** 2
	case x.Cmp(big1024) <= 0:
		// (x ** 2 // 4 ) + ( 96 * x - 3072)
		x = new(big.Int).Add(
			new(big.Int).Div(new(big.Int).Mul(x, x), big4),
			new(big.Int).Sub(new(big.Int).Mul(big96, x), big3072),
		)
	default:
		// (x ** 2 // 16) + (480 * x - 199680)
		x = new(big.Int).Add(
			new(big.Int).Div(new(big.Int).Mul(x, x), big16),
			new(big.Int).Sub(new(big.Int).Mul(big480, x), big199680),
		)
	}
	return x
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bigModExp) RequiredGas(input []byte) uint64 {
	var (
		baseLen = new(big.Int).SetBytes(getData(input, 0, 32))
		expLen  = new(big.Int).SetBytes(getData(input, 32, 32))
		modLen  = new(big.Int).SetBytes(getData(input, 64, 32))
	)
	if len(input) > 96 {
		input = input[96:]
	} else {
		input = input[:0]
	}
	// Retrieve the head 32 bytes of exp for the adjusted exponent length
	var expHead *big.Int
	if big.NewInt(int64(len(input))).Cmp(baseLen) <= 0 {
		expHead = new(big.Int)
	} else {
		if expLen.Cmp(big32) > 0 {
			expHead = new(big.Int).SetBytes(getData(input, baseLen.Uint64(), 32))
		} else {
			expHead = new(big.Int).SetBytes(getData(input, baseLen.Uint64(), expLen.Uint64()))
		}
	}
	// Calculate the adjusted exponent length
	var msb int
	if bitlen := expHead.BitLen(); bitlen > 0 {
		msb = bitlen - 1
	}
	adjExpLen := new(big.Int)
	if expLen.Cmp(big32) > 0 {
		adjExpLen.Sub(expLen, big32)
		adjExpLen.Mul(big8, adjExpLen)
	}
	adjExpLen.Add(adjExpLen, big.NewInt(int64(msb)))
	// Calculate the gas cost of the operation
	gas := new(big.Int).Set(math.BigMax(modLen, baseLen))
	if c.eip2565 {
		// EIP-2565 has three changes
		// 1. Different multComplexity (inlined here)
		// in EIP-2565 (https://eips.ethereum.org/EIPS/eip-2565):
		//
		// def mult_complexity(x):
		//    ceiling(x/8)^2
		//
		//where is x is max(length_of_MODULUS, length_of_BASE)
		gas = gas.Add(gas, big7)
		gas = gas.Div(gas, big8)
		gas.Mul(gas, gas)

		gas.Mul(gas, math.BigMax(adjExpLen, big1))
		// 2. Different divisor (`GQUADDIVISOR`) (3)
		gas.Div(gas, big3)
		if gas.BitLen() > 64 {
			return math.MaxUint64
		}
		// 3. Minimum price of 200 gas
		if gas.Uint64() < 200 {
			return 200
		}
		return gas.Uint64()
	}
	gas = modexpMultComplexity(gas)
	gas.Mul(gas, math.BigMax(adjExpLen, big1))
	gas.Div(gas, big20)

	if gas.BitLen() > 64 {
		return math.MaxUint64
	}
	return gas.Uint64()
}

func (c *bigModExp) Run(input []byte) ([]byte, error) {
	var (
		baseLen = new(big.Int).SetBytes(getData(input, 0, 32)).Uint64()
		expLen  = new(big.Int).SetBytes(getData(input, 32, 32)).Uint64()
		modLen  = new(big.Int).SetBytes(getData(input, 64, 32)).Uint64()
	)
	if len(input) > 96 {
		input = input[96:]
	} else {
		input = input[:0]
	}
	// Handle a special case when both the base and mod length is zero
	if baseLen == 0 && modLen == 0 {
		return []byte{}, nil
	}
	// Retrieve the operands and execute the exponentiation
	var (
		base = new(big.Int).SetBytes(getData(input, 0, baseLen))
		exp  = new(big.Int).SetBytes(getData(input, baseLen, expLen))
		mod  = new(big.Int).SetBytes(getData(input, baseLen+expLen, modLen))
		v    []byte
	)
	switch {
	case mod.BitLen() == 0:
		// Modulo 0 is undefined, return zero
		return libcommon.LeftPadBytes([]byte{}, int(modLen)), nil
	case base.Cmp(libcommon.Big1) == 0:
		//If base == 1, then we can just return base % mod (if mod >= 1, which it is)
		v = base.Mod(base, mod).Bytes()
	//case mod.Bit(0) == 0:
	//	// Modulo is even
	//	v = math.FastExp(base, exp, mod).Bytes()
	default:
		// Modulo is odd
		v = base.Exp(base, exp, mod).Bytes()
	}
	return libcommon.LeftPadBytes(v, int(modLen)), nil
}

// newCurvePoint unmarshals a binary blob into a bn256 elliptic curve point,
// returning it, or an error if the point is invalid.
func newCurvePoint(blob []byte) (*bn256.G1, error) {
	p := new(bn256.G1)
	if _, err := p.Unmarshal(blob); err != nil {
		return nil, err
	}
	return p, nil
}

// newTwistPoint unmarshals a binary blob into a bn256 elliptic curve point,
// returning it, or an error if the point is invalid.
func newTwistPoint(blob []byte) (*bn256.G2, error) {
	p := new(bn256.G2)
	if _, err := p.Unmarshal(blob); err != nil {
		return nil, err
	}
	return p, nil
}

// runBn256Add implements the Bn256Add precompile, referenced by both
// Byzantium and Istanbul operations.
func runBn256Add(input []byte) ([]byte, error) {
	x, err := newCurvePoint(getData(input, 0, 64))
	if err != nil {
		return nil, err
	}
	y, err := newCurvePoint(getData(input, 64, 64))
	if err != nil {
		return nil, err
	}
	res := new(bn256.G1)
	res.Add(x, y)
	return res.Marshal(), nil
}

// bn256Add implements a native elliptic curve point addition conforming to
// Istanbul consensus rules.
type bn256AddIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256AddIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn256AddGasIstanbul
}

func (c *bn256AddIstanbul) Run(input []byte) ([]byte, error) {
	return runBn256Add(input)
}

// bn256AddByzantium implements a native elliptic curve point addition
// conforming to Byzantium consensus rules.
type bn256AddByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256AddByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn256AddGasByzantium
}

func (c *bn256AddByzantium) Run(input []byte) ([]byte, error) {
	return runBn256Add(input)
}

// runBn256ScalarMul implements the Bn256ScalarMul precompile, referenced by
// both Byzantium and Istanbul operations.
func runBn256ScalarMul(input []byte) ([]byte, error) {
	p, err := newCurvePoint(getData(input, 0, 64))
	if err != nil {
		return nil, err
	}
	res := new(bn256.G1)
	res.ScalarMult(p, new(big.Int).SetBytes(getData(input, 64, 32)))
	return res.Marshal(), nil
}

// bn256ScalarMulIstanbul implements a native elliptic curve scalar
// multiplication conforming to Istanbul consensus rules.
type bn256ScalarMulIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256ScalarMulIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn256ScalarMulGasIstanbul
}

func (c *bn256ScalarMulIstanbul) Run(input []byte) ([]byte, error) {
	return runBn256ScalarMul(input)
}

// bn256ScalarMulByzantium implements a native elliptic curve scalar
// multiplication conforming to Byzantium consensus rules.
type bn256ScalarMulByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256ScalarMulByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn256ScalarMulGasByzantium
}

func (c *bn256ScalarMulByzantium) Run(input []byte) ([]byte, error) {
	return runBn256ScalarMul(input)
}

var (
	// true32Byte is returned if the bn256 pairing check succeeds.
	true32Byte = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}

	// false32Byte is returned if the bn256 pairing check fails.
	false32Byte = make([]byte, 32)

	// errBadPairingInput is returned if the bn256 pairing input is invalid.
	errBadPairingInput = errors.New("bad elliptic curve pairing size")
)

// runBn256Pairing implements the Bn256Pairing precompile, referenced by both
// Byzantium and Istanbul operations.
func runBn256Pairing(input []byte) ([]byte, error) {
	// Handle some corner cases cheaply
	if len(input)%192 > 0 {
		return nil, errBadPairingInput
	}
	// Convert the input into a set of coordinates
	var (
		cs []*bn256.G1
		ts []*bn256.G2
	)
	for i := 0; i < len(input); i += 192 {
		c, err := newCurvePoint(input[i : i+64])
		if err != nil {
			return nil, err
		}
		t, err := newTwistPoint(input[i+64 : i+192])
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
		ts = append(ts, t)
	}
	// Execute the pairing checks and return the results
	if bn256.PairingCheck(cs, ts) {
		return true32Byte, nil
	}
	return false32Byte, nil
}

// bn256PairingIstanbul implements a pairing pre-compile for the bn256 curve
// conforming to Istanbul consensus rules.
type bn256PairingIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256PairingIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn256PairingBaseGasIstanbul + uint64(len(input)/192)*params.Bn256PairingPerPointGasIstanbul
}

func (c *bn256PairingIstanbul) Run(input []byte) ([]byte, error) {
	return runBn256Pairing(input)
}

// bn256PairingByzantium implements a pairing pre-compile for the bn256 curve
// conforming to Byzantium consensus rules.
type bn256PairingByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256PairingByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn256PairingBaseGasByzantium + uint64(len(input)/192)*params.Bn256PairingPerPointGasByzantium
}

func (c *bn256PairingByzantium) Run(input []byte) ([]byte, error) {
	return runBn256Pairing(input)
}

type blake2F struct{}

func (c *blake2F) RequiredGas(input []byte) uint64 {
	// If the input is malformed, we can't calculate the gas, return 0 and let the
	// actual call choke and fault.
	if len(input) != blake2FInputLength {
		return 0
	}
	return uint64(binary.BigEndian.Uint32(input[0:4]))
}

const (
	blake2FInputLength        = 213
	blake2FFinalBlockBytes    = byte(1)
	blake2FNonFinalBlockBytes = byte(0)
)

var (
	errBlake2FInvalidInputLength = errors.New("invalid input length")
	errBlake2FInvalidFinalFlag   = errors.New("invalid final flag")
)

func (c *blake2F) Run(input []byte) ([]byte, error) {
	// Make sure the input is valid (correct length and final flag)
	if len(input) != blake2FInputLength {
		return nil, errBlake2FInvalidInputLength
	}
	if input[212] != blake2FNonFinalBlockBytes && input[212] != blake2FFinalBlockBytes {
		return nil, errBlake2FInvalidFinalFlag
	}
	// Parse the input into the Blake2b call parameters
	var (
		rounds = binary.BigEndian.Uint32(input[0:4])
		final  = input[212] == blake2FFinalBlockBytes

		h [8]uint64
		m [16]uint64
		t [2]uint64
	)
	for i := 0; i < 8; i++ {
		offset := 4 + i*8
		h[i] = binary.LittleEndian.Uint64(input[offset : offset+8])
	}
	for i := 0; i < 16; i++ {
		offset := 68 + i*8
		m[i] = binary.LittleEndian.Uint64(input[offset : offset+8])
	}
	t[0] = binary.LittleEndian.Uint64(input[196:204])
	t[1] = binary.LittleEndian.Uint64(input[204:212])

	// Execute the compression function, extract and return the result
	blake2b.F(&h, m, t, final, rounds)

	output := make([]byte, 64)
	for i := 0; i < 8; i++ {
		offset := i * 8
		binary.LittleEndian.PutUint64(output[offset:offset+8], h[i])
	}
	return output, nil
}

var (
	errBLS12381InvalidInputLength          = errors.New("invalid input length")
	errBLS12381InvalidFieldElementTopBytes = errors.New("invalid field element top bytes")
	errBLS12381G1PointSubgroup             = errors.New("g1 point is not on correct subgroup")
	errBLS12381G2PointSubgroup             = errors.New("g2 point is not on correct subgroup")
)

// bls12381G1Add implements EIP-2537 G1Add precompile.
type bls12381G1Add struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G1Add) RequiredGas(input []byte) uint64 {
	return params.Bls12381G1AddGas
}

func (c *bls12381G1Add) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G1Add precompile.
	// > G1 addition call expects `256` bytes as an input that is interpreted as byte concatenation of two G1 points (`128` bytes each).
	// > Output is an encoding of addition operation result - single G1 point (`128` bytes).
	if len(input) != 256 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	var p0, p1 *bls12381.G1Affine

	// Decode G1 point p_0
	if p0, err = decodePointG1(input[:128]); err != nil {
		return nil, err
	}
	// Decode G1 point p_1
	if p1, err = decodePointG1(input[128:]); err != nil {
		return nil, err
	}

	// Compute r = p_0 + p_1
	p0.Add(p0, p1)

	// Encode the G1 point result into 128 bytes
	return encodePointG1(p0), nil
}

// bls12381G1MultiExp implements EIP-2537 G1MultiExp precompile.
type bls12381G1MultiExp struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G1MultiExp) RequiredGas(input []byte) uint64 {
	// Calculate G1 point, scalar value pair length
	k := len(input) / 160
	if k == 0 {
		// Return 0 gas for small input length
		return 0
	}
	// Lookup discount value for G1 point, scalar value pair length
	var discount uint64
	if dLen := len(params.Bls12381MSMDiscountTableG1); k < dLen {
		discount = params.Bls12381MSMDiscountTableG1[k-1]
	} else {
		discount = params.Bls12381MSMDiscountTableG1[dLen-1]
	}
	// Calculate gas and return the result
	return (uint64(k) * params.Bls12381G1MulGas * discount) / 1000
}

func (c *bls12381G1MultiExp) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G1MultiExp precompile.
	// G1 multiplication call expects `160*k` bytes as an input that is interpreted as byte concatenation of `k` slices each of them being a byte concatenation of encoding of G1 point (`128` bytes) and encoding of a scalar value (`32` bytes).
	// Output is an encoding of multiexponentiation operation result - single G1 point (`128` bytes).
	k := len(input) / 160
	if len(input) == 0 || len(input)%160 != 0 {
		return nil, errBLS12381InvalidInputLength
	}
	points := make([]bls12381.G1Affine, k)
	scalars := make([]fr.Element, k)

	// Decode point scalar pairs
	for i := 0; i < k; i++ {
		off := 160 * i
		t0, t1, t2 := off, off+128, off+160
		// Decode G1 point
		p, err := decodePointG1(input[t0:t1])
		if err != nil {
			return nil, err
		}
		// Fast subgroup check
		if !p.IsInSubGroup() {
			return nil, errBLS12381G1PointSubgroup
		}
		points[i] = *p
		// Decode scalar value
		scalars[i] = *new(fr.Element).SetBytes(input[t1:t2])
	}

	// Compute r = e_0 * p_0 + e_1 * p_1 + ... + e_(k-1) * p_(k-1)
	r := new(bls12381.G1Affine)
	r.MultiExp(points, scalars, ecc.MultiExpConfig{})

	// Encode the G1 point to 128 bytes
	return encodePointG1(r), nil
}

// bls12381G2Add implements EIP-2537 G2Add precompile.
type bls12381G2Add struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G2Add) RequiredGas(input []byte) uint64 {
	return params.Bls12381G2AddGas
}

func (c *bls12381G2Add) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G2Add precompile.
	// > G2 addition call expects `512` bytes as an input that is interpreted as byte concatenation of two G2 points (`256` bytes each).
	// > Output is an encoding of addition operation result - single G2 point (`256` bytes).
	if len(input) != 512 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	var p0, p1 *bls12381.G2Affine

	// Decode G2 point p_0
	if p0, err = decodePointG2(input[:256]); err != nil {
		return nil, err
	}
	// Decode G2 point p_1
	if p1, err = decodePointG2(input[256:]); err != nil {
		return nil, err
	}

	// Compute r = p_0 + p_1
	r := new(bls12381.G2Affine)
	r.Add(p0, p1)

	// Encode the G2 point into 256 bytes
	return encodePointG2(r), nil
}

// bls12381G2MultiExp implements EIP-2537 G2MultiExp precompile.
type bls12381G2MultiExp struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G2MultiExp) RequiredGas(input []byte) uint64 {
	// Calculate G2 point, scalar value pair length
	k := len(input) / 288
	if k == 0 {
		// Return 0 gas for small input length
		return 0
	}
	// Lookup discount value for G2 point, scalar value pair length
	var discount uint64
	if dLen := len(params.Bls12381MSMDiscountTableG2); k < dLen {
		discount = params.Bls12381MSMDiscountTableG2[k-1]
	} else {
		discount = params.Bls12381MSMDiscountTableG2[dLen-1]
	}
	// Calculate gas and return the result
	return (uint64(k) * params.Bls12381G2MulGas * discount) / 1000
}

func (c *bls12381G2MultiExp) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G2MultiExp precompile logic
	// > G2 multiplication call expects `288*k` bytes as an input that is interpreted as byte concatenation of `k` slices each of them being a byte concatenation of encoding of G2 point (`256` bytes) and encoding of a scalar value (`32` bytes).
	// > Output is an encoding of multiexponentiation operation result - single G2 point (`256` bytes).
	k := len(input) / 288
	if len(input) == 0 || len(input)%288 != 0 {
		return nil, errBLS12381InvalidInputLength
	}
	points := make([]bls12381.G2Affine, k)
	scalars := make([]fr.Element, k)

	// Decode point scalar pairs
	for i := 0; i < k; i++ {
		off := 288 * i
		t0, t1, t2 := off, off+256, off+288
		// Decode G2 point
		p, err := decodePointG2(input[t0:t1])
		if err != nil {
			return nil, err
		}
		// Fast subgroup check
		if !p.IsInSubGroup() {
			return nil, errBLS12381G2PointSubgroup
		}
		points[i] = *p
		// Decode scalar value
		scalars[i] = *new(fr.Element).SetBytes(input[t1:t2])
	}

	// Compute r = e_0 * p_0 + e_1 * p_1 + ... + e_(k-1) * p_(k-1)
	r := new(bls12381.G2Affine)
	r.MultiExp(points, scalars, ecc.MultiExpConfig{})

	// Encode the G2 point to 256 bytes.
	return encodePointG2(r), nil
}

// bls12381Pairing implements EIP-2537 Pairing precompile.
type bls12381Pairing struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381Pairing) RequiredGas(input []byte) uint64 {
	return params.Bls12381PairingBaseGas + uint64(len(input)/384)*params.Bls12381PairingPerPairGas
}

func (c *bls12381Pairing) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 Pairing precompile logic.
	// > Pairing call expects `384*k` bytes as an inputs that is interpreted as byte concatenation of `k` slices. Each slice has the following structure:
	// > - `128` bytes of G1 point encoding
	// > - `256` bytes of G2 point encoding
	// > Output is a `32` bytes where last single byte is `0x01` if pairing result is equal to multiplicative identity in a pairing target field and `0x00` otherwise
	// > (which is equivalent of Big Endian encoding of Solidity values `uint256(1)` and `uin256(0)` respectively).
	k := len(input) / 384
	if len(input) == 0 || len(input)%384 != 0 {
		return nil, errBLS12381InvalidInputLength
	}

	var (
		p []bls12381.G1Affine
		q []bls12381.G2Affine
	)

	// Decode pairs
	for i := 0; i < k; i++ {
		off := 384 * i
		t0, t1, t2 := off, off+128, off+384

		// Decode G1 point
		p1, err := decodePointG1(input[t0:t1])
		if err != nil {
			return nil, err
		}
		// Decode G2 point
		p2, err := decodePointG2(input[t1:t2])
		if err != nil {
			return nil, err
		}

		// 'point is on curve' check already done,
		// Here we need to apply subgroup checks.
		if !p1.IsInSubGroup() {
			return nil, errBLS12381G1PointSubgroup
		}
		if !p2.IsInSubGroup() {
			return nil, errBLS12381G2PointSubgroup
		}
		p = append(p, *p1)
		q = append(q, *p2)
	}
	// Prepare 32 byte output
	out := make([]byte, 32)

	// Compute pairing and set the result
	ok, err := bls12381.PairingCheck(p, q)
	if err == nil && ok {
		out[31] = 1
	}
	return out, nil
}

func decodePointG1(in []byte) (*bls12381.G1Affine, error) {
	if len(in) != 128 {
		return nil, errors.New("invalid g1 point length")
	}
	// decode x
	x, err := decodeBLS12381FieldElement(in[:64])
	if err != nil {
		return nil, err
	}
	// decode y
	y, err := decodeBLS12381FieldElement(in[64:])
	if err != nil {
		return nil, err
	}
	elem := bls12381.G1Affine{X: x, Y: y}
	if !elem.IsOnCurve() {
		return nil, errors.New("invalid point: not on curve")
	}

	return &elem, nil
}

// decodePointG2 given encoded (x, y) coordinates in 256 bytes returns a valid G2 Point.
func decodePointG2(in []byte) (*bls12381.G2Affine, error) {
	if len(in) != 256 {
		return nil, errors.New("invalid g2 point length")
	}
	x0, err := decodeBLS12381FieldElement(in[:64])
	if err != nil {
		return nil, err
	}
	x1, err := decodeBLS12381FieldElement(in[64:128])
	if err != nil {
		return nil, err
	}
	y0, err := decodeBLS12381FieldElement(in[128:192])
	if err != nil {
		return nil, err
	}
	y1, err := decodeBLS12381FieldElement(in[192:])
	if err != nil {
		return nil, err
	}

	p := bls12381.G2Affine{X: bls12381.E2{A0: x0, A1: x1}, Y: bls12381.E2{A0: y0, A1: y1}}
	if !p.IsOnCurve() {
		return nil, errors.New("invalid point: not on curve")
	}
	return &p, err
}

// decodeBLS12381FieldElement decodes BLS12-381 elliptic curve field element.
// Removes top 16 bytes of 64 byte input.
func decodeBLS12381FieldElement(in []byte) (fp.Element, error) {
	if len(in) != 64 {
		return fp.Element{}, errors.New("invalid field element length")
	}
	// check top bytes
	for i := 0; i < 16; i++ {
		if in[i] != byte(0x00) {
			return fp.Element{}, errBLS12381InvalidFieldElementTopBytes
		}
	}
	var res [48]byte
	copy(res[:], in[16:])

	return fp.BigEndian.Element(&res)
}

// encodePointG1 encodes a point into 128 bytes.
func encodePointG1(p *bls12381.G1Affine) []byte {
	out := make([]byte, 128)
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[16:]), p.X)
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[64+16:]), p.Y)
	return out
}

// encodePointG2 encodes a point into 256 bytes.
func encodePointG2(p *bls12381.G2Affine) []byte {
	out := make([]byte, 256)
	// encode x
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[16:16+48]), p.X.A0)
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[80:80+48]), p.X.A1)
	// encode y
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[144:144+48]), p.Y.A0)
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[208:208+48]), p.Y.A1)
	return out
}

// bls12381MapFpToG1 implements EIP-2537 MapG1 precompile.
type bls12381MapFpToG1 struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381MapFpToG1) RequiredGas(input []byte) uint64 {
	return params.Bls12381MapFpToG1Gas
}

func (c *bls12381MapFpToG1) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 Map_To_G1 precompile.
	// > Field-to-curve call expects `64` bytes as an input that is interpreted as a an element of the base field.
	// > Output of this call is `128` bytes and is G1 point following respective encoding rules.
	if len(input) != 64 {
		return nil, errBLS12381InvalidInputLength
	}

	// Decode input field element
	fe, err := decodeBLS12381FieldElement(input)
	if err != nil {
		return nil, err
	}

	// Compute mapping
	r := bls12381.MapToG1(fe)

	// Encode the G1 point to 128 bytes
	return encodePointG1(&r), nil
}

// bls12381MapFp2ToG2 implements EIP-2537 MapG2 precompile.
type bls12381MapFp2ToG2 struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381MapFp2ToG2) RequiredGas(input []byte) uint64 {
	return params.Bls12381MapFp2ToG2Gas
}

func (c *bls12381MapFp2ToG2) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 Map_FP2_TO_G2 precompile logic.
	// > Field-to-curve call expects `128` bytes as an input that is interpreted as a an element of the quadratic extension field.
	// > Output of this call is `256` bytes and is G2 point following respective encoding rules.
	if len(input) != 128 {
		return nil, errBLS12381InvalidInputLength
	}

	// Decode input field element
	c0, err := decodeBLS12381FieldElement(input[:64])
	if err != nil {
		return nil, err
	}
	c1, err := decodeBLS12381FieldElement(input[64:])
	if err != nil {
		return nil, err
	}

	// Compute mapping
	r := bls12381.MapToG2(bls12381.E2{A0: c0, A1: c1})

	// Encode the G2 point to 256 bytes
	return encodePointG2(&r), nil
}

// pointEvaluation implements the EIP-4844 point evaluation precompile
// to check if a value is part of a blob at a specific point with a KZG proof.
type pointEvaluation struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *pointEvaluation) RequiredGas(input []byte) uint64 {
	return params.PointEvaluationGas
}

func (c *pointEvaluation) Run(input []byte) ([]byte, error) {
	return libkzg.PointEvaluationPrecompile(input)
}

// blsSignatureVerify implements bls signature verification precompile.
type blsSignatureVerify struct{}

const (
	msgHashLength         = uint64(32)
	signatureLength       = uint64(96)
	singleBlsPubkeyLength = uint64(48)
)

// RequiredGas returns the gas required to execute the pre-compiled contract.
// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *blsSignatureVerify) RequiredGas(input []byte) uint64 {
	msgAndSigLength := msgHashLength + signatureLength
	inputLen := uint64(len(input))
	if inputLen <= msgAndSigLength ||
		(inputLen-msgAndSigLength)%singleBlsPubkeyLength != 0 {
		return params.BlsSignatureVerifyBaseGas
	}
	pubKeyNumber := (inputLen - msgAndSigLength) / singleBlsPubkeyLength
	return params.BlsSignatureVerifyBaseGas + pubKeyNumber*params.BlsSignatureVerifyPerKeyGas
}

// Run input:
// msg      | signature | [{bls pubkey}] |
// 32 bytes | 96 bytes  | [{48 bytes}]   |
func (c *blsSignatureVerify) Run(input []byte) ([]byte, error) {
	msgAndSigLength := msgHashLength + signatureLength
	inputLen := uint64(len(input))
	if inputLen <= msgAndSigLength ||
		(inputLen-msgAndSigLength)%singleBlsPubkeyLength != 0 {
		log.Debug("blsSignatureVerify input size wrong", "inputLen", inputLen)
		return nil, ErrExecutionReverted
	}

	var msg [32]byte
	msgBytes := getData(input, 0, msgHashLength)
	copy(msg[:], msgBytes)

	signatureBytes := getData(input, msgHashLength, signatureLength)
	sig, err := bls.NewSignatureFromBytes(signatureBytes)
	if err != nil {
		log.Debug("blsSignatureVerify invalid signature", "err", err)
		return nil, ErrExecutionReverted
	}

	pubKeyNumber := (inputLen - msgAndSigLength) / singleBlsPubkeyLength
	pubKeys := make([]bls.PublicKey, pubKeyNumber)
	for i := uint64(0); i < pubKeyNumber; i++ {
		pubKeyBytes := getData(input, msgAndSigLength+i*singleBlsPubkeyLength, singleBlsPubkeyLength)
		pubKey, err := bls.NewPublicKeyFromBytes(pubKeyBytes)
		if err != nil {
			log.Debug("blsSignatureVerify invalid pubKey", "err", err)
			return nil, ErrExecutionReverted
		}
		pubKeys[i] = pubKey
	}

	if pubKeyNumber > 1 {
		if !sig.VerifyAggregate(msgBytes, pubKeys) {
			return big0.Bytes(), nil
		}
	} else {
		if !sig.Verify(msgBytes, pubKeys[0]) {
			return big0.Bytes(), nil
		}
	}

	return big1.Bytes(), nil
}

// verifyDoubleSignEvidence implements bsc header verification precompile.
type verifyDoubleSignEvidence struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *verifyDoubleSignEvidence) RequiredGas(input []byte) uint64 {
	return params.DoubleSignEvidenceVerifyGas
}

type DoubleSignEvidence struct {
	ChainId      *big.Int
	HeaderBytes1 []byte
	HeaderBytes2 []byte
}

const (
	extraSeal = 65
)

var (
	errInvalidEvidence = errors.New("invalid double sign evidence")
)

// Run input: rlp encoded DoubleSignEvidence
// return:
// signer address| evidence height|
// 20 bytes      | 32 bytes       |
func (c *verifyDoubleSignEvidence) Run(input []byte) ([]byte, error) {
	evidence := &DoubleSignEvidence{}
	err := rlp.DecodeBytes(input, evidence)
	if err != nil {
		return nil, ErrExecutionReverted
	}

	header1 := &types.Header{}
	err = rlp.DecodeBytes(evidence.HeaderBytes1, header1)
	if err != nil {
		return nil, ErrExecutionReverted
	}

	header2 := &types.Header{}
	err = rlp.DecodeBytes(evidence.HeaderBytes2, header2)
	if err != nil {
		return nil, ErrExecutionReverted
	}

	// basic check
	if len(header1.Number.Bytes()) > 32 || len(header2.Number.Bytes()) > 32 { // block number should be less than 2^256
		return nil, errInvalidEvidence
	}
	if header1.Number.Cmp(header2.Number) != 0 {
		return nil, errInvalidEvidence
	}
	if header1.ParentHash != header2.ParentHash {
		return nil, errInvalidEvidence
	}

	if len(header1.Extra) < extraSeal || len(header2.Extra) < extraSeal {
		return nil, errInvalidEvidence
	}
	sig1 := header1.Extra[len(header1.Extra)-extraSeal:]
	sig2 := header2.Extra[len(header2.Extra)-extraSeal:]
	if bytes.Equal(sig1, sig2) {
		return nil, errInvalidEvidence
	}

	// check sig
	msgHash1 := types.SealHash(header1, evidence.ChainId)
	msgHash2 := types.SealHash(header2, evidence.ChainId)
	if bytes.Equal(msgHash1.Bytes(), msgHash2.Bytes()) {
		return nil, errInvalidEvidence
	}
	pubkey1, err := secp256k1.RecoverPubkey(msgHash1.Bytes(), sig1)
	if err != nil {
		return nil, ErrExecutionReverted
	}
	pubkey2, err := secp256k1.RecoverPubkey(msgHash2.Bytes(), sig2)
	if err != nil {
		return nil, ErrExecutionReverted
	}
	if !bytes.Equal(pubkey1, pubkey2) {
		return nil, errInvalidEvidence
	}

	returnBz := make([]byte, 52) // 20 + 32
	signerAddr := crypto.Keccak256(pubkey1[1:])[12:]
	evidenceHeightBz := header1.Number.Bytes()
	copy(returnBz[:20], signerAddr)
	copy(returnBz[52-len(evidenceHeightBz):], evidenceHeightBz)

	return returnBz, nil
}

// P256VERIFY (secp256r1 signature verification)
// implemented as a native contract
type p256Verify struct{}

// RequiredGas returns the gas required to execute the precompiled contract
func (c *p256Verify) RequiredGas(input []byte) uint64 {
	return params.P256VerifyGas
}

// Run executes the precompiled contract with given 160 bytes of param, returning the output and the used gas
func (c *p256Verify) Run(input []byte) ([]byte, error) {
	// Required input length is 160 bytes
	const p256VerifyInputLength = 160
	// Check the input length
	if len(input) != p256VerifyInputLength {
		// Input length is invalid
		return nil, nil
	}

	// Extract the hash, r, s, x, y from the input
	hash := input[0:32]
	r, s := new(big.Int).SetBytes(input[32:64]), new(big.Int).SetBytes(input[64:96])
	x, y := new(big.Int).SetBytes(input[96:128]), new(big.Int).SetBytes(input[128:160])

	// Verify the secp256r1 signature
	if secp256r1.Verify(hash, r, s, x, y) {
		// Signature is valid
		return libcommon.LeftPadBytes(big1.Bytes(), 32), nil
	} else {
		// Signature is invalid
		return nil, nil
	}
}
