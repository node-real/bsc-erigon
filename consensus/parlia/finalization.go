package parlia

import (
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type FinalizationService struct {
	SafeBlockHash     libcommon.Hash
	FinalizeBlockHash libcommon.Hash
	mux               sync.RWMutex
}

var fs *FinalizationService

func RegisterService() {
	fs = NewFinalizationService()
}

func GetFinalizationService() *FinalizationService {
	return fs
}

func NewFinalizationService() *FinalizationService {
	return &FinalizationService{
		SafeBlockHash:     libcommon.Hash{},
		FinalizeBlockHash: libcommon.Hash{},
	}
}

func (fs *FinalizationService) UpdateFinality(snap *Snapshot) {
	if snap.Attestation != nil {
		fs.mux.Lock()
		defer fs.mux.Unlock()
		fs.FinalizeBlockHash = snap.Attestation.SourceHash
		fs.SafeBlockHash = snap.Attestation.TargetHash
	}
}

func (fs *FinalizationService) GetFinalization() (libcommon.Hash, libcommon.Hash) {
	fs.mux.RLock()
	defer fs.mux.RUnlock()
	return fs.SafeBlockHash, fs.FinalizeBlockHash
}

func (fs *FinalizationService) GetFinalizeBlockHash() libcommon.Hash {
	fs.mux.RLock()
	defer fs.mux.RUnlock()
	return fs.FinalizeBlockHash
}

func (fs *FinalizationService) GetSafeBlockHash() libcommon.Hash {
	fs.mux.RLock()
	defer fs.mux.RUnlock()
	return fs.SafeBlockHash
}
