package finality

import (
	"sync"

	libcommon "github.com/erigontech/erigon-lib/common"
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

func (fs *FinalizationService) UpdateFinality(finalizedHash libcommon.Hash, safeHash libcommon.Hash) {
	fs.mux.Lock()
	defer fs.mux.Unlock()
	fs.FinalizeBlockHash = finalizedHash
	fs.SafeBlockHash = safeHash
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
