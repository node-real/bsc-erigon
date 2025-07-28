package finality

import (
	"sync"

	"github.com/erigontech/erigon-lib/common"
)

type FinalizationService struct {
	SafeBlockHash     common.Hash
	FinalizeBlockHash common.Hash
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
		SafeBlockHash:     common.Hash{},
		FinalizeBlockHash: common.Hash{},
	}
}

func (fs *FinalizationService) UpdateFinality(finalizedHash common.Hash, safeHash common.Hash) {
	fs.mux.Lock()
	defer fs.mux.Unlock()
	fs.FinalizeBlockHash = finalizedHash
	fs.SafeBlockHash = safeHash
}

func (fs *FinalizationService) GetFinalization() (common.Hash, common.Hash) {
	fs.mux.RLock()
	defer fs.mux.RUnlock()
	return fs.SafeBlockHash, fs.FinalizeBlockHash
}

func (fs *FinalizationService) GetFinalizeBlockHash() common.Hash {
	fs.mux.RLock()
	defer fs.mux.RUnlock()
	return fs.FinalizeBlockHash
}

func (fs *FinalizationService) GetSafeBlockHash() common.Hash {
	fs.mux.RLock()
	defer fs.mux.RUnlock()
	return fs.SafeBlockHash
}
