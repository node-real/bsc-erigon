package freezeblocks

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"os"
	"path/filepath"
	"sync/atomic"
)

type BscSnapshots struct {
	indicesReady  atomic.Bool
	segmentsReady atomic.Bool

	Salt uint32

	BlobSidecars *segments

	dir         string
	tmpdir      string
	segmentsMax atomic.Uint64 // all types of .seg files are available - up to this number
	idxMax      atomic.Uint64 // all types of .idx files are available - up to this number
	cfg         ethconfig.BlocksFreezing
	logger      log.Logger
	// allows for pruning segments - this is the min availible segment
	segmentsMin atomic.Uint64
}

// NewBscSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewBscSnapshots(cfg ethconfig.BlocksFreezing, dirs datadir.Dirs, logger log.Logger) *BscSnapshots {
	return &BscSnapshots{dir: dirs.Snap, tmpdir: dirs.Tmp, cfg: cfg, BlobSidecars: &segments{}, logger: logger}
}

func (s *BscSnapshots) IndicesMax() uint64  { return s.idxMax.Load() }
func (s *BscSnapshots) SegmentsMax() uint64 { return s.segmentsMax.Load() }
func (s *BscSnapshots) SegmentsMin() uint64 { return s.segmentsMin.Load() }

func (s *BscSnapshots) LogStat(str string) {
	s.logger.Info(fmt.Sprintf("[snapshots:%s] Stat", str),
		"blocks", fmt.Sprintf("%dk", (s.SegmentsMax()+1)/1000),
		"indices", fmt.Sprintf("%dk", (s.IndicesMax()+1)/1000))
}

func (s *CaplinSnapshots) LS() {
	if s == nil {
		return
	}
	if s.BlobSidecars != nil {
		for _, seg := range s.BlobSidecars.segments {
			if seg.Decompressor == nil {
				continue
			}
			log.Info("[agg] ", "f", seg.Decompressor.FileName(), "words", seg.Decompressor.Count())
		}
	}
}

func (s *BscSnapshots) SegFilePaths(from, to uint64) []string {
	var res []string
	for _, seg := range s.BlobSidecars.segments {
		if seg.from >= from && seg.to <= to {
			res = append(res, seg.FilePath())
		}
	}
	return res
}

func (s *BscSnapshots) BlocksAvailable() uint64 {
	return min(s.segmentsMax.Load(), s.idxMax.Load())
}

func (s *BscSnapshots) Close() {
	if s == nil {
		return
	}
	s.BlobSidecars.lock.Lock()
	defer s.BlobSidecars.lock.Unlock()
	s.closeWhatNotInList(nil)
}

// ReopenList stops on optimistic=false, continue opening files on optimistic=true
func (s *BscSnapshots) ReopenList(fileNames []string, optimistic bool) error {
	s.BlobSidecars.lock.Lock()
	defer s.BlobSidecars.lock.Unlock()

	s.closeWhatNotInList(fileNames)
	var segmentsMax uint64
	var segmentsMaxSet bool
Loop:
	for _, fName := range fileNames {
		f, _, ok := snaptype.ParseFileName(s.dir, fName)
		if !ok {
			continue
		}

		if f.Type.Enum() == snaptype.CaplinEnums.BlobSidecars {
			var sn *Segment
			var exists bool
			for _, sn2 := range s.BlobSidecars.segments {
				if sn2.Decompressor == nil { // it's ok if some segment was not able to open
					continue
				}
				if fName == sn2.FileName() {
					sn = sn2
					exists = true
					break
				}
			}
			if !exists {
				sn = &Segment{segType: snaptype.BlobSidecars, version: f.Version, Range: Range{f.From, f.To}}
			}
			if err := sn.reopenSeg(s.dir); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if optimistic {
						continue Loop
					} else {
						break Loop
					}
				}
				if optimistic {
					s.logger.Warn("[snapshots] open segment", "err", err)
					continue Loop
				} else {
					return err
				}
			}

			if !exists {
				// it's possible to iterate over .seg file even if you don't have index
				// then make segment available even if index open may fail
				s.BlobSidecars.segments = append(s.BlobSidecars.segments, sn)
			}
			if err := sn.reopenIdxIfNeed(s.dir, optimistic); err != nil {
				return err
			}
			if f.To > 0 {
				segmentsMax = f.To - 1
			} else {
				segmentsMax = 0
			}
			segmentsMaxSet = true
		}
	}

	if segmentsMaxSet {
		s.segmentsMax.Store(segmentsMax)
	}
	s.segmentsReady.Store(true)
	s.idxMax.Store(s.idxAvailability())
	s.indicesReady.Store(true)

	return nil
}

func (s *BscSnapshots) idxAvailability() uint64 {
	var blockNum uint64
	for _, seg := range s.BlobSidecars.segments {
		if seg.Index() == nil {
			break
		}
		blockNum = seg.to - 1
	}
	return blockNum
}

func (s *BscSnapshots) ReopenFolder() error {
	files, _, err := SegmentsBsc(s.dir, s.segmentsMin.Load())
	if err != nil {
		return err
	}
	list := make([]string, 0, len(files))
	for _, f := range files {
		_, fName := filepath.Split(f.Path)
		list = append(list, fName)
	}
	return s.ReopenList(list, false)
}

func (s *BscSnapshots) closeWhatNotInList(l []string) {
Loop:
	for i, sn := range s.BlobSidecars.segments {
		if sn.Decompressor == nil {
			continue Loop
		}
		_, name := filepath.Split(sn.FilePath())
		for _, fName := range l {
			if fName == name {
				continue Loop
			}
		}
		sn.close()
		s.BlobSidecars.segments[i] = nil
	}
	var i int
	for i = 0; i < len(s.BlobSidecars.segments) && s.BlobSidecars.segments[i] != nil && s.BlobSidecars.segments[i].Decompressor != nil; i++ {
	}
	tail := s.BlobSidecars.segments[i:]
	s.BlobSidecars.segments = s.BlobSidecars.segments[:i]
	for i = 0; i < len(tail); i++ {
		if tail[i] != nil {
			tail[i].close()
			tail[i] = nil
		}
	}
}

type BscView struct {
	s      *BscSnapshots
	closed bool
}

func (s *BscSnapshots) View() *BscView {
	v := &BscView{s: s}
	v.s.BlobSidecars.lock.RLock()
	return v
}

func (v *BscView) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.s.BlobSidecars.lock.RUnlock()
}

func (v *BscView) BlobSidecars() []*Segment { return v.s.BlobSidecars.segments }

func (v *BscView) BlobSidecarsSegment(blockNum uint64) (*Segment, bool) {
	for _, seg := range v.BlobSidecars() {
		if !(blockNum >= seg.from && blockNum < seg.to) {
			continue
		}
		return seg, true
	}
	return nil, false
}

func dumpBlobsRange(ctx context.Context, db kv.RoDB, store services.BlobStorage, from uint64, to uint64, salt uint32, dirs datadir.Dirs, workers int, lvl log.Lvl, logger log.Logger) error {
	tmpDir, snapDir := dirs.Tmp, dirs.Snap

	segName := snaptype.BlobSidecars.FileName(0, from, to)
	f, _, _ := snaptype.ParseFileName(snapDir, segName)

	sn, err := seg.NewCompressor(ctx, "Snapshot BlobSidecars", f.Path, tmpDir, seg.MinPatternScore, workers, lvl, logger)
	if err != nil {
		return err
	}
	defer sn.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Generate .seg file, which is just the list of beacon blocks.
	for i := from; i < to; i++ {
		// read root.
		blockHash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			return err
		}

		blobTxCount, err := store.BlobTxCount(ctx, blockHash)
		if err != nil {
			return err
		}
		if blobTxCount == 0 {
			sn.AddWord(nil)
			continue
		}
		sidecars, found, err := store.ReadBlobSidecars(ctx, i, blockHash)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("blob sidecars not found for block %d", i)
		}
		dataRLP, err := rlp.EncodeToBytes(sidecars)
		if err != nil {
			return err
		}
		if err := sn.AddWord(dataRLP); err != nil {
			return err
		}
		if i%20_000 == 0 {
			logger.Log(lvl, "Dumping beacon blobs", "progress", i)
		}

	}
	if err := sn.Compress(); err != nil {
		return fmt.Errorf("compress: %w", err)
	}
	// Generate .idx file, which is the slot => offset mapping.
	p := &background.Progress{}

	return BlobSimpleIdx(ctx, f, salt, tmpDir, p, lvl, logger)
}

func DumpBlobs(ctx context.Context, blobStorage services.BlobStorage, db kv.RoDB, fromSlot, toSlot uint64, salt uint32, dirs datadir.Dirs, compressWorkers int, lvl log.Lvl, logger log.Logger) error {
	for i := fromSlot; i < toSlot; i = chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BlobSidecars, nil) {
		blocksPerFile := snapcfg.MergeLimit("", snaptype.CaplinEnums.BlobSidecars, i)

		if toSlot-i < blocksPerFile {
			break
		}
		to := chooseSegmentEnd(i, toSlot, snaptype.CaplinEnums.BlobSidecars, nil)
		logger.Log(lvl, "Dumping blobs sidecars", "from", i, "to", to)
		if err := dumpBlobsRange(ctx, db, blobStorage, i, to, salt, dirs, compressWorkers, lvl, logger); err != nil {
			return err
		}
	}
	return nil
}

func (s *BscSnapshots) BuildMissingIndices(ctx context.Context, logger log.Logger) error {
	if s == nil {
		return nil
	}
	// if !s.segmentsReady.Load() {
	// 	return fmt.Errorf("not all snapshot segments are available")
	// }

	// wait for Downloader service to download all expected snapshots
	segments, _, err := SegmentsBsc(s.dir, 0)
	if err != nil {
		return err
	}
	noneDone := true
	for index := range segments {
		segment := segments[index]
		// The same slot=>offset mapping is used for both beacon blocks and blob sidecars.
		if segment.Type.Enum() != snaptype.CaplinEnums.BlobSidecars {
			continue
		}
		if segment.Type.HasIndexFiles(segment, logger) {
			continue
		}
		p := &background.Progress{}
		noneDone = false
		if err := BlobSimpleIdx(ctx, segment, s.Salt, s.tmpdir, p, log.LvlDebug, logger); err != nil {
			return err
		}
	}
	if noneDone {
		return nil
	}

	return s.ReopenFolder()
}

func (s *BscSnapshots) ReadBlobSidecars(blockNum uint64) ([]*types.BlobSidecar, error) {
	view := s.View()
	defer view.Close()

	var buf []byte

	seg, ok := view.BlobSidecarsSegment(blockNum)
	if !ok {
		return nil, nil
	}

	idxNum := seg.Index()

	if idxNum == nil {
		return nil, nil
	}
	blockOffset := idxNum.OrdinalLookup(blockNum - idxNum.BaseDataID())

	gg := seg.MakeGetter()
	gg.Reset(blockOffset)
	if !gg.HasNext() {
		return nil, nil
	}

	buf, _ = gg.Next(buf)
	if len(buf) == 0 {
		return nil, nil
	}

	var sidecars []*types.BlobSidecar
	err := rlp.DecodeBytes(buf, &sidecars)
	if err != nil {
		return nil, err
	}

	return sidecars, nil
}

func (s *BscSnapshots) FrozenBlobs() uint64 {
	ret := uint64(0)
	for _, seg := range s.BlobSidecars.segments {
		ret = max(ret, seg.to)
	}
	return ret
}

func BlobSimpleIdx(ctx context.Context, sn snaptype.FileInfo, salt uint32, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
	num := make([]byte, binary.MaxVarintLen64)
	if err := snaptype.BuildIndex(ctx, sn, salt, sn.From, tmpDir, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if i%20_000 == 0 {
			logger.Log(lvl, fmt.Sprintf("Generating idx for %s", sn.Type.Name()), "progress", i)
		}
		p.Processed.Add(1)
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	}, logger); err != nil {
		return fmt.Errorf("idx: %w", err)
	}

	return nil
}
