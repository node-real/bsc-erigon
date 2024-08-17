package snaptype

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common/background"
	libsnaptype "github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	initTypes()
}

func initTypes() {
	bscTypes := append(BlockSnapshotTypes, BscSnapshotTypes()...)

	snapcfg.RegisterKnownTypes(networkname.BSCChainName, bscTypes)
	snapcfg.RegisterKnownTypes(networkname.ChapelChainName, bscTypes)
}

func BscSnapshotTypes() []libsnaptype.Type {
	return []libsnaptype.Type{libsnaptype.BlobSidecars}
}

var (
	BlobSidercars = libsnaptype.RegisterType(
		libsnaptype.CaplinEnums.BlobSidecars,
		"blobsidecars",
		libsnaptype.Versions{
			Current:      1, //2,
			MinSupported: 1,
		},
		nil,
		[]libsnaptype.Index{libsnaptype.CaplinIndexes.BlobSidecarSlot},
		libsnaptype.IndexBuilderFunc(
			func(ctx context.Context, info libsnaptype.FileInfo, salt uint32, _ *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				num := make([]byte, binary.MaxVarintLen64)
				if err := libsnaptype.BuildIndex(ctx, info, salt, info.From, tmpDir, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
					if i%20_000 == 0 {
						logger.Log(lvl, fmt.Sprintf("Generating idx for %s", info.Type.Name()), "progress", i)
					}
					p.Processed.Add(1)
					n := binary.PutUvarint(num, i)
					if err := idx.AddKey(num[:n], offset); err != nil {
						return err
					}
					return nil
				}, logger); err != nil {
					return fmt.Errorf("can't index %s: %w", info.Name(), err)
				}
				return nil
			}),
	)
)
