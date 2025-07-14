package snaptype

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/snaptype"
	"github.com/erigontech/erigon-lib/version"
)

func init() {
	initTypes()
}

func initTypes() {
	bscTypes := append(BlockSnapshotTypes, BscSnapshotTypes...)
	snapcfg.RegisterKnownTypes(networkname.BSC, bscTypes)
	snapcfg.RegisterKnownTypes(networkname.Chapel, bscTypes)
}

var (
	BlobSidecars = snaptype.RegisterType(
		Enums.BscBlobs,
		"bscblobsidecars",
		snaptype.Versions{
			Current:      version.V1_0, //2,
			MinSupported: version.V1_0,
		},
		nil,
		[]snaptype.Index{Indexes.BscBlobNum},
		snaptype.IndexBuilderFunc(
			func(ctx context.Context, info snaptype.FileInfo, salt uint32, _ *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (err error) {
				num := make([]byte, binary.MaxVarintLen64)
				cfg := recsplit.RecSplitArgs{
					Enums:      true,
					BucketSize: 2000,
					LeafSize:   8,
					TmpDir:     tmpDir,
					Salt:       &salt,
					BaseDataID: info.From,
				}
				if err := snaptype.BuildIndex(ctx, info, cfg, log.LvlDebug, p, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
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
	BscSnapshotTypes = []snaptype.Type{BlobSidecars}
)
