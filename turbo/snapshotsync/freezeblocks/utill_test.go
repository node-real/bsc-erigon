package freezeblocks

import (
	"github.com/ledgerwatch/erigon/core/types"
	"reflect"
	"testing"
)

func TestGetBlobSidecars(t *testing.T) {

	tests := []struct {
		name        string
		blockNumber uint64
		want        types.BlobSidecars
	}{
		{
			name:        "test1",
			blockNumber: uint64(39565743),
			want:        nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetBlobSidecars(tt.blockNumber); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetBlobSidecars() = %v, want %v", got, tt.want)
			}
		})
	}
}
