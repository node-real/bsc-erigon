package blob_storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rlp"
	"github.com/golang/snappy"
	"io"
	"sync"
)

var writerPool = sync.Pool{
	New: func() any {
		return snappy.NewBufferedWriter(nil)
	},
}

func snappyWrite(w io.Writer, data []byte, prefix ...byte) error {
	// create prefix for length of packet
	lengthBuf := make([]byte, 10)
	vin := binary.PutUvarint(lengthBuf, uint64(len(data)))

	// Create writer size
	wr := bufio.NewWriterSize(w, 10+len(data))
	defer wr.Flush()
	// Write length of packet
	wr.Write(prefix)
	wr.Write(lengthBuf[:vin])

	// start using streamed snappy compression
	sw, _ := writerPool.Get().(*snappy.Writer)
	sw.Reset(wr)
	defer func() {
		sw.Flush()
		writerPool.Put(sw)
	}()
	_, err := sw.Write(data)
	return err
}

func snappyReader(r io.Reader, val *types.BlobSidecar) error {
	// Read varint for length of message.
	encodedLn, _, err := ssz_snappy.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("unable to read varint from message prefix: %v", err)
	}
	if encodedLn > uint64(16*datasize.MB) {
		return errors.New("payload too big")
	}
	sr := snappy.NewReader(r)
	raw := make([]byte, encodedLn)
	if _, err := io.ReadFull(sr, raw); err != nil {
		return fmt.Errorf("unable to readPacket: %w", err)
	}

	err = rlp.DecodeBytes(raw, val)
	if err != nil {
		return fmt.Errorf("enable to unmarshall message: %v", err)
	}
	return nil
}
