package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"sync"

	pb "github.com/danchia/ddb/proto"
	"github.com/golang/protobuf/proto"
)

const (
	MaxRecordBytes uint32 = 100 * 1024 * 1024
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// Writer writes log entries to the write ahead log.
// Thread-safe.
type Writer struct {
	f         *os.File
	bufWriter *bufio.Writer
	mu        sync.Mutex
	buf       *proto.Buffer
	crc       hash.Hash32
}

func NewWriter(name string) (*Writer, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	wal := &Writer{
		f:         f,
		bufWriter: bufio.NewWriter(f),
		buf:       proto.NewBuffer(nil),
		crc:       crc32.New(crcTable),
	}

	return wal, nil
}

func (w *Writer) Append(l *pb.LogRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf.Reset()
	err := w.buf.Marshal(l)
	if err != nil {
		return err
	}
	data := w.buf.Bytes()
	dataLen := len(data)
	if uint32(dataLen) > MaxRecordBytes {
		return fmt.Errorf("log record has encoded size %d that exceeds %d", dataLen, MaxRecordBytes)
	}

	w.crc.Reset()
	if _, err := w.crc.Write(data); err != nil {
		return err
	}
	c := w.crc.Sum32()

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(dataLen))
	binary.LittleEndian.PutUint32(scratch[4:8], c)

	_, err = w.bufWriter.Write(scratch[:])
	if err != nil {
		return err
	}

	_, err = w.bufWriter.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.bufWriter.Flush()
	return w.f.Close()
}
