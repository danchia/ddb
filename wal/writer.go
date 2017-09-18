package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	pb "github.com/danchia/ddb/proto"
	"github.com/golang/protobuf/proto"
)

const (
	MaxRecordBytes uint32 = 100 * 1024 * 1024
)

type Writer struct {
	f *os.File
}

func NewWriter(name string) (*Writer, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	wal := &Writer{f}

	return wal, nil
}

func (w *Writer) Append(l *pb.LogRecord) error {
	//TODO: Use a proto.Buffer here instead.
	data, err := proto.Marshal(l)
	if err != nil {
		return err
	}
	dataLen := len(data)
	if uint32(dataLen) > MaxRecordBytes {
		return fmt.Errorf("log record has encoded size %d that exceeds %d", dataLen, MaxRecordBytes)
	}

	c := crc32.ChecksumIEEE(data)

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(dataLen))
	binary.LittleEndian.PutUint32(scratch[4:8], c)

	_, err = w.f.Write(scratch[:])
	if err != nil {
		return err
	}

	_, err = w.f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (w *Writer) Sync() error {
	return w.f.Sync()
}

func (w *Writer) Close() error {
	return w.f.Close()
}
