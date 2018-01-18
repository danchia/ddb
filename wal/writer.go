//    Copyright 2018 Google Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
)

const (
	// MaxRecordBytes is the largest size a single record can be.
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
	nextSeq   int64
	opts      Options
	filename  string
	size      int64
}

type Options struct {
	Dirname    string
	TargetSize int64
}

func NewWriter(nextSeq int64, opts Options) (*Writer, error) {
	wal := &Writer{
		buf:     proto.NewBuffer(nil),
		crc:     crc32.New(crcTable),
		nextSeq: nextSeq,
		opts:    opts,
	}

	if err := wal.rollover(); err != nil {
		return nil, err
	}

	return wal, nil
}

func logName(nextSeq int64, o Options) string {
	return fmt.Sprintf("%s%cwal-%d.log", o.Dirname, os.PathSeparator, nextSeq)
}

func (w *Writer) rollover() error {
	fn := logName(w.nextSeq, w.opts)

	glog.Infof("Rolling over WAL from %v to %v.", w.filename, fn)

	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			return err
		}
		if err := w.f.Sync(); err != nil {
			return err
		}
		if err := w.f.Close(); err != nil {
			return err
		}
	}
	f, err := os.Create(fn)
	if err != nil {
		return err
	}

	w.filename = fn
	w.f = f
	w.bufWriter = bufio.NewWriter(f)
	w.size = 0

	return nil
}

// Append appends a log record to the WAL. The log record is modified with the log sequence number.
func (w *Writer) Append(l *pb.LogRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.size > w.opts.TargetSize {
		if err := w.rollover(); err != nil {
			glog.Warningf("Error while attempting to rollover WAL: %v", err)
			return err
		}
	}

	l.Sequence = w.nextSeq
	w.nextSeq++

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
	w.size += int64(dataLen) + 8

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
