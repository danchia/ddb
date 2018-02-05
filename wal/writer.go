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
	nextSeq  int64
	buf      *proto.Buffer
	crc      hash.Hash32
	filename string
	size     int64
	opts     Options
	mu       sync.Mutex

	f         *os.File
	bufWriter *bufio.Writer
	recordCh  chan rawRecord

	closeCh       chan struct{}
	closeResultCh chan error
}

type Options struct {
	Dirname    string
	TargetSize int64
}

func NewWriter(nextSeq int64, opts Options) (*Writer, error) {
	writer := &Writer{
		buf:           proto.NewBuffer(nil),
		crc:           crc32.New(crcTable),
		nextSeq:       nextSeq,
		opts:          opts,
		recordCh:      make(chan rawRecord, 1000),
		closeCh:       make(chan struct{}),
		closeResultCh: make(chan error),
	}
	if err := writer.rollover(nextSeq); err != nil {
		return nil, err
	}
	go writer.writeLoop()
	return writer, nil
}

type rawRecord struct {
	seq      int64
	data     []byte
	checkSum uint32
	cb       func(error)
}

// Append appends a log record to the WAL. The log record is modified with the log sequence number.
// cb is invoked serially, in log sequence number order.
func (w *Writer) Append(l *pb.LogRecord, cb func(error)) {
	glog.V(2).Infof("wal.Append %v", l)

	w.mu.Lock()
	defer w.mu.Unlock()

	r, err := w.formRecord(l)
	if err != nil {
		cb(err)
	}
	r.cb = cb

	w.recordCh <- r
}

func (w *Writer) formRecord(l *pb.LogRecord) (rawRecord, error) {
	l.Sequence = w.nextSeq
	w.nextSeq++

	w.buf.Reset()
	err := w.buf.Marshal(l)
	if err != nil {
		return rawRecord{}, err
	}
	data := w.buf.Bytes()
	dataLen := len(data)
	if uint32(dataLen) > MaxRecordBytes {
		return rawRecord{}, fmt.Errorf("log record has encoded size %d that exceeds %d", dataLen, MaxRecordBytes)
	}

	w.crc.Reset()
	if _, err := w.crc.Write(data); err != nil {
		return rawRecord{}, err
	}
	c := w.crc.Sum32()

	dataCopy := make([]byte, dataLen)
	copy(dataCopy, data)

	r := rawRecord{
		seq:      l.Sequence,
		data:     dataCopy,
		checkSum: c,
	}

	return r, nil
}

func logName(nextSeq int64, o Options) string {
	return fmt.Sprintf("%s%cwal-%d.log", o.Dirname, os.PathSeparator, nextSeq)
}

func (w *Writer) rollover(seq int64) error {
	fn := logName(seq, w.opts)

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

func (w *Writer) writeLoop() {
	// TODO: error handling
	callbacks := make([]func(error), 0)
Main:
	for {
		callbacks = callbacks[:0]
		// wait for first record
		select {
		case r := <-w.recordCh:
			if err := w.writeRawRecord(r); err != nil {
				r.cb(err)
			} else {
				callbacks = append(callbacks, r.cb)
			}
		case <-w.closeCh:
			break Main
		}

		// write out all remaining records
	L:
		for {
			select {
			case r := <-w.recordCh:
				if err := w.writeRawRecord(r); err != nil {
					r.cb(err)
				} else {
					callbacks = append(callbacks, r.cb)
				}
			default:
				break L
			}
		}

		// sync, then notify.
		glog.V(4).Infof("Notifying %v callbacks", len(callbacks))
		err := w.sync()
		for _, cb := range callbacks {
			cb(err)
		}
	}

	glog.V(2).Info("wal.writeLoop shutting down")

	if err := w.bufWriter.Flush(); err != nil {
		w.closeResultCh <- err
	}
	w.closeResultCh <- w.f.Close()
}

func (w *Writer) writeRawRecord(r rawRecord) error {
	glog.V(4).Infof("wal writing raw record for seq %v", r.seq)

	if w.size > w.opts.TargetSize {
		if err := w.rollover(r.seq); err != nil {
			glog.Warningf("Error while attempting to rollover WAL: %v", err)
			return err
		}
	}

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(r.data)))
	binary.LittleEndian.PutUint32(scratch[4:8], r.checkSum)

	if _, err := w.bufWriter.Write(scratch[:]); err != nil {
		return err
	}
	w.size += int64(len(r.data)) + 8

	if _, err := w.bufWriter.Write(r.data); err != nil {
		return err
	}

	return nil
}

func (w *Writer) sync() error {
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	w.closeCh <- struct{}{}
	return <-w.closeResultCh
}
