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
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	pb "github.com/danchia/ddb/proto"
	"github.com/golang/protobuf/proto"
)

// Scanner reads log records from a write ahead log directory.
// Not thread-safe.
type Scanner struct {
	dirname string
	// list of log files to scan, in ascending seqNo.
	filenameInfos []filenameInfo

	curIndex   int
	curScanner *fileScanner

	err error
}

// NewScanner returns a log scanner over all the log files found in dirname.
// Returns ErrNotExist if there are no log files.
func NewScanner(dirname string) (*Scanner, error) {
	parsedNames, err := listLogFiles(dirname)
	if err != nil {
		return nil, err
	}

	return &Scanner{dirname: dirname, filenameInfos: parsedNames}, nil
}

// Scan advances the fileScanner to the next log record, which will then be
// available through the Record method. It returns false when the scan stops,
// either by reaching the end of all logs or on error.
func (s *Scanner) Scan() bool {
	for {
		if !s.maybeAdvanceFileScanner() {
			return false
		}

		hasNext := s.curScanner.Scan()
		if hasNext {
			return true
		}
		if s.curScanner.Err() != nil {
			return false
		}
		// reached end of current file
		s.curScanner = nil
	}
}

// returns whether attempted advance was successful
func (s *Scanner) maybeAdvanceFileScanner() bool {
	if s.curScanner == nil {
		if s.curIndex >= len(s.filenameInfos) {
			return false
		}
		fi := s.filenameInfos[s.curIndex]
		s.curIndex++

		fileScanner, err := newFileScanner(filepath.Join(s.dirname, fi.name))
		if err != nil {
			s.err = err
			return false
		}
		s.curScanner = fileScanner
	}
	return true
}

// Record returns the current record.
// Only valid until the next Scan() call.
// Caller should not modify returned proto.
func (s *Scanner) Record() *pb.LogRecord {
	return s.curScanner.Record()
}

// Err returns last error, if any.
func (s *Scanner) Err() error {
	if s.err != nil {
		return s.err
	}
	if s.curScanner != nil {
		return s.curScanner.err
	}
	return nil
}

// fileScanner reads log records from a write ahead log.
// Not thread-safe.
type fileScanner struct {
	f   *os.File
	err error
	l   *pb.LogRecord
	h   hash.Hash32
}

func newFileScanner(name string) (*fileScanner, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	s := &fileScanner{
		f: f,
		l: &pb.LogRecord{},
		h: crc32.New(crcTable),
	}
	return s, nil
}

// Scan advances the fileScanner to the next log record, which will then be
// available through the Record method. It returns false when the scan stops,
// either by reaching the end of the log or on error.
func (s *fileScanner) Scan() bool {
	s.l.Reset()

	var scratch [8]byte
	if _, s.err = io.ReadFull(s.f, scratch[:]); s.err != nil {
		if s.err == io.EOF {
			// Expected error.
			s.err = nil
		}
		return false
	}
	dataLen := binary.LittleEndian.Uint32(scratch[0:4])
	crc := binary.LittleEndian.Uint32(scratch[4:8])

	// TODO: reuse buffers
	data := make([]byte, dataLen, dataLen)

	if _, s.err = io.ReadFull(s.f, data); s.err != nil {
		return false
	}
	s.h.Reset()
	if _, s.err = s.h.Write(data); s.err != nil {
		return false
	}
	c := s.h.Sum32()
	if c != crc {
		s.err = fmt.Errorf("checksum mismatch. expected %d, got %d", crc, c)
		return false
	}

	if s.err = proto.Unmarshal(data, s.l); s.err != nil {
		return false
	}

	return true
}

// Returns the current record.
// Only valid until the next Scan() call.
// Caller should not modify returned proto.
func (s *fileScanner) Record() *pb.LogRecord {
	return s.l
}

// Returns last error, if any.
func (s *fileScanner) Err() error {
	return s.err
}
