package wal

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"

	pb "github.com/danchia/ddb/proto"
	"github.com/golang/protobuf/proto"
)

// Scanner reads log records from a write ahead log.
// Not thread-safe.
type Scanner struct {
	f   *os.File
	err error
	l   *pb.LogRecord
	h   hash.Hash32
}

func NewScanner(name string) (*Scanner, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	s := &Scanner{
		f: f,
		l: &pb.LogRecord{},
		h: crc32.New(crcTable),
	}
	return s, nil
}

// Scan advances the Scanner to the next log record, which will then be
// available through the Record method. It returns false when the scan stops,
// either by reaching the end of the log or on error.
func (s *Scanner) Scan() bool {
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
		s.err = fmt.Errorf("Checksum mismatch. Expected %d, got %d.", crc, c)
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
func (s *Scanner) Record() *pb.LogRecord {
	return s.l
}

// Returns last error, if any.
func (s *Scanner) Err() error {
	return s.err
}
