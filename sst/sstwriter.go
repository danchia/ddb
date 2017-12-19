package sst

import (
	"bufio"
	"os"

	"github.com/golang/glog"
	"github.com/google/orderedcode"
)

type Writer struct {
	f      *os.File
	w      *bufio.Writer
	tmpKey []byte
}

func NewWriter(filename string) (*Writer, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		f: f,
		w: bufio.NewWriter(f),
	}
	if err := writeUint64(w.w, SstMagic); err != nil {
		return nil, err
	}
	return w, nil
}

// Append writes a new row to the SSTable.
// Must be called in order, i.e. key asc, timestamp desc
func (s *Writer) Append(key string, timestamp int64, value []byte) error {
	if len(key) > MaxKeySize {
		glog.Fatalf("Tried to Append key larger than max keysize. key: %s", key)
	}
	tmpKey, err := orderedcode.Append(s.tmpKey[:0], key, orderedcode.Decr(timestamp))
	s.tmpKey = tmpKey
	if err != nil {
		return err
	}
	if err := writeUvarInt64(s.w, uint64(len(tmpKey))); err != nil {
		return err
	}
	if err := writeUvarInt64(s.w, uint64(len(value)+1)); err != nil {
		return err
	}
	if _, err := s.w.Write(tmpKey); err != nil {
		return err
	}
	if value == nil {
		if err := s.w.WriteByte(typeNil); err != nil {
			return err
		}
	} else {
		if err := s.w.WriteByte(typeBytes); err != nil {
			return err
		}
		if _, err := s.w.Write(value); err != nil {
			return err
		}
	}

	return nil
}

// Close finalizes the SST being written.
func (s *Writer) Close() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Close()
}
