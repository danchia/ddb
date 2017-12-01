package storage

import (
	"bufio"
	"os"

	"github.com/golang/glog"
)

type SSTWriter struct {
	f *os.File
	w *bufio.Writer
}

func NewSSTWriter(filename string) (*SSTWriter, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	w := &SSTWriter{
		f: f,
		w: bufio.NewWriter(f),
	}
	if err := writeUint64(w.w, SstMagic); err != nil {
		return nil, err
	}
	return w, nil
}

// Append writes a new row to the SSTable.
// Must be called in strictly increasing key order.
func (s *SSTWriter) Append(key string, value []byte) error {
	if len(key) > MaxKeySize {
		glog.Fatalf("Tried to Append key larger than max keysize. key: %s", key)
	}
	if err := writeUvarInt64(s.w, uint64(len(key))); err != nil {
		return err
	}
	if err := writeUvarInt64(s.w, uint64(len(value))); err != nil {
		return err
	}
	if _, err := s.w.WriteString(key); err != nil {
		return err
	}
	if _, err := s.w.Write(value); err != nil {
		return err
	}

	return nil
}

// Close finalizes the SST being written.
func (s *SSTWriter) Close() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Close()
}
