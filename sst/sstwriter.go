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

package sst

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"os"

	"github.com/golang/glog"
)

type Writer struct {
	f   *os.File
	w   *bufio.Writer
	crc hash.Hash32

	lastKey string
	offset  uint64

	dataBlockB  *dataBlockBuilder
	indexBlockB *indexBlockBuilder
}

func NewWriter(filename string) (*Writer, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		f:           f,
		w:           bufio.NewWriter(f),
		crc:         crc32.New(crcTable),
		dataBlockB:  newDataBlockBuilder(),
		indexBlockB: newIndexBlockBuilder(),
	}
	return w, nil
}

// Append writes a new row to the SSTable.
// Must be called in order, i.e. key asc, timestamp desc
func (s *Writer) Append(key string, timestamp int64, value []byte) error {
	if s.dataBlockB.EstimatedSizeBytes() > blockSize {
		if err := s.flushBlock(); err != nil {
			return err
		}
	}
	s.lastKey = key
	return s.dataBlockB.Append(key, timestamp, value)
}

func (s *Writer) flushBlock() error {
	blockData, err := s.dataBlockB.Finish()
	if err != nil {
		return err
	}

	bh := blockHandle{s.offset, uint64(len(blockData))}
	s.indexBlockB.Append(s.lastKey, bh)

	if err := s.writeChecksummedBlock(blockData); err != nil {
		return err
	}

	s.dataBlockB.Reset()
	return nil
}

// Close finalizes the SST being written.
func (s *Writer) Close() error {
	if err := s.flushBlock(); err != nil {
		return err
	}
	indexHandle, err := s.writeIndexBlock()
	if err != nil {
		return err
	}
	if err := s.writeFooter(indexHandle); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Close()
}

// writeIndexBlock writes the index block and returns a blockHandle pointing to it.
func (s *Writer) writeIndexBlock() (blockHandle, error) {
	d, err := s.indexBlockB.Finish()
	if err != nil {
		return blockHandle{}, err
	}
	bh := blockHandle{s.offset, uint64(len(d))}
	return bh, s.writeChecksummedBlock(d)
}

func (s *Writer) writeChecksummedBlock(d []byte) error {
	if _, err := s.w.Write(d); err != nil {
		return err
	}

	s.crc.Reset()
	if _, err := s.crc.Write(d); err != nil {
		// Technically should not be possible
		return err
	}
	c := s.crc.Sum32()
	if err := writeUint32(s.w, c); err != nil {
		return err
	}
	s.offset += uint64(len(d)) + 4

	return nil
}

func (s *Writer) writeFooter(indexHandle blockHandle) error {
	footer := new(bytes.Buffer)
	indexHandle.EncodeTo(footer)
	for footer.Len() < binary.MaxVarintLen64 {
		footer.WriteByte(0)
	}

	s.crc.Reset()
	s.crc.Write(footer.Bytes())
	c := s.crc.Sum32()
	writeUint32(footer, c)

	if err := writeUint64(footer, SstMagic); err != nil {
		return err
	}

	d := footer.Bytes()
	if len(d) != footerSize {
		glog.Fatalf("writerFooter generated footer of wrong length: %v", d)
	}

	_, err := s.w.Write(d)
	return err
}
