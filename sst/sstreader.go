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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/golang/glog"
)

// Reader is an SSTable reader.
// Threadsafe.
type Reader struct {
	f        *os.File
	fLength  int64
	filename string

	indexBlockHandle blockHandle
}

func NewReader(filename string) (*Reader, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	fInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	r := &Reader{
		f:        f,
		fLength:  fInfo.Size(),
		filename: filename,
	}
	if err := r.readFooter(); err != nil {
		return nil, fmt.Errorf("error while reading footer: %v", err)
	}
	return r, nil
}

type Iter struct {
	r *Reader
}

func (r *Reader) Find(key string) (value []byte, ts int64, err error) {
	bh, err := r.findDataBlock(key)
	if err != nil {
		return nil, 0, err
	}
	glog.V(4).Infof("reading data block %v", bh)
	data, err := r.readRawBlock(bh)
	if err != nil {
		return nil, 0, err
	}

	db := newDataBlock(data)
	return db.Find(key)
}

// findDataBlock finds the first data block containing key.
// Returns ErrNotFound if key could not be found.
func (r *Reader) findDataBlock(key string) (blockHandle, error) {
	var res blockHandle

	glog.V(4).Infof("reading index block %v", r.indexBlockHandle)
	ibd, err := r.readRawBlock(r.indexBlockHandle)
	if err != nil {
		return res, err
	}
	ib := newIndexBlock(ibd)

	return ib.Find(key)
}

func (r *Reader) readRawBlock(h blockHandle) ([]byte, error) {
	raw := make([]byte, h.size+4)
	if _, err := r.f.ReadAt(raw, int64(h.offset)); err != nil {
		return nil, err
	}
	bd := raw[:h.size]
	if !verifyChecksum(bd, raw[h.size:]) {
		glog.V(2).Infof("sst block corrupt, checksum mismatch. blockHandle: %v", h)
		return nil, ErrCorruption
	}
	return bd, nil
}

func (r *Reader) readFooter() error {
	if r.fLength < footerSize {
		glog.Warningf("sst file is too small to have footer. file: %v", r.filename)
		return ErrCorruption
	}
	footer := make([]byte, footerSize)
	if _, err := r.f.ReadAt(footer, r.fLength-footerSize); err != nil {
		return err
	}
	if binary.LittleEndian.Uint64(footer[footerSize-8:]) != SstMagic {
		glog.Warningf("sst footer has invalid magic. file: %v", r.filename)
		return ErrCorruption
	}

	if !verifyChecksum(footer[:footerSize-12], footer[footerSize-12:footerSize-8]) {
		glog.Warningf("sst footer corrupted for %v", r.filename)
		return ErrCorruption
	}
	ibh, err := newBlockHandle(bytes.NewReader(footer))
	if err != nil {
		return err
	}
	r.indexBlockHandle = ibh
	return nil
}

func verifyChecksum(data []byte, sum []byte) bool {
	crc := crc32.New(crcTable)
	crc.Write(data)
	c := crc.Sum32()
	ec := binary.LittleEndian.Uint32(sum)
	if ec != c {
		glog.V(2).Infof("crc got, want: %v %v", c, ec)
	}
	return ec == c
}
