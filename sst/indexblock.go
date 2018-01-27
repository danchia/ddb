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
	"io"
)

// TODO: there's a lot of duplication with datablock. Worth trying to merge?
type indexBlockBuilder struct {
	buf           *bytes.Buffer
	prefixEncoder *prefixEncoder

	bhBuffer *bytes.Buffer
}

func newIndexBlockBuilder() *indexBlockBuilder {
	return &indexBlockBuilder{
		buf:           new(bytes.Buffer),
		bhBuffer:      new(bytes.Buffer),
		prefixEncoder: newPrefixEncoder(16),
	}
}

func (b *indexBlockBuilder) Append(key string, bh blockHandle) error {
	b.bhBuffer.Reset()
	bh.EncodeTo(b.bhBuffer)
	if err := b.prefixEncoder.EncodeInto(b.buf, []byte(key), uint32(b.buf.Len())); err != nil {
		return err
	}
	if err := writeUvarInt64(b.buf, uint64(b.bhBuffer.Len())); err != nil {
		return err
	}
	if _, err := b.buf.Write(b.bhBuffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (b *indexBlockBuilder) Finish() ([]byte, error) {
	if err := b.prefixEncoder.WriteRestarts(b.buf); err != nil {
		return nil, err
	}
	return b.buf.Bytes(), nil
}

type indexBlock struct {
	r        *bytes.Reader
	restarts []int
}

func newIndexBlock(d []byte) *indexBlock {
	nRestarts := int(binary.LittleEndian.Uint32(d[len(d)-4:]))
	restarts := make([]int, 0, nRestarts)
	restartOffset := len(d) - 4 - 4*nRestarts
	for o := restartOffset; o < len(d)-4; o += 4 {
		restarts = append(restarts, int(binary.LittleEndian.Uint32(d[o:o+4])))
	}

	return &indexBlock{
		r:        bytes.NewReader(d[:restartOffset]),
		restarts: restarts,
	}
}

func (b *indexBlock) Find(key string) (blockHandle, error) {
	var bh blockHandle
	kb := make([]byte, 0, MaxSstKeySize)
	i, j := 0, len(b.restarts)-1
	for i < j {
		// lean right when there's two elements
		h := int(uint(i+j+1) >> 1)
		if _, err := b.r.Seek(int64(b.restarts[h]), io.SeekStart); err != nil {
			return bh, err
		}
		eKey, err := prefixDecodeFrom(b.r, nil, kb)
		if err != nil {
			return bh, err
		}
		readKey := string(eKey)
		if readKey < key {
			i = h
		} else {
			j = h - 1
		}
	}

	if _, err := b.r.Seek(int64(b.restarts[i]), io.SeekStart); err != nil {
		return bh, err
	}

	var lastKey []byte
	for b.r.Len() > 0 {
		eKey, err := prefixDecodeFrom(b.r, lastKey, kb)
		lastKey = eKey
		readKey := string(eKey)
		valueLen, err := binary.ReadUvarint(b.r)
		if err != nil {
			return bh, err
		}
		if key <= readKey {
			bh, err = newBlockHandle(b.r)
			return bh, err
		}
		if _, err = b.r.Seek(int64(valueLen), io.SeekCurrent); err != nil {
			return bh, err
		}
	}
	return bh, ErrNotFound
}
