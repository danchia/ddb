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

	"github.com/google/orderedcode"
)

type dataBlockBuilder struct {
	buf           *bytes.Buffer
	prefixEncoder *prefixEncoder

	// scratch space for key encoding
	tmpKey []byte
}

func newDataBlockBuilder() *dataBlockBuilder {
	return &dataBlockBuilder{
		buf:           new(bytes.Buffer),
		prefixEncoder: newPrefixEncoder(16),
	}
}

// Append writes a new row to the block.
func (b *dataBlockBuilder) Append(key string, timestamp int64, value []byte) error {
	tmpKey, err := orderedcode.Append(b.tmpKey[:0], key, orderedcode.Decr(timestamp))
	b.tmpKey = tmpKey
	if err != nil {
		return err
	}
	if err := b.prefixEncoder.EncodeInto(b.buf, tmpKey, uint32(b.buf.Len())); err != nil {
		return err
	}
	if err := writeUvarInt64(b.buf, uint64(len(value)+1)); err != nil {
		return err
	}
	if value == nil {
		if err := b.buf.WriteByte(typeNil); err != nil {
			return err
		}
	} else {
		if err := b.buf.WriteByte(typeBytes); err != nil {
			return err
		}
		if _, err := b.buf.Write(value); err != nil {
			return err
		}
	}
	return nil
}

// EstimatedSizeBytes returns the estimated current size of the block.
func (b *dataBlockBuilder) EstimatedSizeBytes() int64 {
	return int64(b.buf.Len()) + int64(len(b.prefixEncoder.Restarts()))*4 + 4
}

// Finish finishes building the block and returns a slice to its contents.
// Returned slice is valid until Reset() is called.
func (b *dataBlockBuilder) Finish() ([]byte, error) {
	if err := b.prefixEncoder.WriteRestarts(b.buf); err != nil {
		return nil, err
	}
	return b.buf.Bytes(), nil
}

// Reset resets the block.
// However, scratch memory may be retained.
func (b *dataBlockBuilder) Reset() {
	b.buf.Reset()
	b.prefixEncoder.Reset()
}

type dataBlock struct {
	r        *bytes.Reader
	restarts []int
}

func newDataBlock(d []byte) *dataBlock {
	nRestarts := int(binary.LittleEndian.Uint32(d[len(d)-4:]))
	restarts := make([]int, 0, nRestarts)
	restartOffset := len(d) - 4 - 4*nRestarts
	for o := restartOffset; o < len(d)-4; o += 4 {
		restarts = append(restarts, int(binary.LittleEndian.Uint32(d[o:o+4])))
	}

	return &dataBlock{
		r:        bytes.NewReader(d[:restartOffset]),
		restarts: restarts,
	}
}

func (b *dataBlock) Find(key string) (value []byte, ts int64, err error) {
	kb := make([]byte, 0, MaxSstKeySize)
	i, j := 0, len(b.restarts)-1
	for i < j {
		// when there 2 elements, pick the right one
		h := int(uint(i+j+1) >> 1)
		if _, err := b.r.Seek(int64(b.restarts[h]), io.SeekStart); err != nil {
			return nil, 0, err
		}
		eKey, err := prefixDecodeFrom(b.r, nil, kb)
		if err != nil {
			return nil, 0, err
		}
		readKey, _, err := parseEKey(string(eKey))
		if err != nil {
			return nil, 0, err
		}
		if readKey < key {
			// key_h is < key, so everything before not relevant.
			i = h
		} else {
			// key_h is >= key, so it and every after is not relevant.
			j = h - 1
		}
	}

	// at this point, either key_i < key
	// or i == 0 and key_i has unknown relation to key.

	if _, err := b.r.Seek(int64(b.restarts[i]), io.SeekStart); err != nil {
		return nil, 0, err
	}

	var lastKey []byte
	for b.r.Len() > 0 {
		eKey, err := prefixDecodeFrom(b.r, lastKey, kb)
		if err != nil {
			return nil, 0, err
		}
		lastKey = eKey

		readKey, ts, err := parseEKey(string(eKey))
		if err != nil {
			return nil, 0, err
		}
		valueLen, err := binary.ReadUvarint(b.r)
		if err != nil {
			return nil, 0, err
		}

		if readKey == key {
			value := make([]byte, valueLen)
			if _, err = io.ReadFull(b.r, value); err != nil {
				return nil, 0, err
			}
			if value[0] == typeNil {
				return nil, ts, nil
			}
			return value[1:], ts, nil
		}
		if readKey > key {
			return nil, 0, ErrNotFound
		}

		if _, err = b.r.Seek(int64(valueLen), io.SeekCurrent); err != nil {
			return nil, 0, err
		}
	}
	return nil, 0, ErrNotFound
}

func parseEKey(eKey string) (key string, ts int64, err error) {
	var readKey string
	if _, err = orderedcode.Parse(string(eKey), &readKey, orderedcode.Decr(&ts)); err != nil {
		return "", 0, err
	}
	return readKey, ts, nil
}
