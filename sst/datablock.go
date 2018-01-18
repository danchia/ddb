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
	buf *bytes.Buffer

	// scratch space for key encoding
	tmpKey []byte
}

func newDataBlockBuilder() *dataBlockBuilder {
	return &dataBlockBuilder{
		buf: new(bytes.Buffer),
	}
}

// Append writes a new row to the block.
func (b *dataBlockBuilder) Append(key string, timestamp int64, value []byte) error {
	tmpKey, err := orderedcode.Append(b.tmpKey[:0], key, orderedcode.Decr(timestamp))
	b.tmpKey = tmpKey
	if err != nil {
		return err
	}
	if err := writeUvarInt64(b.buf, uint64(len(tmpKey))); err != nil {
		return err
	}
	if err := writeUvarInt64(b.buf, uint64(len(value)+1)); err != nil {
		return err
	}
	if _, err := b.buf.Write(tmpKey); err != nil {
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
	return int64(b.buf.Len())
}

// Finish finishes building the block and returns a slice to its contents.
// Returned slice is valid until Reset() is called.
func (b *dataBlockBuilder) Finish() []byte {
	return b.buf.Bytes()
}

// Reset resets the block.
// However, scratch memory may be retained.
func (b *dataBlockBuilder) Reset() {
	b.buf.Reset()
}

type dataBlock struct {
	r *bytes.Reader
}

func newDataBlock(d []byte) *dataBlock {
	return &dataBlock{r: bytes.NewReader(d)}
}

func (b *dataBlock) Find(key string) (value []byte, ts int64, err error) {
	kb := make([]byte, 0, MaxKeySize)
	for {
		eKeyLen, err := binary.ReadUvarint(b.r)
		if err != nil {
			if err == io.EOF {
				return nil, 0, ErrNotFound
			}
			return nil, 0, err
		}
		valueLen, err := binary.ReadUvarint(b.r)
		if err != nil {
			return nil, 0, err
		}

		kb = kb[:eKeyLen]
		if _, err = io.ReadFull(b.r, kb); err != nil {
			return nil, 0, err
		}
		eKey := string(kb)
		var readKey string
		var ts int64
		if _, err = orderedcode.Parse(eKey, &readKey, orderedcode.Decr(&ts)); err != nil {
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
}
