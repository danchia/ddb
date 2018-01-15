package sst

import (
	"bytes"
	"encoding/binary"
	"io"
)

type indexBlockBuilder struct {
	buf *bytes.Buffer

	bhBuffer *bytes.Buffer
}

func newIndexBlockBuilder() *indexBlockBuilder {
	return &indexBlockBuilder{
		buf:      new(bytes.Buffer),
		bhBuffer: new(bytes.Buffer),
	}
}

func (b *indexBlockBuilder) Append(key string, bh blockHandle) error {
	b.bhBuffer.Reset()
	bh.EncodeTo(b.bhBuffer)
	if err := writeUvarInt64(b.buf, uint64(len(key))); err != nil {
		return err
	}
	if err := writeUvarInt64(b.buf, uint64(b.bhBuffer.Len())); err != nil {
		return err
	}
	if _, err := b.buf.WriteString(key); err != nil {
		return err
	}
	if _, err := b.buf.Write(b.bhBuffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (b *indexBlockBuilder) Finish() []byte {
	return b.buf.Bytes()
}

type indexBlock struct {
	r *bytes.Reader
}

func newIndexBlock(d []byte) *indexBlock {
	return &indexBlock{
		r: bytes.NewReader(d),
	}
}

func (b *indexBlock) Find(key string) (blockHandle, error) {
	var h blockHandle
	kb := make([]byte, 0, MaxKeySize)
	for {
		keyLen, err := binary.ReadUvarint(b.r)
		if err != nil {
			if err == io.EOF {
				return h, ErrNotFound
			}
			return h, err
		}
		valueLen, err := binary.ReadUvarint(b.r)
		if err != nil {
			return h, err
		}
		kb = kb[:keyLen]
		if _, err = io.ReadFull(b.r, kb); err != nil {
			return h, err
		}
		if key <= string(kb) {
			h, err = newBlockHandle(b.r)
			return h, err
		}
		if _, err = b.r.Seek(int64(valueLen), io.SeekCurrent); err != nil {
			return h, err
		}
	}
}
