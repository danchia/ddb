package sst

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	SstMagic   = uint64(0xe489f8a9d479536b)
	MaxKeySize = 8 * 1024

	footerSize = binary.MaxVarintLen64 + 4 + 8
)

const (
	typeNil   = 1
	typeBytes = 2
)

const blockSize = 16 * 1024

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrNotFound   = errors.New("not found")
	ErrCorruption = errors.New("corruption detected")
)

type blockHandle struct {
	offset uint64
	// size is the size of the block. Does not include checksum.
	size uint64
}

func newBlockHandle(r io.ByteReader) (blockHandle, error) {
	bh := blockHandle{}
	var err error
	if bh.offset, err = binary.ReadUvarint(r); err != nil {
		return bh, err
	}
	if bh.size, err = binary.ReadUvarint(r); err != nil {
		return bh, err
	}
	return bh, nil
}

func (h *blockHandle) EncodeTo(w io.Writer) error {
	if err := writeUvarInt64(w, h.offset); err != nil {
		return err
	}
	return writeUvarInt64(w, h.size)
}
