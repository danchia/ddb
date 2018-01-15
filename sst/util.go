package sst

import (
	"encoding/binary"
	"io"
)

func writeUint32(w io.Writer, x uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], x)
	if _, err := w.Write(b[:]); err != nil {
		return err
	}
	return nil
}

func writeUint64(w io.Writer, x uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], x)
	if _, err := w.Write(b[:]); err != nil {
		return err
	}
	return nil
}

func writeUvarInt64(w io.Writer, x uint64) error {
	var b [binary.MaxVarintLen64]byte
	bl := binary.PutUvarint(b[:], x)
	_, err := w.Write(b[0:bl])
	return err
}

func readAtUvarInt64(r io.ReaderAt, offset int64) (x uint64, n int64, err error) {
	br := &byteReader{r, offset}
	x, err = binary.ReadUvarint(br)
	n = br.offset - offset
	return
}

type byteReader struct {
	r      io.ReaderAt
	offset int64
}

func (r *byteReader) ReadByte() (byte, error) {
	var b [1]byte
	if _, err := r.r.ReadAt(b[:], r.offset); err != nil {
		return 0, nil
	}
	r.offset++
	return b[0], nil
}
