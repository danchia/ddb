package sst

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/golang/glog"
)

type prefixEncoder struct {
	restartInterval int32
	restarts        []uint32
	lastKey         []byte
	count           int32
}

func newPrefixEncoder(restartInterval int32) *prefixEncoder {
	return &prefixEncoder{
		restartInterval: restartInterval,
		lastKey:         make([]byte, 0, MaxSstKeySize),
		restarts:        []uint32{0},
	}
}

// EncodeInto encodes k into w, using shared prefix compression.
// Restarts are created every restartInterval, saving offset as the restart point.
func (e *prefixEncoder) EncodeInto(w io.Writer, k []byte, offset uint32) error {
	var shared int

	if e.count < e.restartInterval {
		minL := len(e.lastKey)
		if minL > len(k) {
			minL = len(k)
		}

		for shared < minL && e.lastKey[shared] == k[shared] {
			shared++
		}
	} else {
		e.count = 0
		e.restarts = append(e.restarts, offset)
	}
	e.count++

	nonShared := len(k) - shared

	if err := writeUvarInt64(w, uint64(shared)); err != nil {
		return err
	}
	if err := writeUvarInt64(w, uint64(nonShared)); err != nil {
		return err
	}
	if nonShared > 0 {
		if _, err := w.Write(k[shared:]); err != nil {
			return err
		}
	}

	e.lastKey = append(e.lastKey[0:0], k...)
	return nil
}

// Restarts returns the restart points.
func (e *prefixEncoder) Restarts() []uint32 {
	return e.restarts
}

// Reset resets the prefix encoder.
func (e *prefixEncoder) Reset() {
	e.restarts = e.restarts[:1]
	e.lastKey = e.lastKey[:0]
	e.count = 0
}

// WriteRestarts writes out the restarts to w
func (e *prefixEncoder) WriteRestarts(w io.Writer) error {
	for _, r := range e.restarts {
		if err := writeUint32(w, uint32(r)); err != nil {
			return err
		}
	}
	if err := writeUint32(w, uint32(len(e.restarts))); err != nil {
		return err
	}
	return nil
}

// prefixDecodeFrom decodes a prefix coded key from r. lastKey is nil for restart points.
// scratch will be used to created the returned key.
func prefixDecodeFrom(r *bytes.Reader, lastKey []byte, scratch []byte) ([]byte, error) {
	shared, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	nonShared, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	scratch = scratch[:shared]
	if uint64(len(lastKey)) < shared {
		glog.Warningf("decodeFrom: shared is %v, but lastKey is len %v", shared, len(lastKey))
		return nil, ErrCorruption
	}
	copy(scratch, lastKey[:shared])

	scratch = scratch[:shared+nonShared]
	if _, err := io.ReadFull(r, scratch[shared:]); err != nil {
		return nil, err
	}

	return scratch, nil
}
