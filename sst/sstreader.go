package sst

import "os"
import "fmt"
import "github.com/golang/glog"
import "encoding/binary"
import "github.com/google/orderedcode"

// Reader is an SSTable reader.
// Threadsafe.
type Reader struct {
	f        *os.File
	fLength  int64
	filename string
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
	if ok := r.verifyMagic(0); !ok {
		return nil, fmt.Errorf("invalid magic at start of file %s", filename)
	}
	return r, nil
}

type Iter struct {
	r *Reader
}

func (r *Reader) Find(key string) ([]byte, error) {
	// No index, so have to do the a dumb scan.
	kb := make([]byte, 0, MaxKeySize)
	offset := int64(8) // skip magic

	for offset < r.fLength {
		keyLen, n, err := readAtUvarInt64(r.f, offset)
		if err != nil {
			return nil, err
		}
		offset += n

		valueLen, n, err := readAtUvarInt64(r.f, offset)
		if err != nil {
			return nil, err
		}
		offset += n

		kb = kb[0:keyLen]
		if _, err := r.f.ReadAt(kb, offset); err != nil {
			return nil, err
		}
		offset += int64(keyLen)

		eKey := string(kb)
		var readKey string
		var ts int64
		if _, err := orderedcode.Parse(eKey, &readKey, orderedcode.Decr(&ts)); err != nil {
			return nil, err
		}

		if readKey == key {
			value := make([]byte, valueLen)
			if _, err := r.f.ReadAt(value, offset); err != nil {
				return nil, err
			}
			if value[0] == typeNil {
				return nil, nil
			}
			return value[1:], nil
		}
		if readKey > key {
			return nil, nil
		}

		offset += int64(valueLen)
	}
	return nil, nil
}

// verifyMagic returns true is magic at offset is valid.
func (r *Reader) verifyMagic(offset int64) bool {
	var b [8]byte
	if _, err := r.f.ReadAt(b[:], offset); err != nil {
		glog.V(2).Infof("File error while verifying magic for %s:%d. %s",
			r.filename, offset, err)
		return false
	}
	return binary.LittleEndian.Uint64(b[:]) == SstMagic
}