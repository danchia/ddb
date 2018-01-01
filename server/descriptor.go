package server

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"

	pb "github.com/danchia/ddb/proto"
)

const descriptorPrefix = "descriptor."

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// Descriptor describes all important DB state.
// Not thread-safe, access should be externally synchronized.
type Descriptor struct {
	// Current is the current descriptor
	Current *pb.DescriptorProto

	// The directory under which descriptors are persisted
	dir string
	// Every descriptor version written has an increasing number appended to it.
	version int64

	h hash.Hash32
}

// NewDescriptor returns a Descriptor for a brand new database.
// Most use cases should probably use LoadDescriptor instead.
func NewDescriptor(dir string) *Descriptor {
	return &Descriptor{
		Current: &pb.DescriptorProto{},
		dir:     dir,
		version: 0,
	}
}

// LoadDescriptor loads the highest numbered descriptor found in dir.
func LoadDescriptor(dir string) (*Descriptor, error) {
	fn, version, err := findLatestFile(dir)
	if os.IsNotExist(err) {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	glog.Infof("Opening descriptor %v", fn)

	f, err := os.Open(filepath.Join(dir, fn))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var scratch [8]byte
	if _, err := io.ReadFull(f, scratch[:]); err != nil {
		return nil, err
	}
	dataLen := binary.LittleEndian.Uint32(scratch[0:4])
	crc := binary.LittleEndian.Uint32(scratch[4:8])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}
	// verify checksum
	hasher := crc32.New(crcTable)
	if _, err := hasher.Write(data); err != nil {
		return nil, err
	}
	if hasher.Sum32() != crc {
		return nil, errors.New("descriptor file had invalid CRC")
	}

	d := new(pb.DescriptorProto)
	if err := proto.Unmarshal(data, d); err != nil {
		return nil, err
	}

	return &Descriptor{
		Current: d,
		dir:     dir,
		version: version + 1,
	}, nil
}

func findLatestFile(dir string) (name string, version int64, err error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", 0, err
	}
	maxVersion := int64(-1)
	maxName := ""
	for _, fi := range fis {
		name := fi.Name()
		if !strings.HasPrefix(name, descriptorPrefix) {
			continue
		}
		versionString := strings.TrimPrefix(name, descriptorPrefix)
		version, err := strconv.ParseInt(versionString, 10, 64)
		if err != nil {
			return "", 0, err
		}
		if version > maxVersion {
			maxVersion = version
			maxName = name
		}
	}
	if maxVersion == -1 {
		return "", 0, os.ErrNotExist
	}
	return maxName, version, nil
}

// Save persists Descriptor to stable storage.
func (d *Descriptor) Save() error {
	data, err := proto.Marshal(d.Current)
	if err != nil {
		return err
	}
	dataLen := len(data)
	hasher := crc32.New(crcTable)
	if _, err := hasher.Write(data); err != nil {
		return err
	}
	crc := hasher.Sum32()

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[:4], uint32(dataLen))
	binary.LittleEndian.PutUint32(scratch[4:], crc)

	curFn := d.filenameFor(d.version)
	d.version++
	nextFn := d.filenameFor(d.version)
	f, err := os.Create(nextFn)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)

	if _, err := w.Write(scratch[:]); err != nil {
		f.Close()
		return err
	}

	if _, err := w.Write(data[:]); err != nil {
		f.Close()
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// delete old descriptor file, but don't worry about errors
	if err := os.Remove(curFn); err != nil {
		glog.Warningf("error removing old descriptor %v: %v", curFn, err)
	}

	return nil
}

func (d *Descriptor) filenameFor(version int64) string {
	return filepath.Join(d.dir, fmt.Sprintf("%v%d", descriptorPrefix, version))
}
