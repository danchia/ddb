package server

import (
	"fmt"
	"math"
	"path"
	"sync"
	"time"

	"github.com/danchia/ddb/memtable"
	pb "github.com/danchia/ddb/proto"
	"github.com/danchia/ddb/sst"
	"github.com/golang/glog"
)

type storage struct {
	memtable  *memtable.Memtable
	imemtable *memtable.Memtable

	ssts []*sst.Reader

	opts storageOptions
	mu   sync.Mutex
}

type storageOptions struct {
	sstDir            string
	memtableFlushSize int64
}

func newStorage(o storageOptions) *storage {
	return &storage{
		memtable: memtable.New(),
		opts:     o,
	}
}

func (s *storage) Apply(m *pb.Mutation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch m.Type {
	case pb.Mutation_PUT:
		s.memtable.Insert(m.Key, m.Timestamp, m.Value)
	case pb.Mutation_DELETE:
		s.memtable.Insert(m.Key, m.Timestamp, nil)
	default:
		glog.Fatalf("Mutation with unrecognized type: %v", m)
	}

	if s.memtable.SizeBytes() > s.opts.memtableFlushSize && s.imemtable == nil {
		go s.flushMemtable()
	}
}

func (s *storage) Find(key string) ([]byte, error) {
	s.mu.Lock()

	v, found := s.memtable.Find(key)
	if found {
		s.mu.Unlock()
		return v, nil
	}
	if s.imemtable != nil {
		v, found = s.imemtable.Find(key)
		if found {
			s.mu.Unlock()
			return v, nil
		}
	}

	ssts := make([]*sst.Reader, len(s.ssts))
	copy(ssts, s.ssts)
	// Don't hold lock while reading from SST.
	s.mu.Unlock()

	var value []byte
	valueTs := int64(math.MinInt64)

	for _, s := range ssts {
		v, ts, err := s.Find(key)
		if err == sst.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		if ts > valueTs {
			value = v
			valueTs = ts
		}
	}

	return value, nil
}

func (s *storage) flushMemtable() {
	s.mu.Lock()
	m := s.memtable
	s.imemtable = m
	s.memtable = memtable.New()
	s.mu.Unlock()

	ts := time.Now().UnixNano()
	fn := path.Join(s.opts.sstDir, fmt.Sprintf("%020d.sst", ts))

	glog.Infof("flushing memtable of size %v to %v", m.SizeBytes(), fn)

	writer, err := sst.NewWriter(fn)
	if err != nil {
		glog.Fatalf("error opening SST while flushing memtable: %v", err)
	}
	it := m.NewIterator()
	for it.Next() {
		if err := writer.Append(it.Key(), it.Timestamp(), it.Value()); err != nil {
			glog.Fatalf("error appending SST while flushing memtable: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		glog.Fatalf("error closing SST while flushing memtable: %v", err)
	}

	glog.Infof("flush completed for %v", fn)

	reader, err := sst.NewReader(fn)
	if err != nil {
		glog.Fatalf("error opneing SST that was just flushed: %v", err)
	}

	s.mu.Lock()
	s.imemtable = nil
	s.ssts = append(s.ssts, reader)
	s.mu.Unlock()
}
