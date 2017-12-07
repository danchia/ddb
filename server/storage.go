package server

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/danchia/ddb/memtable"
	pb "github.com/danchia/ddb/proto"
	"github.com/danchia/ddb/sst"
	"github.com/golang/glog"
)

type storage struct {
	memtable *memtable.Memtable
	opts     storageOptions

	mu sync.Mutex
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

	if s.memtable.SizeBytes() > s.opts.memtableFlushSize {
		go s.flushMemtable()
	}
}

func (s *storage) Find(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO read from SST
	return s.memtable.Find(key), nil
}

func (s *storage) flushMemtable() {
	s.mu.Lock()
	m := s.memtable
	s.memtable = memtable.New()
	s.mu.Unlock()

	ts := time.Now().UnixNano()
	fn := fmt.Sprintf("%v%v%020d.sst", s.opts.sstDir, os.PathSeparator, ts)

	glog.Infof("flushing memtable of size %v to %v", m.SizeBytes(), fn)

	writer, err := sst.NewSSTWriter(fn)
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
}
