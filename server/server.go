package server

import (
	"context"
	"os"
	"sync"
	"time"

	pb "github.com/danchia/ddb/proto"
	"github.com/danchia/ddb/wal"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	MaxKeySize   uint32 = 4 * 1024
	MaxValueSize uint32 = 512 * 1024
)

type Server struct {
	mu sync.Mutex

	storage   *storage
	logWriter *wal.Writer
}

func NewServer() *Server {
	s := Server{}

	storageOpts := storageOptions{
		sstDir:            "/tmp/ddb_sst",
		memtableFlushSize: 100,
	}
	s.storage = newStorage(storageOpts)

	if err := s.recoverLog(); err != nil {
		glog.Fatalf("Failed to recover log file: %v", err)
	}

	// FIXME: next seqno should be from recovered log.
	logWriter, err := wal.NewWriter("/tmp/ddb.log", 1)
	if err != nil {
		glog.Fatalf("Error creating WAL writer: %v", err)
	}
	s.logWriter = logWriter

	return &s
}

func (s *Server) recoverLog() error {
	sc, err := wal.NewScanner("/tmp/ddb.log")
	if os.IsNotExist(err) {
		glog.Infof("no log files found")
		return nil
	}
	if err != nil {
		return err
	}

	n := int64(0)

	for sc.Scan() {
		r := sc.Record()
		n++

		if glog.V(4) {
			glog.V(4).Infof("Read wal record: %v", r)
		}

		if r.Mutation == nil {
			glog.Fatalf("Record %d had no mutation: %v", n, r)
		}
		s.storage.Apply(r.Mutation)
	}

	glog.Infof("Scanned %d log entries.", n)

	return sc.Err()
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, err := s.storage.Find(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "internal error: %v", err)
	}
	if value == nil {
		return nil, status.Errorf(codes.NotFound, "Could not find key %v.", req.Key)
	}
	// TODO: return timestamp of value
	return &pb.GetResponse{Key: req.Key, Value: value}, nil
}

func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	if err := validateValue(req.Value); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ts := time.Now().UnixNano() / 1000

	l := &pb.LogRecord{
		Mutation: &pb.Mutation{
			Key:       req.Key,
			Timestamp: ts,
			Value:     req.Value,
			Type:      pb.Mutation_PUT,
		},
	}

	if err := s.logWriter.Append(l); err != nil {
		return nil, err
	}
	if err := s.logWriter.Sync(); err != nil {
		return nil, err
	}

	s.storage.Apply(l.Mutation)

	return &pb.SetResponse{Timestamp: ts}, nil
}

func validateKey(k string) error {
	if k == "" {
		return status.Error(codes.InvalidArgument, "Key cannot be empty.")
	}
	if uint32(len(k)) > MaxKeySize {
		return status.Errorf(codes.InvalidArgument, "Key must be <= %d bytes", MaxKeySize)
	}
	return nil
}

func validateValue(v []byte) error {
	if uint32(len(v)) > MaxValueSize {
		return status.Errorf(codes.InvalidArgument, "Value must be <= %d bytes.", MaxValueSize)
	}
	return nil
}
