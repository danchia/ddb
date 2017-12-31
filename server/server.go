package server

import (
	"context"
	"os"
	"path/filepath"
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

	opts Options

	storage   *storage
	logWriter *wal.Writer
}

type Options struct {
	SstDir            string
	MemtableFlushSize int64

	LogDir        string
	TargetLogSize int64
}

func DefaultOptions(baseDir string) Options {
	return Options{
		SstDir:            filepath.Join(baseDir, "ddb_sst"),
		MemtableFlushSize: 16 * 1024 * 1024,

		LogDir:        filepath.Join(baseDir, "ddb_log"),
		TargetLogSize: 8 * 1024 * 1024,
	}
}

func NewServer(opts Options) *Server {
	s := Server{opts: opts}

	ensureDir(opts.LogDir)
	ensureDir(opts.SstDir)

	storageOpts := storageOptions{
		sstDir:            opts.SstDir,
		memtableFlushSize: opts.MemtableFlushSize,
	}
	s.storage = newStorage(storageOpts)

	nextSeq, err := s.recoverLog()
	if err != nil {
		glog.Fatalf("Failed to recover log file: %v", err)
	}

	logOpts := wal.Options{Dirname: opts.LogDir, TargetSize: opts.TargetLogSize}
	logWriter, err := wal.NewWriter(nextSeq, logOpts)
	if err != nil {
		glog.Fatalf("Error creating WAL writer: %v", err)
	}
	s.logWriter = logWriter

	return &s
}

func ensureDir(dir string) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		glog.Fatalf("error while ensuring directory %v: %v", dir, err)
	}
}

func (s *Server) recoverLog() (nextSeq int64, err error) {
	sc, err := wal.NewScanner("/tmp/ddb_log")
	if os.IsNotExist(err) {
		glog.Infof("no log files found")
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	n := int64(0)
	seqNo := int64(-1)

	for sc.Scan() {
		r := sc.Record()
		n++

		if glog.V(4) {
			glog.V(4).Infof("Read wal record: %v", r)
		}

		seqNo = r.Sequence
		if r.Mutation == nil {
			glog.Fatalf("Record %d had no mutation: %v", n, r)
		}
		s.storage.Apply(r.Mutation)
	}

	glog.Infof("Scanned %d log entries.", n)

	if seqNo == -1 {
		// TODO: it's possible that if we truncate the log and don't have any new mutations
		// we won't get a sequence number, even if we can recover it from the file metadata.
		glog.Fatalf("seqNo was not recovered")
	}
	return seqNo, sc.Err()
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

	// TODO: this needs to be monotonically increasing. hybrid logical clocks?
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
