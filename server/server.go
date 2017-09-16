package server

import (
	"context"
	"sync"

	pb "github.com/danchia/ddb/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	MaxKeySize   uint32 = 4 * 1024
	MaxValueSize uint32 = 512 * 1024
)

type Server struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewServer() *Server {
	s := Server{}
	s.data = make(map[string][]byte)

	return &s
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[req.Key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Could not find key %q.", req.Key)
	}
	return &pb.GetResponse{req.Key, value}, nil
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

	s.data[req.Key] = req.Value
	return &pb.SetResponse{}, nil
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
