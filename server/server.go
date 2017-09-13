package server

import (
	"context"
	"sync"

	pb "github.com/danchia/ddb/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	validateKey(req.Key)

	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[req.Key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Could not find key %q.", req.Key)
	}
	return &pb.GetResponse{req.Key, value}, nil
}

func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	validateKey(req.Key)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[req.Key] = req.Value
	return &pb.SetResponse{}, nil
}

func validateKey(k string) error {
	if k == "" {
		return status.Errorf(codes.InvalidArgument, "Key cannot be empty.")
	}
	return nil
}
