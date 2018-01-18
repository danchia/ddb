//    Copyright 2018 Google Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package server

import (
	"context"
	"path/filepath"

	pb "github.com/danchia/ddb/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	MaxKeySize   uint32 = 4 * 1024
	MaxValueSize uint32 = 512 * 1024
)

type Server struct {
	opts Options
	db   *database
}

type Options struct {
	SstDir            string
	MemtableFlushSize int64

	LogDir        string
	TargetLogSize int64

	DescriptorDir string
}

func DefaultOptions(baseDir string) Options {
	return Options{
		SstDir:            filepath.Join(baseDir, "sst"),
		MemtableFlushSize: 16 * 1024 * 1024,

		LogDir:        filepath.Join(baseDir, "log"),
		TargetLogSize: 8 * 1024 * 1024,

		DescriptorDir: baseDir,
	}
}

func NewServer(opts Options) *Server {
	return &Server{
		opts: opts,
		db:   newDatabase(opts),
	}
}

func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	return s.db.Set(ctx, req)
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}

	value, err := s.db.Find(ctx, req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "internal error: %v", err)
	}
	if value == nil {
		return nil, status.Errorf(codes.NotFound, "Could not find key %v.", req.Key)
	}
	// TODO: return timestamp of value
	return &pb.GetResponse{Key: req.Key, Value: value}, nil
}
