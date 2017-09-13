package server

import (
	"context"
	"testing"

	pb "github.com/danchia/ddb/proto"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSetThenGet(t *testing.T) {
	s := NewServer()

	key := "abcd"
	value := []byte{1, 2, 3, 4}
	_, err := s.Set(context.Background(), &pb.SetRequest{Key: key, Value: value})
	if err != nil {
		t.Errorf("Set - Unexpected error %v", err)
	}

	r, err := s.Get(context.Background(), &pb.GetRequest{Key: key})
	if err != nil {
		t.Errorf("Get - Unexpected error %v", err)
	}
	want := pb.GetResponse{Key: key, Value: value}
	if !proto.Equal(r, &want) {
		t.Errorf("Get, got %+v want %+v", *r, want)
	}
}

func TestGetNotFound(t *testing.T) {
	s := NewServer()

	r, err := s.Get(context.Background(), &pb.GetRequest{Key: "blah"})
	if r != nil {
		t.Errorf("Get, unexpected response %v", r)
	}
	status, ok := status.FromError(err)
	if !ok {
		t.Errorf("Get, unexpected error %v", err)
	}
	if status.Code() != codes.NotFound {
		t.Errorf("Get, got %v, want NotFound status", status)
	}
}

func TestInvalidKeyGet(t *testing.T) {
	s := NewServer()

	r, err := s.Get(context.Background(), &pb.GetRequest{Key: ""})
	if r != nil || err == nil {
		t.Errorf("Get with empty key, got %v, %v want nil, NotFound", r, err)
	}
	status, ok := status.FromError(err)
	if !ok {
		t.Errorf("Unexpected err, got %v want NotFound", err)
	}
	if status.Code() != codes.InvalidArgument {
		t.Errorf("Get with empty key, got status %v want InvalidArgument", status)
	}
}

func TestInvalidKeySet(t *testing.T) {
	s := NewServer()

	r, err := s.Set(context.Background(), &pb.SetRequest{Key: ""})
	if r != nil || err == nil {
		t.Errorf("Set with empty key, got %v, %v want nil, InvalidArgument", r, err)
	}
	status, ok := status.FromError(err)
	if !ok {
		t.Errorf("Unexpected err, got %v want NotFound", err)
	}
	if status.Code() != codes.InvalidArgument {
		t.Errorf("Set with empty key, got status %v want InvalidArgument", status)
	}
}
