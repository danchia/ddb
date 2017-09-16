package server

import (
	"bytes"
	"context"
	"strings"
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

	var tests = []struct {
		key  string
		want codes.Code
	}{
		{"", codes.InvalidArgument},
		{strings.Repeat("a", int(MaxKeySize+1)), codes.InvalidArgument},
	}

	for _, tt := range tests {
		r, err := s.Get(context.Background(), &pb.GetRequest{Key: tt.key})
		if r != nil || err == nil {
			t.Errorf("Get with invalid key %v, got %v, %v want nil, %v", tt.key, r, err, tt.want)
		}
		status, ok := status.FromError(err)
		if !ok {
			t.Errorf("Unexpected err, got %v want %v", err, tt.want)
		}
		if status.Code() != tt.want {
			t.Errorf("Get with invalid key %v, got status %v want %v", tt.key, status, tt.want)
		}
	}
}

func TestInvalidSet(t *testing.T) {
	s := NewServer()

	var tests = []struct {
		key   string
		value []byte
		want  codes.Code
	}{
		{"", []byte{}, codes.InvalidArgument},
		{"a", bytes.Repeat([]byte{1}, int(MaxValueSize+1)), codes.InvalidArgument},
	}

	for _, tt := range tests {
		r := &pb.SetRequest{Key: tt.key, Value: tt.value}
		rs, err := s.Set(context.Background(), r)
		if rs != nil || err == nil {
			t.Errorf("Set with invalid req %v, got %v, %v want nil, %v", r, rs, err, tt.want)
		}
		status, ok := status.FromError(err)
		if !ok {
			t.Errorf("Unexpected err, got %v want %v", err, tt.want)
		}
		if status.Code() != tt.want {
			t.Errorf("Set with invalid req %v, got status %v want %v", r, status, tt.want)
		}
	}
}
