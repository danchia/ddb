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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	pb "github.com/danchia/ddb/proto"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSetThenGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s := NewServer(DefaultOptions(dir))

	key := "abcd"
	value := []byte{1, 2, 3, 4}
	_, err = s.Set(context.Background(), &pb.SetRequest{Key: key, Value: value})
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
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s := NewServer(DefaultOptions(dir))

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
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s := NewServer(DefaultOptions(dir))

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
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s := NewServer(DefaultOptions(dir))

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

func TestManyReadsAndWrites(t *testing.T) {
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	opts := DefaultOptions(dir)
	opts.MemtableFlushSize = 1024 // make more SSTs sooner
	s := NewServer(opts)

	keys := 300

	for i := 0; i < keys; i++ {
		if i%100 == 0 {
			glog.V(2).Infof("Iteration %v", i)
		}

		bytes := make([]byte, 100)
		bytes[0] = byte(i)

		r := &pb.SetRequest{Key: fmt.Sprintf("%v", i), Value: bytes}
		_, err := s.Set(context.Background(), r)
		if err != nil {
			t.Fatalf("Set - Unexpected error %v", err)
		}

		// validate existing keys
		for j := 0; j <= i; j++ {
			expectedKey := fmt.Sprintf("%v", j)
			expectedValue := make([]byte, 100)
			expectedValue[0] = byte(j)

			r := &pb.GetRequest{Key: expectedKey}
			rs, err := s.Get(context.Background(), r)
			if err != nil {
				t.Fatalf("Get i,j %v,%v - Unexpected error %v", i, j, err)
			}

			if rs.GetKey() != expectedKey || !cmp.Equal(rs.Value, expectedValue) {
				t.Fatalf("Get(%v)=%v,%+v, want %v %+v", j, rs.GetKey(), rs.GetValue(), expectedKey, expectedValue)
			}
		}
	}

	/*
		// validate all
		for j := 0; j <= keys; j++ {
			expectedKey := fmt.Sprintf("%v", j)
			expectedValue := make([]byte, 100)
			expectedValue[0] = byte(j)

			r := &pb.GetRequest{Key: expectedKey}
			rs, err := s.Get(context.Background(), r)
			if err != nil {
				t.Errorf("Get j %v - Unexpected error %v", j, err)
			}

			if rs.GetKey() != expectedKey || !cmp.Equal(rs.Value, expectedValue) {
				t.Errorf("Get(%v)=%v,%+v, want %v %+v", j, rs.GetKey(), rs.GetValue(), expectedKey, expectedValue)
			}
		} */
}
