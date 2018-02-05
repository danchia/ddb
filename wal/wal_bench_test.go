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

package wal

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	pb "github.com/danchia/ddb/proto"
	"github.com/golang/protobuf/proto"
)

func BenchmarkConcurrentAppend(b *testing.B) {
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	l := &pb.LogRecord{
		Mutation: &pb.Mutation{
			Key:   strings.Repeat("a", 100),
			Value: []byte{},
			Type:  pb.Mutation_PUT,
		},
	}
	b.SetBytes(int64(proto.Size(l)))

	opts := Options{Dirname: dir, TargetSize: 128 * 1024 * 1024}
	w, err := NewWriter(0, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	b.ResetTimer()

	b.RunParallel(func(tpb *testing.PB) {
		for tpb.Next() {
			c := make(chan error)
			w.Append(l, func(err error) { c <- err })
			<-c
		}
	})

	b.StopTimer()

}
