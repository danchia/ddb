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

func BenchmarkAppend100NoSync(b *testing.B)   { benchmark(b, 100, 0) }
func BenchmarkAppend100NoBatch(b *testing.B)  { benchmark(b, 100, 1) }
func BenchmarkAppend100Batch10(b *testing.B)  { benchmark(b, 100, 10) }
func BenchmarkAppend100Batch100(b *testing.B) { benchmark(b, 100, 100) }

func BenchmarkAppend1000NoSync(b *testing.B)   { benchmark(b, 1000, 0) }
func BenchmarkAppend1000NoBatch(b *testing.B)  { benchmark(b, 1000, 1) }
func BenchmarkAppend1000Batch10(b *testing.B)  { benchmark(b, 1000, 10) }
func BenchmarkAppend1000Batch100(b *testing.B) { benchmark(b, 1000, 100) }

func benchmark(b *testing.B, dataSize, batchSize int) {
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	l := &pb.LogRecord{
		Mutation: &pb.Mutation{
			Key:   strings.Repeat("a", dataSize),
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

	for i := 0; i < b.N; i++ {
		w.Append(l)
		if batchSize > 0 && i%batchSize == 0 {
			w.Sync()
		}
	}

	b.StopTimer()
}
