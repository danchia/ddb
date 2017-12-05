package wal

import (
	"io/ioutil"
	"os"
	"path/filepath"
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
	fname := filepath.Join(dir, "1.log")

	l := &pb.LogRecord{
		Mutation: &pb.Mutation{
			Key:   strings.Repeat("a", dataSize),
			Value: []byte{},
			Type:  pb.Mutation_PUT,
		},
	}
	b.SetBytes(int64(proto.Size(l)))

	w, err := NewWriter(fname, 0)
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
