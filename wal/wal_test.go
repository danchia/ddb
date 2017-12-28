package wal

import (
	"io/ioutil"
	"os"
	"testing"

	pb "github.com/danchia/ddb/proto"
	"github.com/golang/protobuf/proto"
)

func TestReadWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	opts := Options{
		Dirname:    dir,
		TargetSize: 50,
	}

	w, err := NewWriter(1, opts)
	if err != nil {
		t.Fatal(err)
	}
	expectedRecords := []*pb.LogRecord{
		&pb.LogRecord{Mutation: &pb.Mutation{Key: "a"}},
		&pb.LogRecord{Mutation: &pb.Mutation{Key: "b"}},
	}
	for i, r := range expectedRecords {
		if err = w.Append(r); err != nil {
			t.Fatal(err)
		}
		if r.Sequence != int64(i+1) {
			t.Errorf("r.Sequence = %v, want %v", r.Sequence, i+1)
		}
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := NewScanner(dir)
	records := make([]*pb.LogRecord, 0)

	for s.Scan() {
		r := s.Record()
		records = append(records, proto.Clone(r).(*pb.LogRecord))
	}
	if s.Err() != nil {
		t.Fatal(s.Err())
	}
	if len(expectedRecords) != len(records) {
		t.Fatalf("TestReadWrite wrote %d records, read %d records.",
			len(expectedRecords), len(records))
	}
	for i, e := range expectedRecords {
		actual := records[i]
		if !proto.Equal(e, actual) {
			t.Errorf("TestReadWrite read %v, wanted %v.", actual, e)
		}
	}
}
