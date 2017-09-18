package wal

import (
	"io/ioutil"
	"os"
	"path/filepath"
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
	fname := filepath.Join(dir, "1.log")

	w, err := NewWriter(fname)
	if err != nil {
		t.Fatal(err)
	}
	expectedRecords := []*pb.LogRecord{
		&pb.LogRecord{Mutation: &pb.Mutation{Key: "a"}},
		&pb.LogRecord{Mutation: &pb.Mutation{Key: "b"}},
	}
	for _, r := range expectedRecords {
		if err = w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := NewScanner(fname)
	records := make([]*pb.LogRecord, 0)

	for s.Scan() {
		r := s.Record()
		records = append(records, proto.Clone(r).(*pb.LogRecord))
	}
	if s.Err() != nil {
		t.Fatal(s.Err())
	}
	if len(expectedRecords) != len(records) {
		t.Errorf("TestReadWrite wrote %d records, read %d records.",
			len(expectedRecords), len(records))
	}
	for i, e := range expectedRecords {
		actual := records[i]
		if !proto.Equal(e, actual) {
			t.Errorf("TestReadWrite read %v, wanted %v.", actual, e)
		}
	}
}
