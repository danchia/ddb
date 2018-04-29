package server

import (
	"testing"

	"github.com/golang/glog"
	"github.com/google/go-cmp/cmp"
)

func TestMergeEmpty(t *testing.T) {
	iter1 := NewIterFromRows([]KTV{})
	merging, err := newMergingIter([]Iter{iter1})
	if err != nil {
		t.Fatal(err)
	}

	if hasNext, err := merging.Next(); hasNext != false || err != nil {
		t.Errorf("Next() = (%v, %v), want (false, nil)", hasNext, err)
	}
}

func TestMerge(t *testing.T) {
	iter1 := NewIterFromRows([]KTV{
		{"abc", 0, []byte("1")},
		{"abe", 0, []byte("1")},
	})
	iter2 := NewIterFromRows([]KTV{
		{"abd", 0, []byte("1")},
		{"abe", 1, []byte("1")},
	})

	merging, err := newMergingIter([]Iter{iter1, iter2})
	if err != nil {
		t.Fatal(err)
	}

	var got []KTV

	for {
		hasNext, err := merging.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !hasNext {
			break
		}

		row := KTV{merging.Key(), merging.Timestamp(), merging.Value()}
		got = append(got, row)

		glog.V(4).Infof("got: %v", row)
	}

	expected := []KTV{
		{"abc", 0, []byte("1")},
		{"abd", 0, []byte("1")},
		{"abe", 1, []byte("1")},
		{"abe", 0, []byte("1")},
	}

	if diff := cmp.Diff(got, expected); diff != "" {
		t.Errorf("merging differs (-got +want): %v", diff)
	}
}
