package sst

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

type kv struct {
	Key       string
	Timestamp int64
	Value     []byte
}

func TestFind(t *testing.T) {
	tests := []struct {
		name    string
		write   []kv
		findKey string
		wantV   []byte
		wantTs  int64
		wantErr error
	}{
		{
			"Empty SST, not found.",
			[]kv{},
			"abc",
			nil, 0, ErrNotFound,
		},
		{
			"One nil-value entry SST, found.",
			[]kv{kv{"a", 1, nil}},
			"a",
			nil, 1, nil,
		},
		{
			"One entry SST, found.",
			[]kv{kv{"a", 1, []byte("1")}},
			"a",
			[]byte("1"), 1, nil,
		},
		{
			"One entry SST, not found.",
			[]kv{kv{"a", 1, []byte("1")}},
			"ab",
			nil, 0, ErrNotFound,
		},
		{
			"Five entry SST, found start.",
			[]kv{
				kv{"a", 1, []byte("1")},
				kv{"b", 1, []byte("2")},
				kv{"c", 1, []byte("3")},
				kv{"d", 1, []byte("4")},
				kv{"e", 1, []byte("5")},
			},
			"a",
			[]byte("1"), 1, nil,
		},
		{
			"Five entry SST, found.",
			[]kv{
				kv{"a", 1, []byte("1")},
				kv{"b", 10, []byte("2")},
				kv{"b", 1, []byte("3")},
				kv{"d", 1, []byte("4")},
				kv{"e", 1, []byte("5")},
			},
			"b",
			[]byte("2"), 10, nil,
		},
		{
			"Five entry SST, found end.",
			[]kv{
				kv{"a", 13, []byte("1")},
				kv{"b", 13, []byte("2")},
				kv{"c", 13, []byte("3")},
				kv{"d", 13, []byte("4")},
				kv{"e", 13, []byte("5")},
			},
			"e",
			[]byte("5"), 13, nil,
		},
		{
			"Five entry SST, not found.",
			[]kv{
				kv{"a", 13, []byte("1")},
				kv{"b", 13, []byte("2")},
				kv{"c", 13, []byte("3")},
				kv{"d", 13, []byte("4")},
				kv{"e", 13, []byte("5")},
			},
			"ee",
			nil, 0, ErrNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "ssttest")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			fname := filepath.Join(dir, "1.sst")

			w, err := NewWriter(fname)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range tt.write {
				if err := w.Append(entry.Key, entry.Timestamp, entry.Value); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			r, err := NewReader(fname)
			if err != nil {
				t.Fatal(err)
			}

			if gotV, gotTs, err := r.Find(tt.findKey); err != tt.wantErr || gotTs != tt.wantTs || !cmp.Equal(gotV, tt.wantV) {
				t.Errorf("Find(%v)=%v,%v,%v want %v,%v,%v", tt.findKey, gotV, gotTs, err, tt.wantV, tt.wantTs, tt.wantErr)
			}
		})
	}
}
