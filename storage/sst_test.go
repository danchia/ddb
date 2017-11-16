package storage

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

type kv struct {
	Key   string
	Value []byte
}

func TestFind(t *testing.T) {
	tests := []struct {
		name    string
		write   []kv
		findKey string
		want    []byte
	}{
		{
			"Empty SST, not found.",
			[]kv{},
			"abc",
			nil,
		},
		{
			"One entry SST, found.",
			[]kv{kv{"a", []byte("1")}},
			"a",
			[]byte("1"),
		},
		{
			"One entry SST, not found.",
			[]kv{kv{"a", []byte("1")}},
			"ab",
			nil,
		},
		{
			"Five entry SST, found start.",
			[]kv{
				kv{"a", []byte("1")},
				kv{"b", []byte("2")},
				kv{"c", []byte("3")},
				kv{"d", []byte("4")},
				kv{"e", []byte("5")},
			},
			"a",
			[]byte("1"),
		},
		{
			"Five entry SST, found.",
			[]kv{
				kv{"a", []byte("1")},
				kv{"b", []byte("2")},
				kv{"c", []byte("3")},
				kv{"d", []byte("4")},
				kv{"e", []byte("5")},
			},
			"c",
			[]byte("3"),
		},
		{
			"Five entry SST, found end.",
			[]kv{
				kv{"a", []byte("1")},
				kv{"b", []byte("2")},
				kv{"c", []byte("3")},
				kv{"d", []byte("4")},
				kv{"e", []byte("5")},
			},
			"e",
			[]byte("5"),
		},
		{
			"Five entry SST, not found.",
			[]kv{
				kv{"a", []byte("1")},
				kv{"b", []byte("2")},
				kv{"c", []byte("3")},
				kv{"d", []byte("4")},
				kv{"e", []byte("5")},
			},
			"ee",
			nil,
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

			w, err := NewSSTWriter(fname)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range tt.write {
				if err := w.Append(entry.Key, entry.Value); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			r, err := NewSSTReader(fname)
			if err != nil {
				t.Fatal(err)
			}

			if got, err := r.Find(tt.findKey); err != nil && !cmp.Equal(got, tt.want) {
				t.Errorf("Find(%s)=%s, error=%s want %s", tt.findKey, got, err, tt.want)
			}
		})
	}
}
