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

package sst

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/golang/glog"
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
				kv{"aa", 1, []byte("2")},
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
				kv{"abaa", 1, []byte("1")},
				kv{"abbb", 10, []byte("2")},
				kv{"abbbd", 1, []byte("3")},
				kv{"abc", 1, []byte("4")},
				kv{"e", 1, []byte("5")},
			},
			"abbbd",
			[]byte("3"), 1, nil,
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

			r, err := NewReader(fname, nil)
			if err != nil {
				t.Fatal(err)
			}

			if gotV, gotTs, err := r.Find(context.Background(), tt.findKey); err != tt.wantErr || gotTs != tt.wantTs || !cmp.Equal(gotV, tt.wantV) {
				t.Errorf("Find(%v)=%#v,%v,%v want %#v,%v,%v", tt.findKey, gotV, gotTs, err, tt.wantV, tt.wantTs, tt.wantErr)
			}
		})
	}
}

func TestIter(t *testing.T) {
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

	for i := 0; i < 1000; i++ {
		var value []byte
		if i%7 != 0 {
			value = []byte{byte(i)}
		}
		w.Append(fmt.Sprint(i), int64(i+1), value)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(fname, nil)
	if err != nil {
		t.Fatal(err)
	}

	iter, err := r.NewIter()
	if err != nil {
		t.Fatal(err)
	}

	cur := 0
	for {
		hasNext, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !hasNext {
			break
		}

		glog.V(8).Infof("Reading row %v", cur)

		var wantValue []byte
		if cur%7 != 0 {
			wantValue = []byte{byte(cur)}
		}
		if iter.Key() != fmt.Sprint(cur) ||
			iter.Timestamp() != int64(cur+1) || !cmp.Equal(iter.Value(), wantValue) {

			t.Errorf(
				"iter got (%v, %v, %v), want (%v, %v, %v)",
				iter.Key(), iter.Timestamp(), iter.Value(),
				cur, cur+1, wantValue)
		}
		cur++
	}

	if cur != 1000 {
		t.Errorf("Only read %d out of 1000 values.", cur)
	}
}

func TestRandomData(t *testing.T) {
	dir, err := ioutil.TempDir("", "ssttest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fname := filepath.Join(dir, "1.sst")

	type tsValue struct {
		ts    int64
		value []byte
	}
	data := make(map[string]tsValue)

	w, err := NewWriter(fname)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8000; i++ {
		r := rand.Int()
		key := fmt.Sprintf("%09d", r)
		tsV := tsValue{int64(i), []byte(key)}
		data[key] = tsV
	}

	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		tsV := data[key]
		if err := w.Append(key, tsV.ts, tsV.value); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	{
		r, err := NewReader(fname, nil)
		if err != nil {
			t.Fatal(err)
		}
		iter, err := r.NewIter()
		if err != nil {
			t.Fatal(err)
		}
		for idx, key := range keys {
			tsV := data[key]
			hasNext, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !hasNext {
				t.Fatalf("Missing key %v after %v rows", key, idx)
			}
			if iter.Key() != key || iter.Timestamp() != tsV.ts || !cmp.Equal(iter.Value(), tsV.value) {
				t.Errorf("Iter %v,%#v,%v want %v,%#v,%v", iter.Key(), iter.Value(), iter.Timestamp(),
					key, tsV.value, tsV.ts)
			}

		}
	}

	{
		r, err := NewReader(fname, nil)
		if err != nil {
			t.Fatal(err)
		}
		for key, tsV := range data {
			if gotV, gotTs, err := r.Find(context.Background(), key); err != nil || gotTs != tsV.ts || !cmp.Equal(gotV, tsV.value) {
				t.Errorf("Find(%v)=%#v,%v,%v want %#v,%v,%v", key, gotV, gotTs, err, tsV.value, tsV.ts, nil)
			}
		}
	}
}
