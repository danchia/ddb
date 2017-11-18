package memtable

import (
	"encoding/hex"
	"math/rand"
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
		insert  []kv
		findKey string
		want    []byte
	}{
		{"0 entry, not found", []kv{}, "abcd", nil},
		{"1 entry, found", []kv{kv{"abc", []byte("123")}}, "abc", []byte("123")},
		{"1 entry, not found after", []kv{kv{"abc", []byte("123")}}, "ab", nil},
		{"1 entry, not found before", []kv{kv{"abc", []byte("123")}}, "abcd", nil},
		{"3 entry, find first",
			[]kv{kv{"a", []byte("1")}, kv{"b", []byte("2")}, kv{"c", []byte("3")}},
			"a", []byte("1")},
		{"3 entry, find second",
			[]kv{kv{"a", []byte("1")}, kv{"b", []byte("2")}, kv{"c", []byte("3")}},
			"b", []byte("2")},
		{"3 entry reversed, find second",
			[]kv{kv{"c", []byte("1")}, kv{"b", []byte("2")}, kv{"a", []byte("3")}},
			"b", []byte("2")},
	}
	for _, tt := range tests {
		m := New()
		t.Run(tt.name, func(t *testing.T) {
			for _, kv := range tt.insert {
				m.Insert(kv.Key, kv.Value)
			}

			if got := m.Find(tt.findKey); !cmp.Equal(got, tt.want) {
				t.Errorf("Find(%v) = %v, want %v.", tt.findKey, got, tt.want)
			}
		})
	}
}

func TestIterator(t *testing.T) {
	tests := []struct {
		name   string
		insert []kv
		want   []kv
	}{
		{"0 entries", []kv{}, []kv{}},
		{"1 entry",
			[]kv{kv{"abc", []byte("123")}},
			[]kv{kv{"abc", []byte("123")}}},
		{"3 entries sorted",
			[]kv{kv{"a", []byte("1")}, kv{"b", []byte("2")}, kv{"c", []byte("3")}},
			[]kv{kv{"a", []byte("1")}, kv{"b", []byte("2")}, kv{"c", []byte("3")}}},
		{"3 entries reversed",
			[]kv{kv{"c", []byte("1")}, kv{"b", []byte("2")}, kv{"a", []byte("3")}},
			[]kv{kv{"a", []byte("3")}, kv{"b", []byte("2")}, kv{"c", []byte("1")}}},
		{"3 entries random",
			[]kv{kv{"a", []byte("1")}, kv{"c", []byte("2")}, kv{"b", []byte("3")}},
			[]kv{kv{"a", []byte("1")}, kv{"b", []byte("3")}, kv{"c", []byte("2")}}},
	}
	for _, tt := range tests {
		m := New()
		t.Run(tt.name, func(t *testing.T) {
			for _, kv := range tt.insert {
				m.Insert(kv.Key, kv.Value)
			}
			got := make([]kv, 0)

			i := m.NewIterator()
			for i.Next() {
				got = append(got, kv{i.Key(), i.Value()})
			}
			i.Close()

			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("Iterator differs: (-got +want)\n%s", diff)
			}
		})
	}

}

func TestRandomData(t *testing.T) {
	reference := make(map[string][]byte)
	m := New()

	for i := 0; i < 100000; i++ {
		k := randomString(1, 30)
		v := randomBytes(5, 50)
		if _, ok := reference[k]; ok {
			// key existed, skip this data point.
			continue
		}
		reference[k] = v
		m.Insert(k, v)
	}

	// perform some random probes of keys that exist
	for i := 0; i < 50000; i++ {
		var rk string
		var want []byte
		for rk, want = range reference {
			break
		}
		if got := m.Find(rk); !cmp.Equal(got, want) {
			t.Errorf("Find(%v) = %v, want %v",
				hex.EncodeToString([]byte(rk)), hex.EncodeToString(got), hex.EncodeToString(want))
		}
	}

	// perform some random probes of keys that don't exist
	for i := 0; i < 50000; i++ {
		rk := randomString(1, 40)
		_, ok := reference[rk]
		if ok {
			continue
		}

		if got := m.Find(rk); got != nil {
			t.Errorf("Find(%v) = %v, want nil",
				hex.EncodeToString([]byte(rk)), hex.EncodeToString(got))
		}
	}
}

func TestPickLevel(t *testing.T) {
	m := New()

	counts := make(map[int]int)
	trials := 10000000
	for i := 0; i < trials; i++ {
		counts[m.pickLevel()]++
	}
	// verify the first 10 levels
	e := trials / 2
	for i := 0; i < 10; i++ {
		wl := int(float64(e) * 0.8)
		wh := int(float64(e) * 1.2)
		if got := counts[i]; !(got >= wl && got <= wh) {
			t.Errorf("Counts[%v]: %v want [%v, %v]", i, got, wl, wh)
		}
		e /= 2
	}
}

func randomString(minLength, maxLength int) string {
	return string(randomBytes(minLength, maxLength))
}

func randomBytes(minLength, maxLength int) []byte {
	l := rand.Intn(maxLength-minLength) + minLength
	r := make([]byte, l)
	for i := 0; i < l; i++ {
		r[i] = byte(rand.Int31n(256))
	}
	return r
}
