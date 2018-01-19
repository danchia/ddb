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
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestHitAndMiss(t *testing.T) {
	c := NewCache(10)

	if got, want := c.Get("a"), []byte(nil); !cmp.Equal(got, want) {
		t.Errorf("Empty cache, Get(a)=%v != %v", got, want)
	}

	c.Insert("a", []byte{1})
	if got, want := c.Get("a"), []byte{1}; !cmp.Equal(got, want) {
		t.Errorf("One entry, Get(a)=%v != %v", got, want)
	}
	if got, want := c.Get("b"), []byte(nil); !cmp.Equal(got, want) {
		t.Errorf("Unrelated key, Get(b)=%v != %v", got, want)
	}
}

func TestFreqUsed(t *testing.T) {
	c := NewCache(100)

	c.Insert("a", []byte{100})

	for i := 0; i < 110; i++ {
		k := fmt.Sprintf("%d", 1000+i)
		c.Insert(k, []byte{23})
		if i%5 == 0 {
			if got, want := c.Get(k), []byte{23}; !cmp.Equal(got, want) {
				t.Errorf("Just inserted, Get(%v)=%v != %v", k, got, want)
			}
		}
		if got, want := c.Get("a"), []byte{100}; !cmp.Equal(got, want) {
			t.Errorf("Hot key should be present, Get(a)=%v != %v", got, want)
		}
	}
}

func TestEvict(t *testing.T) {
	c := NewCache(5)

	c.Insert("a", []byte{1})
	c.Insert("b", []byte{1})
	if got, want := c.Get("a"), []byte(nil); !cmp.Equal(got, want) {
		t.Errorf("a should be evicted, Get(a)=%v != %v", got, want)
	}

	c.Get("b") // promote b into old segment.
	c.Insert("c", []byte{1})
	if got, want := c.Get("b"), []byte{1}; !cmp.Equal(got, want) {
		t.Errorf("b should still be present, Get(b)=%v != %v", got, want)
	}
}
