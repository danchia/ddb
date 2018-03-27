//    Copyright 2018 Google LLC
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
	"github.com/spaolacci/murmur3"
)

const (
	bloomHashes int = 7
	bloomBits   int = 160000
)

// bloom implements a Bloom filter.
// TODO(danchia): make size / hashes configurable.
type bloom struct {
	bits []byte
}

func newBloom() *bloom {
	return &bloom{
		bits: make([]byte, bloomBits/8+1),
	}
}

func newBloomFromBytes(b []byte) *bloom {
	return &bloom{
		bits: b,
	}
}

// Bytes() returns the bytes backing the bloom filter.
func (b *bloom) Bytes() []byte {
	return b.bits
}

// Add adds `key` to the bloom filter.
func (b *bloom) Add(key []byte) {
	h1 := murmur3.Sum32(key)
	h2 := (h1 >> 17) | (h1 << 15)
	// Mix hash according to Kirsch and Mitzenmacher
	for i := 0; i < bloomHashes; i++ {
		p := h1 % uint32(bloomBits)
		b.bits[p/8] |= (1 << (p % 8))
		h1 += h2
	}
}

// Test returns whether `key` is found.
func (b *bloom) Test(key []byte) bool {
	h1 := murmur3.Sum32(key)
	h2 := (h1 >> 17) | (h1 << 15)
	for i := 0; i < bloomHashes; i++ {
		p := h1 % uint32(bloomBits)
		if b.bits[p/8]&(1<<(p%8)) == 0 {
			return false
		}
		h1 += h2
	}

	return true
}
