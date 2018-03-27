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
	"fmt"
	"math/rand"
	"testing"
)

func TestBasic(t *testing.T) {
	b := newBloom()
	b.Add([]byte("abc"))
	b.Add([]byte("bcd"))

	if got := b.Test([]byte("abc")); !got {
		t.Errorf("b.Test(abc)=%v, wanted true", got)
	}
	if got := b.Test([]byte("bcd")); !got {
		t.Errorf("b.Test(bcd)=%v, wanted true", got)
	}
	if got := b.Test([]byte("d")); got {
		t.Errorf("b.Test(d)=%v, wanted false", got)
	}
}

func TestPercentile(t *testing.T) {
	var total, bad int
	threshold := 0.01
	for i := 0; i < 50; i++ {
		fp := runTrial(16000, t)
		if fp > 2*threshold {
			t.Errorf("Exceedingly bad FP rate: %v", fp)
		}
		if fp > threshold {
			t.Logf("FP rate: %v", fp)
			bad++
		}
		total++
	}
	if float64(bad)/float64(total) > 0.995 {
		t.Errorf("Bloom filters did not have expected false positive rate. "+
			"%v out of %v had rate > %v", bad, total, threshold)
	}
}

// runTrial returns the false positive ratio
func runTrial(n int, t *testing.T) float64 {
	b := newBloom()
	keys := make(map[string]struct{})

	for i := 0; i < n; i++ {
		key := fmt.Sprint(rand.Int31())
		keys[key] = struct{}{}
		b.Add([]byte(key))
	}

	// Validate all added keys still test ok.
	for k := range keys {
		if !b.Test([]byte(k)) {
			t.Fatalf("b.Test(%v)=false, expected true for added key", string(k))
		}
	}

	// Validate random keys
	var hits, total int
	for i := 0; i < 5000; i++ {
		key := fmt.Sprint(rand.Int31())
		if _, found := keys[key]; !found {
			if b.Test([]byte(key)) {
				hits++
			}
		}
		total++
	}

	return float64(hits) / float64(total)
}
