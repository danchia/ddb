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

package server

import (
	"container/heap"
)

// mergingIter is an iterator that merges from many Iter.
type mergingIter struct {
	h *iterHeap

	curKey   string
	curTs    int64
	curValue []byte

	iters []Iter
}

func newMergingIter(iters []Iter) (*mergingIter, error) {
	mi := &mergingIter{
		h:     new(iterHeap),
		iters: iters,
	}

	for _, iter := range iters {
		hasNext, err := iter.Next()
		if err != nil {
			mi.Close()
			return nil, err
		}
		if hasNext {
			*mi.h = append(*mi.h, iter)
		}
	}
	heap.Init(mi.h)
	return mi, nil
}

// Next advances the iterator. Returns true if there is a next value.
func (i *mergingIter) Next() (bool, error) {
	if i.h.Len() == 0 {
		return false, nil
	}

	iter := heap.Pop(i.h).(Iter)
	i.curKey = iter.Key()
	i.curTs = iter.Timestamp()
	i.curValue = iter.Value()

	hasNext, err := iter.Next()
	if err != nil {
		return false, err
	}

	if hasNext {
		heap.Push(i.h, iter)
	}

	return true, nil
}

// Key returns the current key.
func (i *mergingIter) Key() string { return i.curKey }

// Timestamp returns the current timestamp.
func (i *mergingIter) Timestamp() int64 { return i.curTs }

// Value returns the current value.
func (i *mergingIter) Value() []byte { return i.curValue }

// Close closes the iterator by closing all the underlying iters.
func (i *mergingIter) Close() {
	for _, it := range i.iters {
		it.Close()
	}
}

type iterHeap []Iter

func (h iterHeap) Len() int { return len(h) }

func (h iterHeap) Less(i, j int) bool {
	it1 := h[i]
	it2 := h[j]

	if it1.Key() != it2.Key() {
		return it1.Key() < it2.Key()
	}

	return it1.Timestamp() > it2.Timestamp()
}

func (h iterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iterHeap) Push(x interface{}) {
	*h = append(*h, x.(Iter))
}

func (h *iterHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
