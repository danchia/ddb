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
	"container/list"
	"sync"

	"github.com/golang/glog"
)

// Cache is a cache of key -> bytes. Implements a segmented LRU algorithm.
type Cache struct {
	entries map[string]*list.Element
	// Elements sorted by recency order (front is most recent)
	young *list.List
	old   *list.List

	youngSize       int64
	oldSize         int64
	targetYoungSize int64
	targetOldSize   int64

	nextID uint64

	mu sync.Mutex
}

// NewCache returns a Cache.
func NewCache(targetSize int64) *Cache {
	targetYoungSize := targetSize / 5
	targetOldSize := targetSize - targetYoungSize

	if targetYoungSize <= 0 || targetOldSize <= 0 {
		glog.Fatalf("targetSize %v resulted in young, old of %v, %v",
			targetSize, targetYoungSize, targetOldSize)
	}

	return &Cache{
		entries:         make(map[string]*list.Element),
		young:           list.New(),
		old:             list.New(),
		targetYoungSize: targetYoungSize,
		targetOldSize:   targetOldSize,
	}
}

type cacheEntry struct {
	key  string
	data []byte
	old  bool
}

// NewID returns a new unique ID, typically used by callers to partition to cache space
// by pre-pending the ID to all cache keys.
func (c *Cache) NewID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	return id
}

// Get retrieves k from cache.
// Returns nil if not found.
func (c *Cache) Get(key string) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, found := c.entries[key]
	if found {
		ce := e.Value.(cacheEntry)
		if ce.old {
			c.old.MoveToFront(e)
		} else {
			ce.old = true

			c.old.PushFront(c.young.Remove(e))

			s := int64(len(ce.data))
			c.oldSize += s
			c.youngSize -= s

			c.runEviction()
		}
		return ce.data
	}

	return nil
}

func (c *Cache) Insert(key string, data []byte) {
	size := int64(len(data))

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, found := c.entries[key]; found {
		return
	}

	ce := cacheEntry{key: key, data: data}
	c.entries[key] = c.young.PushFront(ce)
	c.youngSize += size

	c.runEviction()
}

// Requires c.mu be held.
func (c *Cache) runEviction() {
	for c.oldSize > c.targetOldSize {
		evicted := c.old.Remove(c.old.Back()).(cacheEntry)
		c.young.PushFront(evicted)

		s := int64(len(evicted.data))
		c.oldSize -= s
		c.youngSize += s
	}
	for c.youngSize > c.targetYoungSize {
		evicted := c.young.Remove(c.young.Back()).(cacheEntry)
		delete(c.entries, evicted.key)
		c.youngSize -= int64(len(evicted.data))
	}
}
