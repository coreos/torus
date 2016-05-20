package distributor

import (
	"container/list"
	"sync"
)

// cache implements an LRU cache.
type cache struct {
	cache    map[string]*list.Element
	priority *list.List
	maxSize  int
	mut      sync.Mutex
}

type kv struct {
	key   string
	value interface{}
}

func newCache(size int) *cache {
	var lru cache
	lru.maxSize = size
	lru.priority = list.New()
	lru.cache = make(map[string]*list.Element)
	return &lru
}

func (lru *cache) Put(key string, value interface{}) {
	if lru == nil {
		return
	}
	lru.mut.Lock()
	defer lru.mut.Unlock()
	if _, ok := lru.get(key); ok {
		return
	}
	if len(lru.cache) == lru.maxSize {
		lru.removeOldest()
	}
	lru.priority.PushFront(kv{key: key, value: value})
	lru.cache[key] = lru.priority.Front()
}

func (lru *cache) Get(key string) (interface{}, bool) {
	if lru == nil {
		return nil, false
	}
	lru.mut.Lock()
	defer lru.mut.Unlock()
	return lru.get(key)
}

func (lru *cache) get(key string) (interface{}, bool) {
	if element, ok := lru.cache[key]; ok {
		lru.priority.MoveToFront(element)
		return element.Value.(kv).value, true
	}
	return nil, false
}

func (lru *cache) removeOldest() {
	last := lru.priority.Remove(lru.priority.Back())
	delete(lru.cache, last.(kv).key)
}
