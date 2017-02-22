package distributor

import (
	"container/list"
	"reflect"
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
	if v, ok := lru.get(key); ok {
		if reflect.DeepEqual(v, value) {
			return
		} else {
			// Actually find() is not necessary, but it makes sure to remove
			// correct element and the find loop would be finished soon, since
			// the element is in the front now.
			if old := lru.find(lru.priority.Front(), v); old != nil {
				lru.priority.Remove(old)
			} else {
				clog.Fatalf("read cache is corrupted. Please restart the process.")
			}
		}
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

func (lru *cache) find(e *list.Element, v interface{}) *list.Element {
	if e == nil {
		return nil
	}
	if reflect.DeepEqual(e.Value.(kv).value, v) {
		return e
	}
	return lru.find(e.Next(), v)
}
