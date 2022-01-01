package lru

import (
	"container/list"
	"sync"
)

type lruShard struct {
	mu sync.RWMutex
	totalBytes uint64
	elemsCount int64
	maxBytes   uint64
	evictList *list.List
	elems     map[uint64]*list.Element
}

func newLruShard(maxBytes uint64) *lruShard {
	return &lruShard{
		maxBytes: maxBytes,
		evictList: list.New(),
		elems: make(map[uint64]*list.Element),
	}
}

type entry struct {
	key   uint64
	value []byte
}

func (ls *lruShard) get(key uint64) ([]byte, bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if elem, ok := ls.elems[key]; ok {
		ls.evictList.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	} else {
		return nil, false
	}
}

// Add value to lru map under key and returns true if eviction happened
func (ls *lruShard) add(key uint64, value []byte) ([]byte, bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Check for existing item
	if elem, ok := ls.elems[key]; ok {
		ls.evictList.MoveToFront(elem)
		ls.totalBytes -= uint64(len(elem.Value.(*entry).value))
		elem.Value.(*entry).value = value
		ls.totalBytes += uint64(len(value))
	} else {
		// add new item
		elem = ls.evictList.PushFront(&entry{
			key:   key,
			value: value,
		})

		ls.totalBytes += uint64(len(value))
		ls.elemsCount++
		ls.elems[key] = elem
		mustEvict := ls.totalBytes > ls.maxBytes

		if mustEvict {
			return ls.removeOldestUnderLock()
		}
	}

	return nil, false
}

func (ls *lruShard) purge() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	for k := range ls.elems {
		delete(ls.elems, k)
	}

	ls.totalBytes = 0
	ls.elemsCount = 0

	ls.evictList.Init()
}

func (ls *lruShard) remove(key uint64) ([]byte, bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	elem, ok := ls.elems[key]
	if !ok {
		return nil, false
	}
	return ls.removeElementUnderLock(elem), true
}

func (ls *lruShard) removeOldestUnderLock() ([]byte, bool) {
	elem := ls.evictList.Back()
	if elem != nil {
		return ls.removeElementUnderLock(elem), true
	} else {
		return nil, false
	}
}

func (ls *lruShard) removeElementUnderLock(elem *list.Element) []byte {
	ls.evictList.Remove(elem)
	kv := elem.Value.(*entry)
	ls.elems[kv.key] = nil
	delete(ls.elems, kv.key)
	ls.totalBytes -= uint64(len(kv.value))
	ls.elemsCount--
	return kv.value
}

func (ls *lruShard) keys() []uint64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	keys := make([]uint64, 0, ls.elemsCount)
	for k := range ls.elems {
		keys = append(keys, k)
	}
	return keys
}
