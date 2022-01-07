package lru

import (
	"container/list"
	"sync"
)

type lruShard struct {
	mu sync.RWMutex
	lmu sync.Mutex
	totalBytes uint64
	elemsCount int64
	maxBytes   uint64
	evictList *list.List
	elems     map[uint64]*list.Element
	onEvict   OnEvict
}

func newLruShard(maxBytes uint64, onEvict OnEvict) *lruShard {
	return &lruShard{
		maxBytes: maxBytes,
		evictList: list.New(),
		elems: make(map[uint64]*list.Element),
		onEvict: onEvict,
	}
}

type entry struct {
	key   uint64
	value []byte
}

func (ls *lruShard) get(key uint64) ([]byte, bool) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	if elem, ok := ls.elems[key]; ok {
		ls.lmu.Lock()
		ls.evictList.MoveToFront(elem)
		ls.lmu.Unlock()
		return elem.Value.(*entry).value, true
	} else {
		return nil, false
	}
}

// Add value to lru map under key and returns true if eviction happened
func (ls *lruShard) add(key uint64, value []byte) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// until we can safely insert a value of new length
	// remove the oldest entries
	var evicted bool
	for ls.totalBytes + uint64(len(value)) > ls.maxBytes {
		evictedKey, evictedValue, ok := ls.removeOldestUnderLock()
		if !ok {
			break
		}
		evicted = true
		if ls.onEvict != nil {
			ls.onEvict(evictedKey, evictedValue)
		}
	}

	// Check for existing item
	if elem, ok := ls.elems[key]; ok {
		ls.lmu.Lock()
		ls.evictList.MoveToFront(elem)
		ls.lmu.Unlock()
		ls.totalBytes -= uint64(len(elem.Value.(*entry).value))
		elem.Value.(*entry).value = value
		ls.totalBytes += uint64(len(value))
		return evicted
	}

	// add new item
	ls.lmu.Lock()
	elem := ls.evictList.PushFront(&entry{
		key:   key,
		value: value,
	})
	ls.lmu.Unlock()

	ls.totalBytes += uint64(len(value))
	ls.elemsCount++
	ls.elems[key] = elem
	return evicted
}

func (ls *lruShard) purge() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	for k := range ls.elems {
		delete(ls.elems, k)
	}

	ls.totalBytes = 0
	ls.elemsCount = 0

	ls.lmu.Lock()
	ls.evictList.Init()
	ls.lmu.Unlock()
}

func (ls *lruShard) remove(key uint64) ([]byte, bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	elem, ok := ls.elems[key]
	if !ok {
		return nil, false
	}

	_, value := ls.removeElementUnderLock(elem)
	return value, true
}

func (ls *lruShard) removeOldestUnderLock() (uint64, []byte, bool) {
	ls.lmu.Lock()
	elem := ls.evictList.Back()
	ls.lmu.Unlock()

	if elem != nil {
		k, v := ls.removeElementUnderLock(elem)
		return k, v, true
	} else {
		return 0, nil, false
	}
}

func (ls *lruShard) removeElementUnderLock(elem *list.Element) (uint64, []byte) {
	ls.lmu.Lock()
	ls.evictList.Remove(elem)
	ls.lmu.Unlock()

	kv := elem.Value.(*entry)
	ls.elems[kv.key] = nil
	delete(ls.elems, kv.key)
	ls.totalBytes -= uint64(len(kv.value))
	ls.elemsCount--
	return kv.key, kv.value
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
