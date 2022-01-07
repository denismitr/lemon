package lru

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"sync"
	"sync/atomic"
)

var ErrIllegalCapacity = errors.New("illegal lru cache capacity")
var ErrInvalidSharding = errors.New("invalid sharding")

type OnEvict func(k uint64, v []byte)

type Cache struct {
	maxBytes uint64
	capacity uint64
	count    int64
	shards   []*lruShard
	onEvict  OnEvict
}


func NewCache(shards int, maxTotalBytes uint64, onEvict OnEvict) (*Cache, error) {
	if maxTotalBytes <= 2 {
		return nil, ErrIllegalCapacity
	}

	if shards < 2 {
		return nil, ErrInvalidSharding
	}

	c := Cache{
		maxBytes: maxTotalBytes,
		capacity: uint64(shards),
		shards:   make([]*lruShard, shards),
	}

	shardMaxBytes := maxTotalBytes / c.capacity
	for i := range c.shards {
		c.shards[i] = newLruShard(shardMaxBytes, onEvict)
	}

	return &c, nil
}

func (c *Cache) OnEvict(fn OnEvict) {
	c.onEvict = fn
}

// Add value to cache under key and returns true if eviction happened
func (c *Cache) Add(key uint64, value []byte) bool {
	shard := c.getShard(key)
	evicted := shard.add(key, value)

	if !evicted {
		atomic.AddInt64(&c.count, 1)
	}

	return evicted
}

func (c *Cache) Get(key uint64) ([]byte, bool) {
	shard := c.getShard(key)
	return shard.get(key)
}

func (c *Cache) Remove(key uint64) {
	shard := c.getShard(key)
	shard.remove(key)
}

func (c *Cache) Purge() {
	var wg sync.WaitGroup

	wg.Add(len(c.shards))
	for i := range c.shards {
		go func(i int) {
			defer wg.Done()
			c.shards[i].purge()
		}(i)
	}

	wg.Wait()
}

func (c *Cache) Count() int {
	return int(atomic.LoadInt64(&c.count))
}

func (c *Cache) Keys() []uint64 {
	count := atomic.LoadInt64(&c.count)
	keys := make([]uint64, 0, count)

	for i := range c.shards {
		keys = append(keys, c.shards[i].keys()...)
	}

	return keys
}

func (c *Cache) getShard(key uint64) *lruShard {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, key)
	hash := xxhash.Sum64(bs)
	return c.shards[hash%c.capacity]
}
