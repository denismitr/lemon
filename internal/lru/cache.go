package lru

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
)

var ErrIllegalCapacity = errors.New("illegal lru cache capacity")
var ErrInvalidSharding = errors.New("invalid sharding")

type Cache struct {
	maxBytes uint64
	capacity uint64
	shards []*lruShard
}

func NewCache(shards int, maxTotalBytes uint64) (*Cache, error) {
	if maxTotalBytes <= 2 {
		return nil, ErrIllegalCapacity
	}

	if shards <= 2 {
		return nil, ErrInvalidSharding
	}

	c := Cache{
		maxBytes: maxTotalBytes,
		capacity: uint64(shards),
		shards:   make([]*lruShard, shards),
	}

	shardMaxBytes := maxTotalBytes / c.capacity
	for i := range c.shards {
		c.shards[i] = newLruShard(shardMaxBytes)
	}

	return &c, nil
}

// Add value to cache under key and returns true if eviction happened
func (c *Cache) Add(key uint64, value []byte) bool {
	shard := c.getShard(key)
	return shard.add(key, value)
}

func (c *Cache) Get(key uint64) ([]byte, bool) {
	shard := c.getShard(key)
	return shard.get(key)
}

func (c *Cache) Remove(key uint64) {
	shard := c.getShard(key)
	shard.remove(key)
}

func (c *Cache) getShard(key uint64) *lruShard {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, key)
	hash := xxhash.Sum64(bs)
	return c.shards[hash%c.capacity]
}
