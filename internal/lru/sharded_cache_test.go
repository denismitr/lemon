package lru

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCache_Add(t *testing.T) {
	t.Run("just add with no eviction", func(t *testing.T) {
		evicted := 0
		onEvict := func(k uint64, v []byte) {
			evicted++
		}

		c, err := NewShardedCache(2, 1024, onEvict)
		require.NoError(t, err)

		for i := 0; i < 100; i += 5 {
			c.Add(uint64(i), []byte(fmt.Sprintf("Value %d", i)))
		}

		for i := 0; i < 100; i += 5 {
			v, ok := c.Get(uint64(i))
			require.True(t, ok)
			require.NotNil(t, v)
			assert.Exactly(t, []byte(fmt.Sprintf("Value %d", i)), v)
		}

		require.Equal(t, 0, evicted)
	})

	t.Run("add with eviction", func(t *testing.T) {
		evicted := 0
		onEvict := func(k uint64, v []byte) {
			evicted++
		}

		c, err := NewShardedCache(2, 100, onEvict)
		require.NoError(t, err)

		c.OnEvict(onEvict)

		for i := 0; i < 100; i += 5 {
			c.Add(uint64(i), []byte(fmt.Sprintf("Value %d", i)))
		}

		for i := 0; i < 100; i += 5 {
			c.Get(uint64(i))
		}

		require.Equal(t, 8, evicted)

		count := c.Count()
		assert.Equal(t, 12, count)

		expectedKeys := []uint64{60, 65, 85, 90, 95, 25, 80, 45, 50, 55, 70, 75}
		for _, k := range expectedKeys {
			_, ok := c.Get(k)
			assert.Truef(t, ok, "expected key %d to be in cache", k)
		}
	})
}

func BenchmarkShards(b *testing.B) {
	createRoutines := func(c *ShardedCache, stdValue []byte, ch <-chan uint64) {
		const n = 20
		for i := 0; i < n; i++ {
			go func() {
				for key := range ch {
					c.Add(key, stdValue)
					c.Get(key)
				}
			}()
		}
	}

	b.Run("2 shards and 20 routines", func(b *testing.B) {
		c, _ := NewShardedCache(2, 1024*1024*40, nil)
		p := []byte(`standard payload 123 foo bar`)
		ch := make(chan uint64)

		createRoutines(c, p, ch)

		l := uint64(b.N)
		for i := uint64(0); i < l; i++ {
			ch <- i
		}

		close(ch)
	})

	b.Run("5 shards and 20 routines", func(b *testing.B) {
		c, _ := NewShardedCache(5, 1024*1024*40, nil)
		p := []byte(`standard payload 123 foo bar`)
		ch := make(chan uint64)

		createRoutines(c, p, ch)

		l := uint64(b.N)
		for i := uint64(0); i < l; i++ {
			ch <- i
		}

		close(ch)
	})

	b.Run("10 shards no evictions", func(b *testing.B) {
		c, _ := NewShardedCache(10, 1024*1024*40, nil)
		p := []byte(`standard payload 123 foo bar`)
		ch := make(chan uint64)

		createRoutines(c, p, ch)

		l := uint64(b.N)
		for i := uint64(0); i < l; i++ {
			ch <- i
		}

		close(ch)
	})

	b.Run("20 shards no evictions", func(b *testing.B) {
		c, _ := NewShardedCache(20, 1024*1024*40, nil)
		p := []byte(`standard payload 123 foo bar`)
		ch := make(chan uint64)

		createRoutines(c, p, ch)

		l := uint64(b.N)
		for i := uint64(0); i < l; i++ {
			ch <- i
		}

		close(ch)
	})
}
