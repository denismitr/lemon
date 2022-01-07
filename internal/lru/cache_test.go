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

		c, err := NewCache(2, 1024, onEvict)
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

		c, err := NewCache(2, 100, onEvict)
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
