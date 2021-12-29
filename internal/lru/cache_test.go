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

		c, err := NewCache(2, 1024)
		require.NoError(t, err)

		c.OnEvict(onEvict)

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

		c, err := NewCache(2, 100)
		require.NoError(t, err)

		c.OnEvict(onEvict)

		for i := 0; i < 100; i += 5 {
			c.Add(uint64(i), []byte(fmt.Sprintf("Value %d", i)))
		}

		for i := 0; i < 100; i += 5 {
			c.Get(uint64(i))
		}

		require.Equal(t, 8, evicted)

		// todo: check remaining keys
	})
}