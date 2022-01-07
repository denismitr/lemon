package lru

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_lru(t *testing.T) {
	t.Run("it evicts only when bytes limit is reached and new key is added", func(t *testing.T) {
		evicted := 0
		var evictedKeys []uint64
		var evictedValues [][]byte

		onEvict := func(k uint64, v []byte) {
			evicted++
			evictedKeys = append(evictedKeys, k)
			evictedValues = append(evictedValues, v)
		}

		var keyA uint64 = 1
		vA := []byte(`a1234567890abcdefgh`)
		lru := newLruShard(20 * 4, onEvict)
		assert.False(t, lru.add(keyA, vA))
		assert.Equal(t, 0, evicted)

		var keyB uint64 = 33
		vB := []byte(`b1234567890abcdefgh`)
		assert.False(t, lru.add(keyB, vB))
		assert.Equal(t, 0, evicted)

		vC := []byte(`c1234567890abcdefgh`)
		var keyC uint64 = 99
		assert.False(t, lru.add(keyC, vC))
		assert.Equal(t, 0, evicted)

		var keyD uint64 = 134
		vD := []byte(`d1234567890abcdefgh`)
		assert.False(t, lru.add(keyD, vD))
		assert.Equal(t, 0, evicted)

		// query for all keys except for c
		// hence it should be the first candidate for eviction
		{
			v, ok := lru.get(keyA)
			require.True(t, ok)
			require.NotNil(t, v)
			require.Exactly(t, vA, v)
		}

		{
			v, ok := lru.get(keyB)
			require.True(t, ok)
			require.NotNil(t, v)
			require.Exactly(t, vB, v)
		}

		{
			v, ok := lru.get(keyD)
			require.True(t, ok)
			require.NotNil(t, v)
			require.Exactly(t, vD, v)
		}

		// new entry should cause eviction
		var keyE uint64 = 456
		originalE := []byte(`e1234567890abcdefgh`)
		assert.True(t, lru.add(keyE, originalE))
		assert.Equal(t, 1, evicted)
		assert.Equal(t, keyC, evictedKeys[0])
		assert.Exactly(t, vC, evictedValues[0], "value c under keyC should have been evicted")

		{
			// keyC should not be in cache
			v, ok := lru.get(keyC)
			require.False(t, ok)
			require.Nil(t, v)
		}
	})
}
