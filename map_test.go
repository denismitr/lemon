package lemon

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestLemonMap(t *testing.T) {
	t.Run("preserve original createdAt", func(t *testing.T) {
		now := int(time.Now().UnixMilli())
		tgs := newTags()
		require.NoError(t, tgs.set(CreatedAt, now))

		ent := entry{
			tags: tgs,
		}

		updateTime := int(time.Now().Add(5 * time.Second).UnixMilli())
		m := M{
			CreatedAt: updateTime,
			UpdatedAt: updateTime,
		}

		require.NoError(t, m.applyTo(&ent))
		assert.Exactly(t, M{CreatedAt: now, UpdatedAt: updateTime}, ent.tags.asMap())
	})

	t.Run("getters", func(t *testing.T) {
		m := M{
			"intVal1":   123,
			"intVal2":   -9848774,
			"floatVal1": 456.3244,
			"floatVal2": -0.224,
			"floatVal3": 4.1,
			"strVal1":   "foo",
			"strVal2":   "bar",
			"boolVal1":  true,
			"boolVal2":  false,
		}

		assert.True(t, m.HasInt("intVal1"))
		assert.Equal(t, 123, m.Int("intVal1"))
		assert.True(t, m.HasInt("intVal2"))
		assert.Equal(t, -9848774, m.Int("intVal2"))

		assert.True(t, m.HasFloat("floatVal1"))
		assert.Equal(t, 456.3244, m.Float("floatVal1"))
		assert.True(t, m.HasFloat("floatVal2"))
		assert.Equal(t, -0.224, m.Float("floatVal2"))
		assert.True(t, m.HasFloat("floatVal3"))
		assert.Equal(t, 4.1, m.Float("floatVal3"))

		assert.True(t, m.HasString("strVal1"))
		assert.Equal(t, "foo", m.String("strVal1"))
		assert.True(t, m.HasString("strVal2"))
		assert.Equal(t, "bar", m.String("strVal2"))

		assert.True(t, m.HasBool("boolVal1"))
		assert.Equal(t, true, m.Bool("boolVal1"))
		assert.True(t, m.HasBool("boolVal2"))
		assert.Equal(t, false, m.Bool("boolVal2"))
	})
}
