package lemon

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_stringIndex(t *testing.T) {
	t.Run("single item can be added to empty index and than removed", func(t *testing.T) {
		si := make(stringIndex)

		si.add("foo", "bar", 4)
		offsets := si.findOffsets("foo", "bar")
		assert.Equal(t, []int{4}, offsets)

		si.removeOffsetAndShift("foo", "bar", 4)
		offsets = si.findOffsets("foo", "bar")
		assert.Equal(t, []int{}, offsets)
	})

	t.Run("multiple items for one tag can be added to empty index and than removed", func(t *testing.T) {
		si := make(stringIndex)

		si.add("foo", "bar", 4)
		si.add("foo", "bar", 0)
		si.add("foo", "bar", 2)
		si.add("foo", "bar", 200)
		si.add("foo", "baz", 98)
		si.add("foo", "baz", 3)
		si.add("foo", "baz", 1)
		si.add("foo", "boo", 0)
		si.add("foo", "a1234", 4)
		si.add("foo", "a1234", 123)
		si.add("foo", "a1234", 98)

		barOffsets := si.findOffsets("foo", "bar")
		assert.Equal(t, []int{0, 2, 4, 200}, barOffsets)

		si.removeOffsetAndShift("foo", "bar", 2)
		barOffsets = si.findOffsets("foo", "bar")
		assert.Equal(t, []int{0, 3, 199}, barOffsets)

		si.removeOffsetAndShift("foo", "bar", 199)
		barOffsets = si.findOffsets("foo", "bar")
		assert.Equal(t, []int{0, 3}, barOffsets)
	})

	t.Run("it can safely retrieve non existing tag", func(t *testing.T) {
		si := make(stringIndex)
		offsets := si.findOffsets("foo", "bar")
		assert.NotNil(t, offsets)
	})
}
