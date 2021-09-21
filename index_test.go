package lemon

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_AddAndRemove_ByEntry(t *testing.T) {
	t.Parallel()

	e1 := newEntry("e1", nil)
	e1.tags = newTags()
	e1.tags.integers["int_foobar"] = 9
	e1.tags.integers["int_bar_baz"] = 23
	e1.tags.floats["float_foobar"] = 11.41
	e1.tags.floats["float_bar_baz"] = 22.434
	e1.tags.booleans["bool_foobar"] = true
	e1.tags.booleans["bool_bar_baz"] = false
	e1.tags.strings["str_foobar"] = "foobar"
	e1.tags.strings["str_bar_baz"] = "bar_baz"

	e2 := newEntry("e2", nil)
	e2.tags = newTags()
	e2.tags.integers["int_foobar"] = 9
	e2.tags.integers["int_bar_baz"] = 23
	e2.tags.floats["float_foobar"] = 11.41
	e2.tags.floats["float_bar_baz"] = 10.40
	e2.tags.booleans["bool_foobar"] = true
	e2.tags.booleans["bool_bar_baz"] = false
	e2.tags.strings["str_foobar"] = "foobar"
	e2.tags.strings["str_bar_baz"] = "bar_baz"

	e3 := newEntry("e3", nil)
	e3.tags = newTags()
	e3.tags.integers["int_123"] = 9
	e3.tags.integers["int_abc"] = 23
	e3.tags.floats["float_123"] = 11.41
	e3.tags.floats["float_abc"] = 22.434
	e3.tags.booleans["bool_123"] = true
	e3.tags.booleans["bool_abc"] = false
	e3.tags.strings["str_123"] = "123"
	e3.tags.strings["str_abc"] = "abc"

	t.Run("adding and removing floats", func(t *testing.T) {
		ti := newTagIndex()

		require.NoError(t, ti.add("float_foobar", e1.tags.floats["float_foobar"], e1))
		require.NoError(t, ti.add("float_foobar", e2.tags.floats["float_foobar"], e2))

		require.NoError(t, ti.add("float_bar_baz", e1.tags.floats["float_bar_baz"], e1))
		require.NoError(t, ti.add("float_bar_baz", e2.tags.floats["float_bar_baz"], e2))

		require.NoError(t, ti.add("float_abc", e3.tags.floats["float_abc"], e3))
		require.NoError(t, ti.add("float_123", e3.tags.floats["float_123"], e3))

		assert.Equal(t, 4, len(ti.data))
		//entriesBeforeRemove := ti.getEntriesFor("float_foobar", 11.41)
		//assert.Equal(t, 2, len(entriesBeforeRemove))
		//assert.Equal(t, e1, entriesBeforeRemove[e1.key.String()])
		//assert.Equal(t, e2, entriesBeforeRemove[e2.key.String()])

		ti.removeEntry(e2)

		// expect keys count not change after removal of one entry
		// no keys are left without entries
		assert.Equal(t, 4, len(ti.data))

		//entriesAfterRemove := ti.getEntriesFor("float_foobar", 11.41)
		//assert.Equal(t, 1, len(entriesAfterRemove))
		//assert.Equal(t, e1, entriesAfterRemove[e1.key.String()])
	})

	//t.Run("adding and removing integers", func(t *testing.T) {
	//	ti := newTagIndex()
	//
	//	require.NoError(t, ti.add("int_foobar", e1.tags.integers["int_foobar"], e1))
	//	require.NoError(t, ti.add("int_foobar", e2.tags.integers["int_foobar"], e2))
	//
	//	require.NoError(t, ti.add("int_bar_baz", e1.tags.integers["int_bar_baz"], e1))
	//	require.NoError(t, ti.add("int_bar_baz", e2.tags.integers["int_bar_baz"], e2))
	//
	//	assert.Equal(t, 2, ti.keys())
	//	entriesBeforeRemove := ti.getEntriesFor("int_foobar", 9)
	//	assert.Equal(t, 2, len(entriesBeforeRemove))
	//	assert.Equal(t, e1, entriesBeforeRemove[e1.key.String()])
	//	assert.Equal(t, e2, entriesBeforeRemove[e2.key.String()])
	//
	//	ti.removeEntry(e2)
	//
	//	assert.Equal(t, 2, ti.keys())
	//	entriesAfterRemove := ti.getEntriesFor("int_foobar", 9)
	//	assert.Equal(t, 1, len(entriesAfterRemove))
	//	assert.Equal(t, e1, entriesAfterRemove[e1.key.String()])
	//})
	//
	//t.Run("adding and removing booleans", func(t *testing.T) {
	//	ti := newTagIndex()
	//
	//	require.NoError(t, ti.add("bool_foobar", e1.tags.booleans["bool_foobar"], e1))
	//	require.NoError(t, ti.add("bool_foobar", e2.tags.booleans["bool_foobar"], e2))
	//
	//	require.NoError(t, ti.add("bool_bar_baz", e1.tags.booleans["bool_bar_baz"], e1))
	//	require.NoError(t, ti.add("bool_bar_baz", e2.tags.booleans["bool_bar_baz"], e2))
	//
	//	// expect 2 keys be present in tagIndex
	//	assert.Equal(t, 2, ti.keys())
	//
	//	// expect 2 entries with tag "bool_foobar" and value true
	//	entriesBeforeRemove := ti.getEntriesFor("bool_foobar", true)
	//	assert.Equal(t, 2, len(entriesBeforeRemove))
	//	assert.Equal(t, e1, entriesBeforeRemove[e1.key.String()])
	//	assert.Equal(t, e2, entriesBeforeRemove[e2.key.String()])
	//
	//	// remove entry e2
	//	ti.removeEntry(e2)
	//
	//	assert.Equal(t, 2, ti.keys())
	//	entriesAfterRemove := ti.getEntriesFor("bool_foobar", true)
	//	assert.Equal(t, 1, len(entriesAfterRemove))
	//	assert.Equal(t, e1, entriesAfterRemove[e1.key.String()])
	//
	//	ti.removeEntry(e1)
	//
	//	// expect no more entries left in tagIndex
	//	// therefore it should become empty
	//	assert.Equal(t, 0, ti.keys())
	//
	//	// expect no entries left after e1 is removed for tag "bool_foobar" and value true
	//	entriesAfterRemove = ti.getEntriesFor("bool_foobar", true)
	//	assert.Equal(t, 0, len(entriesAfterRemove))
	//})
	//
	//t.Run("adding and removing strings", func(t *testing.T) {
	//	ti := newTagIndex()
	//
	//	require.NoError(t, ti.add("str_foobar", e1.tags.strings["str_foobar"], e1))
	//	require.NoError(t, ti.add("str_foobar", e2.tags.strings["str_foobar"], e2))
	//
	//	require.NoError(t, ti.add("str_bar_baz", e1.tags.strings["str_bar_baz"], e1))
	//	require.NoError(t, ti.add("str_bar_baz", e2.tags.strings["str_bar_baz"], e2))
	//
	//	// expect 2 keys be present in tagIndex
	//	assert.Equal(t, 2, ti.keys())
	//
	//	// expect 2 entries with tag "str_foobar" and value true
	//	entriesBeforeRemove := ti.getEntriesFor("str_foobar", "foobar")
	//	assert.Equal(t, 2, len(entriesBeforeRemove))
	//	assert.Equal(t, e1, entriesBeforeRemove[e1.key.String()])
	//	assert.Equal(t, e2, entriesBeforeRemove[e2.key.String()])
	//
	//	// remove entry e2
	//	ti.removeEntry(e2)
	//
	//	assert.Equal(t, 2, ti.keys())
	//	entriesAfterRemove := ti.getEntriesFor("str_foobar", "foobar")
	//	assert.Equal(t, 1, len(entriesAfterRemove))
	//	assert.Equal(t, e1, entriesAfterRemove[e1.key.String()])
	//
	//	ti.removeEntry(e1)
	//
	//	// expect no more entries left in tagIndex
	//	// therefore it should become empty
	//	assert.Equal(t, 0, ti.keys())
	//
	//	// expect no entries left after e1 is removed for tag "str_foobar" and value true
	//	entriesAfterRemove = ti.getEntriesFor("str_foobar", "foobar")
	//	assert.Equal(t, 0, len(entriesAfterRemove))
	//})
}
