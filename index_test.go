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
	e1.tags["int_foobar"] = &tag{data: 9, dt: intDataType}
	e1.tags["int_bar_baz"] = &tag{data: 23, dt: intDataType}
	e1.tags["float_foobar"] = &tag{data: 11.41, dt: floatDataType}
	e1.tags["float_bar_baz"] = &tag{data: 22.434, dt: floatDataType}
	e1.tags["bool_foobar"] = &tag{data: true, dt: boolDataType}
	e1.tags["bool_bar_baz"] = &tag{data: false, dt: boolDataType}
	e1.tags["str_foobar"] = &tag{data: "foobar", dt: strDataType}
	e1.tags["str_bar_baz"] = &tag{data: "bar_baz", dt: strDataType}

	e2 := newEntry("e2", nil)
	e2.tags = newTags()
	e2.tags["int_foobar"] = &tag{data: 9, dt: intDataType}
	e2.tags["int_bar_baz"] = &tag{data: 23, dt: intDataType}
	e2.tags["float_foobar"] = &tag{data: 11.41, dt: floatDataType}
	e2.tags["float_bar_baz"] = &tag{data: 22.434, dt: floatDataType}
	e2.tags["bool_foobar"] = &tag{data: true, dt: boolDataType}
	e2.tags["bool_bar_baz"] = &tag{data: false, dt: boolDataType}
	e2.tags["str_foobar"] = &tag{data: "foobar", dt: strDataType}
	e2.tags["str_bar_baz"] = &tag{data: "bar_baz", dt: strDataType}

	e3 := newEntry("e3", nil)
	e3.tags = newTags()
	e3.tags["int_123"] = &tag{data: 9, dt: intDataType}
	e3.tags["int_abc"] = &tag{data: 23, dt: intDataType}
	e3.tags["float_123"] = &tag{data: 11.41, dt: floatDataType}
	e3.tags["float_abc"] = &tag{data: 22.434, dt: floatDataType}
	e3.tags["bool_123"] = &tag{data: true, dt: boolDataType}
	e3.tags["bool_abc"] = &tag{data: false, dt: boolDataType}
	e3.tags["str_123"] = &tag{data: "123", dt: strDataType}
	e3.tags["str_abc"] = &tag{data: "abc", dt: strDataType}

	t.Run("adding and removing floats", func(t *testing.T) {
		ti := newTagIndex()

		require.NoError(t, ti.add("float_foobar", e1.tags["float_foobar"].data, e1))
		require.NoError(t, ti.add("float_foobar", e2.tags["float_foobar"].data, e2))

		require.NoError(t, ti.add("float_bar_baz", e1.tags["float_bar_baz"].data, e1))
		require.NoError(t, ti.add("float_bar_baz", e2.tags["float_bar_baz"].data, e2))

		require.NoError(t, ti.add("float_abc", e3.tags["float_abc"].data, e3))
		require.NoError(t, ti.add("float_123", e3.tags["float_123"].data, e3))

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
