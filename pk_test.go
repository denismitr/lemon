package lemon

import (
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/btree"
	"strings"
	"testing"
)

func TestIndex_Less_2segments(t *testing.T) {
	tt := []struct {
		key1 string
		key2 string
		less bool
	}{
		{"user:11", "user:100", true},
		{"user:1", "user:999", true},
		{"user:100", "user:11", false},
		{"usera", "userb", true},
		{"userc", "userb", false},
		{"user:a", "user:b", true},
		{"user:a:2", "user:b:1", true},
		{"user:a", "user:b:0", true},
		{"user", "user:1", true},
		{"product", "user", true},
		{"product:9", "user:1", true},
		{"user:1", "user:1:pets", true},
		{"item:8976", "item:8976", false},
		{"product:1145", "product:1144", false},
		{"product:1145", "product:1146", true},
	}

	for _, tc := range tt {
		t.Run(tc.key1+"_"+tc.key2, func(t *testing.T) {
			idxA := newPK(tc.key1)
			idxB := newPK(tc.key2)

			assert.Equal(t, tc.less, idxA.Less(idxB))
		})
	}
}

func TestPK_Match(t *testing.T) {
	tt := []struct {
		key string
		pattern string
		exp bool
	}{
		{"user:11", "user:*", true},
		{"product:kitchen", "product:*", true},
		{"foo:bar", "*", true},
		{"foo:bar", "foo", false},
		{"foo:bar", "user:123:*", false},
	}

	for _, tc := range tt {
		t.Run(tc.pattern, func(t *testing.T) {
			pk := newPK(tc.key)
			pk.Match(strings.Split(tc.pattern, ":"))
		})
	}
}

func Test_KeyAscend(t *testing.T) {
	t.Run("users", func(t *testing.T) {
		idx := btree.New(byPrimaryKeys)
		idx.Set(newEntry("user:10", nil))
		idx.Set(newEntry("user:100", nil))
		idx.Set(newEntry("user:101", nil))
		idx.Set(newEntry("user:1", nil))
		idx.Set(newEntry("user:001", nil))
		idx.Set(newEntry("user:123", nil))
		idx.Set(newEntry("user:2", nil))
		idx.Set(newEntry("user:002", nil))
		idx.Set(newEntry("user:20", nil))
		idx.Set(newEntry("user:22", nil))
		idx.Set(newEntry("user:220", nil))
		idx.Set(newEntry("user:11", nil))
		idx.Set(newEntry("user:12", nil))
		idx.Set(newEntry("user:1000", nil))
		idx.Set(newEntry("user:01", nil))
		idx.Set(newEntry("user:02", nil))
		idx.Set(newEntry("user:30", nil))

		var result []string
		idx.Ascend(nil, func(i interface{}) bool {
			result = append(result, i.(*entry).key.String())
			return true
		})

		assert.Equal(t, []string{
			"user:001",
			"user:002",
			"user:01",
			"user:02",
			"user:1",
			"user:2",
			"user:10",
			"user:11",
			"user:12",
			"user:20",
			"user:22",
			"user:30",
			"user:100",
			"user:101",
			"user:123",
			"user:220",
			"user:1000",
		}, result)

		assert.Equal(t, 17, len(result))
	})
}

