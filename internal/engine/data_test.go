package engine

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIndex_Less_2segments(t *testing.T) {
	tt := []struct{
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
	}

	for _, tc := range tt {
		t.Run(tc.key1 + "_" + tc.key2, func(t *testing.T) {
			idxA := index{key: tc.key1}
			idxB := index{key: tc.key2}

			assert.Equal(t, tc.less, idxA.Less(&idxB))
		})
	}
}
