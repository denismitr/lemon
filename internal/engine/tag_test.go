package engine

import (
	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
	"testing"
)


func Test_BoolTag_Less(t *testing.T) {
	tt := []struct {
		k1   string
		v1   bool
		k2   string
		v2   bool
		less bool
	}{
		{"http:success", true, "http:success", false, false},
		{"http:success", true, "http:error", false, false},
	}

	for _, tc := range tt {
		t.Run(tc.k1+"_"+tc.k2, func(t *testing.T) {
			idxA := BoolTag{K: tc.k1, V: tc.v1}
			idxB := BoolTag{K: tc.k2, V: tc.v2}

			assert.Equal(t, tc.less, idxA.Less(&idxB))
		})
	}
}

func Test_BoolTag_BTree(t *testing.T) {
	btr := btree.New(2)
	btr.ReplaceOrInsert(&BoolTag{K: "http:success", V: true, offset: 1})
	btr.ReplaceOrInsert(&BoolTag{K: "http:success", V: true, offset: 8})
	btr.ReplaceOrInsert(&BoolTag{K: "http:success", V: false, offset: 2})
	btr.ReplaceOrInsert(&BoolTag{K: "http:error", V: false, offset: 3})
	btr.ReplaceOrInsert(&BoolTag{K: "http:error", V: true, offset: 4})
	btr.ReplaceOrInsert(&BoolTag{K: "http:error", V: true, offset: 7})
	btr.ReplaceOrInsert(&BoolTag{K: "http:z", V: false, offset: 5})
	btr.ReplaceOrInsert(&BoolTag{K: "http:z", V: true, offset: 6})

	var offsetsHttpError []int
	btr.AscendRange(
		&BoolTag{K: "http:error", V: true, offset: 0},
		&BoolTag{K: "http:error", V: true, offset: 8},
		func (i btree.Item) bool {
			offset := i.(*BoolTag).offset
			offsetsHttpError = append(offsetsHttpError, offset)
			return true
		},
	)

	assert.Equal(t, []int{7, 4}, offsetsHttpError)

	var offsetsNoSuccess []int
	btr.AscendRange(
		&BoolTag{K: "http:success", V: false, offset: 0},
		&BoolTag{K: "http:success", V: false, offset: 8},
		func (i btree.Item) bool {
			offset := i.(*BoolTag).offset
			offsetsNoSuccess = append(offsetsNoSuccess, offset)
			return true
		},
	)

	assert.Equal(t, []int{2}, offsetsNoSuccess)

	var offsetsSuccess []int
	btr.AscendRange(
		&BoolTag{K: "http:success", V: true, offset: 0},
		&BoolTag{K: "http:success", V: true, offset: 8},
		func (i btree.Item) bool {
			offset := i.(*BoolTag).offset
			offsetsSuccess = append(offsetsSuccess, offset)
			return true
		},
	)

	assert.Equal(t, []int{8, 1}, offsetsSuccess)
}