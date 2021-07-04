package engine

import (
	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
	"testing"
)


func Test_BoolTagIndex_Less(t *testing.T) {
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
			idxA := BoolTagIndex{k: tc.k1, v: tc.v1}
			idxB := BoolTagIndex{k: tc.k2, v: tc.v2}

			assert.Equal(t, tc.less, idxA.Less(&idxB))
		})
	}
}

func Test_BoolTagIndex_BTree(t *testing.T) {
	btr := btree.New(2)
	btr.ReplaceOrInsert(&BoolTagIndex{k: "http:success", v: true, offset: 1})
	btr.ReplaceOrInsert(&BoolTagIndex{k: "http:success", v: true, offset: 8})
	btr.ReplaceOrInsert(&BoolTagIndex{k: "http:success", v: false, offset: 2})
	btr.ReplaceOrInsert(&BoolTagIndex{k: "http:error", v: false, offset: 3})
	btr.ReplaceOrInsert(&BoolTagIndex{k: "http:error", v: true, offset: 4})
	btr.ReplaceOrInsert(&BoolTagIndex{k: "http:error", v: true, offset: 7})
	btr.ReplaceOrInsert(&BoolTagIndex{k: "http:z", v: false, offset: 5})
	btr.ReplaceOrInsert(&BoolTagIndex{k: "http:z", v: true, offset: 6})

	var offsetsHttpError []int
	btr.AscendRange(
		&BoolTagIndex{k: "http:error", v: true, offset: 0},
		&BoolTagIndex{k: "http:error", v: true, offset: 8},
		func (i btree.Item) bool {
			offset := i.(*BoolTagIndex).offset
			offsetsHttpError = append(offsetsHttpError, offset)
			return true
		},
	)

	assert.Equal(t, []int{7, 4}, offsetsHttpError)

	var offsetsNoSuccess []int
	btr.AscendRange(
		&BoolTagIndex{k: "http:success", v: false, offset: 0},
		&BoolTagIndex{k: "http:success", v: false, offset: 8},
		func (i btree.Item) bool {
			offset := i.(*BoolTagIndex).offset
			offsetsNoSuccess = append(offsetsNoSuccess, offset)
			return true
		},
	)

	assert.Equal(t, []int{2}, offsetsNoSuccess)

	var offsetsSuccess []int
	btr.AscendRange(
		&BoolTagIndex{k: "http:success", v: true, offset: 0},
		&BoolTagIndex{k: "http:success", v: true, offset: 8},
		func (i btree.Item) bool {
			offset := i.(*BoolTagIndex).offset
			offsetsSuccess = append(offsetsSuccess, offset)
			return true
		},
	)

	assert.Equal(t, []int{8, 1}, offsetsSuccess)
}