package lemon

//func Test_BoolTag_Less(t *testing.T) {
//	tt := []struct {
//		k1   string
//		v1   bool
//		k2   string
//		v2   bool
//		less bool
//	}{
//		{"http:success", true, "http:success", false, false},
//		{"http:success", true, "http:error", false, false},
//	}
//
//	for _, tc := range tt {
//		t.Run(tc.k1+"_"+tc.k2, func(t *testing.T) {
//			idxA := boolTag{Name: tc.k1, Value: tc.v1}
//			idxB := boolTag{Name: tc.k2, Value: tc.v2}
//
//			assert.Equal(t, tc.less, idxA.Less(&idxB))
//		})
//	}
//}
//
//func Test_BoolTag_BTree(t *testing.T) {
//	btr := btree.New(2)
//	btr.ReplaceOrInsert(&boolTag{Name: "http:success", Value: true, offset: 1})
//	btr.ReplaceOrInsert(&boolTag{Name: "http:success", Value: true, offset: 8})
//	btr.ReplaceOrInsert(&boolTag{Name: "http:success", Value: false, offset: 2})
//	btr.ReplaceOrInsert(&boolTag{Name: "http:error", Value: false, offset: 3})
//	btr.ReplaceOrInsert(&boolTag{Name: "http:error", Value: true, offset: 4})
//	btr.ReplaceOrInsert(&boolTag{Name: "http:error", Value: true, offset: 7})
//	btr.ReplaceOrInsert(&boolTag{Name: "http:z", Value: false, offset: 5})
//	btr.ReplaceOrInsert(&boolTag{Name: "http:z", Value: true, offset: 6})
//
//	var offsetsHttpError []int
//	btr.AscendRange(
//		&boolTag{Name: "http:error", Value: true, offset: 0},
//		&boolTag{Name: "http:error", Value: true, offset: 8},
//		func (i btree.Item) bool {
//			offset := i.(*boolTag).offset
//			offsetsHttpError = append(offsetsHttpError, offset)
//			return true
//		},
//	)
//
//	assert.Equal(t, []int{7, 4}, offsetsHttpError)
//
//	var offsetsNoSuccess []int
//	btr.AscendRange(
//		&boolTag{Name: "http:success", Value: false, offset: 0},
//		&boolTag{Name: "http:success", Value: false, offset: 8},
//		func (i btree.Item) bool {
//			offset := i.(*boolTag).offset
//			offsetsNoSuccess = append(offsetsNoSuccess, offset)
//			return true
//		},
//	)
//
//	assert.Equal(t, []int{2}, offsetsNoSuccess)
//
//	var offsetsSuccess []int
//	btr.AscendRange(
//		&boolTag{Name: "http:success", Value: true, offset: 0},
//		&boolTag{Name: "http:success", Value: true, offset: 8},
//		func (i btree.Item) bool {
//			offset := i.(*boolTag).offset
//			offsetsSuccess = append(offsetsSuccess, offset)
//			return true
//		},
//	)
//
//	assert.Equal(t, []int{8, 1}, offsetsSuccess)
//}
