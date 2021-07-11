package engine

import "github.com/google/btree"

type Tags struct {
	Booleans []BoolTagIndex
}

type TagIndex interface {
	btree.Item
	setOffset(int)
}

type BoolTagIndex struct {
	K      string
	V      bool
	offset int
}

func NewBoolTagIndex(k string, v bool) *BoolTagIndex {
	return &BoolTagIndex{
		K: k,
		V: v,
	}
}

func (ti *BoolTagIndex) setOffset(offset int) {
	ti.offset = offset
}

func (ti *BoolTagIndex) Less(than btree.Item) bool {
	other := than.(*BoolTagIndex)

	if ti.K < other.K {
		return true
	} else if ti.K != other.K {
		return false
	}

	if ti.V == false && other.V == true {
		return true
	} else if ti.V != other.V {
		return false
	}

	if ti.offset < other.offset {
		return true
	}

	return true
}
