package engine

import "github.com/google/btree"

type Tags struct {
	Booleans []TagIndex
}

type TagIndex interface {
	btree.Item
	setOffset(int)
}

type BoolTagIndex struct {
	k      string
	v      bool
	offset int
}

func NewBoolTagIndex(k string, v bool) *BoolTagIndex {
	return &BoolTagIndex{
		k: k,
		v: v,
	}
}

func (ti *BoolTagIndex) setOffset(offset int) {
	ti.offset = offset
}

func (ti *BoolTagIndex) Less(than btree.Item) bool {
	other := than.(*BoolTagIndex)

	if ti.k < other.k {
		return true
	} else if ti.k != other.k {
		return false
	}

	if ti.v == true && other.v == false {
		return true
	} else if ti.v != other.v {
		return false
	}

	if ti.offset < other.offset {
		return true
	}

	return true
}
