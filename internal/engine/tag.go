package engine

import "github.com/google/btree"

type Tags struct {
	Booleans []BoolTag  `json:"b"`
	FloatTag []FloatTag `json:"f"`
	IntTag   []IntTag   `json:"i"`
	StrTag   []StrTag   `json:"s"`
}

type TagIndex interface {
	btree.Item
	setOffset(int)
}

type BoolTag struct {
	K      string `json:"k"`
	V      bool   `json:"v"`
	offset int
}

func NewBoolTagIndex(k string, v bool, offset int) *BoolTag {
	return &BoolTag{
		K:      k,
		V:      v,
		offset: offset,
	}
}

func (ti *BoolTag) setOffset(offset int) {
	ti.offset = offset
}

func (ti *BoolTag) Less(than btree.Item) bool {
	other := than.(*BoolTag)

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

type FloatTag struct {
	Key   string  `json:"k"`
	Value float64 `json:"v"`
}

type IntTag struct {
	Key   string `json:"k"`
	Value int    `json:"v"`
}

type StrTag struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

type TagSetter func(tags *Tags)

func BoolTagSetter(k string, v bool) TagSetter {
	return func(tags *Tags) {
		tags.Booleans = append(tags.Booleans, BoolTag{K: k, V: v})
	}
}
