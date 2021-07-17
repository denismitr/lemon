package lemon

import (
	"github.com/google/btree"
)

type TagType uint8

const (
	BoolTagType TagType = iota
)

type Tagger func(t *Tags)

type Tag interface {
	Name() string
	Type() TagType
	TagIndex() TagIndex
}

func BoolTag(name string, value bool) Tagger {
	return func(t *Tags) {
		t.Booleans = append(t.Booleans, boolTag{Name: name, Value: value})
	}
}

func StrTag(name string, value string) Tagger {
	return func(t *Tags) {
		t.Strings = append(t.Strings, strTag{Name: name, Value: value})
	}
}

type Tags struct {
	Booleans []boolTag  `json:"b"`
	FloatTag []floatTag `json:"f"`
	IntTag   []intTag   `json:"i"`
	Strings  []strTag   `json:"s"`
}

type TagIndex interface {
	btree.Item
	setOffset(int)
}

type boolTag struct {
	Name   string `json:"k"`
	Value  bool   `json:"v"`
	offset int
}

func NewBoolTagIndex(k string, v bool, offset int) *boolTag {
	return &boolTag{
		Name:   k,
		Value:  v,
		offset: offset,
	}
}

func (ti *boolTag) setOffset(offset int) {
	ti.offset = offset
}

func (ti *boolTag) Less(than btree.Item) bool {
	other := than.(*boolTag)

	if ti.Name < other.Name {
		return true
	} else if ti.Name != other.Name {
		return false
	}

	if ti.Value == false && other.Value == true {
		return true
	} else if ti.Value != other.Value {
		return false
	}

	if ti.offset < other.offset {
		return true
	}

	return true
}

type floatTag struct {
	Key   string  `json:"k"`
	Value float64 `json:"v"`
}

type intTag struct {
	Key   string `json:"k"`
	Value int    `json:"v"`
}

type strTag struct {
	Name  string `json:"k"`
	Value string `json:"v"`
}

type TagSetter func(tags *Tags)

func BoolTagSetter(k string, v bool) TagSetter {
	return func(tags *Tags) {
		tags.Booleans = append(tags.Booleans, boolTag{Name: k, Value: v})
	}
}



