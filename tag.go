package lemon

import (
	"github.com/google/btree"
)

type TagType string

const (
	BoolTagType TagType = "bool"
	FloatTagType TagType = "float"
	StrTagType TagType = "str"
	IntTagType TagType = "int"
)

type Tagger func(t *Tags)

type Tag interface {
	Name() string
	Type() TagType
	String() string
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
	setPk(int)
}

type boolTag struct {
	Name   string
	Value  bool
	pk PK
}

func (bt *boolTag) setPk(pk PK) {
	bt.pk = pk
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
	Name  string
	Value string
	pk PK
}

func (st *strTag) setPk(pk PK) {
	st.pk = pk
}

func byBooleans(a, b interface{}) bool {
	i1, i2 := a.(*boolTag), b.(*boolTag)
	if i1.Value == false && i2.Value == true {
		return true
	}

	if i1.Value == true && i2.Value == false {
		return false
	}

	return i1.pk.Less(i2.pk) // todo: call PK comparison function
}

func byStrings(a, b interface{}) bool {
	i1, i2 := a.(*strTag), b.(*strTag)
	if i1.Value < i2.Value {
		return true
	}

	if i1.Value > i2.Value {
		return false
	}

	return i1.pk.Less(i2.pk) // todo: call PK comparison function
}


