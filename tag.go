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
	Booleans []boolTag
	FloatTag []floatTag
	IntTag   []intTag
	Strings  []strTag
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

func byStrings(a, b interface{}) bool {
	i1, i2 := a.(*strTag), b.(*strTag)
	return i1.Value < i2.Value
}


