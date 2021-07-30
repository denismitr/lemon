package lemon

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

type boolTag struct {
	Name   string
	Value  bool
}

type floatTag struct {
	Key   string
	Value float64
}

type intTag struct {
	Key   string
	Value int
}

type strTag struct {
	Name  string
	Value string
}

func byStrings(a, b interface{}) bool {
	i1, i2 := a.(*strTag), b.(*strTag)
	return i1.Value < i2.Value
}


