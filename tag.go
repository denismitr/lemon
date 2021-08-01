package lemon

type Tagger func(t *Tags)

func BoolTag(name string, value bool) Tagger {
	return func(t *Tags) {
		t.booleans[name] = value
	}
}

func StrTag(name string, value string) Tagger {
	return func(t *Tags) {
		t.strings[name] = value
	}
}

type Tags struct {
	booleans map[string]bool
	floats   map[string]float64
	integers map[string]int
	strings  map[string]string
}

func newTags() *Tags {
	return &Tags{
		booleans: make(map[string]bool),
		floats:   make(map[string]float64),
		integers: make(map[string]int),
		strings:  make(map[string]string),
	}
}

func (t *Tags) Booleans() map[string]bool {
	return t.booleans
}

func (t *Tags) Strings() map[string]string {
	return t.strings
}

func (t *Tags) GetString(name string) string {
	return t.strings[name]
}

func (t *Tags) GetBool(name string) bool {
	return t.booleans[name]
}

type bTag struct {
	name  string
	value bool
}

type strTag struct {
	name  string
	value string
}

func byStrings(a, b interface{}) bool {
	i1, i2 := a.(*strTag), b.(*strTag)
	return i1.value < i2.value
}
