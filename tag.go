package lemon

type Tagger func(t *Tags)

func BoolTag(name string, value bool) Tagger {
	return func(t *Tags) {
		t.booleans = append(t.booleans, bTag{name: name, value: value})
	}
}

func StrTag(name string, value string) Tagger {
	return func(t *Tags) {
		t.strings = append(t.strings, strTag{name: name, value: value})
	}
}

type Tags struct {
	booleans []bTag
	floats   []floatTag
	integers []intTag
	strings  []strTag
}

func (t *Tags) Booleans() map[string]bool {
	result := make(map[string]bool, len(t.booleans))
	for _, bt := range t.booleans {
		result[bt.name] = bt.value
	}
	return result
}

func (t *Tags) Strings() map[string]string {
	result := make(map[string]string, len(t.strings))
	for _, bt := range t.strings {
		result[bt.name] = bt.value
	}
	return result
}

func (t *Tags) GetString(name string) string {
	for _, st := range t.strings {
		if st.name == name {
			return st.value
		}
	}
	return ""
}

func (t *Tags) GetBool(name string) bool {
	for _, bt := range t.booleans {
		if bt.name == name {
			return bt.value
		}
	}
	return false
}

type bTag struct {
	name  string
	value bool
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
	name  string
	value string
}

func byStrings(a, b interface{}) bool {
	i1, i2 := a.(*strTag), b.(*strTag)
	return i1.value < i2.value
}


