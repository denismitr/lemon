package lemon

import "github.com/pkg/errors"

type MetaApplier interface {
	applyTo(e *entry)
}

func WithTags() *TagApplier {
	return &TagApplier{
		keys:     make(map[string]indexType),
		booleans: make(map[string]bool),
		floats:   make(map[string]float64),
		integers: make(map[string]int),
		strings:  make(map[string]string),
	}
}

func (ta *TagApplier) Bool(name string, value bool) *TagApplier {
	ta.keys[name] = boolDataType
	ta.booleans[name] = value
	return ta
}

func (ta *TagApplier) Str(name, value string) *TagApplier {
	ta.keys[name] = strDataType
	ta.strings[name] = value
	return ta
}

func (ta *TagApplier) Int(name string, value int) *TagApplier {
	ta.keys[name] = intDataType
	ta.integers[name] = value
	return ta
}

func (ta *TagApplier) Float(name string, value float64) *TagApplier {
	ta.keys[name] = floatDataType
	ta.floats[name] = value
	return ta
}

func (ta *TagApplier) Map(m M) *TagApplier {
	for n, v := range m {
		switch typedValue := v.(type) {
		case string:
			ta.strings[n] = typedValue
		case bool:
			ta.booleans[n] = typedValue
		case int:
			ta.integers[n] = typedValue
		default:
			ta.err = errors.Wrapf(ErrInvalidTagType, "%T", v)
		}
	}

	return ta
}

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

func IntTag(name string, value int) Tagger {
	return func(t *Tags) {
		t.integers[name] = value
	}
}

func FloatTag(name string, value float64) Tagger {
	return func(t *Tags) {
		t.floats[name] = value
	}
}

type TagApplier struct {
	err      error
	keys     map[string]indexType
	booleans map[string]bool
	floats   map[string]float64
	integers map[string]int
	strings  map[string]string
}

func (ta *TagApplier) applyTo(e *entry) {
	if e.tags == nil {
		e.tags = newTags()
	}

	for n, v := range ta.booleans {
		e.tags.booleans[n] = v
	}

	for n, v := range ta.strings {
		e.tags.strings[n] = v
	}

	for n, v := range ta.integers {
		e.tags.integers[n] = v
	}

	for n, v := range ta.floats {
		e.tags.floats[n] = v
	}
}

type Tags struct {
	booleans map[string]bool
	floats   map[string]float64
	integers map[string]int
	strings  map[string]string
}

func (t *Tags) applyTo(e *entry) {
	e.tags = t
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

func (t *Tags) Floats() map[string]float64 {
	return t.floats
}

func (t *Tags) GetString(name string) string {
	return t.strings[name]
}

func (t *Tags) GetBool(name string) bool {
	return t.booleans[name]
}

func (t *Tags) GetInt(name string) int {
	return t.integers[name]
}

type bTag struct {
	name  string
	value bool
}

type strTag struct {
	name  string
	value string
}

type intTag struct {
	value   int
	entries []*entry
}

type entityContainer interface {
	getEntry(key string) *entry
	getEntries() map[string]*entry
}

type floatTag struct {
	value   float64
	entries map[string]*entry
}

func (ft *floatTag) getEntry(key string) *entry {
	return ft.entries[key]
}

func byStrings(a, b interface{}) bool {
	i1, i2 := a.(*strTag), b.(*strTag)
	return i1.value < i2.value
}
