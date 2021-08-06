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
		case float64:
			ta.floats[n] = typedValue
		default:
			ta.err = errors.Wrapf(ErrInvalidTagType, "%T", v)
		}
	}

	return ta
}

type Tagger func(t *tags)

func BoolTag(name string, value bool) Tagger {
	return func(t *tags) {
		t.booleans[name] = value
	}
}

func StrTag(name string, value string) Tagger {
	return func(t *tags) {
		t.strings[name] = value
	}
}

func IntTag(name string, value int) Tagger {
	return func(t *tags) {
		t.integers[name] = value
	}
}

func FloatTag(name string, value float64) Tagger {
	return func(t *tags) {
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

type tags struct {
	booleans map[string]bool
	floats   map[string]float64
	integers map[string]int
	strings  map[string]string
}

func (t *tags) applyTo(e *entry) {
	e.tags = t
}

func newTags() *tags {
	return &tags{
		booleans: make(map[string]bool),
		floats:   make(map[string]float64),
		integers: make(map[string]int),
		strings:  make(map[string]string),
	}
}

type entries map[string]*entry

func (e entries) setEntry(ent *entry) {
	e[ent.key.String()] = ent
}

func (e entries) getEntry(key string) *entry {
	return e[key]
}

func (e entries) getEntries() map[string]*entry {
	return e
}

type boolTag struct {
	value bool
	entries
}

func newBoolTag(value bool) *boolTag {
	return &boolTag{
		value: value,
		entries: make(entries),
	}
}

type strTag struct {
	value string
	entries
}

func newStrTag(value string) *strTag {
	return &strTag{
		value: value,
		entries: make(entries),
	}
}

type intTag struct {
	value   int
	entries
}

func newIntTag(value int) *intTag {
	return &intTag{
		value: value,
		entries: make(entries),
	}
}

type entryContainer interface {
	setEntry(ent *entry)
	getEntry(key string) *entry
	getEntries() map[string]*entry
}

type floatTag struct {
	value   float64
	entries
}

func newFloatTag(value float64) *floatTag {
	return &floatTag{
		value: value,
		entries: make(entries),
	}
}