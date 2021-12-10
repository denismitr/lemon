package lemon

import (
	"github.com/pkg/errors"
	"time"
)

type ContentTypeIdentifier string

const (
	CreatedAt   = "_ca"
	UpdatedAt   = "_ua"
	ContentType = "_ct"

	JSON    ContentTypeIdentifier = "json"
	String  ContentTypeIdentifier = "str"
	Bytes   ContentTypeIdentifier = "bytes"
	Integer ContentTypeIdentifier = "int"
	Bool    ContentTypeIdentifier = "bool"
)

var ErrTagNameConflict = errors.New("tag name conflict")
var ErrTagNameNotFound = errors.New("tag name not found")

type MetaApplier interface {
	applyTo(e *entry) error
}

func WithTimestamps() MetaApplier {
	return M{
		CreatedAt: int(time.Now().UnixMilli()),
		UpdatedAt: int(time.Now().UnixMilli()),
	}
}

func WithContentType(ct ContentTypeIdentifier) MetaApplier {
	return M{
		ContentType: string(ct),
	}
}

func WithTags() *TagApplier {
	return &TagApplier{
		tags: newTags(),
	}
}

func (ta *TagApplier) append(name string, value interface{}, dt indexType) *TagApplier {
	if _, exists := ta.tags[name]; exists {
		ta.err = errors.Wrapf(ErrTagNameConflict, "tag with name %s already exists", name)
	} else {
		tg := &tag{dt: dt, data: value}
		ta.tags[name] = tg
	}

	return ta
}

func (ta *TagApplier) Bool(name string, value bool) *TagApplier {
	return ta.append(name, value, boolDataType)
}

func (ta *TagApplier) Str(name, value string) *TagApplier {
	return ta.append(name, value, strDataType)
}

func (ta *TagApplier) Int(name string, value int) *TagApplier {
	return ta.append(name, value, intDataType)
}

func (ta *TagApplier) Float(name string, value float64) *TagApplier {
	return ta.append(name, value, floatDataType)
}

func (ta *TagApplier) Timestamps() *TagApplier {
	ta.append(CreatedAt, int(time.Now().UnixMilli()), intDataType)
	ta.append(UpdatedAt, int(time.Now().UnixMilli()), intDataType)
	return ta
}

func (ta *TagApplier) ContentType(ct string) *TagApplier {
	return ta.append(ContentType, ct, strDataType)
}

func (ta *TagApplier) Map(m M) *TagApplier {
	for n, v := range m {
		switch v.(type) {
		case string:
			ta.append(n, v, strDataType)
		case bool:
			ta.append(n, v, boolDataType)
		case int:
			ta.append(n, v, intDataType)
		case float64:
			ta.append(n, v, floatDataType)
		default:
			ta.err = errors.Wrapf(ErrInvalidTagType, "%T", v)
		}
	}

	return ta
}

type Tagger func(t tags)

func boolTagger(name string, value bool) Tagger {
	return func(t tags) {
		t[name] = &tag{dt: boolDataType, data: value}
	}
}

func strTagger(name, value string) Tagger {
	return func(t tags) {
		t[name] = &tag{dt: strDataType, data: value}
	}
}

func intTagger(name string, value int) Tagger {
	return func(t tags) {
		t[name] = &tag{dt: intDataType, data: value}
	}
}

func floatTagger(name string, value float64) Tagger {
	return func(t tags) {
		t[name] = &tag{dt: floatDataType, data: value}
	}
}

type TagApplier struct {
	err  error
	tags tags
}

func (ta *TagApplier) applyTo(e *entry) error {
	e.tags = ta.tags
	return nil
}

type tag struct {
	dt   indexType
	data interface{}
}

type tags map[string]*tag

func (t tags) applyTo(e *entry) {
	e.tags = t
}

func (t tags) set(name string, v interface{}) {
	_, ok := t[name]
	if ok {
		delete(t, name)
	}

	newTag := &tag{}
	switch v.(type) {
	case int:
		newTag.dt = intDataType
	case bool:
		newTag.dt = boolDataType
	case string:
		newTag.dt = strDataType
	case float64:
		newTag.dt = floatDataType
	}

	newTag.data = v
	t[name] = newTag
}

func (t tags) removeByName(name string) {
	_, ok := t[name]
	if ok {
		delete(t, name)
	}
}

func (t tags) count() int {
	return len(t)
}

func (t tags) exists(name string) bool {
	_, ok := t[name]
	return ok
}

func (t tags) asMap() M {
	m := make(M, len(t))
	for k, tg := range t {
		m[k] = tg.data
	}
	return m
}

func newTags() tags {
	return make(tags)
}

func newTagsFromMap(m M) (tags, error) {
	tt := newTags()

	for n, v := range m {
		if _, ok := tt[n]; ok {
			return nil, errors.Wrapf(ErrTagNameConflict, "tag name %s already taken", n)
		}

		newTag := &tag{data: v}
		switch v.(type) {
		case int:
			newTag.dt = intDataType
		case string:
			newTag.dt = strDataType
		case bool:
			newTag.dt = boolDataType
		case float64:
			newTag.dt = floatDataType
		}

		tt[n] = newTag
	}

	return tt, nil
}

type entries map[string]*entry

func (e entries) setEntry(ent *entry) {
	e[ent.key.String()] = ent
}

func (e entries) getEntry(key string) *entry {
	return e[key]
}

func (e entries) hasEntry(key string) bool {
	return e[key] != nil
}

func (e entries) getEntries() map[string]*entry {
	return e
}

func (e entries) remove(key string) {
	delete(e, key)
}

type boolTag struct {
	value bool
	entries
}

func newBoolTag(value bool) *boolTag {
	return &boolTag{
		value:   value,
		entries: make(entries),
	}
}

type strTag struct {
	value string
	entries
}

func newStrTag(value string) *strTag {
	return &strTag{
		value:   value,
		entries: make(entries),
	}
}

type intTag struct {
	value int
	entries
}

func newIntTag(value int) *intTag {
	return &intTag{
		value:   value,
		entries: make(entries),
	}
}

type entryContainer interface {
	setEntry(ent *entry)
	getEntry(key string) *entry
	hasEntry(key string) bool
	getEntries() map[string]*entry
	remove(key string)
}

type floatTag struct {
	value float64
	entries
}

func newFloatTag(value float64) *floatTag {
	return &floatTag{
		value:   value,
		entries: make(entries),
	}
}
