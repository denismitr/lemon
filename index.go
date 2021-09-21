package lemon

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/tidwall/btree"
)

var ErrInvalidIndexType = errors.New("invalid index type")

type indexType uint8

const (
	nilDataType indexType = iota
	floatDataType
	intDataType
	strDataType
	boolDataType
)

type index struct {
	dt  indexType
	btr *btree.BTree
}

type tagIndex struct {
	data map[string]*index
}

func newTagIndex() *tagIndex {
	return &tagIndex{
		data: make(map[string]*index),
	}
}

func (ti *tagIndex) removeEntry(ent *entry) {
	if ent.tags == nil {
		return
	}

	for name, value := range ent.tags.floats {
		ti.remove(name, floatDataType, ent, &floatTag{value: value})
	}

	for name, value := range ent.tags.integers {
		ti.remove(name, intDataType, ent, &intTag{value: value})
	}

	for name, value := range ent.tags.strings {
		ti.remove(name, strDataType, ent, &strTag{value: value})
	}

	for name, value := range ent.tags.booleans {
		ti.remove(name, boolDataType, ent, &boolTag{value: value})
	}
}

func (ti *tagIndex) remove(name string, dt indexType, ent *entry, lookup interface{}) {
	idx := ti.data[name]
	if idx == nil || idx.dt != dt {
		return
	}

	item := idx.btr.Get(lookup)
	if item == nil {
		return
	}

	entryCollection := item.(entryContainer)
	if !entryCollection.hasEntry(ent.key.String()) {
		return
	}

	entryCollection.remove(ent.key.String())
	if len(entryCollection.getEntries()) == 0 {
		idx.btr.Delete(item)
	}

	if idx.btr.Len() == 0 {
		delete(ti.data, name)
	}
}

func (ti *tagIndex) mustRemoveEntryByNameAndValue(name string, v interface{}, ent *entry) error {
	idx := ti.data[name]
	if idx == nil {
		return errors.Wrapf(ErrTagNameNotFound, "name %s is not in any db index", name)
	}

	switch typedValue := v.(type) {
	case float64:
		ti.remove(name, floatDataType, ent, &floatTag{value: typedValue})
	case int:
		ti.remove(name, intDataType, ent, &intTag{value: typedValue})
	case bool:
		ti.remove(name, boolDataType, ent, &boolTag{value: typedValue})
	case string:
		ti.remove(name, strDataType, ent, &strTag{value: typedValue})
	default:
		panic(fmt.Sprintf("Invalid type %T", v))
	}

	return nil
}

func (ti *tagIndex) removeEntryByNameAndType(name string, dt indexType, ent *entry) {
	idx := ti.data[name]
	if idx == nil {
		return
	}

	switch dt {
	case intDataType:
		for n, v := range ent.tags.integers {
			if n == name {
				ti.remove(name, intDataType, ent, &intTag{value: v})
			}
		}
	case floatDataType:
		for n, v := range ent.tags.floats {
			if n == name {
				ti.remove(name, floatDataType, ent, &floatTag{value: v})
			}
		}
	case strDataType:
		for n, v := range ent.tags.strings {
			if n == name {
				ti.remove(name, strDataType, ent, &strTag{value: v})
			}
		}
	case boolDataType:
		for n, v := range ent.tags.booleans {
			if n == name {
				ti.remove(name, boolDataType, ent, &boolTag{value: v})
			}
		}
	}
}

func resolveIndexIfNotExists(idx *index, dataType indexType, less func(a, b interface{}) bool) (*index, error) {
	if idx == nil {
		idx = &index{
			dt:  dataType,
			btr: btree.NewNonConcurrent(less),
		}
	}

	if idx.dt != dataType {
		return nil, ErrInvalidIndexType
	}

	return idx, nil
}

func (ti *tagIndex) add(name string, value interface{}, ent *entry) error {
	idx := ti.data[name]

	var tag entryContainer
	var dataType indexType
	var err error

	switch typedValue := value.(type) {
	case float64:
		dataType = floatDataType
		idx, err = resolveIndexIfNotExists(idx, dataType, byFloats)
		tag = newFloatTag(typedValue)
	case int:
		dataType = intDataType
		idx, err = resolveIndexIfNotExists(idx, dataType, byIntegers)
		tag = newIntTag(typedValue)
	case string:
		dataType = strDataType
		idx, err = resolveIndexIfNotExists(idx, dataType, byStrings)
		tag = newStrTag(typedValue)
	case bool:
		dataType = boolDataType
		idx, err = resolveIndexIfNotExists(idx, dataType, byBooleans)
		tag = newBoolTag(typedValue)
	default:
		return errors.Errorf("Type %T is invalid", value)
	}

	if err != nil {
		return err
	}

	ti.data[name] = idx

	existing := idx.btr.Get(tag)
	if existing != nil {
		c, ok := existing.(entryContainer)
		if !ok {
			panic("invalid type casting") // fixme
		}

		c.setEntry(ent)
	} else {
		tag.setEntry(ent)
		idx.btr.Set(tag)
	}

	return nil
}

type tagFilter struct {
	key tagKey
	m matcher
	idx *index
	tag interface{}
}

func createFloatTagFilter(ti *tagIndex, key tagKey, v float64) (*tagFilter, error) {
	idx := ti.data[key.name]
	if idx == nil {
		return nil, errors.Wrapf(ErrTagNameNotFound, "tag name %s", key.name)
	}

	if idx.dt != floatDataType {
		return nil, errors.Wrapf(ErrInvalidTagType, "tag with name %s is not of type float", key.name)
	}

	f := tagFilter{}
	f.idx = idx
	f.key = key
	f.tag = newFloatTag(v)
	f.m = key.getFloatMatcher(v)

	return &f, nil
}

func createBoolTagFilter(ti *tagIndex, key tagKey, v bool) (*tagFilter, error) {
	idx := ti.data[key.name]
	if idx == nil {
		return nil, errors.Wrapf(ErrTagNameNotFound, "tag name %s", key.name)
	}

	if idx.dt != boolDataType {
		return nil, errors.Wrapf(ErrInvalidTagType, "tag with name %s is not of type boolean", key.name)
	}

	f := tagFilter{}
	f.idx = idx
	f.key = key
	f.tag = newBoolTag(v)
	f.m = key.getBoolMatcher(v)

	return &f, nil
}

func createIntegerTagFilter(ti *tagIndex, key tagKey, v int) (*tagFilter, error) {
	idx := ti.data[key.name]
	if idx == nil {
		return nil, errors.Wrapf(ErrTagNameNotFound, "tag name %s", key.name)
	}

	if idx.dt != intDataType {
		return nil, errors.Wrapf(ErrInvalidTagType, "tag with name %s is not of type integer", key.name)
	}

	f := tagFilter{}
	f.idx = idx
	f.key = key
	f.tag = newIntTag(v)
	f.m = key.getIntMatcher(v)

	return &f, nil
}

func createStringTagFilter(ti *tagIndex, key tagKey, v string) (*tagFilter, error) {
	idx := ti.data[key.name]
	if idx == nil {
		return nil, errors.Wrapf(ErrTagNameNotFound, "tag name %s", key.name)
	}

	if idx.dt != strDataType {
		return nil, errors.Wrapf(ErrInvalidTagType, "tag with name %s is not of type string", key.name)
	}

	f := tagFilter{}
	f.idx = idx
	f.key = key
	f.tag = newStrTag(v)
	f.m = key.getStringMatcher(v)

	return &f, nil
}

func (ti *tagIndex) filterEntities(tf *tagFilter, fes *filterEntriesSink) {
	scanIter := func(found interface{}) bool {
		if found == nil {
			return true
		}

		tag, ok := found.(entryContainer)
		if !ok {
			panic("how can intIndex item not be of type *intTag?")
		}

		if tag.getEntries() == nil {
			return true
		}

		for _, ent := range tag.getEntries() {
			if tf.m(ent) {
				fes.add(ent)
			}
		}

		return true
	}

	switch tf.key.comp {
	case equal:
		found := tf.idx.btr.Get(tf.tag)
		if found == nil {
			return
		}

		tag, ok := found.(entryContainer)
		if !ok {
			panic("not an entity container") // fixme
		}

		for _, ent := range tag.getEntries() {
			fes.add(ent)
		}
	case greaterThan:
		tf.idx.btr.Ascend(tf.tag, scanIter)
	case lessThan:
		tf.idx.btr.Descend(tf.tag, scanIter)
	}
}

func lt(tr *btree.BTree, a, b interface{}) bool { return tr.Less(a, b) }
func eq(a, b interface{}) bool { return a.(*entry).key.Equal(&b.(*entry).key) }
func gt(tr *btree.BTree, a, b interface{}) bool { return tr.Less(b, a) }
func gte(tr *btree.BTree, a, b interface{}) bool { return tr.Less(b, a) || a.(*entry).key.Equal(&b.(*entry).key) }

func ascendRange(
	btr *btree.BTree,
	greaterOrEqual interface{},
	lessThan interface{},
	iter func(item interface{}) bool,
) {
	btr.Ascend(greaterOrEqual, func(item interface{}) bool {
		return (lt(btr, item, lessThan) || eq(item, lessThan)) && iter(item)
	})
}

func descendRange(
	btr *btree.BTree,
	greaterOrEqual interface{},
	lessThan interface{},
	iter func(item interface{}) bool,
) {
	btr.Descend(lessThan, func(item interface{}) bool {
		return gte(btr, item, greaterOrEqual) && iter(item)
	})
}

func descendGreaterThan(
	btr *btree.BTree,
	greaterOrEqual interface{},
	iter func(item interface{}) bool,
) {
	btr.Descend(nil, func(item interface{}) bool {
		return gte(btr, item, greaterOrEqual) && iter(item)
	})
}

func byIntegers(a, b interface{}) bool {
	i1, i2 := a.(*intTag), b.(*intTag)
	return i1.value < i2.value
}

func byFloats(a, b interface{}) bool {
	i1, i2 := a.(*floatTag), b.(*floatTag)
	return i1.value < i2.value
}

func byStrings(a, b interface{}) bool {
	i1, i2 := a.(*strTag), b.(*strTag)
	return i1.value < i2.value
}

func byBooleans(a, b interface{}) bool {
	i1, i2 := a.(*boolTag), b.(*boolTag)
	if !i1.value && i2.value {
		return true
	}

	return false
}