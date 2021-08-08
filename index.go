package lemon

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/tidwall/btree"
)

var ErrInvalidIndexType = errors.New("invalid index type")

type indexType uint8

const invalidTagTypeConversion = "invalid tag type conversion"

const (
	floatDataType = iota
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

func (ti *tagIndex) keys() int {
	return len(ti.data)
}

func (ti *tagIndex) getEntriesFor(name string, v interface{}) map[string]*entry {
	idx := ti.data[name]
	if idx == nil {
		return nil
	}

	switch typedValue := v.(type) {
	case float64:
		item := idx.btr.Get(&floatTag{value: typedValue})
		if item == nil {
			return nil
		}
		return item.(entryContainer).getEntries()
	case int:
		item := idx.btr.Get(&intTag{value: typedValue})
		if item == nil {
			return nil
		}
		return item.(entryContainer).getEntries()
	case bool:
		item := idx.btr.Get(&boolTag{value: typedValue})
		if item == nil {
			return nil
		}
		return item.(entryContainer).getEntries()
	case string:
		item := idx.btr.Get(&strTag{value: typedValue})
		if item == nil {
			return nil
		}
		return item.(entryContainer).getEntries()
	}

	return nil
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

func (ti *tagIndex) removeEntryByTag(name string, v interface{}, ent *entry) {
	idx := ti.data[name]
	if idx == nil {
		return
	}

	var item interface{}
	switch typedValue := v.(type) {
	case float64:
		item = idx.btr.Get(&floatTag{value: typedValue})
		if item == nil {
			return
		}
		tag, ok := item.(*floatTag)
		if !ok {
			panic(invalidTagTypeConversion)
		}
		if tag.entries[ent.key.String()] != nil {
			delete(tag.entries, ent.key.String())
		}
	}
}

func resolveIndexIfNotExists(idx *index, dataType indexType, less func(a, b interface{}) bool) (*index, error) {
	if idx == nil {
		idx = &index{
			dt:  dataType,
			btr: btree.New(less),
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

func (ti *tagIndex) getEqualTagEntities(idx *index, entry interface{}) map[string]*entry {
	found := idx.btr.Get(entry)
	if found == nil {
		return nil
	}

	tag, ok := found.(entryContainer)
	if !ok {
		panic("not an entity container") // fixme
	}

	return tag.getEntries()
}

func (ti *tagIndex) filterEntities(tagKey tagKey, v interface{}, ft *filterEntries) {
	idx := ti.data[tagKey.name]
	if idx == nil {
		return
	}

	var item interface{}
	switch typedValue := v.(type) {
	case float64:
		item = newFloatTag(typedValue)
	case int:
		item = newIntTag(typedValue)
	case string:
		item = newStrTag(typedValue)
	case bool:
		item = newBoolTag(typedValue)
	default:
		panic(fmt.Sprintf("Type %T is not supported", v))
	}

	switch tagKey.comp {
	case equal:
		for _, ent := range ti.getEqualTagEntities(idx, item) {
			ft.add(ent)
		}
	case greaterThan:
		idx.btr.Ascend(item, func(found interface{}) bool {
			if found == nil {
				return true
			}

			tag, ok := found.(entryContainer)
			if !ok {
				panic(fmt.Sprintf("how can intIndex item not be of type *intTag?"))
			}

			if tag.getEntries() == nil {
				return true
			}

			for _, ent := range tag.getEntries() {
				ft.add(ent)
			}

			return true
		})
	}
}

func lt(tr *btree.BTree, a, b interface{}) bool { return tr.Less(a, b) }
func gt(tr *btree.BTree, a, b interface{}) bool { return tr.Less(b, a) }

func ascendRange(
	btr *btree.BTree,
	greaterOrEqual interface{},
	lessThan interface{},
	iter func(item interface{}) bool,
) {
	btr.Ascend(greaterOrEqual, func(item interface{}) bool {
		return lt(btr, item, lessThan) && iter(item)
	})
}

func descendRange(
	btr *btree.BTree,
	greaterOrEqual interface{},
	lessThan interface{},
	iter func(item interface{}) bool,
) {
	btr.Descend(lessThan, func(item interface{}) bool {
		return gt(btr, item, greaterOrEqual) && iter(item)
	})
}

func descendGreaterThan(
	btr *btree.BTree,
	greaterOrEqual interface{},
	iter func(item interface{}) bool,
) {
	btr.Descend(nil, func(item interface{}) bool {
		return gt(btr, item, greaterOrEqual) && iter(item)
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