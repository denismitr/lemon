package lemon

import (
	"fmt"
	"github.com/pkg/errors"
	btr "github.com/tidwall/btree"
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
	btr *btr.BTree
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
		if ti.data[name] == nil {
			continue
		}

		idx := ti.data[name]
		item := idx.btr.Get(&floatTag{value: value})
		if item == nil {
			continue
		}

		tag := item.(*floatTag)
		if tag.entries[ent.key.String()] == nil {
			continue
		}
		delete(tag.entries, ent.key.String())
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
			dt: dataType,
			btr: btr.New(less),
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

func (ti *tagIndex) getEqualTagEntities(idx *index, item interface{}) map[string]*entry {
	found := idx.btr.Get(item)
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

func lt(tr *btr.BTree, a, b interface{}) bool { return tr.Less(a, b) }
func gt(tr *btr.BTree, a, b interface{}) bool { return tr.Less(b, a) }

func ascendRange(
	btr *btr.BTree,
	greaterOrEqual interface{},
	lessThan interface{},
	iter func(item interface{}) bool,
) {
	btr.Ascend(greaterOrEqual, func(item interface{}) bool {
		return lt(btr, item, lessThan) && iter(item)
	})
}

func descendRange(
	btr *btr.BTree,
	greaterOrEqual interface{},
	lessThan interface{},
	iter func(item interface{}) bool,
) {
	btr.Descend(lessThan, func(item interface{}) bool {
		return gt(btr, item, greaterOrEqual) && iter(item)
	})
}

func descendGreaterThan(
	btr *btr.BTree,
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
	if i1.value == false && i2.value == true {
		return true
	}

	return false
}