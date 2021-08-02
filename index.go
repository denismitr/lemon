package lemon

import (
	btr "github.com/tidwall/btree"
)

type intIndex map[string]*btr.BTree

func newIntIndex() intIndex {
	return make(map[string]*btr.BTree)
}

func (ii intIndex) add(name string, v int, ent *entry) {
	if ii[name] == nil {
		ii[name] = btr.New(byIntegers)
		ii[name].Set(&intTag{value: v, entries: []*entry{ent}})
		return
	}

	item := ii[name].Get(&intTag{value: v})
	if item == nil {
		ii[name].Set(&intTag{value: v, entries: []*entry{ent}})
	} else {
		tag, ok := item.(*intTag)
		if !ok {
			panic("how can item not be of type intTag?")
		}
		tag.entries = append(tag.entries, ent)
	}
}

func (ii intIndex) findEntries(name string, v int) []*entry {
	if ii[name] == nil {
		return nil
	}

	item := ii[name].Get(&intTag{value: v})
	if item == nil {
		return nil
	}

	tag, ok := item.(*intTag)
	if !ok {
		panic("how can item not be of type intTag?")
	}

	return tag.entries
}

func (ii intIndex) removeEntryByTag(name string, v int, ent *entry) bool {
	if ii[name] == nil {
		return false
	}

	item := ii[name].Get(&intTag{value: v})
	if item != nil {
		tag, ok := item.(*intTag)
		if !ok {
			panic("how can item not be of type intTag?")
		}

		pos := 0
		for i := range tag.entries {
			if tag.entries[i].key.Equal(&ent.key) {
				pos = i
				break
			}
		}

		copy(tag.entries[pos:], tag.entries[pos+1:])
		tag.entries[len(tag.entries) - 1] = nil
		tag.entries = tag.entries[:len(tag.entries) - 1]
	}

	return false
}

func (ii intIndex) removeEntry(ent *entry) {
	if ent.tags == nil  {
		return
	}

	for name, value := range ent.tags.integers {
		if ii[name] == nil {
			continue
		}

		item := ii[name].Get(value)
		if item == nil {
			continue
		}

		tag, ok := item.(*intTag)
		if !ok {
			panic("how can item not be of type intTag?")
		}

		pos := 0
		for i := range tag.entries {
			if tag.entries[i].key.Equal(&ent.key) {
				pos = i
				break
			}
		}

		copy(tag.entries[pos:], tag.entries[pos+1:])
		tag.entries[len(tag.entries) - 1] = nil
		tag.entries = tag.entries[:len(tag.entries) - 1]
	}
}

type stringIndex map[string]map[string][]*entry

func newStringIndex() stringIndex {
	return make(map[string]map[string][]*entry)
}

func (si stringIndex) add(tagName, v string, ent *entry) {
	if si[tagName] == nil {
		si[tagName] = make(map[string][]*entry)
	}

	si[tagName][v] = append(si[tagName][v], ent)
}

func (si stringIndex) removeEntryByTag(tagName, v string, ent *entry) bool {
	if si[tagName] == nil {
		return false
	}

	for i, e := range si[tagName][v] {
		if e.key.Equal(&ent.key) {
			si[tagName][v] = append(si[tagName][v][:i], si[tagName][v][i+1:]...)
			return true
		}
	}

	return false
}

func (si stringIndex) removeEntry(ent *entry) {
	if ent.tags == nil  {
		return
	}

	for n, v := range ent.tags.strings {
		if si[n] == nil {
			continue
		}

		for i, e := range si[n][v] {
			if e.key.Equal(&ent.key) {
				si[n][v] = append(si[n][v][:i], si[n][v][i+1:]...)
			}
		}
	}
}

type boolIndex map[string]map[bool][]*entry

func newBoolIndex() boolIndex {
	return make(map[string]map[bool][]*entry)
}

func (bi boolIndex) add(tagName string, v bool, ent *entry) {
	if bi[tagName] == nil {
		bi[tagName] = make(map[bool][]*entry)
	}

	bi[tagName][v] = append(bi[tagName][v], ent)
}

func (bi boolIndex) removeEntryByTag(tagName string, v bool, ent *entry) bool {
	if bi[tagName] == nil {
		return false
	}

	for i, e := range bi[tagName][v] {
		if e.key.Equal(&ent.key) {
			bi[tagName][v] = append(bi[tagName][v][:i], bi[tagName][v][i+1:]...)
			return true
		}
	}

	return false
}

func (bi boolIndex) removeEntry(ent *entry) {
	if ent.tags == nil {
		return
	}

	for n, v := range ent.tags.booleans {
		if bi[n] == nil {
			continue
		}

		for i, e := range bi[n][v] {
			if e.key.Equal(&ent.key) {
				bi[n][v] = append(bi[n][v][:i], bi[n][v][i+1:]...)
			}
		}
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