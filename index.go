package lemon

import (
	btr "github.com/tidwall/btree"
)


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

func (si stringIndex) findEntries(k, v string) []*entry {
	return si[k][v]
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
	if ent.tags == nil || ent.tags.booleans == nil {
		return
	}

	for _, sTag := range ent.tags.strings {
		if si[sTag.name] == nil {
			continue
		}

		for i, e := range si[sTag.name][sTag.value] {
			if e.key.Equal(&ent.key) {
				si[sTag.name][sTag.value] = append(si[sTag.name][sTag.value][:i], si[sTag.name][sTag.value][i+1:]...)
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

func (bi boolIndex) findEntries(name string, v bool) []*entry {
	return bi[name][v]
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
	if ent.tags == nil || ent.tags.booleans == nil {
		return
	}

	for _, bTag := range ent.tags.booleans {
		if bi[bTag.name] == nil {
			continue
		}

		for i, e := range bi[bTag.name][bTag.value] {
			if e.key.Equal(&ent.key) {
				bi[bTag.name][bTag.value] = append(bi[bTag.name][bTag.value][:i], bi[bTag.name][bTag.value][i+1:]...)
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
		// todo: check item type
		return gt(btr, item, greaterOrEqual) && iter(item)
	})
}