package lemon

import (
	btr "github.com/tidwall/btree"
	"strings"
)

type PK string

func newPK(k string) PK {
	return PK(k)
}

func (pk PK) String() string {
	return string(pk)
}

func (pk PK) Less(other PK) bool {
	ourSegments := strings.Split(string(pk), ":")
	otherSegments := strings.Split(string(other), ":")
	l := smallestSegmentLen(ourSegments, otherSegments)

	prevEq := false
	for i := 0; i < l; i++ {
		// try to compare as ints
		bothInts, a, b := convertToINTs(ourSegments[i], otherSegments[i])
		if bothInts {
			if a != b {
				return a < b
			} else {
				prevEq = true
				continue
			}
		}

		// try to compare as strings
		if ourSegments[i] != otherSegments[i]  {
			return ourSegments[i] < otherSegments[i]
		} else {
			prevEq = ourSegments[i] == otherSegments[i]
		}
	}

	return prevEq && len(otherSegments) > len(ourSegments)
}

func byPrimaryKeys(a, b interface{}) bool {
	i1, i2 := a.(*entry), b.(*entry)
	return i1.key.Less(i2.key) // todo: call PK comparison function
}

type tagIndex struct {
	btr *btr.BTree
	name string
	typ TagType
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

func (si stringIndex) findEntries(k, v string) []*entry {
	return si[k][v]
}

func (si stringIndex) removeEntryByTag(tagName, v string, ent *entry) bool {
	if si[tagName] == nil {
		return false
	}

	for i, e := range si[tagName][v] {
		if e.key == ent.key {
			si[tagName][v] = append(si[tagName][v][:i], si[tagName][v][i+1:]...)
			return true
		}
	}

	return false
}

func (si stringIndex) removeEntry(ent *entry) {
	if ent.tags == nil || ent.tags.Booleans == nil {
		return
	}

	for _, sTag := range ent.tags.Strings {
		if si[sTag.Name] == nil {
			continue
		}

		for i, e := range si[sTag.Name][sTag.Value] {
			if e.key == ent.key {
				si[sTag.Name][sTag.Value] = append(si[sTag.Name][sTag.Value][:i], si[sTag.Name][sTag.Value][i+1:]...)
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
		if e.key == ent.key {
			bi[tagName][v] = append(bi[tagName][v][:i], bi[tagName][v][i+1:]...)
			return true
		}
	}

	return false
}

func (bi boolIndex) removeEntry(ent *entry) {
	if ent.tags == nil || ent.tags.Booleans == nil {
		return
	}

	for _, bTag := range ent.tags.Booleans {
		if bi[bTag.Name] == nil {
			continue
		}

		for i, e := range bi[bTag.Name][bTag.Value] {
			if e.key == ent.key {
				bi[bTag.Name][bTag.Value] = append(bi[bTag.Name][bTag.Value][:i], bi[bTag.Name][bTag.Value][i+1:]...)
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
	iter func(ent *entry) bool,
) {
	btr.Ascend(greaterOrEqual, func(item interface{}) bool {
		// todo: check item type
		return lt(btr, item, lessThan) && iter(item.(*entry))
	})
}

func descendRange(
	btr *btr.BTree,
	greaterOrEqual interface{},
	lessThan interface{},
	iter func(ent *entry) bool,
) {
	btr.Descend(lessThan, func(item interface{}) bool {
		// todo: check item type
		return gt(btr, item, greaterOrEqual) && iter(item.(*entry))
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