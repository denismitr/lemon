package lemon

import (
	"github.com/google/btree"
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

func (si stringIndex) removeEntry(tagName, v string, ent *entry) bool {
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

func (bi boolIndex) removeEntry(tagName string, v bool, ent *entry) bool {
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

func descendLessOrEqual(
	tr *btr.BTree,
	pivot interface{},
	iter func(ent *entry) bool,
) {
	tr.Descend(pivot, iter)
}