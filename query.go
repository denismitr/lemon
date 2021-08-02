package lemon

import (
	"strings"
	"sync"
)

type M map[string]interface{}

type KeyRange struct {
	From, To string
}

type comparator int8

const (
	equal comparator = iota
	greaterThan
)

type tagKey struct {
	name string
	comp comparator
}

type QueryTags struct {
	booleans map[tagKey]bool
	strings  map[tagKey]string
	integers map[tagKey]int
}

func QT() *QueryTags {
	return &QueryTags{
		booleans: make(map[tagKey]bool),
		strings: make(map[tagKey]string),
		integers: make(map[tagKey]int),
	}
}

func (qt *QueryTags) BoolTagEq(name string, value bool) *QueryTags {
	qt.booleans[tagKey{name: name, comp: equal}] = value
	return qt
}

func (qt *QueryTags) StrTagEq(name string, value string) *QueryTags {
	qt.strings[tagKey{name: name, comp: equal}] = value
	return qt
}

func (qt *QueryTags) IntTagEq(name string, value int) *QueryTags {
	qt.integers[tagKey{name: name, comp: equal}] = value
	return qt
}

func (qt *QueryTags) IntTagGt(name string, value int) *QueryTags {
	qt.integers[tagKey{name: name, comp: greaterThan}] = value
	return qt
}

type Order string

const (
	AscOrder  Order = "ASC"
	DescOrder Order = "DESC"
)

type queryOptions struct {
	order    Order
	keyRange *KeyRange
	prefix   string
	patterns []string
	allTags  *QueryTags
}

func (fo *queryOptions) Match(patten string) *queryOptions {
	fo.patterns = strings.Split(patten, ":")
	return fo
}

func (fo *queryOptions) Order(o Order) *queryOptions {
	fo.order = o
	return fo
}

func (fo *queryOptions) KeyRange(from, to string) *queryOptions {
	fo.keyRange = &KeyRange{From: from, To: to}
	return fo
}

func (fo *queryOptions) Prefix(p string) *queryOptions {
	fo.prefix = p
	return fo
}

func (fo *queryOptions) HasAllTags(qt *QueryTags) *queryOptions {
	fo.allTags = qt
	return fo
}

func (fo *queryOptions) matchTags(e *entry) bool {
	if fo.allTags == nil {
		return true
	}

	if e.tags == nil {
		return false
	}

	matchesExpected := 0
	actualMatches := 0
	for k, v := range fo.allTags.booleans {
		matchesExpected++
		switch k.comp {
		case equal:
			if e.tags.booleans[k.name] == v {
				actualMatches++
			}
		}
	}

	for k, v := range fo.allTags.strings {
		matchesExpected++
		switch k.comp {
		case equal:
			if e.tags.strings[k.name] == v {
				actualMatches++
			}
		}
	}

	for k, v := range fo.allTags.integers {
		matchesExpected++
		switch k.comp {
		case equal:
			if e.tags.integers[k.name] == v {
				actualMatches++
			}
		case greaterThan:
			if e.tags.integers[k.name] > v {
				actualMatches++
			}
		}
	}

	return matchesExpected == actualMatches
}

func Q() *queryOptions {
	return &queryOptions{order: AscOrder}
}

type filterEntries struct {
	sync.RWMutex
	patterns []string
	entries  map[string]*entry
}

func newFilterEntries(patterns []string) *filterEntries {
	return &filterEntries{
		patterns: patterns,
		entries:  make(map[string]*entry),
	}
}

func (fe *filterEntries) add(ent *entry) {
	fe.Lock()
	defer fe.Unlock()

	if !ent.key.Match(fe.patterns) {
		return
	}

	if fe.entries[ent.key.String()] == nil {
		fe.entries[ent.key.String()] = ent
	}
}

func (fe *filterEntries) exists(ent *entry) bool {
	fe.RLock()
	defer fe.RUnlock()
	return fe.entries[ent.key.String()] != nil
}
