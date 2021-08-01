package lemon

import (
	"strings"
	"sync"
)

type M map[string]interface{}

type KeyRange struct {
	From, To string
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
	allTags  *Tags
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

func (fo *queryOptions) AllTags(taggers ...Tagger) *queryOptions {
	if fo.allTags == nil {
		fo.allTags = newTags()
	}

	for _, t := range taggers {
		t(fo.allTags)
	}

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
	for n, v := range fo.allTags.booleans {
		matchesExpected++
		if e.tags.booleans[n] == v {
			actualMatches++
		}
	}

	for n, v := range fo.allTags.strings {
		matchesExpected++
		if e.tags.strings[n] == v {
			actualMatches++
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
