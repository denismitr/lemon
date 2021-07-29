package lemon

import (
	"strings"
	"sync"
)

type D map[string]interface{}

type KeyRange struct {
	From, To string
}

type Order string

const (
	Ascend  Order = "ASC"
	Descend Order = "DESC"
)

type queryTags struct {
	boolTags []boolTag
	strTags  []strTag
}

type queryOptions struct {
	order    Order
	keyRange *KeyRange
	prefix   string
	patterns []string
	tags     *queryTags
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

func (fo *queryOptions) BoolTag(name string, v bool) *queryOptions {
	if fo.tags == nil {
		fo.tags = &queryTags{}
	}

	fo.tags.boolTags = append(fo.tags.boolTags, boolTag{Name: name, Value: v})
	return fo
}

func Q() *queryOptions {
	return &queryOptions{order: Ascend}
}

type filterEntries struct {
	sync.RWMutex
	entries map[string]*entry
}

func newFilterEntries() *filterEntries {
	return &filterEntries{
		entries: make(map[string]*entry),
	}
}

func (fe *filterEntries) add(ent *entry) {
	fe.Lock()
	defer fe.Unlock()

	if fe.entries[ent.key.String()] == nil {
		fe.entries[ent.key.String()] = ent
	}
}

func (fe *filterEntries) exists(ent *entry) bool {
	fe.RLock()
	defer fe.RUnlock()
	return fe.entries[ent.key.String()] != nil
}
