package lemon

import (
	"github.com/pkg/errors"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidQueryOptions = errors.New("invalid query options")
)

type KeyRange struct {
	From, To string
}

type comparator int8

const (
	equal comparator = iota
	greaterThan
	lessThan
)

type tagKey struct {
	name string
	comp comparator
}

type matcher func(ent *entry) bool

func (tk *tagKey) getStringMatcher(v string) matcher {
	return func(ent *entry) bool {
		sv, ok := ent.tags.strings[tk.name]
		if !ok {
			return false
		}

		if tk.comp == equal {
			return sv == v
		} else if tk.comp == greaterThan {
			return sv > v
		} else if tk.comp == lessThan {
			return sv < v
		}

		return false
	}
}

func (tk *tagKey) getFloatMatcher(v float64) matcher {
	return func(ent *entry) bool {
		sv, ok := ent.tags.floats[tk.name]
		if !ok {
			return false
		}

		if tk.comp == equal {
			return sv == v
		} else if tk.comp == greaterThan {
			return sv > v
		} else if tk.comp == lessThan {
			return sv < v
		}

		return false
	}
}

func (tk *tagKey) getIntMatcher(v int) matcher {
	return func(ent *entry) bool {
		sv, ok := ent.tags.integers[tk.name]
		if !ok {
			return false
		}

		switch tk.comp {
		case equal:
			return sv == v
		case greaterThan:
			return sv > v
		case lessThan:
			return sv < v
		}

		return false
	}
}

func (tk *tagKey) getBoolMatcher(v bool) matcher {
	return func(ent *entry) bool {
		bv, ok := ent.tags.booleans[tk.name]
		if !ok {
			return false
		}

		if tk.comp == equal {
			return bv == v
		} else if tk.comp == greaterThan {
			return bv && !v
		} else if tk.comp == lessThan {
			return !bv && v
		}

		return false
	}
}

type QueryTags struct {
	booleans map[tagKey]bool
	strings  map[tagKey]string
	integers map[tagKey]int
	floats   map[tagKey]float64
}

func QT() *QueryTags {
	return &QueryTags{
		booleans: make(map[tagKey]bool),
		strings:  make(map[tagKey]string),
		integers: make(map[tagKey]int),
		floats:   make(map[tagKey]float64),
	}
}

func (qt *QueryTags) BoolTagEq(name string, value bool) *QueryTags {
	qt.booleans[tagKey{name: name, comp: equal}] = value
	return qt
}

func (qt *QueryTags) StrTagEq(name, value string) *QueryTags {
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

func (qt *QueryTags) IntTagLt(name string, value int) *QueryTags {
	qt.integers[tagKey{name: name, comp: lessThan}] = value
	return qt
}

func (qt *QueryTags) FloatTagEq(name string, value float64) *QueryTags {
	qt.floats[tagKey{name: name, comp: equal}] = value
	return qt
}

func (qt *QueryTags) FloatTagGt(name string, value float64) *QueryTags {
	qt.floats[tagKey{name: name, comp: greaterThan}] = value
	return qt
}

func (qt *QueryTags) CreatedAfter(t time.Time) *QueryTags {
	after := int(t.UnixMilli())
	return qt.IntTagGt(CreatedAt, after)
}

func (qt *QueryTags) UpdatedAfter(t time.Time) *QueryTags {
	after := int(t.UnixMilli())
	return qt.IntTagGt(UpdatedAt, after)
}

func (qt *QueryTags) CreatedBefore(t time.Time) *QueryTags {
	after := int(t.UnixMilli())
	return qt.IntTagLt(CreatedAt, after)
}

func (qt *QueryTags) UpdatedBefore(t time.Time) *QueryTags {
	after := int(t.UnixMilli())
	return qt.IntTagLt(UpdatedAt, after)
}

type Order string

const (
	AscOrder  Order = "ASC"
	DescOrder Order = "DESC"
)

type QueryOptions struct {
	order     Order
	keyRange  *KeyRange
	prefix    string
	patterns  []string
	allTags   *QueryTags
	byTagName string
}

func (qo *QueryOptions) needSortingByKeys() bool {
	return qo.byTagName == ""
}

func (qo *QueryOptions) Match(patten string) *QueryOptions {
	qo.patterns = strings.Split(patten, ":")
	return qo
}

func (qo *QueryOptions) KeyOrder(o Order) *QueryOptions {
	qo.order = o
	return qo
}

func (qo *QueryOptions) KeyRange(from, to string) *QueryOptions {
	qo.keyRange = &KeyRange{From: from, To: to}
	return qo
}

func (qo *QueryOptions) Prefix(p string) *QueryOptions {
	qo.prefix = p
	return qo
}

func (qo *QueryOptions) ByTagName(key string) *QueryOptions {
	qo.byTagName = key
	return qo
}

func (qo *QueryOptions) HasAllTags(qt *QueryTags) *QueryOptions {
	qo.allTags = qt
	return qo
}

func (qo *QueryOptions) Validate() error {
	if qo.byTagName != "" && qo.keyRange != nil {
		return errors.Wrap(ErrInvalidQueryOptions, "cannot combine by tag name and primary key range options")
	}

	if qo.byTagName != "" && qo.allTags != nil {
		return errors.Wrap(ErrInvalidQueryOptions, "cannot combine by tag name and all tags options")
	}

	return nil
}

func Q() *QueryOptions {
	return &QueryOptions{order: AscOrder}
}

type filterEntriesSink struct {
	sync.RWMutex
	keys     []PK
	patterns []string
	entries  map[string]*entry
}

func newFilteredEntriesSink(patterns []string) *filterEntriesSink {
	return &filterEntriesSink{
		patterns: patterns,
		keys:     make([]PK, 0),
		entries:  make(map[string]*entry),
	}
}

func (fe *filterEntriesSink) iterate(qo *QueryOptions, it entryIterator) {
	fe.RLock()
	defer fe.RUnlock()

	if qo.needSortingByKeys() {
		if qo.order == AscOrder {
			sort.Slice(fe.keys, func(i, j int) bool {
				return fe.keys[i].Less(fe.keys[j])
			})
		} else {
			sort.Slice(fe.keys, func(i, j int) bool {
				return fe.keys[j].Less(fe.keys[i])
			})
		}
	}

	for i := range fe.keys {
		if cont := it(fe.entries[fe.keys[i].String()]); !cont {
			break
		}
	}
}

func (fe *filterEntriesSink) add(entries ...*entry) {
	fe.Lock()
	defer fe.Unlock()

	for _, ent := range entries {
		if !ent.key.Match(fe.patterns) {
			return
		}

		if fe.entries[ent.key.String()] == nil {
			fe.keys = append(fe.keys, ent.key)
			fe.entries[ent.key.String()] = ent
		}
	}
}

func (fe *filterEntriesSink) addMap(entries map[string]*entry) {
	fe.Lock()
	defer fe.Unlock()

	for strKey, ent := range entries {
		if !ent.key.Match(fe.patterns) {
			return
		}

		if fe.entries[strKey] == nil {
			fe.keys = append(fe.keys, ent.key)
			fe.entries[strKey] = ent
		}
	}
}

//func (fe *filterEntriesSink) exists(ent *entry) bool {
//	fe.RLock()
//	defer fe.RUnlock()
//	return fe.entries[ent.key.String()] != nil
//}

func (fe *filterEntriesSink) empty() bool {
	fe.RLock()
	defer fe.RUnlock()
	return len(fe.keys) == 0
}
