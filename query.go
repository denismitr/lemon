package lemon

import (
	"sort"
	"strings"
	"sync"
)

type M map[string]interface{}

func (m M) String(k string) string {
	v, ok := m[k].(string)
	if !ok {
		return ""
	}
	return v
}

func (m M) Int(k string) int {
	v, ok := m[k].(int)
	if !ok {
		return 0
	}
	return v
}

func (m M) Bool(k string) bool {
	v, ok := m[k].(bool)
	if !ok {
		return false
	}
	return v
}

func (m M) Float(k string) float64 {
	v, ok := m[k].(float64)
	if !ok {
		return 0
	}
	return v
}

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

func (tk *tagKey) getBoolMatcher(v bool) matcher {
	return func(ent *entry) bool {
		bv, ok := ent.tags.booleans[tk.name]
		if !ok {
			return false
		}

		if tk.comp == equal {
			return bv == v
		} else if tk.comp == greaterThan {
			return bv == true && v == false
		} else if tk.comp == lessThan {
			return bv == false && v == true
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

func (qt *QueryTags) FloatTagEq(name string, value float64) *QueryTags {
	qt.floats[tagKey{name: name, comp: equal}] = value
	return qt
}

func (qt *QueryTags) FloatTagGt(name string, value float64) *QueryTags {
	qt.floats[tagKey{name: name, comp: greaterThan}] = value
	return qt
}

type Order string

const (
	AscOrder  Order = "ASC"
	DescOrder Order = "DESC"
)

type QueryOptions struct {
	order    Order
	keyRange *KeyRange
	prefix   string
	patterns []string
	allTags  *QueryTags
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

func (qo *QueryOptions) HasAllTags(qt *QueryTags) *QueryOptions {
	qo.allTags = qt
	return qo
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

func (fe *filterEntriesSink) iterate(order Order, it entryIterator) {
	fe.RLock()
	defer fe.RUnlock()

	if order == AscOrder {
		sort.Slice(fe.keys, func(i, j int) bool {
			return fe.keys[i].Less(fe.keys[j])
		})
	} else {
		sort.Slice(fe.keys, func(i, j int) bool {
			return fe.keys[j].Less(fe.keys[i])
		})
	}

	for i := range fe.keys {
		if cont := it(fe.entries[fe.keys[i].String()]); !cont {
			break
		}
	}
}

func (fe *filterEntriesSink) add(ent *entry) {
	fe.Lock()
	defer fe.Unlock()

	if !ent.key.Match(fe.patterns) {
		return
	}

	if fe.entries[ent.key.String()] == nil {
		fe.keys = append(fe.keys, ent.key)
		fe.entries[ent.key.String()] = ent
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
