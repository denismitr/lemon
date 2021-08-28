package lemon

import (
	"fmt"
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

func (tk *tagKey) matches(ent *entry, v interface{}) bool {
	switch typedValue := v.(type) {
	case float64:
		fv, ok := ent.tags.floats[tk.name]
		if !ok {
			return false
		}

		if tk.comp == equal {
			return fv == typedValue
		} else if tk.comp == greaterThan {
			return fv > typedValue
		} else if tk.comp == lessThan {
			return fv < typedValue
		}
	case int:
		iv, ok := ent.tags.integers[tk.name]
		if !ok {
			return false
		}

		if tk.comp == equal {
			return iv == typedValue
		} else if tk.comp == greaterThan {
			return iv > typedValue
		} else if tk.comp == lessThan {
			return iv < typedValue
		}
	case string:
		sv, ok := ent.tags.strings[tk.name]
		if !ok {
			return false
		}

		if tk.comp == equal {
			return sv == typedValue
		} else if tk.comp == greaterThan {
			return sv > typedValue
		} else if tk.comp == lessThan {
			return sv < typedValue
		}
	case bool:
		bv, ok := ent.tags.booleans[tk.name]
		if !ok {
			return false
		}

		if tk.comp == equal {
			return bv == typedValue
		} else if tk.comp == greaterThan {
			return bv == true && typedValue == false
		} else if tk.comp == lessThan {
			return bv == false && typedValue == true
		}
	default:
		panic(fmt.Sprintf("Type %T is not supported", v))
	}

	return false
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

func (fo *queryOptions) KeyOrder(o Order) *queryOptions {
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

	for k, v := range fo.allTags.floats {
		matchesExpected++
		switch k.comp {
		case equal:
			if e.tags.floats[k.name] == v {
				actualMatches++
			}
		case greaterThan:
			if e.tags.floats[k.name] > v {
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
	keys     []PK
	patterns []string
	entries  map[string]*entry
}

func newFilterEntries(patterns []string) *filterEntries {
	return &filterEntries{
		patterns: patterns,
		keys:     make([]PK, 0),
		entries:  make(map[string]*entry),
	}
}

func (fe *filterEntries) all(order Order, it entryIterator) {
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

func (fe *filterEntries) add(ent *entry) {
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

func (fe *filterEntries) exists(ent *entry) bool {
	fe.RLock()
	defer fe.RUnlock()
	return fe.entries[ent.key.String()] != nil
}

func (fe *filterEntries) empty() bool {
	return len(fe.keys) == 0
}
