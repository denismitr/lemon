package lemon

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	btr "github.com/tidwall/btree"
)

var ErrDocumentNotFound = errors.New("document not found")
var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrConflictingTagType = errors.New("conflicting tag type")

const castPanic = "how could primary keys item not be of type *entry"

type (
	entryReceiver func(ent *entry) bool

	rangeScanner func(
		ctx context.Context,
		lowerBoundPK string,
		upperBoundPK string,
		ir entryReceiver,
		fo *filterEntries,
		patters []string,
	) error

	prefixScanner func(
		ctx context.Context,
		prefix string,
		ir entryReceiver,
		fo *filterEntries,
		patters []string,
	) error

	scanner func(
		ctx context.Context,
		ir entryReceiver,
		fo *filterEntries,
		patters []string,
	) error
)

type Engine struct {
	persistence *persistence
	pks         *btr.BTree
	boolTags    boolIndex
	strTags     stringIndex
}

func newEngine(fullPath string) (*Engine, error) {
	e := &Engine{
		pks:      btr.New(byPrimaryKeys),
		boolTags: newBoolIndex(),
		strTags:  newStringIndex(),
	}

	if fullPath != ":memory:" {
		p, err := newPersistence(fullPath, Sync)
		if err != nil {
			return nil, err
		}
		e.persistence = p
	}

	if initErr := e.init(); initErr != nil {
		return nil, initErr
	}

	return e, nil
}

func (e *Engine) close() error {
	defer func() {
		e.pks = nil
		e.boolTags = nil
		e.strTags = nil
		e.persistence = nil
	}()

	if e.persistence != nil {
		return e.persistence.close()
	}

	return nil
}

func (e *Engine) init() error {
	if e.persistence != nil {
		if err := e.persistence.load(func(d deserializer) error {
			return d.deserialize(e)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) insert(ent *entry) error {
	existing := e.pks.Set(ent)
	if existing != nil {
		return errors.Wrapf(ErrKeyAlreadyExists, "key: %s", ent.key.String())
	}

	if ent.tags != nil {
		e.setEntityTags(ent)
	}

	return nil
}

func (e *Engine) findByKey(key string) (*entry, error) {
	found := e.pks.Get(&entry{key: newPK(key)})
	if found == nil {
		return nil, errors.Wrapf(ErrDocumentNotFound, "key %s does not exist in database", key)
	}

	ent, ok := found.(*entry)
	if !ok {
		panic(castPanic)
	}

	return ent, nil
}

func (e *Engine) findByKeys(pks []string, ir entryReceiver) error {
	for _, k := range pks {
		found := e.pks.Get(newPK(k))
		if found == nil {
			return errors.Wrapf(ErrDocumentNotFound, "key %s does not exist in database", k)
		}

		ent := found.(*entry)

		if next := ir(ent); !next {
			break
		}
	}

	return nil
}

func (e *Engine) remove(key PK) error {
	ent := e.pks.Get(&entry{key: key})
	if ent == nil {
		return errors.Wrapf(ErrDocumentNotFound, "key %s does not exist in DB", key.String())
	}

	e.pks.Delete(&entry{key: key})

	return nil
}

func (e *Engine) update(ent *entry) error {
	existing := e.pks.Set(ent)
	if existing == nil {
		return errors.Wrapf(ErrDocumentNotFound, "could not update non existing document with key %s", ent.key.String())
	}

	existingEnt, ok := existing.(*entry)
	if !ok {
		panic(castPanic)
	}

	if existingEnt.tags != nil {
		e.clearEntityTags(ent)
	}

	if ent.tags != nil {
		e.setEntityTags(ent)
	}

	return nil
}

func (e *Engine) setEntityTags(ent *entry) {
	for _, bt := range ent.tags.Booleans {
		e.boolTags.add(bt.Name, bt.Value, ent)
	}

	for _, st := range ent.tags.Strings {
		e.strTags.add(st.Name, st.Value, ent)
	}
}

func (e *Engine) clearEntityTags(ent *entry) {
	for _, bt := range ent.tags.Booleans {
		e.boolTags.removeEntryByTag(bt.Name, bt.Value, ent)
	}

	for _, st := range ent.tags.Strings {
		e.strTags.removeEntryByTag(st.Name, st.Value, ent)
	}
}

func (e *Engine) Count() int {
	return e.pks.Len()
}

func (e *Engine) scanBetweenDescend(
	ctx context.Context,
	from string,
	to string,
	ir entryReceiver,
	fe *filterEntries,
	patterns []string,
) (err error) {
	// Descend required a reverse order of `from` and `to`
	descendRange(e.pks, &entry{key: newPK(from)}, &entry{key: newPK(to)}, filteringBTreeIterator(ctx, fe, patterns, ir))
	return
}

func (e *Engine) scanBetweenAscend(
	ctx context.Context,
	from string,
	to string,
	ir entryReceiver,
	fe *filterEntries,
	patterns []string,
) (err error) {
	ascendRange(e.pks, &entry{key: newPK(from)}, &entry{key: newPK(to)}, filteringBTreeIterator(ctx, fe, patterns, ir))

	return
}

func (e *Engine) scanPrefixAscend(
	ctx context.Context,
	prefix string,
	ir entryReceiver,
	fe *filterEntries,
	patterns []string,
) (err error) {
	e.pks.Ascend(&entry{key: newPK(prefix)}, filteringBTreeIterator(ctx, fe, patterns, ir))

	return
}

func (e *Engine) scanPrefixDescend(
	ctx context.Context,
	prefix string,
	ir entryReceiver,
	fe *filterEntries,
	patterns []string,
) (err error) {
	descendGreaterThan(e.pks, &entry{key: newPK(prefix)}, filteringBTreeIterator(ctx, fe, patterns, ir))
	return
}

func (e *Engine) scanAscend(
	ctx context.Context,
	ir entryReceiver,
	fe *filterEntries,
	patterns []string,
) (err error) {
	e.pks.Ascend(nil, filteringBTreeIterator(ctx, fe, patterns, ir))
	return
}

func (e *Engine) scanDescend(
	ctx context.Context,
	ir entryReceiver,
	fe *filterEntries,
	patterns []string,
) (err error) {
	e.pks.Descend(nil, filteringBTreeIterator(ctx, fe, patterns, ir))
	return
}

func (e *Engine) filterEntities(q *queryOptions) *filterEntries {
	if q == nil || q.tags == nil {
		return nil
	}

	ft := newFilterEntries(q.patterns)

	if q.tags.boolTags != nil && e.boolTags != nil {
		for _, bt := range q.tags.boolTags {
			entries := e.boolTags[bt.Name][bt.Value]
			if entries == nil {
				continue
			}

			for _, ent := range entries {
				ft.add(ent)
			}
		}
	}

	if q.tags.strTags != nil && e.strTags != nil {
		for _, st := range q.tags.strTags {
			entries := e.strTags[st.Name][st.Value]
			if entries == nil {
				continue
			}

			for _, ent := range entries {
				ft.add(ent)
			}
		}
	}

	return ft
}

func (e *Engine) put(ent *entry, replace bool) error {
	existing := e.pks.Set(ent)
	if existing != nil {
		if !replace {
			_ = e.pks.Set(existing)
			return errors.Wrapf(ErrKeyAlreadyExists, "key %s", ent.key.String())
		}

		existingEnt, ok := existing.(*entry)
		if !ok {
			panic(castPanic)
		}

		if existingEnt.tags != nil {
			e.clearEntityTags(ent)
		}
	}

	if ent.tags != nil {
		e.setEntityTags(ent)
	}

	return nil
}

func filteringBTreeIterator(
	ctx context.Context,
	fe *filterEntries,
	patterns []string,
	ir entryReceiver,
) func(item interface{}) bool {
	return func(item interface{}) bool {
		if ctx.Err() != nil {
			return false
		}

		ent, ok := item.(*entry)
		if !ok {
			panic(castPanic)
		}

		if fe != nil && (!fe.exists(ent) || !ent.key.Match(patterns)) {
			return true
		}

		return ir(ent)
	}
}

func serializeToValue(d interface{}) ([]byte, error) {
	var v []byte
	if s, isStr := d.(string); isStr {
		v = []byte(s)
	} else {
		b, err := json.Marshal(d)
		if err != nil {
			return nil, errors.Wrapf(err, "could not marshal data %+Value", d)
		}
		v = b
	}

	return v, nil
}
