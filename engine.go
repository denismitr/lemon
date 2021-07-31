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

	scanner func(
		ctx context.Context,
		q *queryOptions,
		fe *filterEntries,
		ir entryReceiver,
	) error
)

type engine struct {
	persistence *persistence
	pks         *btr.BTree
	boolTags    boolIndex
	strTags     stringIndex
}

func newEngine(fullPath string) (*engine, error) {
	e := &engine{
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

func (e *engine) close() error {
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

func (e *engine) init() error {
	if e.persistence != nil {
		if err := e.persistence.load(func(d deserializer) error {
			return d.deserialize(e)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (e *engine) insert(ent *entry) error {
	existing := e.pks.Set(ent)
	if existing != nil {
		return errors.Wrapf(ErrKeyAlreadyExists, "key: %s", ent.key.String())
	}

	if ent.tags != nil {
		e.setEntityTags(ent)
	}

	return nil
}

func (e *engine) findByKey(key string) (*entry, error) {
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

func (e *engine) findByKeys(pks []string, ir entryReceiver) error {
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

func (e *engine) remove(key PK) error {
	ent := e.pks.Get(&entry{key: key})
	if ent == nil {
		return errors.Wrapf(ErrDocumentNotFound, "key %s does not exist in DB", key.String())
	}

	e.pks.Delete(&entry{key: key})

	return nil
}

func (e *engine) update(ent *entry) error {
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

func (e *engine) setEntityTags(ent *entry) {
	for _, bt := range ent.tags.booleans {
		e.boolTags.add(bt.name, bt.value, ent)
	}

	for _, st := range ent.tags.strings {
		e.strTags.add(st.name, st.value, ent)
	}
}

func (e *engine) clearEntityTags(ent *entry) {
	for _, bt := range ent.tags.booleans {
		e.boolTags.removeEntryByTag(bt.name, bt.value, ent)
	}

	for _, st := range ent.tags.strings {
		e.strTags.removeEntryByTag(st.name, st.value, ent)
	}
}

func (e *engine) Count() int {
	return e.pks.Len()
}

func (e *engine) scanBetweenDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	// Descend required a reverse order of `from` and `to`
	descendRange(
		e.pks,
		&entry{key: newPK(q.keyRange.From)},
		&entry{key: newPK(q.keyRange.To)},
		filteringBTreeIterator(ctx, fe, q, ir),
	)

	return
}

func (e *engine) scanBetweenAscend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	ascendRange(
		e.pks,
		&entry{key: newPK(q.keyRange.From)},
		&entry{key: newPK(q.keyRange.To)},
		filteringBTreeIterator(ctx, fe, q, ir),
	)

	return
}

func (e *engine) scanPrefixAscend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	e.pks.Ascend(&entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, fe, q, ir))

	return
}

func (e *engine) scanPrefixDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	descendGreaterThan(e.pks, &entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) scanAscend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	e.pks.Ascend(nil, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) scanDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	e.pks.Descend(nil, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) filterEntities(q *queryOptions) *filterEntries {
	if q == nil || q.tags == nil {
		return nil
	}

	ft := newFilterEntries(q.patterns)

	if q.tags.boolTags != nil && e.boolTags != nil {
		for _, bt := range q.tags.boolTags {
			entries := e.boolTags[bt.name][bt.value]
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
			entries := e.strTags[st.name][st.value]
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

func (e *engine) put(ent *entry, replace bool) error {
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
	q *queryOptions,
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

		if fe != nil && (!fe.exists(ent) || !ent.key.Match(q.patterns)) {
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
