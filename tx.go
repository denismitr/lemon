package lemon

import (
	"context"
	"github.com/pkg/errors"
)

var ErrKeyDoesNotExist = errors.New("key does not exist in DB")
var ErrTxIsReadOnly = errors.New("transaction is read only")
var ErrTxAlreadyClosed = errors.New("transaction already closed")

type Tx struct {
	readOnly        bool
	e               engine
	ctx             context.Context
	persistCommands []serializer
	updates         []*entry
	replaced        []*entry
	added           []*entry
}

func (x *Tx) lock() {
	if x.readOnly {
		x.e.RLock()
	} else {
		x.e.Lock()
	}
}

func (x *Tx) unlock() {
	if x.readOnly {
		x.e.RUnlock()
	} else {
		x.e.Unlock()
	}
}

func (x *Tx) Commit() error {
	if x.e == nil {
		return ErrTxAlreadyClosed
	}

	defer func() {
		x.unlock()
		x.e = nil
		x.persistCommands = nil
		x.replaced = nil
		x.added = nil
	}()

	if err := x.e.Persist(x.persistCommands); err != nil {
		return err
	}

	for i := range x.updates {
		x.updates[i].committed = true
	}

	for i := range x.added {
		x.added[i].committed = true
	}

	return nil
}

func (x *Tx) Rollback() error {
	defer func() {
		x.unlock()
		x.e = nil
	}()

	for _, ent := range x.replaced {
		if err := x.e.Put(ent, true); err != nil {
			return err
		}
	}

	for _, ent := range x.added {
		if err := x.e.Remove(ent.key); err != nil {
			return err
		}
	}

	return nil
}

func (x *Tx) FlushAll() error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	x.persistCommands = append(x.persistCommands, &flushAllCmd{})

	return x.e.FlushAll(func(ent *entry) {
		if ent.committed {
			x.replaced = append(x.replaced, ent)
		}
	})
}

func (x *Tx) Has(key string) bool {
	return x.e.Exists(key)
}

func (x *Tx) Get(key string) (*Document, error) { // fixme: decide on ref or value
	ent, err := x.e.FindByKey(key)
	if err != nil {
		return nil, err
	}

	return newDocumentFromEntry(ent), nil
}

func (x *Tx) MGet(keys ...string) (map[string]*Document, error) {
	docs := make(map[string]*Document, len(keys))
	if err := x.e.IterateByKeys(keys, func(ent *entry) bool {
		docs[ent.key.String()] = newDocumentFromEntry(ent)
		return true
	}); err != nil {
		return nil, err
	}

	return docs, nil
}

func (x *Tx) Insert(key string, data interface{}, metaAppliers ...MetaApplier) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	v, err := serializeToValue(data)
	if err != nil {
		return err
	}

	ent := newEntry(key, v)
	for _, applier := range metaAppliers {
		applier.applyTo(ent)
	}

	if err := x.e.Insert(ent); err != nil {
		return err
	}

	x.persistCommands = append(x.persistCommands, ent)
	x.added = append(x.added, ent)

	return nil
}

func (x *Tx) InsertOrReplace(key string, data interface{}, metaAppliers ...MetaApplier) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	v, err := serializeToValue(data)
	if err != nil {
		return err
	}

	ent := newEntry(key, v)
	for _, applier := range metaAppliers {
		applier.applyTo(ent)
	}

	existing, err := x.e.FindByKey(key)
	if err != nil && !errors.Is(err, ErrKeyDoesNotExist) {
		return err
	}

	if existing != nil {
		if updateErr := x.e.Put(ent, true); updateErr != nil {
			return updateErr
		}

		x.updates = append(x.updates, ent)
		if existing.committed {
			x.persistCommands = append(x.persistCommands, &deleteCmd{key: existing.key})
			x.replaced = append(x.replaced, existing)
		}
	} else {
		if insertErr := x.e.Put(ent, false); insertErr != nil {
			return insertErr
		}

		x.added = append(x.added, ent)
	}

	x.persistCommands = append(x.persistCommands, ent)

	return nil
}

// Tag adds new tags or replaces existing tags only those specified in the keys of the given map of tags
// in a document with argument `key` if found
func (x *Tx) Tag(key string, m M) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	ent, err := x.e.FindByKey(key)
	if err != nil {
		return err
	}

	// save a copy of the updated entry in case of rollback
	x.replaced = append(x.replaced, ent.clone())

	for name, v := range m {
		if err := x.e.UpsertTag(name, v, ent); err != nil {
			return err
		}
	}

	nt, err := newTagsFromMap(m)
	if err != nil {
		return err
	}

	// on commit commands should be persisted in order
	x.persistCommands = append(x.persistCommands, &tagCmd{newPK(key), nt})

	return nil
}

func (x *Tx) Untag(key string, tagNames ...string) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	ent, err := x.e.FindByKey(key)
	if err != nil {
		return err
	}

	// save a copy of the updated entry in case of rollback
	x.replaced = append(x.replaced, ent.clone())
	x.updates = append(x.updates, ent)

	for _, name := range tagNames {
		if err := x.e.RemoveTag(name, ent); err != nil {
			return err
		}
	}

	// on commit commands should be persisted in order
	x.persistCommands = append(x.persistCommands, &untagCmd{newPK(key), tagNames})

	return nil
}

func (x *Tx) Scan(ctx context.Context, opts *QueryOptions, cb func(d *Document) bool) error {
	ir := func(ent *entry) bool {
		d := newDocumentFromEntry(ent)
		return cb(d)
	}

	if err := x.applyScanner(ctx, opts, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) CountByQuery(opts *QueryOptions) (int, error) {
	var counter int

	ir := func(_ *entry) bool {
		counter++
		return true
	}

	if err := x.applyScanner(x.ctx, opts, ir); err != nil {
		return counter, err
	}

	return counter, nil
}

func (x *Tx) Find(ctx context.Context, q *QueryOptions, dest *[]Document) error {
	ir := func(ent *entry) bool {
		*dest = append(*dest, *newDocumentFromEntry(ent))
		return true
	}

	if err := x.applyScanner(ctx, q, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) applyScanner(ctx context.Context, q *QueryOptions, it entryIterator) error {
	if q == nil {
		q = Q()
	}

	if q.order == "" {
		q.order = AscOrder
	}

	fe, err := x.e.FilterEntriesByTags(q)
	if err != nil {
		panic(err)
	}

	// if we have entries chosen by secondary indexes
	// and filtered by primary key patterns
	// we can just sort by keys, iterate and return
	if fe != nil && !fe.empty() {
		fe.iterate(q.order, it)
		return nil
	}

	// scanner is a function that is chosen dynamically depending
	// on the query options
	sc, err := x.e.ChooseBestScanner(q)
	if err != nil {
		return err
	}

	if err := sc(ctx, q, it); err != nil {
		return err
	}

	return nil
}

func (x *Tx) Remove(keys ...string) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	for _, k := range keys {
		found, err := x.e.FindByKey(k)
		if err != nil {
			return err
		}

		if err := x.e.Remove(found.key); err != nil {
			return err
		}

		x.replaced = append(x.replaced, found)
		x.persistCommands = append(x.persistCommands, &deleteCmd{found.key})
	}

	return nil
}

func (x *Tx) Count() int {
	return x.e.Count()
}
