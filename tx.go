package lemon

import (
	"context"
	"github.com/denismitr/glog"
	"github.com/pkg/errors"
)

var ErrKeyDoesNotExist = errors.New("key does not exist in DB")
var ErrTxIsReadOnly = errors.New("transaction is read only")
var ErrTxAlreadyClosed = errors.New("transaction already closed")

type Tx struct {
	readOnly        bool
	ee              executionEngine
	ctx             context.Context
	persistCommands []serializable
	updated         []*entry
	replaced        []*entry
	added           []*entry
	lg              glog.Logger
}

func (x *Tx) lock() {
	if x.readOnly {
		x.ee.RLock()
	} else {
		x.ee.Lock()
	}
}

func (x *Tx) unlock() {
	if x.readOnly {
		x.ee.RUnlock()
	} else {
		x.ee.Unlock()
	}
}

func (x *Tx) Commit() error {
	if x.ee == nil {
		return ErrTxAlreadyClosed
	}

	defer func() {
		x.unlock()
		x.ee = nil
		x.persistCommands = nil
		x.replaced = nil
		x.added = nil
	}()

	if err := x.ee.Persist(x.persistCommands); err != nil {
		return err
	}

	for i := range x.updated {
		x.updated[i].committed = true
	}

	for i := range x.added {
		x.added[i].committed = true
	}

	return nil
}

func (x *Tx) Rollback() error {
	if x.ee == nil {
		return ErrTxAlreadyClosed
	}

	defer func() {
		x.unlock()
		x.ee = nil
		x.persistCommands = nil
		x.replaced = nil
		x.added = nil
	}()

	if x.readOnly {
		return nil
	}

	for _, ent := range x.replaced {
		if err := x.ee.Put(ent, true); err != nil {
			return err
		}
	}

	for _, ent := range x.added {
		if err := x.ee.Remove(ent.key); err != nil {
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

	return x.ee.FlushAll(func(ent *entry) {
		if ent.committed {
			x.replaced = append(x.replaced, ent)
		}
	})
}

func (x *Tx) Has(key string) bool {
	return x.ee.Exists(key)
}

func (x *Tx) Get(key string) (*Document, error) {
	ent, err := x.ee.FindByKey(key)
	if err != nil {
		return nil, err
	}

	if ent.value == nil {
		if err := x.ee.LoadEntryValue(ent); err != nil {
			return nil, err
		}
	}

	return newDocumentFromEntry(ent), nil
}

// MGetContext - multi get by keys with context
func (x *Tx) MGetContext(ctx context.Context, keys ...string) (map[string]*Document, error) {
	docs := make(map[string]*Document, len(keys))
	if err := x.ee.IterateByKeys(keys, func(ent *entry) bool {
		if ctx.Err() != nil {
			return false
		}

		if ent.value == nil {
			if err := x.ee.LoadEntryValue(ent); err != nil {
				x.lg.Error(err)
			}
		}

		docs[ent.key.String()] = newDocumentFromEntry(ent)

		return true
	}); err != nil {
		return nil, err
	}

	return docs, ctx.Err()
}

// MGet - multi get by keys
func (x *Tx) MGet(keys ...string) (map[string]*Document, error) {
	return x.MGetContext(context.Background(), keys...)
}

func (x *Tx) Insert(key string, data interface{}, metaAppliers ...MetaApplier) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	if x.ee == nil {
		return ErrTxAlreadyClosed
	}

	v, contentTypeIdentifier, err := serializeToValue(data)
	if err != nil {
		return err
	}

	metaAppliers = append(metaAppliers, WithContentType(contentTypeIdentifier))

	ent := newEntry(key, v)
	ent.tags = newTags()
	for _, applier := range metaAppliers {
		if err := applier.applyTo(ent); err != nil {
			return err
		}
	}

	if err := x.ee.Insert(ent); err != nil {
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

	v, contentTypeIdentifier, err := serializeToValue(data)
	if err != nil {
		return err
	}

	metaAppliers = append(metaAppliers, WithContentType(contentTypeIdentifier))

	newEnt := newEntry(key, v)
	newEnt.tags = newTags()
	for _, applier := range metaAppliers {
		if err := applier.applyTo(newEnt); err != nil {
			return err
		}
	}

	existingEnt, err := x.ee.FindByKey(key)
	if err != nil && !errors.Is(err, ErrKeyDoesNotExist) {
		return err
	}

	if existingEnt != nil {
		preserveCreatedAt(existingEnt, newEnt)

		if updateErr := x.ee.Put(newEnt, true); updateErr != nil {
			return updateErr
		}

		x.updated = append(x.updated, newEnt)
		if existingEnt.committed {
			x.persistCommands = append(x.persistCommands, &deleteCmd{key: existingEnt.key})
			x.replaced = append(x.replaced, existingEnt)
		}
	} else {
		if insertErr := x.ee.Put(newEnt, false); insertErr != nil {
			return insertErr
		}

		x.added = append(x.added, newEnt)
	}

	x.persistCommands = append(x.persistCommands, newEnt)

	return nil
}

func preserveCreatedAt(existingEnt, newEnt *entry) {
	if existingEnt.tags == nil {
		return
	}

	if createdAt, ok := existingEnt.tags[CreatedAt]; ok {
		newEnt.tags[CreatedAt].dt = intDataType
		newEnt.tags[CreatedAt] = createdAt
	}
}

// Tag adds new tags or replaces existing tags only those specified in the keys of the given map of tags
// in a document with argument `key` if found
func (x *Tx) Tag(key string, m M) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	ent, err := x.ee.FindByKey(key)
	if err != nil {
		return err
	}

	// save a copy of the updated entry in case of rollback
	x.replaced = append(x.replaced, ent.clone())

	for name, v := range m {
		if err := x.ee.UpsertTag(name, v, ent); err != nil {
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

	ent, err := x.ee.FindByKey(key)
	if err != nil {
		return err
	}

	// save a copy of the updated entry in case of rollback
	x.replaced = append(x.replaced, ent.clone())
	x.updated = append(x.updated, ent)

	for _, name := range tagNames {
		if err := x.ee.RemoveTag(name, ent); err != nil {
			return err
		}
	}

	// on commit commands should be persisted in order
	x.persistCommands = append(x.persistCommands, &untagCmd{newPK(key), tagNames})

	return nil
}

func (x *Tx) Scan(opts *QueryOptions, cb func(d *Document) bool) error {
	ir := func(ent *entry) bool {
		if ent.value == nil {
			if err := x.ee.LoadEntryValue(ent); err != nil {
				// fixme: log
			}
		}
		d := newDocumentFromEntry(ent)
		return cb(d)
	}

	if err := x.applyScanner(x.ctx, opts, ir); err != nil {
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

// Find documents by query options
func (x *Tx) Find(q *QueryOptions) ([]*Document, error) {
	var result []*Document
	ir := func(ent *entry) bool {
		if ent.value == nil {
			if err := x.ee.LoadEntryValue(ent); err != nil {
				// fixme: log
			}
		}
		result = append(result, newDocumentFromEntry(ent))
		return true
	}

	if err := x.applyScanner(x.ctx, q, ir); err != nil {
		return nil, err
	}

	return result, nil
}

func (x *Tx) applyScanner(ctx context.Context, qo *QueryOptions, it entryIterator) error {
	if qo == nil {
		qo = Q()
	}

	if err := qo.Validate(); err != nil {
		return err
	}

	if qo.order == "" {
		qo.order = AscOrder
	}

	fe, err := x.ee.FilterEntriesByTags(qo)
	if err != nil {
		return err
	}

	// if we have entries chosen by secondary indexes
	// and filtered by primary key patterns
	// we can just sort by keys, iterate and return
	if fe != nil && !fe.empty() {
		fe.iterate(qo, it)
		return nil
	}

	// scanner is a function that is chosen dynamically depending
	// on the query options
	sc, err := x.ee.ChooseBestScanner(qo)
	if err != nil {
		return err
	}

	if err := sc(ctx, qo, it); err != nil {
		return err
	}

	return nil
}

func (x *Tx) Remove(keys ...string) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	for _, k := range keys {
		found, err := x.ee.FindByKey(k)
		if err != nil {
			return err
		}

		if err := x.ee.Remove(found.key); err != nil {
			return err
		}

		x.replaced = append(x.replaced, found)
		x.persistCommands = append(x.persistCommands, &deleteCmd{found.key})
	}

	return nil
}

func (x *Tx) Count() int {
	return x.ee.Count()
}
