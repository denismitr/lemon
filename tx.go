package lemon

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
)

var ErrKeyDoesNotExist = errors.New("key does not exist in DB")
var ErrTxIsReadOnly = errors.New("transaction is read only")
var ErrTxAlreadyClosed = errors.New("transaction already closed")

type Tx struct {
	readOnly bool
	buf      *bytes.Buffer
	e        *engine
	ctx      context.Context
	commands []serializer
	modified []*entry
	added    []*entry
}

func (x *Tx) Commit() error {
	if x.e == nil {
		return ErrTxAlreadyClosed
	}

	if x.e.persistence != nil && x.commands != nil {
		for _, cmd := range x.commands {
			cmd.serialize(x.buf)
		}

		if err := x.e.persistence.write(x.buf); err != nil {
			return err
		}
	}

	x.e = nil

	return nil
}

func (x *Tx) Rollback() error {
	x.e.mu.Lock()
	defer x.e.mu.Unlock()

	for _, ent := range x.modified {
		if err := x.e.putUnderLock(ent, true); err != nil {
			return err
		}
	}

	for _, ent := range x.added {
		if err := x.e.removeUnderLock(ent.key); err != nil {
			return err
		}
	}

	x.e = nil

	return nil
}

func (x *Tx) Get(key string) (*Document, error) { // fixme: decide on ref or value
	ent, err := x.e.findByKey(key)
	if err != nil {
		return nil, err
	}

	return newDocumentFromEntry(ent), nil
}

func (x *Tx) MGet(key ...string) (map[string]*Document, error) {
	docs := make(map[string]*Document)
	if err := x.e.findByKeys(key, func(ent *entry) bool {
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

	if err := x.e.insert(ent); err != nil {
		return err
	}

	x.commands = append(x.commands, ent)
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

	x.e.mu.Lock()
	defer x.e.mu.Unlock()

	existing, err := x.e.findByKeyUnderLock(key)
	if err != nil && !errors.Is(err, ErrKeyDoesNotExist) {
		return err
	}

	if existing != nil {
		if updateErr := x.e.putUnderLock(ent, true); updateErr != nil {
			return updateErr
		}

		x.commands = append(x.commands, &deleteCmd{key: existing.key})
		x.modified = append(x.modified, existing)
	} else {
		if insertErr := x.e.putUnderLock(ent, false); insertErr != nil {
			return insertErr
		}

		x.added = append(x.added, ent)
	}

	x.commands = append(x.commands, ent)

	return nil
}

func (x *Tx) Scan(ctx context.Context, opts *queryOptions, cb func(d Document) bool) error {
	ir := func(ent *entry) bool {
		d := newDocumentFromEntry(ent)
		return cb(*d)
	}

	if err := x.applyScanner(ctx, opts, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) Find(ctx context.Context, q *queryOptions, dest *[]Document) error {
	ir := func(ent *entry) bool {
		*dest = append(*dest, *newDocumentFromEntry(ent))
		return true
	}

	if err := x.applyScanner(ctx, q, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) applyScanner(ctx context.Context, q *queryOptions, ir entryReceiver) error {
	if q == nil {
		q = Q()
	}

	fe := x.e.filterEntities(q)
	var sc scanner

	if q.keyRange != nil {
		if q.order == AscOrder {
			sc = x.e.scanBetweenAscend
		} else {
			sc = x.e.scanBetweenDescend
		}
	} else if q.prefix != "" {
		if q.order == AscOrder {
			sc = x.e.scanPrefixAscend
		} else {
			sc = x.e.scanPrefixDescend
		}
	} else {
		if q.order == AscOrder {
			sc = x.e.scanAscend
		} else {
			sc = x.e.scanDescend
		}
	}

	if err := sc(ctx, q, fe, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) Remove(keys ...string) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	for _, k := range keys {
		found, err := x.e.findByKey(k)
		if err != nil {
			return err
		}


		if err := x.e.remove(found.key); err != nil {
			return err
		}

		x.modified = append(x.modified, found)
		x.commands = append(x.commands, &deleteCmd{found.key})
	}

	return nil
}

func (x *Tx) Count() int {
	return x.e.Count()
}
