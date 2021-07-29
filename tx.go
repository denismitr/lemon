package lemon

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
)

var ErrKeyDoesNotExist = errors.New("key does not exist in DB")
var ErrTxIsReadOnly = errors.New("transaction is read only")

type Tx struct {
	readOnly bool
	buf *bytes.Buffer
	e *Engine
	ctx context.Context
	commands []serializer
}

func (x *Tx) Commit() error {
	if x.e.persistence != nil && x.commands != nil {
		for _, cmd := range x.commands{
			cmd.serialize(x.buf)
		}

		if err := x.e.persistence.write(x.buf); err != nil {
			return err
		}
	}

	return nil
}

func (x *Tx) Get(key string) (*Document, error) { // fixme: decide on ref or value
	ent, err := x.e.findByKey(key)
	if err != nil {
		return nil, err
	}

	return newDocumentFromEntry(ent), nil
}

func (x *Tx) MGet(key ...string) ([]*Document, error) { // fixme: decide on ref or value
	docs := make([]*Document, 0)
	if err := x.e.findByKeys(key, func(ent *entry) bool {
		docs = append(docs, newDocumentFromEntry(ent))
		return true
	}); err != nil {
		return nil, err
	}

	return docs, nil
}

func (x *Tx) Insert(key string, data interface{}, taggers ...Tagger) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	ts := Tags{}
	for _, t := range taggers {
		t(&ts)
	}

	v, err := serializeToValue(data)
	if err != nil {
		return err
	}

	ent := newEntry(key, v, &ts)

	if err := x.e.insert(ent); err != nil {
		return err
	}

	x.commands = append(x.commands, ent)

	return nil
}

func (x *Tx) InsertOrReplace(key string, data interface{}, taggers ...Tagger) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	ts := Tags{}
	for _, t := range taggers {
		t(&ts)
	}

	v, err := serializeToValue(data)
	if err != nil {
		return err
	}

	ent := newEntry(key, v, &ts)

	if err := x.e.insert(ent); err != nil {
		if errors.Is(err, ErrKeyAlreadyExists) {
			if updateErr := x.e.update(ent); updateErr != nil {
				return updateErr
			} else {
				return nil
			}
		}

		return err
	}

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

	fe := x.e.filterEntities(q.tags)

	if q.keyRange != nil {
		var sc rangeScanner
		if q.order == Ascend {
			sc = x.e.scanBetweenAscend
		} else {
			sc = x.e.scanBetweenDescend
		}

		if err := sc(ctx, q.keyRange.From, q.keyRange.To, ir, fe); err != nil {
			return err
		}

		return nil
	} else if q.prefix != "" {
		var sc prefixScanner
		if q.order == Ascend {
			sc = x.e.scanPrefixAscend
		} else {
			sc = x.e.scanPrefixDescend
		}

		if err := sc(ctx, q.prefix, ir, fe); err != nil {
			return err
		}

		return nil
	} else {
		var sc scanner
		if q.order == Ascend {
			sc = x.e.scanAscend
		} else {
			sc = x.e.scanDescend
		}

		if err := sc(ctx, ir, fe); err != nil {
			return err
		}

		return nil
	}
}

func (x *Tx) Remove(keys ...string) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	for _, k := range keys {
		pk := newPK(k)
		if err := x.e.remove(pk); err != nil {
			x.commands = append(x.commands, &deleteCmd{pk})
		} else {
			return err
		}
	}

	return nil
}

func (x *Tx) Count() int {
	return x.e.Count()
}