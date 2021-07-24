package lemon

import (
	"context"
	"github.com/pkg/errors"
)

var ErrKeyDoesNotExist = errors.New("key does not exist in DB")
var ErrTxIsReadOnly = errors.New("transaction is read only")

type Tx struct {
	readOnly bool
	e *Engine
	ctx context.Context
}

func (x *Tx) Get(key string) (*Document, error) { // fixme: decide on ref or value
	v, tags, err := x.e.findByKey(key)
	if err != nil {
		return nil, err
	}

	return newDocument(key, v, tags), nil
}

func (x *Tx) MGet(key ...string) ([]*Document, error) { // fixme: decide on ref or value
	docs := make([]*Document, 0)
	if err := x.e.FindByKeys(key, func(k string, b []byte, tags *Tags) bool {
		docs = append(docs, newDocument(k, b, tags))
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

	if err := x.e.Insert(key, data, &ts); err != nil {
		return err
	}

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

	if err := x.e.Insert(key, data, &ts); err != nil {
		if errors.Is(err, ErrKeyAlreadyExists) {
			if updateErr := x.e.Update(key, data, &ts); updateErr != nil {
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
	ir := func(k string, v []byte, tags *Tags) bool {
		d := createDocument(k, v, tags)
		return cb(d)
	}

	if err := x.applyScanner(ctx, opts, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) Find(ctx context.Context, opts *queryOptions, dest *[]Document) error {
	ir := func(k string, v []byte, tags *Tags) bool {
		*dest = append(*dest, createDocument(k, v, tags))
		return true
	}

	if err := x.applyScanner(ctx, opts, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) applyScanner(ctx context.Context, opts *queryOptions, ir ItemReceiver) error {
	if opts == nil {
		opts = Q()
	}


	fo := x.e.getFilteredOffsets(opts.tags)

	if opts.keyRange != nil {
		var sc rangeScanner
		if opts.order == Ascend {
			sc = x.e.scanBetweenAscend
		} else {
			sc = x.e.scanBetweenDescend
		}

		if err := sc(ctx, opts.keyRange.From, opts.keyRange.To, ir, fo); err != nil {
			return err
		}

		return nil
	} else if opts.prefix != "" {
		var sc prefixScanner
		if opts.order == Ascend {
			sc = x.e.scanPrefixAscend
		} else {
			sc = x.e.scanPrefixDescend
		}

		if err := sc(ctx, opts.prefix, ir, fo); err != nil {
			return err
		}

		return nil
	} else {
		var sc scanner
		if opts.order == Ascend {
			sc = x.e.scanAscend
		} else {
			sc = x.e.scanDescend
		}

		if err := sc(ctx, ir, fo); err != nil {
			return err
		}

		return nil
	}
}

func (x *Tx) Remove(keys ...string) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	if err := x.e.RemoveByKeys(keys...); err != nil {
		return err
	}

	return nil
}

func (x *Tx) Count() int {
	return x.e.Count()
}