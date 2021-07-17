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
	v, err := x.e.FindByKey(key)
	if err != nil {
		return nil, err
	}

	return newDocument(key, v), nil
}

func (x *Tx) MGet(key ...string) ([]*Document, error) { // fixme: decide on ref or value
	docs := make([]*Document, 0)
	if err := x.e.FindByKeys(key, func(k string, b []byte) bool {
		docs = append(docs, newDocument(k, b))
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

	if err := x.e.Insert(key, data, ts); err != nil {
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

	if err := x.e.Insert(key, data, ts); err != nil {
		if errors.Is(err, ErrKeyAlreadyExists) {
			if updateErr := x.e.Update(key, data, ts); updateErr != nil {
				return updateErr
			} else {
				return nil
			}
		}

		return err
	}

	return nil
}

func (x *Tx) Scan(ctx context.Context, opts *QueryOptions, cb func(d Document) bool) error {
	ir := func(k string, v []byte) bool {
		d := createDocument(k, v)
		return cb(d)
	}

	if err := x.applyScanner(ctx, opts, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) Find(ctx context.Context, opts *QueryOptions, dest *[]Document) error {
	ir := func(k string, v []byte) bool {
		*dest = append(*dest, createDocument(k, v))
		return true
	}

	if err := x.applyScanner(ctx, opts, ir); err != nil {
		return err
	}

	return nil
}

func (x *Tx) applyScanner(ctx context.Context, opts *QueryOptions, ir ItemReceiver) error {
	if opts == nil {
		opts = Q()
	}

	if opts.KR != nil {
		var scanner RangeScanner
		if opts.O == Ascend {
			scanner = x.e.ScanBetweenAscend
		} else {
			scanner = x.e.ScanBetweenDescend
		}

		if err := scanner(ctx, opts.KR.From, opts.KR.To, ir); err != nil {
			return err
		}

		return nil
	} else if opts.Px != "" {
		var scanner PrefixScanner
		if opts.O == Ascend {
			scanner = x.e.ScanPrefixAscend
		} else {
			scanner = x.e.ScanPrefixDescend
		}

		if err := scanner(ctx, opts.Px, ir); err != nil {
			return err
		}

		return nil
	} else {
		var scanner Scanner
		if opts.O == Ascend {
			scanner = x.e.ScanAscend
		} else {
			scanner = x.e.ScanDescend
		}

		if err := scanner(ctx, ir); err != nil {
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