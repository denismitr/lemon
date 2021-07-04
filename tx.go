package lemon

import (
	"context"
	"github.com/denismitr/lemon/internal/engine"
	"github.com/pkg/errors"
)

var ErrKeyDoesNotExist = errors.New("key does not exist in DB")
var ErrTxIsReadOnly = errors.New("transaction is read only")

type Tx struct {
	readOnly bool
	e *engine.Engine
	ctx context.Context
}

func (x *Tx) Get(key string) (*Document, error) { // fixme: decide on ref or value
	v, err := x.e.FindByKey(key)
	if err != nil {
		if errors.Is(err, engine.ErrDocumentNotFound) {
			return nil, errors.Wrapf(ErrKeyDoesNotExist, "%s", key)
		}

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

func (x *Tx) Insert(key string, data interface{}) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	if err := x.e.Insert(key, data); err != nil {
		return err
	}
	return nil
}

func (x *Tx) InsertOrReplace(key string, data interface{}) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	if err := x.e.Insert(key, data); err != nil {
		if errors.Is(err, engine.ErrKeyAlreadyExists) {
			if updateErr := x.e.Update(key, data); updateErr != nil {
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

func (x *Tx) applyScanner(ctx context.Context, opts *QueryOptions, ir engine.ItemReceiver) error {
	if opts == nil {
		opts = Q()
	}

	if opts.KR != nil {
		var scanner engine.RangeScanner
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
		var scanner engine.PrefixScanner
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
		var scanner engine.Scanner
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