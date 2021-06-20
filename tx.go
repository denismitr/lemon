package lemon

import (
	"context"
	"github.com/denismitr/lemon/internal/engine"
	"github.com/denismitr/lemon/options"
	"github.com/pkg/errors"
)

var ErrKeyDoesNotExist = errors.New("key does not exist in DB")
var ErrTxIsReadOnly = errors.New("transaction is read only")

type Tx struct {
	readOnly bool
	e *engine.Engine
	ctx context.Context
}

func (x *Tx) Get(key string) (*Document, error) {
	v, err := x.e.FindByKey(key)
	if err != nil {
		if errors.Is(err, engine.ErrDocumentNotFound) {
			return nil, errors.Wrapf(ErrKeyDoesNotExist, "%s", key)
		}

		return nil, err
	}

	return newDocument(key, v), nil
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
			if err := x.e.Update(key, data); err != nil {
				return err
			}
		}

		return err
	}

	return nil
}

func (x *Tx) Find(ctx context.Context, opts *options.FindOptions, dest *[]Document) error {
	if opts == nil {
		opts = options.Find()
	}

	if opts.KR != nil {
		var scanner engine.RangeScanner
		if opts.O == options.Ascend {
			scanner = x.e.ScanBetweenAscend
		} else {
			scanner = x.e.ScanBetweenDescend
		}

		if err := scanner(ctx, opts.KR.Lower, opts.KR.Upper, func(k string, v []byte) {
			*dest = append(*dest, createDocument(k, v))
		}); err != nil {
			return err
		}

		return nil
	} else if opts.Px != "" {
		var scanner engine.PrefixScanner
		if opts.O == options.Ascend {
			scanner = x.e.ScanPrefixAscend
		} else {
			scanner = x.e.ScanPrefixDescend
		}

		if err := scanner(ctx, opts.Px, func(k string, v []byte) {
			*dest = append(*dest, createDocument(k, v))
		}); err != nil {
			return err
		}

		return nil
	} else {
		var scanner engine.Scanner
		if opts.O == options.Ascend {
			scanner = x.e.ScanAscend
		} else {
			scanner = x.e.ScanDescend
		}

		if err := scanner(ctx, func(k string, v []byte) {
			*dest = append(*dest, createDocument(k, v))
		}); err != nil {
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