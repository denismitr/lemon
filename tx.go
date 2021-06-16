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

func (x *Tx) Get(key string) (*Document, error) {
	d, err := x.e.FindByKey(key)
	if err != nil {
		if errors.Is(err, engine.ErrDocumentNotFound) {
			return nil, errors.Wrapf(ErrKeyDoesNotExist, "%s", key)
		}

		return nil, err
	}

	return newDocument(key, d.String()), nil
}

func (x *Tx) Insert(key string, data interface{}) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	if err := x.e.Add(key, data); err != nil {
		return err
	}
	return nil
}

func (x *Tx) InsertOrReplace(key string, data interface{}) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	if err := x.e.Add(key, data); err != nil {
		if errors.Is(err, engine.ErrKeyAlreadyExists) {
			if err := x.e.Replace(key, data); err != nil {
				return err
			}
		}

		return err
	}

	return nil
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
