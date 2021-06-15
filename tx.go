package lemondb

import (
	"context"
	"fmt"
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

func (x *Tx) Get(key string) *Result {
	d, err := x.e.FindDocumentByKey(key)
	if err != nil {
		if errors.Is(err, engine.ErrDocumentNotFound) {
			return newErrorResult(key, errors.Wrapf(ErrKeyDoesNotExist, "%s", key))
		}

		return newErrorResult(key, err)
	}

	if d.Key != key {
		panic(fmt.Sprintf("how can keys not be equal %s != %s", d.Key, key))
	}

	return newSuccessResult(key, d.Value)
}

func (x *Tx) Insert(key string, data interface{}) error {
	if x.readOnly {
		return ErrTxIsReadOnly
	}

	if err := x.e.AddDocument(key, data); err != nil {
		return err
	}
	return nil
}
