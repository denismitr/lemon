package lemondb

import (
	"context"
	"github.com/denismitr/lemon/internal/engine"
	"github.com/denismitr/lemon/internal/storage/jsonstorage"
	"github.com/pkg/errors"
	"sync"
)

type LemonDB struct {
	e *engine.Engine
	mu sync.RWMutex
}

type UserCallback func(tx *Tx) error

func New(path string) (*LemonDB, func() error, error) {
	f, err := jsonstorage.OpenOrCreate(path)
	if err != nil {
		return nil, nil, err
	}

	closer := func() error {
		if err := f.Sync(); err != nil {
			return errors.Wrap(err, "could not sync file before closing")
		}

		if err := f.Close(); err != nil {
			return errors.Wrap(err, "could not close file")
		}

		return nil
	}

	s := jsonstorage.NewJSONStorage(f)
	e := engine.New(s)

	if initErr := e.Init(); initErr != nil {
		if cErr := closer(); cErr != nil {
			return nil, nil, errors.Wrap(initErr, cErr.Error())
		}

		return nil, nil, initErr
	}

	return &LemonDB{
		e: e,
	}, closer, nil
}

func (db *LemonDB) ReadTx(ctx context.Context, cb UserCallback) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tx := Tx{e: db.e, ctx: ctx, readOnly: true}
	err := cb(&tx)
	if err != nil {
		return errors.Wrap(err, "db read failed")
	}

	return nil
}

func (db *LemonDB) UpdateTx(ctx context.Context, cb UserCallback) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx := Tx{e: db.e, ctx: ctx, readOnly: false}
	err := cb(&tx)
	if err != nil {
		return errors.Wrap(err, "db write failed")
	}

	return nil
}
