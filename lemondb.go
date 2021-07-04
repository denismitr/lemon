package lemon

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

func New(path string) (*LemonDB, error) {
	s := jsonstorage.New(path)
	e := engine.New(s)

	if initErr := e.Init(); initErr != nil {
		return nil, initErr
	}

	return &LemonDB{e: e}, nil
}

func (db *LemonDB) Count() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.e.Count()
}

func (db *LemonDB) MultiRead(ctx context.Context, cb UserCallback) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tx := Tx{e: db.e, ctx: ctx, readOnly: true}
	err := cb(&tx)
	if err != nil {
		return errors.Wrap(err, "db read failed")
	}

	return nil
}

func (db *LemonDB) MultiUpdate(ctx context.Context, cb UserCallback) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx := Tx{e: db.e, ctx: ctx, readOnly: false}
	err := cb(&tx)
	if err != nil {
		return errors.Wrap(err, "db write failed")
	}

	if err := db.e.Persist(); err != nil {
		return err
	}

	return nil
}
