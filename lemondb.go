package lemon

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
	"sync"
)

type LemonDB struct {
	e *Engine
	mu sync.RWMutex
}

type UserCallback func(tx *Tx) error

func New(path string) (*LemonDB, error) {
	e, err := newEngine(path)
	if err != nil {
		return nil, err
	}

	if initErr := e.init(); initErr != nil {
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

	tx := Tx{e: db.e, ctx: ctx, readOnly: true, buf: &bytes.Buffer{}}
	err := cb(&tx)
	if err != nil {
		return errors.Wrap(err, "db read failed")
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (db *LemonDB) MultiUpdate(ctx context.Context, cb UserCallback) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx := Tx{e: db.e, ctx: ctx, readOnly: false, buf: &bytes.Buffer{}}
	err := cb(&tx)
	if err != nil {
		// todo: rollback
		return errors.Wrap(err, "db write failed")
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
