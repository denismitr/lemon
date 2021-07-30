package lemon

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
	"sync"
)

type DB struct {
	e *Engine
	mu sync.RWMutex
}

type UserCallback func(tx *Tx) error

func New(path string) (*DB, error) {
	e, err := newEngine(path)
	if err != nil {
		return nil, err
	}

	if initErr := e.init(); initErr != nil {
		return nil, initErr
	}

	return &DB{e: e}, nil
}

func (db *DB) Begin(ctx context.Context, readOnly bool) (*Tx, error) {
	tx := Tx{
		e: db.e,
		ctx: ctx,
		readOnly: readOnly,
		buf: &bytes.Buffer{},
	}

	return &tx, nil
}

func (db *DB) Count() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.e.Count()
}

func (db *DB) MultiRead(ctx context.Context, cb UserCallback) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tx, err := db.Begin(ctx, true)
	if err != nil {
		return err
	}

	err = cb(tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return errors.Wrap(err, rbErr.Error())
		}

		return errors.Wrap(err, "db read failed. rolled back")
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (db *DB) MultiUpdate(ctx context.Context, cb UserCallback) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tx, err := db.Begin(ctx, false)
	if err != nil {
		return err
	}

	err = cb(tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return errors.Wrap(err, rbErr.Error())
		}

		return errors.Wrap(err, "db write failed. rolled back")
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
