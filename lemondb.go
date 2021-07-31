package lemon

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
	"sync"
)

type DB struct {
	e *engine
	mu sync.RWMutex
	closed bool
}

type UserCallback func(tx *Tx) error

type Closer func() error
func NullCloser() error { return nil }

func New(path string) (*DB, Closer, error) {
	e, err := newEngine(path)
	if err != nil {
		return nil, NullCloser, err
	}

	db := DB{e: e}

	return &db, db.close, nil
}

func (db *DB) close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.e.close(); err != nil {
		return err
	}

	db.e = nil
	db.closed = true
	return nil
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

func (db *DB) View(ctx context.Context, cb UserCallback) error {
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

func (db *DB) Update(ctx context.Context, cb UserCallback) error {
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
