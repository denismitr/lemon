package lemon

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
)

type DB struct {
	e *engine
}

type UserCallback func(tx *Tx) error

type Closer func() error
func NullCloser() error { return nil }

func Open(path string, engineOptions ...EngineOptions) (*DB, Closer, error) {
	defaultCfg := Config{
		PersistenceStrategy: Sync,
		AutoVacuumIntervals: defaultAutovacuumIntervals,
		AutoVacuumMinSize: defaultAutoVacuumMinSize,
		AutoVacuumOnlyOnClose: false,
	}

	e, err := newEngine(path, defaultCfg)
	if err != nil {
		return nil, NullCloser, err
	}

	for _, optFn := range engineOptions {
		optFn(e)
	}

	db := DB{e: e}

	if err := e.init(); err != nil {
		return nil, NullCloser, err
	}

	return &db, db.close, nil
}

func (db *DB) close() error {
	if err := db.e.close(); err != nil {
		return err
	}

	db.e = nil
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
	return db.e.Count()
}

func (db *DB) View(ctx context.Context, cb UserCallback) error {
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
