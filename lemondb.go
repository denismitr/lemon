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
	defaultCfg := &Config{
		DisableAutoVacuum:     false,
		TruncateFileWhenOpen:  false,
		PersistenceStrategy:   Sync,
		AutoVacuumIntervals:   defaultAutovacuumIntervals,
		AutoVacuumMinSize:     defaultAutoVacuumMinSize,
		AutoVacuumOnlyOnClose: true,
	}

	e, err := newEngine(path, defaultCfg)
	if err != nil {
		return nil, NullCloser, err
	}

	for _, opt := range engineOptions {
		if err := opt.applyTo(e); err != nil {
			return nil, NullCloser, err
		}
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
		e:        db.e,
		ctx:      ctx,
		readOnly: readOnly,
		buf:      &bytes.Buffer{},
	}

	tx.lock()

	return &tx, nil
}

func (db *DB) Count() int {
	tx, err := db.Begin(context.Background(), true)
	if err != nil {
		return 0
	}

	count := tx.Count()

	if err := tx.Commit(); err != nil {
		return 0
	}

	return count
}

func (db *DB) CountByQuery(ctx context.Context, opts *QueryOptions) (int, error) {
	tx, err := db.Begin(ctx, true)
	if err != nil {
		return 0, err
	}

	count, err := tx.CountByQuery(ctx, opts)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return count, nil
}

func (db *DB) Has(key string) bool {
	var result bool
	_ = db.View(context.Background(), func(tx *Tx) error {
		result = tx.Has(key)
		return nil
	})
	return result
}

func (db *DB) Vacuum() error {
	db.e.mu.Lock()
	defer db.e.mu.Unlock()

	if err := db.e.runVacuumUnderLock(); err != nil {
		return err
	}

	return nil
}

func (db *DB) Get(key string) (*Document, error) {
	var doc *Document
	err := db.View(context.Background(), func(tx *Tx) error {
		d, err := tx.Get(key)
		if err != nil {
			return err
		}
		doc = d
		return nil
	})

	return doc, err
}

func (db *DB) MGet(keys ...string) (map[string]*Document, error) {
	var docs map[string]*Document
	if err := db.View(context.Background(), func(tx *Tx) error {
		result, err := tx.MGet(keys...)
		if err != nil {
			return err
		}
		docs = result
		return nil
	}); err != nil {
		return nil, err
	}

	if docs == nil {
		panic("how can result be nil?")
	}

	return docs, nil
}

func (db *DB) Insert(key string, data interface{}, metaAppliers ...MetaApplier) error {
	return db.Update(context.Background(), func(tx *Tx) error {
		return tx.Insert(key, data, metaAppliers...)
	})
}

func (db *DB) InsertOrReplace(key string, data interface{}, metaAppliers ...MetaApplier) error {
	return db.Update(context.Background(), func(tx *Tx) error {
		return tx.InsertOrReplace(key, data, metaAppliers...)
	})
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

func (db *DB) FlushAll(ctx context.Context) error {
	return db.Update(ctx, func(tx *Tx) error {
		return tx.FlushAll()
	})
}
