package lemon

import (
	"context"
	"fmt"
	"github.com/denismitr/glog"
	"github.com/pkg/errors"
	"path/filepath"
	"sync"
)

const InMemory = ":memory:"

type DB struct {
	e  executionEngine
	lg glog.Logger
	mu sync.RWMutex
}

var ErrInternalError = errors.New("LemonDB internal error")

type UserCallback func(tx *Tx) error

type Closer func() error

func NullCloser() error { return nil }

func Open(path string, engineOptions ...EngineOptions) (*DB, Closer, error) {
	defaultCfg := &Config{
		DisableAutoVacuum:     false,
		TruncateFileWhenOpen:  false,
		PersistenceStrategy:   Sync,
		ValueLoadStrategy:     EagerLoad,
		AutoVacuumIntervals:   defaultAutovacuumIntervals,
		AutoVacuumMinSize:     defaultAutoVacuumMinSize,
		AutoVacuumOnlyOnClose: true,
		Log:                   false,
	}

	if path == InMemory {
		defaultCfg.PersistenceStrategy = InMemory
	}

	var lg glog.Logger
	if defaultCfg.Log {
		lg = glog.NewStdoutLogger(glog.Prod, fmt.Sprintf("LemonDB:%s", filepath.Base(path)))
	} else {
		lg = glog.NullLogger{}
	}

	e, err := newDefaultEngine(path, lg, defaultCfg)
	if err != nil {
		return nil, NullCloser, err
	}

	for _, opt := range engineOptions {
		if err := opt.applyTo(e); err != nil {
			return nil, NullCloser, err
		}
	}

	db := DB{e: e, lg: lg}

	if err := e.init(); err != nil {
		return nil, NullCloser, err
	}

	return &db, db.close, nil
}

func (db *DB) close() error {
	if err := db.e.Close(context.Background()); err != nil {
		return err
	}

	db.e = nil
	return nil
}

func (db *DB) Begin(ctx context.Context, readOnly bool) (*Tx, error) {
	tx := Tx{
		ee:       db.e,
		lg:       db.lg,
		ctx:      ctx,
		readOnly: readOnly,
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

func (db *DB) CountByQuery(opts *QueryOptions) (int, error) {
	return db.CountByQueryContext(context.Background(), opts)
}

func (db *DB) CountByQueryContext(ctx context.Context, opts *QueryOptions) (int, error) {
	tx, err := db.Begin(ctx, true)
	if err != nil {
		return 0, err
	}

	count, err := tx.CountByQuery(opts)
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

func (db *DB) Vacuum(ctx context.Context) error {
	return db.e.Vacuum(ctx)
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

func (db *DB) MGetContext(ctx context.Context, keys ...string) (map[string]*Document, error) {
	var docs map[string]*Document
	if err := db.View(context.Background(), func(tx *Tx) error {
		result, err := tx.MGetContext(ctx, keys...)
		if err != nil {
			return err
		}
		docs = result
		return nil
	}); err != nil {
		return nil, err
	}

	if docs == nil {
		return nil, errors.Wrap(ErrInternalError, "result of MGet cannot be nil")
	}

	return docs, nil
}

func (db *DB) MGet(keys ...string) (map[string]*Document, error) {
	return db.MGetContext(context.Background(), keys...)
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

	if err := cb(tx); err != nil {
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

func (db *DB) FlushAll() error {
	return db.Update(context.Background(), func(tx *Tx) error {
		return tx.FlushAll()
	})
}

func (db *DB) FlushAllContext(ctx context.Context) error {
	return db.Update(ctx, func(tx *Tx) error {
		return tx.FlushAll()
	})
}

func (db *DB) Untag(key string, tagNames ...string) error {
	return db.Update(context.Background(), func(tx *Tx) error {
		return tx.Untag(key, tagNames...)
	})
}

func (db *DB) Tag(key string, m M) error {
	return db.Update(context.Background(), func(tx *Tx) error {
		return tx.Tag(key, m)
	})
}

func (db *DB) Scan(qo *QueryOptions, cb func(d *Document) bool) error {
	return db.ScanContext(context.Background(), qo, cb)
}

// ScanContext iterates documents with query options and receives iterator callback
// to go through filtered documents one by one
func (db *DB) ScanContext(ctx context.Context, qo *QueryOptions, cb func(d *Document) bool) error {
	return db.View(ctx, func(tx *Tx) error {
		return tx.Scan(qo, cb)
	})
}

func (db *DB) Find(qo *QueryOptions) ([]*Document, error) {
	return db.FindContext(context.Background(), qo)
}

func (db *DB) FindContext(ctx context.Context, qo *QueryOptions) ([]*Document, error) {
	var docs []*Document
	if err := db.View(ctx, func(tx *Tx) error {
		var err error
		docs, err = tx.Find(qo)
		return err
	}); err != nil {
		return nil, err
	}

	return docs, nil
}
