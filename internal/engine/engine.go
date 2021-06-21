package engine

import (
	"context"
	"encoding/json"
	"github.com/denismitr/lemon/internal/storage"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"strings"
)

var ErrDocumentNotFound = errors.New("document not found")
var ErrKeyAlreadyExists = errors.New("key already exists")

type Engine struct {
	s storage.Storage
	pks *btree.BTree
}

func New(s storage.Storage) *Engine {
	return &Engine{
		s: s,
		pks: btree.New(2),
	}
}

func (e *Engine) Init() error {
	if err := e.s.Load(); err != nil {
		return err
	}

	pks := e.s.PKs()
	for i := range pks  {
		e.pks.ReplaceOrInsert(&index{key: pks[i], offset: i})
	}

	return nil
}

func (e *Engine) Persist() error {
	return e.s.Persist()
}

func (e *Engine) FindByKey(pk string) ([]byte, error) {
	offset, err := e.findOffsetByKey(pk)
	if err != nil {
		return nil, err
	}

	return e.s.GetValueAt(offset)
}

func (e *Engine) findOffsetByKey(key string) (int, error) {
	item := e.pks.Get(&index{key: key})
	if item == nil {
		return 0, errors.Wrapf(ErrDocumentNotFound, "search by primary key %s", key)
	}

	found := item.(*index)

	return found.offset, nil
}

func (e *Engine) RemoveByKeys(pks ...string) error {
	for _, pk := range pks {
		if err := e.removeByKeyFromDataModel(pk); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) removeByKeyFromDataModel(key string) error {
	offset, err := e.findOffsetByKey(key)
	if err != nil {
		return err
	}

	if err := e.s.RemoveAt(offset); err != nil {
		return err
	}

	e.pks.Delete(&index{key: key})

	e.pks.Ascend(func(i btree.Item) bool {
		pk := i.(*index)
		if pk.offset > offset {
			pk.offset--
		}
		return true
	})

	return nil
}

func (e *Engine) Insert(key string, d interface{}) error {
	_, err := e.findOffsetByKey(key)
	if err == nil {
		return errors.Wrapf(ErrKeyAlreadyExists, "%s", key)
	}

	v, err := serializeToValue(d)
	if err != nil {
		return err
	}

	e.s.Append(key, v)
	e.pks.ReplaceOrInsert(&index{key: key, offset: e.s.LastOffset()})

	return nil
}

func (e *Engine) Update(key string, d interface{}) error {
	 offset, err := e.findOffsetByKey(key)
	 if err != nil {
		return err
	}

	v, err := serializeToValue(d)
	if err != nil {
		return err
	}

	if err := e.s.ReplaceValueAt(offset, v); err != nil {
		return err
	}

	return nil
}

func (e *Engine) Count() int {
	return e.s.Len()
}

type ItemReceiver func(k string, v []byte) bool
type RangeScanner func(ctx context.Context, lowerBoundPK string, upperBoundPK string, ir ItemReceiver) error
type PrefixScanner func(ctx context.Context, prefix string, ir ItemReceiver) error
type Scanner func(ctx context.Context, ir ItemReceiver) error

func (e *Engine) ScanBetweenDescend(
	ctx context.Context,
	from string,
	to string,
	ir ItemReceiver,
) (err error) {
	// Descend required a reverse order of `from` and `to`
	e.pks.DescendRange(&index{key: to}, &index{key: from}, func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		v, getErr := e.s.GetValueAt(idx.offset);
		if getErr != nil {
			err = getErr
			return false
		}

		return ir(idx.key, v)
	})

	return
}

func (e *Engine) ScanBetweenAscend(
	ctx context.Context,
	from string,
	to string,
	ir ItemReceiver,
) (err error) {
	e.pks.AscendRange(&index{key: from}, &index{key: to}, func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if v, getErr := e.s.GetValueAt(idx.offset); getErr != nil {
			err = getErr
			return false
		} else {
			return ir(idx.key, v)
		}
	})

	return
}

func (e *Engine) ScanPrefixAscend(
	ctx context.Context,
	prefix string,
	ir ItemReceiver,
) (err error) {
	e.pks.AscendGreaterOrEqual(&index{key: prefix}, func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if !strings.HasPrefix(idx.key, prefix) {
			return false
		}

		if v, getErr := e.s.GetValueAt(idx.offset); getErr != nil {
			err = getErr
			return false
		} else {
			ir(idx.key, v)
		}
		return true
	})

	return
}

func (e *Engine) ScanPrefixDescend(
	ctx context.Context,
	prefix string,
	ir ItemReceiver,
) (err error) {
	e.pks.DescendGreaterThan(&index{key: prefix}, func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if !strings.HasPrefix(idx.key, prefix) {
			return false
		}

		if v, getErr := e.s.GetValueAt(idx.offset); getErr != nil {
			err = getErr
			return false
		} else {
			return ir(idx.key, v)
		}
	})

	return
}

func (e *Engine) ScanAscend(
	ctx context.Context,
	ir ItemReceiver,
) (err error) {
	e.pks.Ascend(func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if v, getErr := e.s.GetValueAt(idx.offset); getErr != nil {
			err = getErr
			return false
		} else {
			return ir(idx.key, v)
		}
	})

	return
}

func (e *Engine) ScanDescend(
	ctx context.Context,
	ir ItemReceiver,
) (err error) {
	e.pks.Descend(func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if v, getErr := e.s.GetValueAt(idx.offset); getErr != nil {
			err = getErr
			return false
		} else {
			return ir(idx.key, v)
		}
	})

	return
}

func serializeToValue(d interface{}) ([]byte, error) {
	var v []byte
	if s, isStr := d.(string); isStr {
		v = []byte(s)
	} else {
		b, err := json.Marshal(d)
		if err != nil {
			return nil, errors.Wrapf(err, "could not marshal data %+v", d)
		}
		v = b
	}

	return v, nil
}