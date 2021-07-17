package lemon

import (
	"context"
	"encoding/json"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"strings"
)

var ErrDocumentNotFound = errors.New("document not found")
var ErrKeyAlreadyExists = errors.New("key already exists")

type ItemReceiver func(k string, v []byte) bool
type RangeScanner func(ctx context.Context, lowerBoundPK string, upperBoundPK string, ir ItemReceiver) error
type PrefixScanner func(ctx context.Context, prefix string, ir ItemReceiver) error
type Scanner func(ctx context.Context, ir ItemReceiver) error

type Engine struct {
	s     *jsonStorage
	pks   *btree.BTree
	bTags *btree.BTree
}

func newEngine(fullPath string) *Engine {
	s := newJsonStorage(fullPath)

	return &Engine{
		s:     s,
		pks:   btree.New(2),
		bTags: btree.New(2),
	}
}

func (e *Engine) Init() error {
	if err := e.s.load(); err != nil {
		return err
	}

	e.s.iterate(func(o int, k string, v []byte, t *Tags) {
		e.pks.ReplaceOrInsert(&index{key: k, offset: o})

		if t != nil {
			for i := range t.Booleans {
				e.bTags.ReplaceOrInsert(NewBoolTagIndex(t.Booleans[i].K, t.Booleans[i].V, o))
			}
		}
	})

	return nil
}

func (e *Engine) Persist() error {
	return e.s.persist()
}

func (e *Engine) FindByKey(pk string) ([]byte, error) {
	offset, err := e.findOffsetByKey(pk)
	if err != nil {
		return nil, err
	}

	return e.s.getValueAt(offset)
}

func (e *Engine) FindByKeys(pks []string, ir ItemReceiver) error {
	for _, k := range pks {
		offset, err := e.findOffsetByKey(k)
		if err != nil {
			continue
		}

		if b, vErr := e.s.getValueAt(offset); vErr != nil {
			return vErr
		} else {
			if next := ir(k, b); !next {
				break
			}
		}
	}

	return nil
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

	if err := e.s.removeAt(offset); err != nil {
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

func (e *Engine) Insert(key string, d interface{}, tags Tags) error {
	_, err := e.findOffsetByKey(key)
	if err == nil {
		return errors.Wrapf(ErrKeyAlreadyExists, "%s", key)
	}

	v, err := serializeToValue(d)
	if err != nil {
		return err
	}

	nextOffset := e.s.nextOffset()
	var tagSetters []TagSetter
	for _, t := range tags.Booleans {
		t.setOffset(nextOffset)
		e.bTags.ReplaceOrInsert(&t)
		tagSetters = append(tagSetters, BoolTagSetter(t.K, t.V))
	}

	e.s.append(key, v, tagSetters...)

	e.pks.ReplaceOrInsert(&index{key: key, offset: nextOffset})

	return nil
}

func (e *Engine) Update(key string, d interface{}, tags Tags) error {
	offset, err := e.findOffsetByKey(key)
	if err != nil {
		return err
	}

	v, err := serializeToValue(d)
	if err != nil {
		return err
	}

	var tagSetters []TagSetter
	for _, t := range tags.Booleans {
		t.setOffset(offset)
		e.bTags.ReplaceOrInsert(&t)
		tagSetters = append(tagSetters, BoolTagSetter(t.K, t.V))
	}

	if err := e.s.replaceValueAt(offset, v, tagSetters...); err != nil {
		return err
	}

	return nil
}

func (e *Engine) Count() int {
	return e.s.len()
}

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
		v, getErr := e.s.getValueAt(idx.offset)
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
		if v, getErr := e.s.getValueAt(idx.offset); getErr != nil {
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

		if v, getErr := e.s.getValueAt(idx.offset); getErr != nil {
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

		if v, getErr := e.s.getValueAt(idx.offset); getErr != nil {
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
		if v, getErr := e.s.getValueAt(idx.offset); getErr != nil {
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
		if v, getErr := e.s.getValueAt(idx.offset); getErr != nil {
			err = getErr
			return false
		} else {
			return ir(idx.key, v)
		}
	})

	return
}

func (e *Engine) LastOffset() int {
	return e.s.lastOffset()
}

func serializeToValue(d interface{}) ([]byte, error) {
	var v []byte
	if s, isStr := d.(string); isStr {
		v = []byte(s)
	} else {
		b, err := json.Marshal(d)
		if err != nil {
			return nil, errors.Wrapf(err, "could not marshal data %+V", d)
		}
		v = b
	}

	return v, nil
}

