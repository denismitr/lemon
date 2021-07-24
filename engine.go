package lemon

import (
	"context"
	"encoding/json"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"strings"
	"sync"
)

var ErrDocumentNotFound = errors.New("document not found")
var ErrKeyAlreadyExists = errors.New("key already exists")

type (
	ItemReceiver func(k string, v []byte, tags *Tags) bool

	rangeScanner func(
		ctx context.Context,
		lowerBoundPK string,
		upperBoundPK string,
		ir ItemReceiver,
		fo *filterOffsets,
	) error

	prefixScanner func(ctx context.Context, prefix string, ir ItemReceiver, fo *filterOffsets) error

	scanner func(ctx context.Context, ir ItemReceiver, fo *filterOffsets) error
)

type Engine struct {
	storage *jsonStorage
	pks     *btree.BTree
	bTags   boolIndex
	sTags   stringIndex
}

func newEngine(fullPath string) *Engine {
	s := newJsonStorage(fullPath)

	return &Engine{
		storage: s,
		pks:     btree.New(2),
		bTags:   make(boolIndex),
		sTags:   make(stringIndex),
	}
}

func (e *Engine) Init() error {
	if err := e.storage.load(); err != nil {
		return err
	}

	e.storage.iterate(func(o int, k string, v []byte, t *Tags) {
		e.pks.ReplaceOrInsert(&index{key: k, offset: o})

		if t != nil {
			for _, bt := range t.Booleans {
				e.bTags.add(bt.Name, bt.Value, o)
			}

			for _, st := range t.Strings {
				e.sTags.add(st.Name, st.Value, o)
			}
		}
	})

	return nil
}

func (e *Engine) persist() error {
	return e.storage.persist()
}

func (e *Engine) findByKey(pk string) ([]byte, *Tags, error) {
	offset, err := e.findOffsetByKey(pk)
	if err != nil {
		return nil, nil, err
	}

	v, err := e.storage.getValueAt(offset)
	if err != nil {
		return nil, nil, err
	}

	tags, err := e.storage.getTagsAt(offset)
	if err != nil {
		return nil, nil, err
	}

	return v, tags, nil
}

func (e *Engine) FindByKeys(pks []string, ir ItemReceiver) error {
	for _, k := range pks {
		offset, err := e.findOffsetByKey(k)
		if err != nil {
			continue
		}

		b, err := e.storage.getValueAt(offset)
		if err != nil {
			return err
		}

		tags, err := e.storage.getTagsAt(offset)
		if err != nil {
			return err
		}

		if next := ir(k, b, tags); !next {
			break
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

	if err := e.storage.removeAt(offset); err != nil {
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

func (e *Engine) Insert(key string, d interface{}, tags *Tags) error {
	_, err := e.findOffsetByKey(key)
	if err == nil {
		return errors.Wrapf(ErrKeyAlreadyExists, "%s", key)
	}

	v, err := serializeToValue(d)
	if err != nil {
		return err
	}

	offset := e.storage.append(key, v, tags)
	e.pks.ReplaceOrInsert(&index{key: key, offset: offset})

	if tags != nil {
		for _, t := range tags.Strings {
			e.sTags.add(t.Name, t.Value, offset)
		}

		for _, t := range tags.Booleans {
			e.bTags.add(t.Name, t.Value, offset)
		}
	}

	return nil
}

func (e *Engine) Update(key string, d interface{}, tags *Tags) error {
	offset, err := e.findOffsetByKey(key)
	if err != nil {
		return err
	}

	v, err := serializeToValue(d)
	if err != nil {
		return err
	}

	existingTags, err := e.storage.getTagsAt(offset)
	if err != nil {
		return err
	}

	if err := e.storage.replaceValueAt(offset, v, tags); err != nil {
		return err
	}

	// removing existing tags
	if existingTags != nil {
		for _, bt := range existingTags.Booleans {
			e.bTags.removeOffset(bt.Name, bt.Value, offset)
		}

		for _, st := range existingTags.Strings {
			e.sTags.removeOffset(st.Name, st.Value, offset)
		}
	}

	if tags != nil {
		for _, t := range tags.Strings {
			e.sTags.add(t.Name, t.Value, offset)
		}

		for _, t := range tags.Booleans {
			e.bTags.add(t.Name, t.Value, offset)
		}
	}

	return nil
}

func (e *Engine) Count() int {
	return e.storage.len()
}

func (e *Engine) scanBetweenDescend(
	ctx context.Context,
	from string,
	to string,
	ir ItemReceiver,
	fo *filterOffsets,
) (err error) {
	// Descend required a reverse order of `from` and `to`
	e.pks.DescendRange(&index{key: to}, &index{key: from}, func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if fo != nil && !fo.exists(idx.offset) {
			return true
		}

		v, vErr := e.storage.getValueAt(idx.offset)
		if vErr != nil {
			err = vErr
			return false
		}

		tags, tErr := e.storage.getTagsAt(idx.offset)
		if tErr != nil {
			err = tErr
			return false
		}

		return ir(idx.key, v, tags)
	})

	return
}

func (e *Engine) scanBetweenAscend(
	ctx context.Context,
	from string,
	to string,
	ir ItemReceiver,
	fo *filterOffsets,
) (err error) {
	e.pks.AscendRange(&index{key: from}, &index{key: to}, func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if fo != nil && !fo.exists(idx.offset) {
			return true
		}

		v, vErr := e.storage.getValueAt(idx.offset)
		if vErr != nil {
			err = vErr
			return false
		}

		tags, tErr := e.storage.getTagsAt(idx.offset)
		if tErr != nil {
			err = tErr
			return false
		}

		return ir(idx.key, v, tags)
	})

	return
}

func (e *Engine) scanPrefixAscend(
	ctx context.Context,
	prefix string,
	ir ItemReceiver,
	fo *filterOffsets,
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

		if fo != nil && !fo.exists(idx.offset) {
			return true
		}

		v, vErr := e.storage.getValueAt(idx.offset);
		if vErr != nil {
			err = vErr
			return false
		}

		tags, tErr := e.storage.getTagsAt(idx.offset)
		if tErr != nil {
			err = tErr
			return false
		}

		return ir(idx.key, v, tags)
	})

	return
}

func (e *Engine) scanPrefixDescend(
	ctx context.Context,
	prefix string,
	ir ItemReceiver,
	fo *filterOffsets,
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

		if fo != nil && !fo.exists(idx.offset) {
			return true
		}

		v, vErr := e.storage.getValueAt(idx.offset)
		if vErr != nil {
			err = vErr
			return false
		}

		tags, tErr := e.storage.getTagsAt(idx.offset)
		if tErr != nil {
			err = tErr
			return false
		}

		return ir(idx.key, v, tags)
	})

	return
}

type filterOffsets struct {
	sync.RWMutex
	offsets map[int]bool
}

func newFilterOffsets() *filterOffsets {
	return &filterOffsets{
		offsets: make(map[int]bool),
	}
}

func (fo *filterOffsets) add(offsets []int) {
	fo.Lock()
	defer fo.Unlock()
	for _, o := range offsets {
		fo.offsets[o] = true
	}
}

func (fo *filterOffsets) exists(offset int) bool {
	fo.RLock()
	defer fo.RUnlock()
	return fo.offsets[offset]
}

func (e *Engine) scanAscend(
	ctx context.Context,
	ir ItemReceiver,
	fo *filterOffsets,
) (err error) {
	e.pks.Ascend(func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if fo != nil && !fo.exists(idx.offset) {
			return true
		}

		v, vErr := e.storage.getValueAt(idx.offset)
		if vErr != nil {
			err = vErr
			return false
		}

		tags, tErr := e.storage.getTagsAt(idx.offset)
		if tErr != nil {
			err = tErr
			return false
		}

		return ir(idx.key, v, tags)
	})

	return
}

func (e *Engine) scanDescend(
	ctx context.Context,
	ir ItemReceiver,
	fo *filterOffsets,
) (err error) {
	e.pks.Descend(func(i btree.Item) bool {
		if ctx.Err() != nil {
			err = ctx.Err()
			return false
		}

		idx := i.(*index)
		if fo != nil && !fo.exists(idx.offset) {
			return true
		}

		v, vErr := e.storage.getValueAt(idx.offset)
		if vErr != nil {
			err = vErr
			return false
		}

		tags, tagsErr := e.storage.getTagsAt(idx.offset)
		if tagsErr != nil {
			err = tagsErr
			return false
		}

		return ir(idx.key, v, tags)
	})

	return
}

func (e *Engine) LastOffset() int {
	return e.storage.lastOffset()
}

func (e *Engine) getFilteredOffsets(tags *queryTags) *filterOffsets {
	if tags == nil {
		return nil
	}

	ft := newFilterOffsets()
	if tags.boolTags != nil && e.bTags != nil {
		for _, bt := range tags.boolTags {
			go ft.add(e.bTags.findOffsets(bt.Name, bt.Value))
		}
	}

	if tags.strTags != nil && e.sTags != nil {
		for _, st := range tags.strTags {
			go ft.add(e.sTags.findOffsets(st.Name, st.Value))
		}
	}

	return ft
}

func serializeToValue(d interface{}) ([]byte, error) {
	var v []byte
	if s, isStr := d.(string); isStr {
		v = []byte(s)
	} else {
		b, err := json.Marshal(d)
		if err != nil {
			return nil, errors.Wrapf(err, "could not marshal data %+Value", d)
		}
		v = b
	}

	return v, nil
}

