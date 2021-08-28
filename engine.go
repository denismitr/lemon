package lemon

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/tidwall/btree"
	"strconv"
	"sync"
	"time"
)

var ErrKeyAlreadyExists = errors.New("key already exists")
//var ErrConflictingTagType = errors.New("conflicting tag type")

const castPanic = "how could primary keys item not be of type *entry"

type (
	entryIterator func(ent *entry) bool

	scanner func(
		ctx context.Context,
		q *queryOptions,
		fe *filterEntries,
		ir entryIterator,
	) error
)

type engine struct {
	dbFile        string
	cfg           *Config
	persistence   *persistence
	pks           *btree.BTree
	tags          *tagIndex
	stopCh        chan struct{}
	runningVacuum bool
	mu            sync.RWMutex
	totalDeletes  uint64
	closed        bool
}

func newEngine(dbFile string, cfg *Config) (*engine, error) {
	e := &engine{
		dbFile: dbFile,
		pks:    btree.NewNonConcurrent(byPrimaryKeys),
		tags:   newTagIndex(),
		stopCh: make(chan struct{}, 1),
		cfg:    cfg,
	}

	return e, nil
}

func (e *engine) asyncFlush(d time.Duration) {
	t := time.NewTicker(d)

	for {
		select {
		case <-e.stopCh:
			t.Stop()
			return
		case <-t.C:
			e.mu.Lock()
			if err := e.persistence.sync(); err != nil {
				panic(err)
			}
			e.mu.Unlock()
		}
	}
}

func (e *engine) scheduleVacuum(d time.Duration) {
	t := time.NewTicker(d)

	for {
		select {
		case <-e.stopCh:
			t.Stop()
			return
		case <-t.C:
			e.mu.Lock()
			if e.runningVacuum && e.totalDeletes < e.cfg.AutoVacuumMinSize {
				e.mu.Unlock()
				continue
			}

			e.runningVacuum = true
			if err := e.runVacuumUnderLock(); err != nil {
				panic(err)
			}
			e.runningVacuum = false
			e.mu.Unlock()
		}
	}
}

func (e *engine) runVacuumUnderLock() error {
	buf := &bytes.Buffer{}

	e.pks.Ascend(nil, func(i interface{}) bool {
		i.(*entry).serialize(buf)
		return true
	})

	if err := e.persistence.writeAndSwap(buf); err != nil {
		return err
	}

	return nil
}

var ErrDatabaseAlreadyClosed = errors.New("database already closed")

func (e *engine) close() error {
	e.mu.Lock()

	if e.closed {
		return ErrDatabaseAlreadyClosed
	}

	if !e.cfg.DisableAutoVacuum {
		if err := e.runVacuumUnderLock(); err != nil {
			return err
		}
	}

	defer func() {
		e.pks = nil
		e.tags = nil
		e.closed = true
		e.persistence = nil
		e.mu.Unlock()
	}()

	close(e.stopCh)

	if e.persistence != nil {
		return e.persistence.close()
	}

	return nil
}

func (e *engine) init() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.dbFile != ":memory:" {
		p, err := newPersistence(e.dbFile, e.cfg.PersistenceStrategy)
		if err != nil {
			return err
		}
		e.persistence = p

		if err := e.persistence.load(func(d deserializer) error {
			return d.deserialize(e)
		}); err != nil {
			return err
		}

		if e.cfg.PersistenceStrategy == Async {
			go e.asyncFlush(e.cfg.AsyncPersistenceIntervals)
		}

		if !e.cfg.DisableAutoVacuum && !e.cfg.AutoVacuumOnlyOnClose {
			go e.scheduleVacuum(e.cfg.AutoVacuumIntervals)
		}
	}

	return nil
}

func (e *engine) insert(ent *entry) error {
	existing := e.pks.Set(ent)
	if existing != nil {
		return errors.Wrapf(ErrKeyAlreadyExists, "key: %s", ent.key.String())
	}

	if ent.tags != nil {
		if err := e.setEntityTagsUnderLock(ent); err != nil {
			return err
		}
	}

	return nil
}

func (e *engine) findByKeyUnderLock(key string) (*entry, error) {
	found := e.pks.Get(&entry{key: newPK(key)})
	if found == nil {
		return nil, errors.Wrapf(ErrKeyDoesNotExist, "key %s does not exist in database", key)
	}

	ent, ok := found.(*entry)
	if !ok {
		panic(castPanic)
	}

	return ent, nil
}

func (e *engine) findByKeys(pks []string, ir entryIterator) error {
	resultCh := make(chan *entry)
	var wg sync.WaitGroup

	for _, k := range pks {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()

			found, err := e.findByKeyUnderLock(k)
			if err != nil {
				// todo: log
				//return errors.Wrapf(ErrKeyDoesNotExist, "key %s does not exist in database", k)
			} else {
				resultCh <- found
			}
		}(k)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for ent := range resultCh {
		if next := ir(ent); !next {
			break
		}
	}

	return nil
}

func (e *engine) remove(key PK) error {
	ent := e.pks.Get(&entry{key: key})
	if ent == nil {
		return errors.Wrapf(ErrKeyDoesNotExist, "key %s does not exist in DB", key.String())
	}

	e.totalDeletes++
	e.pks.Delete(&entry{key: key})

	return nil
}

func (e *engine) removeUnderLock(key PK) error {
	ent := e.pks.Get(&entry{key: key})
	if ent == nil {
		return errors.Wrapf(ErrKeyDoesNotExist, "key %s does not exist in DB", key.String())
	}

	e.totalDeletes++
	e.pks.Delete(&entry{key: key})

	return nil
}

func (e *engine) setEntityTagsUnderLock(ent *entry) error {
	for n, v := range ent.tags.booleans {
		if err := e.tags.add(n, v, ent); err != nil {
			return err
		}
	}

	for n, v := range ent.tags.strings {
		if err := e.tags.add(n, v, ent); err != nil {
			return err
		}
	}

	for n, v := range ent.tags.integers {
		if err := e.tags.add(n, v, ent); err != nil {
			return err
		}
	}

	for n, v := range ent.tags.floats {
		if err := e.tags.add(n, v, ent); err != nil {
			return err
		}
	}

	return nil
}

func (e *engine) clearEntityTagsUnderLock(ent *entry) {
	e.tags.removeEntry(ent)
}

func (e *engine) Count() int {
	return e.pks.Len()
}

func (e *engine) scanBetweenDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryIterator,
) (err error) {
	// Descend required a reverse order of `from` and `to`
	descendRange(
		e.pks,
		&entry{key: newPK(q.keyRange.From)},
		&entry{key: newPK(q.keyRange.To)},
		filteringBTreeIterator(ctx, fe, q, ir),
	)

	return
}

func (e *engine) scanBetweenAscend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryIterator,
) (err error) {
	ascendRange(
		e.pks,
		&entry{key: newPK(q.keyRange.From)},
		&entry{key: newPK(q.keyRange.To)},
		filteringBTreeIterator(ctx, fe, q, ir),
	)

	return
}

func (e *engine) scanPrefixAscend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryIterator,
) (err error) {
	e.pks.Ascend(&entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, fe, q, ir))

	return
}

func (e *engine) scanPrefixDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryIterator,
) (err error) {
	descendGreaterThan(e.pks, &entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) scanAscend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryIterator,
) (err error) {
	e.pks.Ascend(nil, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) scanDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryIterator,
) (err error) {
	e.pks.Descend(nil, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) filterEntities(q *queryOptions) *filterEntries {
	if q == nil || q.allTags == nil {
		return nil
	}

	ft := newFilterEntries(q.patterns)

	for tk, v := range q.allTags.booleans {
		if e.tags.data[tk.name] == nil {
			continue
		}

		e.tags.filterEntities(tk, v, ft)
	}

	for tk, v := range q.allTags.strings {
		if e.tags.data[tk.name] == nil {
			continue
		}

		e.tags.filterEntities(tk, v, ft)
	}

	for tk, v := range q.allTags.integers {
		if e.tags.data[tk.name] == nil {
			continue
		}

		e.tags.filterEntities(tk, v, ft)
	}

	for tk, v := range q.allTags.floats {
		if e.tags.data[tk.name] == nil {
			continue
		}

		e.tags.filterEntities(tk, v, ft)
	}

	return ft
}

// upsertTagUnderLock - updates or inserts a new tag to entity and secondary index
func (e *engine) upsertTagUnderLock(name string, v interface{}, ent *entry) error {
	// just a precaution
	if ent.tags == nil {
		ent.tags = newTags()
	}

	// if tag name exists in entity, remove it from secondary index
	// and remove it from entity itself
	existingTagType, ok := ent.tags.names[name]
	if ok {
		if err := e.tags.mustRemoveEntryByNameAndValue(name, v, ent); err != nil {
			return err
		}

		switch existingTagType {
		case boolDataType:
			delete(ent.tags.booleans, name)
		case intDataType:
			delete(ent.tags.integers, name)
		case floatDataType:
			delete(ent.tags.floats, name)
		case strDataType:
			delete(ent.tags.strings, name)
		}
	}

	// do type check of value
	// same name may now contain a different value type
	switch typedValue := v.(type) {
	case int:
		ent.tags.integers[name] = typedValue
	case bool:
		ent.tags.booleans[name] = typedValue
	case string:
		ent.tags.strings[name] = typedValue
	case float64:
		ent.tags.floats[name] = typedValue
	}

	// add to secondary index
	// todo: avoid another type cast in the add method
	return e.tags.add(name, v, ent)
}

// removeTagUnderLock - removes a tag from entity and secondary index
func (e *engine) removeTagUnderLock(name string, ent *entry) error {
	// if tag name exists in entity, remove it from secondary index
	// and remove it from entity itself
	existingTagType, ok := ent.tags.names[name]
	if ok {
		switch existingTagType {
		case boolDataType:
			e.tags.removeEntryByNameAndType(name, boolDataType, ent)
			delete(ent.tags.booleans, name)
		case intDataType:
			e.tags.removeEntryByNameAndType(name, intDataType, ent)
			delete(ent.tags.integers, name)
		case floatDataType:
			e.tags.removeEntryByNameAndType(name, floatDataType, ent)
			delete(ent.tags.floats, name)
		case strDataType:
			e.tags.removeEntryByNameAndType(name, strDataType, ent)
			delete(ent.tags.strings, name)
		}
	}

	return nil
}

func (e *engine) put(ent *entry, replace bool) error {
	existing := e.pks.Set(ent)
	if existing != nil {
		if !replace {
			_ = e.pks.Set(existing)
			return errors.Wrapf(ErrKeyAlreadyExists, "key %s", ent.key.String())
		}

		existingEnt, ok := existing.(*entry)
		if !ok {
			panic(castPanic)
		}

		if existingEnt.tags != nil {
			e.clearEntityTagsUnderLock(ent)
		}
	}

	if ent.tags != nil {
		if err := e.setEntityTagsUnderLock(ent); err != nil {
			return err
		}
	}

	return nil
}

func (e *engine) flushAll(ff func (ent *entry)) error {
	e.pks.Ascend(nil, func (i interface{}) bool {
		ent := i.(*entry)
		ff(ent)
		return true
	})

	e.pks = btree.NewNonConcurrent(byPrimaryKeys)
	e.tags = newTagIndex()

	return nil
}

func filteringBTreeIterator(
	ctx context.Context,
	fe *filterEntries,
	q *queryOptions,
	ir entryIterator,
) func(item interface{}) bool {
	return func(item interface{}) bool {
		if ctx.Err() != nil {
			return false
		}

		ent, ok := item.(*entry)
		if !ok {
			panic(castPanic)
		}

		if fe != nil && (!q.matchTags(ent) || !ent.key.Match(q.patterns)) {
			return true
		}

		return ir(ent)
	}
}

func serializeToValue(d interface{}) ([]byte, error) {
	switch typedValue := d.(type) {
	case []byte:
		return typedValue, nil
	case int:
		return []byte(strconv.Itoa(typedValue)), nil // fixme: probably we do not want pure ints as values
	case string:
		return []byte(typedValue), nil
	}

	b, err := json.Marshal(d)
	if err != nil {
		return nil, errors.Wrapf(err, "could not marshal data %+v value", d)
	}

	return b, nil
}
