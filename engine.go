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
var ErrDatabaseAlreadyClosed = errors.New("database already closed")
var ErrTagKeyNotFound = errors.New("tag key not found")

const castPanic = "how could primary keys item not be of type *entry"

type (
	entryIterator func(ent *entry) bool
	scanner       func(ctx context.Context, q *QueryOptions, ir entryIterator) error
)

type rwLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

type engine interface {
	rwLocker

	Persist(commands []serializer) error
	RemoveTag(name string, ent *entry) error
	Close(ctx context.Context) error
	Insert(ent *entry) error
	Exists(key string) bool
	FindByKey(key string) (*entry, error)
	IterateByKeys(pks []string, ir entryIterator) error
	Remove(key PK) error
	RemoveEntryFromTagsByNameAndType(name string, dt indexType, ent *entry)
	AddTag(name string, value interface{}, ent *entry) error
	Count() int
	Put(ent *entry, replace bool) error
	FlushAll(ff func(ent *entry)) error
	Vacuum(ctx context.Context) error
	UpsertTag(name string, v interface{}, ent *entry) error
	FilterEntriesByTags(q *QueryOptions) (*filterEntriesSink, error)
	ChooseBestScanner(q *QueryOptions) (scanner, error)
	RemoveEntry(ent *entry)
	SetCfg(cfg *Config)
}

type defaultEngine struct {
	sync.RWMutex

	dbFile        string
	cfg           *Config
	persistence   *persistence
	pks           *btree.BTree
	tags          *tagIndex
	stopCh        chan struct{}
	runningVacuum bool
	totalDeletes  uint64
	closed        bool
}

func (e *defaultEngine) SetCfg(cfg *Config) {
	e.cfg = cfg
}

func (e *defaultEngine) RemoveEntry(ent *entry) {
	e.tags.removeEntry(ent)
	e.pks.Delete(ent)
}

func newDefaultEngine(dbFile string, cfg *Config) (*defaultEngine, error) {
	e := &defaultEngine{
		dbFile: dbFile,
		pks:    btree.NewNonConcurrent(byPrimaryKeys),
		tags:   newTagIndex(),
		stopCh: make(chan struct{}, 1),
		cfg:    cfg,
	}

	return e, nil
}

func (e *defaultEngine) asyncFlush(d time.Duration) {
	t := time.NewTicker(d)

	for {
		select {
		case <-e.stopCh:
			t.Stop()
			return
		case <-t.C:
			e.Lock()
			if err := e.persistence.sync(); err != nil {
				panic(err)
			}
			e.Unlock()
		}
	}
}

func (e *defaultEngine) scheduleVacuum(d time.Duration) {
	t := time.NewTicker(d)

	for {
		select {
		case <-e.stopCh:
			t.Stop()
			return
		case <-t.C:
			e.Lock()
			if e.runningVacuum && e.totalDeletes < e.cfg.AutoVacuumMinSize {
				e.Unlock()
				continue
			}

			e.runningVacuum = true
			// todo: maybe limit run vacuum with context timeout equal to d
			if err := e.runVacuumUnderLock(context.Background()); err != nil {
				panic(err)
			}
			e.runningVacuum = false
			e.Unlock()
		}
	}
}

func (e *defaultEngine) runVacuumUnderLock(ctx context.Context) error {
	if e.persistence == nil {
		return nil
	}

	buf := &bytes.Buffer{}

	e.pks.Ascend(nil, func(i interface{}) bool {
		if err := ctx.Err(); err != nil {
			return false
		}

		i.(*entry).serialize(buf)
		return true
	})

	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "could not finish vacuum")
	}

	if err := e.persistence.writeAndSwap(buf); err != nil {
		return err
	}

	return nil
}

func (e *defaultEngine) Close(ctx context.Context) error {
	e.Lock()

	if e.closed {
		return ErrDatabaseAlreadyClosed
	}

	if !e.cfg.DisableAutoVacuum {
		if err := e.runVacuumUnderLock(ctx); err != nil {
			return err
		}
	}

	defer func() {
		e.pks = nil
		e.tags = nil
		e.closed = true
		e.persistence = nil
		e.Unlock()
	}()

	close(e.stopCh)

	if e.persistence != nil {
		return e.persistence.close()
	}

	return nil
}

func (e *defaultEngine) init() error {
	e.Lock()
	defer e.Unlock()

	if e.dbFile != InMemory {
		p, err := newPersistence(e.dbFile, e.cfg.PersistenceStrategy, e.cfg.TruncateFileWhenOpen)
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

func (e *defaultEngine) Persist(commands []serializer) error {
	if e.persistence == nil {
		return nil
	}

	var buf bytes.Buffer
	for _, cmd := range commands {
		cmd.serialize(&buf)
	}

	if err := e.persistence.write(&buf); err != nil {
		return err
	}

	return nil
}

func (e *defaultEngine) RemoveEntryFromTagsByNameAndType(name string, dt indexType, ent *entry) {
	e.tags.removeEntryByNameAndType(name, dt, ent)
}

func (e *defaultEngine) AddTag(name string, value interface{}, ent *entry) error {
	return e.tags.add(name, value, ent)
}

func (e *defaultEngine) Insert(ent *entry) error {
	existing := e.pks.Set(ent)
	if existing != nil {
		return errors.Wrapf(ErrKeyAlreadyExists, "key: %s", ent.key.String())
	}

	if ent.tags != nil {
		if err := e.setEntityTags(ent); err != nil {
			return err
		}
	}

	return nil
}

func (e *defaultEngine) Exists(key string) bool {
	found := e.pks.Get(&entry{key: newPK(key)})
	return found != nil
}

func (e *defaultEngine) FindByKey(key string) (*entry, error) {
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

// IterateByKeys - takes a slice of primary keys and iterates over matched entries
func (e *defaultEngine) IterateByKeys(pks []string, ir entryIterator) error {
	resultCh := make(chan *entry)
	var wg sync.WaitGroup

	for _, k := range pks {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()

			found, err := e.FindByKey(k)
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

// Remove entry by primary key
func (e *defaultEngine) Remove(key PK) error {
	ent := e.pks.Get(&entry{key: key})
	if ent == nil {
		return errors.Wrapf(ErrKeyDoesNotExist, "key %s does not exist in DB", key.String())
	}

	e.totalDeletes++
	e.pks.Delete(&entry{key: key})

	return nil
}

func (e *defaultEngine) setEntityTags(ent *entry) error {
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

func (e *defaultEngine) clearEntityTags(ent *entry) {
	e.tags.removeEntry(ent)
}

func (e *defaultEngine) Count() int {
	return e.pks.Len()
}

func (e *defaultEngine) scanBetweenDescend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	// Descend required a reverse order of `from` and `to`
	descendRange(
		e.pks,
		&entry{key: newPK(q.keyRange.From)},
		&entry{key: newPK(q.keyRange.To)},
		filteringBTreeIterator(ctx, q, ir),
	)

	return
}

func (e *defaultEngine) scanBetweenAscend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	ascendRange(
		e.pks,
		&entry{key: newPK(q.keyRange.From)},
		&entry{key: newPK(q.keyRange.To)},
		filteringBTreeIterator(ctx, q, ir),
	)

	return
}

func (e *defaultEngine) scanPrefixAscend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	e.pks.Ascend(&entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, q, ir))

	return
}

func (e *defaultEngine) scanPrefixDescend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	descendGreaterThan(e.pks, &entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, q, ir))
	return
}

func (e *defaultEngine) scanAscend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	e.pks.Ascend(nil, filteringBTreeIterator(ctx, q, ir))
	return
}

func (e *defaultEngine) scanDescend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	e.pks.Descend(nil, filteringBTreeIterator(ctx, q, ir))
	return
}

func (e *defaultEngine) ChooseBestScanner(q *QueryOptions) (scanner, error) {
	if q.keyRange != nil {
		if q.order == AscOrder {
			return e.scanBetweenAscend, nil
		} else {
			return e.scanBetweenDescend, nil
		}
	}

	if q.prefix != "" {
		if q.order == AscOrder {
			return e.scanPrefixAscend, nil
		} else {
			return e.scanPrefixDescend, nil
		}
	}

	if q.order == AscOrder {
		return e.scanAscend, nil
	} else {
		return e.scanDescend, nil
	}
}

// FilterEntriesByTags - uses secondary indexes (tags) to filter entries
// and puts them to sink, which it creates to store all the matched entries
func (e *defaultEngine) FilterEntriesByTags(q *QueryOptions) (*filterEntriesSink, error) {
	if q == nil || (q.allTags == nil && q.byTagName == "") {
		return nil, nil
	}

	fes := newFilteredEntriesSink(q.patterns)

	// one tag name scan
	if q.byTagName != "" {
		idx, ok := e.tags.data[q.byTagName]
		if !ok {
			return nil, errors.Wrapf(ErrTagKeyNotFound, "%s", q.byTagName)
		}

		switch q.order {
		case DescOrder:
			idx.btr.Descend(nil, func(item interface{}) bool {
				ents := item.(entryContainer).getEntries()
				fes.addMap(ents)
				return true
			})
		default:
			idx.btr.Ascend(nil, func(item interface{}) bool {
				ents := item.(entryContainer).getEntries()
				fes.addMap(ents)
				return true
			})
		}

		return fes, nil
	}

	errCh := make(chan error, 4)
	var wg sync.WaitGroup

	if len(q.allTags.booleans) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for tk, v := range q.allTags.booleans {
				if e.tags.data[tk.name] == nil {
					continue
				}

				btf, err := createBoolTagFilter(e.tags, tk, v)
				if err != nil {
					errCh <- err
					return
				}

				e.tags.filterEntities(btf, fes)
			}
		}()
	}

	if len(q.allTags.strings) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for tk, v := range q.allTags.strings {
				if e.tags.data[tk.name] == nil {
					continue
				}

				stf, err := createStringTagFilter(e.tags, tk, v)
				if err != nil {
					errCh <- err
					return
				}

				e.tags.filterEntities(stf, fes)
			}
		}()
	}

	if len(q.allTags.integers) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for tk, v := range q.allTags.integers {
				if e.tags.data[tk.name] == nil {
					continue
				}

				itf, err := createIntegerTagFilter(e.tags, tk, v)
				if err != nil {
					errCh <- err
					return
				}

				e.tags.filterEntities(itf, fes)
			}
		}()
	}

	if len(q.allTags.floats) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for tk, v := range q.allTags.floats {
				if e.tags.data[tk.name] == nil {
					continue
				}

				ftf, err := createFloatTagFilter(e.tags, tk, v)
				if err != nil {
					errCh <- err
					return
				}

				e.tags.filterEntities(ftf, fes)
			}
		}()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return nil, err
	default:
		return fes, nil
	}
}

// UpsertTag - updates or inserts a new tag, adding it to the entity
// and corresponding secondary index
func (e *defaultEngine) UpsertTag(name string, v interface{}, ent *entry) error {
	// just a precaution
	if ent.tags == nil {
		ent.tags = newTags()
	}

	// if tag name exists in entity, remove it from secondary index
	// and remove it from entry itself
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

// RemoveTag - removes a tag from entity and secondary index
func (e *defaultEngine) RemoveTag(name string, ent *entry) error {
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

func (e *defaultEngine) Put(ent *entry, replace bool) error {
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
			e.clearEntityTags(ent)
		}
	}

	if ent.tags != nil {
		if err := e.setEntityTags(ent); err != nil {
			return err
		}
	}

	return nil
}

func (e *defaultEngine) FlushAll(ff func(ent *entry)) error {
	e.pks.Ascend(nil, func(i interface{}) bool {
		ent := i.(*entry)
		ff(ent)
		return true
	})

	e.pks = btree.NewNonConcurrent(byPrimaryKeys)
	e.tags = newTagIndex()

	return nil
}

func (e *defaultEngine) Vacuum(ctx context.Context) error {
	if e.persistence == nil {
		return nil
	}

	e.Lock()
	defer e.Unlock()

	if err := e.runVacuumUnderLock(ctx); err != nil {
		return err
	}

	return nil
}

func filteringBTreeIterator(
	ctx context.Context,
	q *QueryOptions,
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

		if !ent.key.Match(q.patterns) {
			return true
		}

		return ir(ent)
	}
}

func serializeToValue(d interface{}) ([]byte, bool, error) {
	switch typedValue := d.(type) {
	case []byte:
		return typedValue, false, nil
	case int:
		return []byte(strconv.Itoa(typedValue)), false, nil // fixme: probably we do not want pure ints as values
	case string:
		return []byte(typedValue), false, nil
	}

	b, err := json.Marshal(d)
	if err != nil {
		return nil, false, errors.Wrapf(err, "could not marshal data %+v value", d)
	}

	return b, true, nil
}
