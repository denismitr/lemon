package lemon

import (
	"context"
	"encoding/json"
	"github.com/denismitr/glog"
	"github.com/pkg/errors"
	"github.com/tidwall/btree"
	"strconv"
	"sync"
	"time"
)

var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrDatabaseAlreadyClosed = errors.New("database already closed")
var ErrTagKeyNotFound = errors.New("tag key not found")

type (
	entryIterator func(ent *entry) bool
	scanner       func(ctx context.Context, q *QueryOptions, ir entryIterator) error
)

type rwLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

type executionEngine interface {
	rwLocker

	Persist(commands []serializable) error
	RemoveTag(name string, ent *entry) error
	Close(ctx context.Context) error
	Insert(ent *entry) error
	Exists(key string) bool
	FindByKey(key string) (*entry, error)
	IterateByKeys(pks []string, ir entryIterator) error
	Remove(key PK) error
	RemoveEntryFromTagsByName(name string, ent *entry) error
	AddTag(name string, value interface{}, ent *entry) error
	Count() int
	Put(ent *entry, replace bool) error
	FlushAll(ff func(ent *entry)) error
	Vacuum(ctx context.Context) error
	UpsertTag(name string, v interface{}, ent *entry) error
	FilterEntriesByTags(q *QueryOptions) (*filterEntriesSink, error)
	ChooseBestScanner(q *QueryOptions) (scanner, error)
	RemoveEntryUnderLock(ent *entry)
	SetCfg(cfg *Config)
	LoadEntryValue(ent *entry) error
}

type defaultEngine struct {
	sync.RWMutex

	lg            glog.Logger
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

func newDefaultEngine(dbFile string, lg glog.Logger, cfg *Config) (*defaultEngine, error) {
	e := &defaultEngine{
		dbFile: dbFile,
		pks:    btree.NewNonConcurrent(byPrimaryKeys),
		tags:   newTagIndex(),
		stopCh: make(chan struct{}, 1),
		cfg:    cfg,
		lg:     lg,
	}

	return e, nil
}

func (ee *defaultEngine) SetCfg(cfg *Config) {
	ee.cfg = cfg
}

func (ee *defaultEngine) asyncFlush(d time.Duration) {
	t := time.NewTicker(d)

	for {
		select {
		case <-ee.stopCh:
			if err := ee.persistence.sync(); err != nil {
				ee.lg.Error(err)
			}

			t.Stop()
			return
		case <-t.C:
			if err := ee.persistence.sync(); err != nil {
				ee.lg.Error(err)
			}
		}
	}
}

func (ee *defaultEngine) scheduleVacuum(d time.Duration) {
	t := time.NewTicker(d)

	for {
		select {
		case <-ee.stopCh:
			t.Stop()
			return
		case <-t.C:
			ee.Lock()
			if ee.runningVacuum && ee.totalDeletes < ee.cfg.AutoVacuumMinSize {
				ee.Unlock()
				continue
			}

			ee.runningVacuum = true
			// todo: maybe limit run vacuum with context timeout equal to d
			if err := ee.runVacuumUnderLock(context.Background()); err != nil {
				ee.lg.Error(err)
				return
			}
			ee.runningVacuum = false
			ee.Unlock()
		}
	}
}

func (ee *defaultEngine) runVacuumUnderLock(ctx context.Context) error {
	if ee.persistence == nil {
		return nil
	}

	rs := ee.persistence.newSerializer()
	rs.reset()

	var pErr error
	ee.pks.Ascend(nil, func(i interface{}) bool {
		if err := ctx.Err(); err != nil {
			return false
		}

		ent := i.(*entry)
		if ent.value == nil && ent.pos.offset != 0 {
			if err := ee.persistence.loadValueToEntry(ent); err != nil {
				ee.lg.Error(err)
				pErr = err
				return false
			}
		}

		if err := ent.serialize(rs); err != nil {
			ee.lg.Error(err)
			pErr = err
			return false
		}

		return true
	})

	if pErr != nil {
		return errors.Wrap(pErr, "could not finish vacuum")
	}

	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "could not finish vacuum")
	}

	if err := ee.persistence.writeAndSwap(rs); err != nil {
		return err
	}

	return nil
}

func (ee *defaultEngine) Close(ctx context.Context) error {
	ee.Lock()

	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	if !ee.cfg.DisableAutoVacuum {
		if err := ee.runVacuumUnderLock(ctx); err != nil {
			return err
		}
	}

	defer func() {
		ee.pks = nil
		ee.tags = nil
		ee.closed = true
		ee.persistence = nil
		ee.Unlock()
	}()

	close(ee.stopCh)

	if ee.cfg.PersistenceStrategy == Async {
		time.Sleep(ee.cfg.AsyncPersistenceIntervals)
	}

	if ee.persistence != nil {
		return ee.persistence.close()
	}

	return nil
}

func (ee *defaultEngine) init() error {
	ee.Lock()
	defer ee.Unlock()

	if ee.dbFile != InMemory {
		p, err := newPersistence(
			ee.dbFile,
			ee.cfg.PersistenceStrategy,
			ee.cfg.TruncateFileWhenOpen,
			ee.cfg.ValueLoadStrategy,
			ee.cfg.MaxCacheSize,
			ee.cfg.OnCacheEvict,
			ee.lg,
		)

		if err != nil {
			return err
		}

		ee.persistence = p

		if err := ee.persistence.load(func(d deserializable) error {
			return d.deserialize(ee)
		}); err != nil {
			return err
		}

		if ee.cfg.PersistenceStrategy == Async {
			go ee.asyncFlush(ee.cfg.AsyncPersistenceIntervals)
		}

		if !ee.cfg.DisableAutoVacuum && !ee.cfg.AutoVacuumOnlyOnClose {
			go ee.scheduleVacuum(ee.cfg.AutoVacuumIntervals)
		}
	} else {
		ee.cfg.ValueLoadStrategy = EagerLoad
	}

	return nil
}

func (ee *defaultEngine) Persist(commands []serializable) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	if ee.persistence == nil {
		return nil
	}

	if err := ee.persistence.save(commands); err != nil {
		return err
	}

	return nil
}

func (ee *defaultEngine) LoadEntryValue(ent *entry) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	if ent.pos.offset != 0 {
		if err := ee.persistence.loadValueToEntry(ent); err != nil {
			return err
		}
	}

	return nil
}

func (ee *defaultEngine) RemoveEntryUnderLock(ent *entry) {
	if ee.closed {
		return
	}

	ee.tags.removeEntry(ent)
	ee.pks.Delete(ent)

	if ee.dbFile != InMemory{
		ee.persistence.removeValueUnderLock(ent.pos)
	}
}

func (ee *defaultEngine) RemoveEntryFromTagsByName(name string, ent *entry) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	return ee.tags.removeEntryByName(name, ent)
}

func (ee *defaultEngine) AddTag(name string, value interface{}, ent *entry) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	return ee.tags.add(name, value, ent)
}

func (ee *defaultEngine) Insert(ent *entry) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	existing := ee.pks.Set(ent)
	if existing != nil {
		return errors.Wrapf(ErrKeyAlreadyExists, "key: %s", ent.key.String())
	}

	if ent.tags != nil {
		if err := ee.setEntityTags(ent); err != nil {
			return err
		}
	}

	return nil
}

func (ee *defaultEngine) Exists(key string) bool {
	if ee.closed {
		return false
	}

	found := ee.pks.Get(&entry{key: newPK(key)})
	return found != nil
}

func (ee *defaultEngine) FindByKey(key string) (*entry, error) {
	if ee.closed {
		return nil, ErrDatabaseAlreadyClosed
	}

	found := ee.pks.Get(&entry{key: newPK(key)})
	if found == nil {
		return nil, errors.Wrapf(ErrKeyDoesNotExist, "key %s does not exist in database", key)
	}

	ent, ok := found.(*entry)
	if !ok {
		return nil, errors.Wrap(ErrInternalError, "could not cast to entry")
	}

	return ent, nil
}

// IterateByKeys - takes a slice of primary keys and iterates over matched entries
func (ee *defaultEngine) IterateByKeys(pks []string, ir entryIterator) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	resultCh := make(chan *entry, len(pks))
	var wg sync.WaitGroup

	for _, k := range pks {
		wg.Add(1)
		go func(k string) {
			ee.RLock()

			defer func() {
				ee.RUnlock()
				wg.Done()
			}()

			found, err := ee.FindByKey(k)
			if err != nil {
				ee.lg.Error(err)
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
func (ee *defaultEngine) Remove(key PK) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	ent := ee.pks.Get(&entry{key: key})
	if ent == nil {
		return errors.Wrapf(ErrKeyDoesNotExist, "key %s does not exist in DB", key.String())
	}

	ee.totalDeletes++
	ee.pks.Delete(&entry{key: key})

	return nil
}

func (ee *defaultEngine) setEntityTags(ent *entry) error {
	for name, t := range ent.tags {
		if err := ee.tags.add(name, t.data, ent); err != nil {
			return err
		}
	}

	return nil
}

func (ee *defaultEngine) clearEntityTags(ent *entry) {
	ee.tags.removeEntry(ent)
}

func (ee *defaultEngine) Count() int {
	return ee.pks.Len()
}

func (ee *defaultEngine) scanBetweenDescend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	// Descend required a reverse order of `from` and `to`
	descendRange(
		ee.pks,
		&entry{key: newPK(q.keyRange.From)},
		&entry{key: newPK(q.keyRange.To)},
		filteringBTreeIterator(ctx, ee.lg, q, ir),
	)

	return
}

func (ee *defaultEngine) scanBetweenAscend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	ascendRange(
		ee.pks,
		&entry{key: newPK(q.keyRange.From)},
		&entry{key: newPK(q.keyRange.To)},
		filteringBTreeIterator(ctx, ee.lg, q, ir),
	)

	return
}

func (ee *defaultEngine) scanPrefixAscend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	ee.pks.Ascend(&entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, ee.lg, q, ir))

	return
}

func (ee *defaultEngine) scanPrefixDescend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	descendGreaterThan(ee.pks, &entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, ee.lg, q, ir))
	return
}

func (ee *defaultEngine) scanAscend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	ee.pks.Ascend(nil, filteringBTreeIterator(ctx, ee.lg, q, ir))
	return
}

func (ee *defaultEngine) scanDescend(
	ctx context.Context,
	q *QueryOptions,
	ir entryIterator,
) (err error) {
	ee.pks.Descend(nil, filteringBTreeIterator(ctx, ee.lg, q, ir))
	return
}

func (ee *defaultEngine) ChooseBestScanner(q *QueryOptions) (scanner, error) {
	if q.keyRange != nil {
		if q.order == AscOrder {
			return ee.scanBetweenAscend, nil
		} else {
			return ee.scanBetweenDescend, nil
		}
	}

	if q.prefix != "" {
		if q.order == AscOrder {
			return ee.scanPrefixAscend, nil
		} else {
			return ee.scanPrefixDescend, nil
		}
	}

	if q.order == AscOrder {
		return ee.scanAscend, nil
	} else {
		return ee.scanDescend, nil
	}
}

// FilterEntriesByTags - uses secondary indexes (tags) to filter entries
// and puts them to sink, which it creates to store all the matched entries
func (ee *defaultEngine) FilterEntriesByTags(q *QueryOptions) (*filterEntriesSink, error) {
	if ee.closed {
		return nil, ErrDatabaseAlreadyClosed
	}

	if q == nil || (q.allTags == nil && q.byTagName == "") {
		return nil, nil
	}

	fes := newFilteredEntriesSink(q.patterns)

	// one tag name scan
	if q.byTagName != "" {
		idx, ok := ee.tags.data[q.byTagName]
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
				if ee.tags.data[tk.name] == nil {
					continue
				}

				btf, err := createBoolTagFilter(ee.tags, tk, v)
				if err != nil {
					errCh <- err
					return
				}

				ee.tags.filterEntities(btf, fes)
			}
		}()
	}

	if len(q.allTags.strings) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for tk, v := range q.allTags.strings {
				if ee.tags.data[tk.name] == nil {
					continue
				}

				stf, err := createStringTagFilter(ee.tags, tk, v)
				if err != nil {
					errCh <- err
					return
				}

				ee.tags.filterEntities(stf, fes)
			}
		}()
	}

	if len(q.allTags.integers) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for tk, v := range q.allTags.integers {
				if ee.tags.data[tk.name] == nil {
					continue
				}

				itf, err := createIntegerTagFilter(ee.tags, tk, v)
				if err != nil {
					errCh <- err
					return
				}

				ee.tags.filterEntities(itf, fes)
			}
		}()
	}

	if len(q.allTags.floats) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for tk, v := range q.allTags.floats {
				if ee.tags.data[tk.name] == nil {
					continue
				}

				ftf, err := createFloatTagFilter(ee.tags, tk, v)
				if err != nil {
					errCh <- err
					return
				}

				ee.tags.filterEntities(ftf, fes)
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
func (ee *defaultEngine) UpsertTag(name string, v interface{}, ent *entry) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	// just a precaution
	if ent.tags == nil {
		ent.tags = newTags()
	}

	// if tag name exists in entity, remove it from secondary index
	// and remove it from entry itself
	_, ok := ent.tags[name]
	if ok {
		if err := ee.tags.mustRemoveEntryByNameAndValue(name, v, ent); err != nil {
			return err
		}

		ent.tags[name] = nil
		delete(ent.tags, name)
	}

	newTag := &tag{data: v}
	// do type check of value
	// same name may now contain a different value type
	switch v.(type) {
	case int:
		newTag.dt = intDataType
	case bool:
		newTag.dt = boolDataType
	case string:
		newTag.dt = strDataType
	case float64:
		newTag.dt = floatDataType
	default:
		return ErrInvalidTagType
	}

	ent.tags[name] = newTag

	// add to secondary index
	return ee.tags.add(name, v, ent)
}

// RemoveTag - removes a tag from entity and secondary index
func (ee *defaultEngine) RemoveTag(name string, ent *entry) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	if err := ee.tags.removeEntryByName(name, ent); err != nil {
		return err
	}

	ent.tags.removeByName(name)
	return nil
}

func (ee *defaultEngine) Put(ent *entry, replace bool) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	existing := ee.pks.Set(ent)
	if existing != nil {
		if !replace {
			_ = ee.pks.Set(existing)
			return errors.Wrapf(ErrKeyAlreadyExists, "key %s", ent.key.String())
		}

		existingEnt, ok := existing.(*entry)
		if !ok {
			return errors.Wrap(ErrInternalError, "could not cast to entry")
		}

		if existingEnt.tags != nil {
			ee.clearEntityTags(ent)
		}
	}

	if ent.tags != nil {
		if err := ee.setEntityTags(ent); err != nil {
			return err
		}
	}

	return nil
}

func (ee *defaultEngine) FlushAll(ff func(ent *entry)) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	ee.pks.Ascend(nil, func(i interface{}) bool {
		ent := i.(*entry)
		ff(ent)
		return true
	})

	ee.pks = btree.NewNonConcurrent(byPrimaryKeys)
	ee.tags = newTagIndex()

	return nil
}

func (ee *defaultEngine) Vacuum(ctx context.Context) error {
	if ee.closed {
		return ErrDatabaseAlreadyClosed
	}

	if ee.persistence == nil {
		return nil
	}

	ee.Lock()
	defer ee.Unlock()

	if err := ee.runVacuumUnderLock(ctx); err != nil {
		return err
	}

	return nil
}

func filteringBTreeIterator(
	ctx context.Context,
	lg glog.Logger,
	q *QueryOptions,
	ir entryIterator,
) func(item interface{}) bool {
	return func(item interface{}) bool {
		if ctx.Err() != nil {
			return false
		}

		ent, ok := item.(*entry)
		if !ok {

		}

		if !ent.key.Match(q.patterns) {
			return true
		}

		return ir(ent)
	}
}

func serializeToValue(d interface{}) ([]byte, ContentTypeIdentifier, error) {
	switch typedValue := d.(type) {
	case []byte:
		return typedValue, Bytes, nil
	case int:
		return []byte(strconv.Itoa(typedValue)), Integer, nil // fixme: probably we do not want pure ints as values
	case string:
		return []byte(typedValue), String, nil
	}

	b, err := json.Marshal(d)
	if err != nil {
		return nil, "", errors.Wrapf(err, "could not marshal data %+v value", d)
	}

	return b, JSON, nil
}
