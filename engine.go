package lemon

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	btr "github.com/tidwall/btree"
	"sync"
	"time"
)

var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrConflictingTagType = errors.New("conflicting tag type")

const castPanic = "how could primary keys item not be of type *entry"

type (
	entryReceiver func(ent *entry) bool

	scanner func(
		ctx context.Context,
		q *queryOptions,
		fe *filterEntries,
		ir entryReceiver,
	) error
)

type engine struct {
	dbFile        string
	cfg           *Config
	persistence   *persistence
	pks           *btr.BTree
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
		pks:    btr.New(byPrimaryKeys),
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
			go e.asyncFlush(time.Second * 1)
		}

		if !e.cfg.DisableAutoVacuum && !e.cfg.AutoVacuumOnlyOnClose {
			go e.scheduleVacuum(e.cfg.AutoVacuumIntervals)
		}
	}

	return nil
}

func (e *engine) insert(ent *entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()

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

func (e *engine) findByKey(key string) (*entry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.findByKeyUnderLock(key)
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

func (e *engine) findByKeys(pks []string, ir entryReceiver) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, k := range pks {
		found := e.pks.Get(newPK(k))
		if found == nil {
			return errors.Wrapf(ErrKeyDoesNotExist, "key %s does not exist in database", k)
		}

		ent := found.(*entry)

		if next := ir(ent); !next {
			break
		}
	}

	return nil
}

func (e *engine) remove(key PK) error {
	e.mu.Lock()
	defer e.mu.Unlock()

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

func (e *engine) update(ent *entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	existing := e.pks.Set(ent)
	if existing == nil {
		return errors.Wrapf(ErrKeyDoesNotExist, "could not update non existing document with key %s", ent.key.String())
	}

	existingEnt, ok := existing.(*entry)
	if !ok {
		panic(castPanic)
	}

	if existingEnt.tags != nil {
		e.clearEntityTagsUnderLock(ent)
	}

	if ent.tags != nil {
		if err := e.setEntityTagsUnderLock(ent); err != nil {
			return err
		}
	}

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
	for n, v := range ent.tags.booleans {
		e.tags.removeEntryByTag(n, v, ent)
	}

	for n, v := range ent.tags.strings {
		e.tags.removeEntryByTag(n, v, ent)
	}

	for n, v := range ent.tags.integers {
		e.tags.removeEntryByTag(n, v, ent)
	}

	for n, v := range ent.tags.floats {
		e.tags.removeEntryByTag(n, v, ent)
	}
}

func (e *engine) Count() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.pks.Len()
}

func (e *engine) scanBetweenDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

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
	ir entryReceiver,
) (err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

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
	ir entryReceiver,
) (err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	e.pks.Ascend(&entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, fe, q, ir))

	return
}

func (e *engine) scanPrefixDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	descendGreaterThan(e.pks, &entry{key: newPK(q.prefix)}, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) scanAscend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	e.pks.Ascend(nil, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) scanDescend(
	ctx context.Context,
	q *queryOptions,
	fe *filterEntries,
	ir entryReceiver,
) (err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	e.pks.Descend(nil, filteringBTreeIterator(ctx, fe, q, ir))
	return
}

func (e *engine) filterEntities(q *queryOptions) *filterEntries {
	e.mu.RLock()
	defer e.mu.RUnlock()

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

func (e *engine) putUnderLock(ent *entry, replace bool) error {
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

func filteringBTreeIterator(
	ctx context.Context,
	fe *filterEntries,
	q *queryOptions,
	ir entryReceiver,
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
