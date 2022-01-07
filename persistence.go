package lemon

import (
	"bufio"
	"bytes"
	"github.com/denismitr/glog"
	"github.com/denismitr/lemon/internal/lru"
	"github.com/pbnjay/memory"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
	"sync"
)

var ErrDbFileWriteFailed = errors.New("database write failed")
var ErrSourceFileReadFailed = errors.New("source file read failed")
var ErrCommandInvalid = errors.New("command invalid")
var ErrStorageFailed = errors.New("storage error")
var ErrIllegalStorageCacheCall = errors.New("illegal storage cache call")

type ValueLoadStrategy string
type PersistenceStrategy string

type commandCode int8

const (
	boolTagFn  = "btg"
	strTagFn   = "stg"
	intTagFn   = "itg"
	floatTagFn = "ftg"
)

const (
	invalidCode commandCode = iota
	setCode
	delCode
	tagCode
	untagCode
	flushAllCode
)

const (
	Async PersistenceStrategy = "async"
	Sync  PersistenceStrategy = "sync"
)

const (
	LazyLoad     ValueLoadStrategy = "lazy"
	EagerLoad    ValueLoadStrategy = "eager"
	BufferedLoad ValueLoadStrategy = "buffered"
)

const (
	valueShards        = 20
	KiloByte    uint64 = 1 << (10 * 1)
	MegaByte    uint64 = 1 << (10 * 2)
	GigaByte    uint64 = 1 << (10 * 3)
)

type cache interface {
	Add(key uint64, value []byte) bool
	Get(key uint64) ([]byte, bool)
	Remove(key uint64)
	Purge()
}

type OnCacheEvict func(bytes int)

type persistence struct {
	mu       sync.RWMutex
	vls      ValueLoadStrategy
	strategy PersistenceStrategy
	parser   *respParser
	f        *os.File
	flushes  int
	cursor   int
	cache    cache
	lg       glog.Logger
}

func newPersistence(
	filepath string,
	strategy PersistenceStrategy,
	truncateFileOnOpen bool,
	vls ValueLoadStrategy,
	maxCacheSize uint64,
	onCacheEvict OnCacheEvict,
	lg glog.Logger,
) (*persistence, error) {
	flags := os.O_CREATE | os.O_RDWR
	if truncateFileOnOpen {
		flags |= os.O_TRUNC
	}

	f, err := os.OpenFile(filepath, flags, 0666)
	if err != nil {
		return nil, err
	}

	p := &persistence{
		f:        f,
		vls:      vls,
		strategy: strategy,
		lg:       lg,
	}

	if err := p.initializeCache(valueShards, maxCacheSize, onCacheEvict); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *persistence) initializeCache(shards, maxCacheSize uint64, onCacheEvict OnCacheEvict) error {
	if p.vls == LazyLoad {
		p.cache = lru.NullCache{}
		return nil
	}

	if p.vls == EagerLoad {
		maxCacheSize = memory.FreeMemory()
	}

	onEvict := func(k uint64, v []byte) {
		if onCacheEvict != nil {
			onCacheEvict(len(v))
		}
	}

	c, err := lru.NewShardedCache(valueShards, maxCacheSize, onEvict)
	if err != nil {
		return err
	}

	p.cache = c

	return nil
}

func (p *persistence) close() error {
	p.mu.Lock()
	defer func() {
		if err := p.f.Close(); err != nil {
			p.lg.Error(err)
		}

		p.parser = nil
		p.f = nil
		p.cache = nil
		p.mu.Unlock()
	}()

	if err := p.f.Sync(); err != nil {
		return errors.Wrapf(err, "could not sync %s", p.f.Name())
	}

	return nil
}

func (p *persistence) load(cb func(d deserializable) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, err := p.f.Stat()
	if err != nil {
		return errors.Wrapf(err, "could not collect file %s stats", p.f.Name())
	}

	// todo: inject
	prs := &respParser{
		vls: p.vls,
	}

	r := bufio.NewReader(p.f)

	n, err := prs.parse(r, p.cache, cb)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if tErr := p.f.Truncate(int64(n)); tErr != nil {
				return errors.Wrapf(tErr, "could not truncate file after pare error")
			}
		}

		return err
	}

	pos, err := p.f.Seek(int64(n), 0)
	if err != nil {
		return errors.Wrapf(ErrStorageFailed, "could not move the cursor: %s", err.Error())
	}

	p.cursor = int(pos)

	return nil
}

func (p *persistence) save(commands []serializable) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	rs := respSerializer{pos: p.cursor}

	// in case we have inserts or updates we need to collect
	// these items to update cache
	// in case of deletes cache must be cleaned
	var changes []*entry
	var deletes []*deleteCmd

	for _, cmd := range commands {
		if err := cmd.serialize(&rs); err != nil {
			return err
		}

		// values need to be put in cache for BufferedLoad strategy
		// but also ent.value has to be set to null for lazy load
		// for EagerLoad value is simply stored in ent.value and
		// after persist to file we are done
		if ent, ok := cmd.(*entry); ok && p.vls != EagerLoad {
			changes = append(changes, ent)
		}

		// values are actually stored in cache only for BufferedLoad strategy
		if del, ok := cmd.(*deleteCmd); ok && p.vls == BufferedLoad {
			deletes = append(deletes, del)
		}
	}

	// write to disk
	if err := p.writeUnderLock(&rs.buf); err != nil {
		return err
	}

	// remove deleted entries data from cache
	for i := range deletes {
		p.removeFromCache(deletes[i])
	}

	// if write was a success we need to update cache
	// otherwise reads can get old values and lru can be inadequate
	// for LazyLoad we need to set ent.value = nil
	for i := range changes {
		p.cacheEntryValue(changes[i])
		changes[i].value = nil
	}

	return nil
}

func (p *persistence) writeUnderLock(buf *bytes.Buffer) error {
	n, err := p.f.Write(buf.Bytes())
	if err != nil {
		if n > 0 {
			// partial write occurred, must rollback the file
			pos, seekErr := p.f.Seek(-int64(n), 1)
			if seekErr != nil {
				return errors.Wrapf(
					ErrInternalError,
					"could not seek file %s to -%d: %v",
					p.f.Name(), n, seekErr,
				)
			}

			if err := p.f.Truncate(pos); err != nil {
				return errors.Wrapf(err, "could not truncate file %s", p.f.Name())
			}
		}

		if sErr := p.f.Sync(); sErr != nil {
			p.lg.Error(sErr)
		}

		return errors.Wrap(ErrDbFileWriteFailed, err.Error())
	}

	if p.strategy == Sync {
		if err := p.f.Sync(); err != nil {
			p.lg.Error(err)
			return err
		}
	}

	p.flushes++
	p.cursor += buf.Len()
	return nil
}

func (p *persistence) sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.f.Sync(); err != nil {
		return errors.Wrapf(err, "cannot sync file %s", p.f.Name())
	}

	return nil
}

func (p *persistence) writeAndSwap(rs *respSerializer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tmpFName := p.f.Name() + ".tmp"
	tmpF, err := os.Create(tmpFName)
	if err != nil {
		return errors.Wrapf(err, "could not create %s file for auto vacuum", tmpFName)
	}

	defer func() {
		if err := tmpF.Close(); err != nil {
			p.lg.Error(errors.Wrapf(err, "could not close tmp file %s", tmpF.Name()))
		}

		if err := os.RemoveAll(tmpFName); err != nil {
			p.lg.Error(errors.Wrapf(err, "could not remove tmp file %s", tmpF.Name()))
		}
	}()

	expectedLen := rs.buf.Len()
	n, err := tmpF.Write(rs.buf.Bytes())
	if err != nil {
		return errors.Wrapf(err, "auto vacuum could not write into %s file", tmpFName)
	}

	if n != expectedLen {
		return errors.Wrapf(err, "auto vacuum could not write all the data into %s file", tmpFName)
	}

	oldName := p.f.Name()
	if err := p.f.Close(); err != nil {
		return errors.Wrapf(err, "auto vacuum could not close %s file to swap it", oldName)
	}

	if rnErr := os.Rename(tmpFName, oldName); rnErr != nil {
		resultErr := errors.Wrapf(rnErr, "auto vacuum could not swap %s file for %s", oldName, tmpFName)
		p.f, err = os.OpenFile(oldName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return errors.Wrapf(resultErr, "and could not reopen old file: %s", err.Error())
		}
		return resultErr
	}

	p.f, err = os.OpenFile(oldName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return errors.Wrapf(err, "could not reopen swapped file: %s", oldName)
	}

	pos, err := p.f.Seek(int64(n), 0)
	if err != nil {
		return errors.Wrapf(ErrStorageFailed, "could not move the cursor in file %s: %s", err, err.Error())
	}

	p.cursor = int(pos)

	return nil
}

func (p *persistence) cacheEntryValue(ent *entry) {
	if p.vls != BufferedLoad || ent.value == nil || ent.pos.offset <= 0 {
		return
	}

	p.cache.Add(ent.pos.offset, ent.value)

	// value is now in cache no need to keep it
	// in the entry
	ent.value = nil
}

func (p *persistence) loadValueToEntry(ent *entry) error {
	if p.vls == EagerLoad {
		return errors.Wrapf(ErrIllegalStorageCacheCall, "for key %s", ent.key.String())
	}

	if p.vls == BufferedLoad {
		if v, ok := p.cache.Get(ent.pos.offset); ok {
			ent.value = v
			return nil
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, err := p.f.Seek(int64(ent.pos.offset), 0); err != nil {
		return errors.Wrapf(
			ErrStorageFailed,
			"could not seek to offset %d in file %s: %s",
			ent.pos.offset, err, err.Error(),
		)
	}

	r := bufio.NewReader(p.f)
	blob := make([]byte, ent.pos.size)
	if _, err := io.ReadFull(r, blob); err != nil {
		return errors.Wrapf(
			ErrStorageFailed,
			"could not read blob at offset %d in file %s: %s",
			ent.pos.offset, err, err.Error(),
		)
	}

	if p.vls != LazyLoad && ent.pos.offset > 0 {
		p.cache.Add(ent.pos.offset, blob)
	}

	ent.value = blob

	return nil
}

func (p *persistence) removeValueUnderLock(pos position) {
	if p.vls == LazyLoad {
		return
	}

	p.cache.Remove(pos.offset)
}

func (p *persistence) newSerializer() *respSerializer {
	return &respSerializer{pos: p.cursor}
}

func (p *persistence) removeFromCache(cmd *deleteCmd) {
	if cmd.pos.offset <= 0 {
		p.lg.Noticef("attempt to remove invalid offset %d from cache", cmd.pos.offset)
		return
	}

	p.cache.Remove(cmd.pos.offset)
}

func (p *persistence) flushBuffer() {
	p.cache.Purge()
}

func resolveTagFnTypeAndArguments(expression string) (prefix string, args []string, err error) {
	for _, p := range []string{boolTagFn, strTagFn, intTagFn, floatTagFn} {
		if strings.HasPrefix(expression, p) {
			prefix = p
			break
		}
	}

	if prefix == "" {
		err = errors.Wrapf(ErrCommandInvalid, "expression %s is invalid", expression)
		return
	}

	argsExp := strings.TrimPrefix(expression, prefix+"(")
	argsExp = strings.TrimSuffix(argsExp, ")")
	args = strings.Split(argsExp, ",")

	if len(args) < 2 {
		err = errors.Wrapf(
			ErrCommandInvalid,
			"expression %s is invalid: too few arguments",
			expression,
		)
	}

	return
}
