package lemon

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

var ErrDbFileWriteFailed = errors.New("database write failed")
var ErrSourceFileReadFailed = errors.New("source file read failed")
var ErrCommandInvalid = errors.New("command invalid")
var ErrParseFailed = errors.New("commands parse error")
var ErrStorageFailed = errors.New("storage error")

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

type persistence struct {
	mu       sync.RWMutex
	strategy PersistenceStrategy
	parser   *parser
	f        *os.File
	flushes  int
	cursor   int
}

func newPersistence(
	filepath string,
	strategy PersistenceStrategy,
	truncateFileOnOpen bool,
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
		strategy: strategy,
	}

	return p, nil
}

func (p *persistence) close() (err error) {
	p.mu.Lock()
	defer func() {
		p.parser = nil
		p.f = nil

		p.mu.Unlock()
	}()

	err = p.f.Sync()
	err = p.f.Close() //fixme

	if err != nil {
		err = errors.Wrap(err, "could not close file")
	}

	return
}

func (p *persistence) load(cb func(d deserializer) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, err := p.f.Stat()
	if err != nil {
		return errors.Wrapf(err, "could not collect file %s stats", p.f.Name())
	}

	prs := &parser{}

	r := bufio.NewReader(p.f)

	n, err := prs.parse(r, cb)
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

func (p *persistence) write(buf *bytes.Buffer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	n, err := p.f.Write(buf.Bytes())
	if err != nil {
		if n > 0 {
			// partial write occurred, must rollback the file
			pos, seekErr := p.f.Seek(-int64(n), 1)
			if seekErr != nil {
				panic(seekErr)
			}

			if err := p.f.Truncate(pos); err != nil {
				return errors.Wrapf(err, "could not truncate file %s", p.f.Name())
			}
		}

		_ = p.f.Sync()
		return errors.Wrap(ErrDbFileWriteFailed, err.Error())
	}

	if p.strategy == Sync {
		_ = p.f.Sync()
	}

	p.flushes++
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

func (p *persistence) writeAndSwap(buf *bytes.Buffer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tmpFName := p.f.Name() + ".tmp"
	tmpF, err := os.Create(tmpFName)
	if err != nil {
		return errors.Wrapf(err, "could not create %s file for auto vacuum", tmpFName)
	}

	defer func() {
		_ = tmpF.Close()
		_ = os.RemoveAll(tmpFName)
	}()

	expectedLen := buf.Len()
	n, err := tmpF.Write(buf.Bytes())
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
		panic("how args can be less than 2 for tag function")
	}

	return
}

func writeRespArray(segments int, buf *bytes.Buffer) {
	buf.WriteRune('*')
	buf.WriteString(strconv.FormatInt(int64(segments), 10))
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}

func writeRespBoolTag(name string, v bool, buf *bytes.Buffer) {
	writeRespFunc(fmt.Sprintf("btg(%s,%v)", name, v), buf)
}

func writeRespStrTag(name, v string, buf *bytes.Buffer) {
	writeRespFunc(fmt.Sprintf("stg(%s,%s)", name, v), buf)
}

func writeRespIntTag(name string, v int, buf *bytes.Buffer) {
	writeRespFunc(fmt.Sprintf("%s(%s,%d)", intTagFn, name, v), buf)
}

func writeRespFloatTag(name string, v float64, buf *bytes.Buffer) {
	writeRespFunc(fmt.Sprintf("%s(%s,%v)", floatTagFn, name, v), buf)
}

func writeRespSimpleString(s string, buf *bytes.Buffer) {
	buf.WriteRune('+')
	buf.WriteString(s)
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}

func writeRespKeyString(s string, buf *bytes.Buffer) {
	buf.WriteRune('$')
	buf.WriteString(strconv.FormatInt(int64(len(s)), 10))
	buf.WriteRune('\r')
	buf.WriteRune('\n')
	buf.WriteString(s)
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}

func writeRespFunc(fn string, buf *bytes.Buffer) {
	buf.WriteRune('+')
	buf.WriteString(fn)
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}

func writeRespBlob(blob []byte, buf *bytes.Buffer) {
	buf.WriteRune('$')
	buf.WriteString(strconv.FormatInt(int64(len(blob)), 10))
	buf.WriteRune('\r')
	buf.WriteRune('\n')
	buf.Write(blob)
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}
