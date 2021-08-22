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
var ErrUnexpectedEOF = errors.New("unexpected end of file")
var ErrParseFailed = errors.New("commands parse error")
var ErrStorageFailed = errors.New("storage error")

type PersistenceStrategy string

type commandCode int8

const (
	boolTagFn = "btg"
	strTagFn = "stg"
	intTagFn = "itg"
	floatTagFn = "ftg"
)

const (
	invalidCode commandCode = iota
	setCode
	delCode
	tagCode
	untagCode
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

func newPersistence(filepath string, strategy PersistenceStrategy) (*persistence, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	p := &persistence{
		f: f,
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

type parser struct {
	totalSize      int
	buf            [1024]byte
	currentCmdSize int
	totalCommands  int
	n              int
	currentLine    uint8
}

func (p *parser) parse(r *bufio.Reader, cb func(d deserializer) error) (int, error) {
	for {
		p.currentCmdSize = 0

		firstByte, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return p.totalSize, nil
			}

			return p.totalSize, errors.Wrap(ErrSourceFileReadFailed, err.Error())
		}

		if firstByte == 0 {
			p.n++
			continue
		}

		if err := r.UnreadByte(); err != nil {
			return p.totalSize, errors.Wrap(ErrSourceFileReadFailed, err.Error())
		}

		segments, err := p.resolveRespArrayFromLine(r)
		if err != nil {
			return p.totalSize, err
		}

		cmdCode, err := p.resolveRespCommandCode(r)
		if err != nil {
			return p.totalSize, err
		}

		switch cmdCode {
		case tagCode:
			key, err := p.resolveRespKey(r)
			if err != nil {
				return p.totalSize, err
			}

			tgs := newTags()
			for j := 0; j < segments; j++ {
				tagger, err := p.resolveTagger(r)
				if err != nil {
					return p.totalSize, err
				}
				tagger(tgs)
			}

			if err := cb(&tagCmd{key: newPK(string(key)), tags: tgs}); err != nil {
				return p.totalSize, err
			}
		case untagCode:
			key, err := p.resolveRespKey(r)
			if err != nil {
				return p.totalSize, err
			}

			names, err := p.resolveNamesToUntag(segments - 2, r)
			if err != nil {
				return p.totalSize, err
			}

			if err := cb(&untagCmd{key: newPK(string(key)), names: names}); err != nil {
				return p.totalSize, err
			}
		case delCode:
			key, err := p.resolveRespKey(r)
			if err != nil {
				return p.totalSize, err
			}

			if err := cb(&deleteCmd{key: newPK(string(key))}); err != nil {
				return p.totalSize, err
			}
		case setCode:
			key, err := p.resolveRespKey(r)
			if err != nil {
				return p.totalSize, err
			}

			value, err := p.resolveRespBlob(r)
			if err != nil {
				return p.totalSize, err
			}

			ent := newEntryWithTags(string(key), value, nil)

			// subtracting command, key and value
			segments -= 3
			if segments > 0 {
				ent.tags = newTags() // fixme
			}

			for j := 0; j < segments; j++ {
				tagger, err := p.resolveTagger(r)
				if err != nil {
					return p.totalSize, err
				}
				tagger(ent.tags)
			}

			if err := cb(ent); err != nil {
				return p.totalSize, err
			}
		}

		p.totalCommands++
		p.totalSize += p.currentCmdSize
	}
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

func (p *parser) resolveRespArrayFromLine(r *bufio.Reader) (int, error) {
	// read a command
	p.currentLine++
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}

		return 0, errors.Wrapf(ErrSourceFileReadFailed, "could not parse array at line #%d: %s", p.currentLine, err.Error())
	}

	// fixme: investigate - seems we are getting phantoms from prev line
	if len(line) == 2 {
		p.currentLine++
		line, _  = r.ReadBytes('\n')
	}

	// should be \*\d{1,}\\r
	// for now we only expects array like commands
	if len(line) < 2 || line[0] != '*' {
		return p.totalSize, errors.Wrapf(
			ErrCommandInvalid,
			"line #%d - %s should actually start with *",
			p.currentLine, string(line))
	}

	cmdBuf := p.buf[:0]
	for _, r := range line[1:] {
		if r >= '0' && r <= '9' {
			cmdBuf = append(cmdBuf, r)
		}
	}

	n, err := strconv.Atoi(string(cmdBuf))
	if err != nil {
		return 0, errors.Wrapf(ErrCommandInvalid, "could not parse command size at line #%d %v", p.currentLine, err)
	}

	p.currentCmdSize += len(line)

	return n, nil
}

func (p *parser) resolveRespCommandCode(r *bufio.Reader) (commandCode, error) {
	p.currentLine++
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return invalidCode, io.ErrUnexpectedEOF
		}

		return invalidCode, err
	}

	if len(line) < 4 {
		return invalidCode, ErrCommandInvalid
	}

	if line[0] != '+' {
		return invalidCode, errors.Wrapf(ErrCommandInvalid, "at line #%d, any command should start with + symbol", p.currentLine)
	}

	p.currentCmdSize += len(line)

	if line[1] == 's' && line[2] == 'e' && line[3] == 't' {
		return setCode, nil
	}

	if line[1] == 'd' && line[2] == 'e' && line[3] == 'l' {
		return delCode, nil
	}

	if line[1] == 't' && line[2] == 'a' && line[3] == 'g' {
		return tagCode, nil
	}

	if line[1] == 'u' && line[2] == 'n' && line[3] == 't' && line[4] == 'a' && line[5] == 'g' {
		return untagCode, nil
	}

	return invalidCode, errors.Wrapf(ErrCommandInvalid, "at line #%d command [%s] is unknown", p.currentLine, string(line))
}

func (p *parser) resolveRespSimpleString(r *bufio.Reader) (string, error) {
	p.currentLine++
	strLine, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", io.ErrUnexpectedEOF
		}

		return "", errors.Wrap(ErrCommandInvalid, err.Error())
	}

	p.currentCmdSize += len(strLine)

	if len(strLine) < 4 || strLine[0] != '+' {
		return "", errors.Wrapf(ErrCommandInvalid, "line #%d - %s is invalid", p.currentLine, string(strLine))
	}

	token := string(strLine[1:len(strLine) - 2])

	return token, nil
}

func (p *parser) resolveTagger(r *bufio.Reader) (Tagger, error) {
	p.currentLine++
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if len(line) < 3 || line[0] != '+' || line[len(line) - 1] == ')' {
		return nil, errors.Wrapf(ErrCommandInvalid, "line #%d - %s does not contain a valid function", p.currentLine, string(line))
	}

	fn := string(line[1:len(line) - 2])
	prefix, args, err := resolveTagFnTypeAndArguments(fn)
	if err != nil {
		return nil, err
	}

	p.currentCmdSize += len(line)

	switch prefix {
	case boolTagFn:
		return BoolTag(args[0], args[1] == "true"), nil
	case strTagFn:
		return StrTag(args[0], args[1]), nil
	case intTagFn:
		v, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.Errorf(
			"tag function itg contains invalid integer %s at line %d - %s",
					args[1], p.currentLine, line,
				)
		}
		return IntTag(args[0], v), nil
	case floatTagFn:
		v, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return nil, errors.Errorf(
				"tag function ftg contains invalid float %s in line #%d - %s",
				args[1], p.currentLine, line)
		}
		return FloatTag(args[0], v), nil
	default:
		panic(fmt.Sprintf("at line #%d tag function %s not supported", p.currentLine, prefix))
	}
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

	argsExp := strings.TrimPrefix(expression, prefix + "(")
	argsExp = strings.TrimSuffix(argsExp, ")")
	args = strings.Split(argsExp, ",")

	if len(args) < 2 {
		panic("how args can be less than 2 for tag function")
	}

	return
}

func (p *parser) resolveRespKey(r *bufio.Reader) ([]byte, error) {
	p.currentLine++
	strInfoLine, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrapf(
			ErrCommandInvalid,
			"could not resolve blob: %s at line #%d",
			err.Error(), p.currentLine)
	}

	p.currentCmdSize += len(strInfoLine)

	if len(strInfoLine) == 0 || strInfoLine[0] != '$' {
		return nil, errors.Wrapf(
			ErrCommandInvalid,
			"line #%d - %s does not contain valid length",
			p.currentLine, string(strInfoLine))
	}

	keyLen, err := strconv.Atoi(string(strInfoLine[1:len(strInfoLine) - 2]))
	if err != nil {
		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	key := make([]byte, keyLen+ 2)
	n, err := io.ReadFull(r, key)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	p.currentCmdSize += n

	if n - 2 != keyLen {
		return nil, errors.Wrapf(
			ErrCommandInvalid,
			"line #%d - %s has invalid key",
			p.currentLine, string(strInfoLine))
	}

	return key[:keyLen], nil
}

func (p *parser) resolveRespBlob(r *bufio.Reader) ([]byte, error) {
	p.currentLine++
	strInfoLine, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrapf(
			ErrCommandInvalid,
			"could not resolve blob at line #%d: %v",
			p.currentLine, err)
	}

	p.currentCmdSize += len(strInfoLine)

	if len(strInfoLine) == 0 || strInfoLine[0] != '$' {
		return nil, errors.Wrapf(ErrCommandInvalid, "line #%d - %s is invalid", p.currentLine, string(strInfoLine))
	}

	blobLen, err := strconv.Atoi(string(strInfoLine[1:len(strInfoLine) - 2]))
	if err != nil {
		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	blob := make([]byte, blobLen + 2)
	n, err := io.ReadFull(r, blob)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	p.currentCmdSize += n

	if n - 2 != blobLen {
		return nil, errors.Wrapf(ErrCommandInvalid, "line #%d - %s blob is invalid", p.currentLine, string(strInfoLine))
	}

	return blob[:blobLen], nil
}

func (p *parser) resolveNamesToUntag(segments int, r *bufio.Reader) ([]string, error) {
	result :=make([]string, segments)

	for i := 0; i < segments; i++ {
		p.currentLine++
		line, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}

			return nil, errors.Wrap(ErrCommandInvalid, err.Error())
		}

		if len(line) < 2 || line[0] != '+' {
			return nil, errors.Wrapf(ErrCommandInvalid, "line #%d - %s does not contain a valid function", p.currentLine,  string(line))
		}

		name := string(line[1:len(line) - 2])
		if name == "" {
			return nil, errors.Wrapf(
				ErrCommandInvalid,
				"line #%d - %s does not contain a valid tag name",
				p.currentLine,
				string(line),
			)
		}

		result[i] = name
	}

	return result, nil
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
