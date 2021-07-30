package lemon

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"strconv"
	"strings"

	"os"
)

var ErrDbFileWriteFailed = errors.New("database write failed")
var ErrSourceFileReadFailed = errors.New("source file read failed")
var ErrCommandInvalid = errors.New("command invalid")
var ErrUnexpectedEof = errors.New("unexpected end of file")
var ErrParseFailed = errors.New("commands parse error")
var ErrStorageFailed = errors.New("storage error")

type persistenceStrategy string

type commandCode int8

const (
	invalidCode commandCode = iota
	setCode
	delCode
)

const (
	Async persistenceStrategy = "async"
	Sync persistenceStrategy = "sync"

)

type persistence struct {
	strategy persistenceStrategy
	parser *parser
	f *os.File
	closer fileCloser
	flushes int
	cursor int
}

type fileCloser func() error

func newPersistence(filepath string, strategy persistenceStrategy) (*persistence, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &persistence{
		f: f,
		closer: f.Close,
		strategy: strategy,
	}, nil
}

func (p *persistence) load(cb func(d deserializer) error) error {
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
	n int
}

func (p *parser) parse(r *bufio.Reader, cb func(d deserializer) error) (int, error) {
	for {
		p.currentCmdSize = 0

		firstByte, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return p.totalSize, nil
			} else {
				return p.totalSize, errors.Wrap(ErrSourceFileReadFailed, err.Error())
			}
		}

		if firstByte == 0 {
			p.n += 1
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
		case delCode:
			key, err := p.resolveRespSimpleString(r)
			if err != nil {
				return p.totalSize, err
			}

			if err := cb(&deleteCmd{key: newPK(key)}); err != nil {
				return p.totalSize, err
			}
		case setCode:
			key, err := p.resolveRespSimpleString(r)
			if err != nil {
				return p.totalSize, err
			}

			value, err := p.resolveRespBlobString(r)
			if err != nil {
				return p.totalSize, err
			}

			ent := newEntry(key, value, nil)

			// subtracting command, key and value
			segments -= 3
			if segments > 0 {
				ent.tags = &Tags{} // fixme
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

		p.totalCommands += 1
		p.totalSize += p.currentCmdSize
	}
}

func (p *persistence) write(buf *bytes.Buffer) error {
	n, err := p.f.Write(buf.Bytes())
	if err != nil {
		if n > 0 {
			// partial write occurred, must rollback the file
			pos, seekErr := p.f.Seek(-int64(n), 1)
			if seekErr != nil {
				panic(seekErr)
			}

			if err := p.f.Truncate(pos); err != nil {
				panic(err)
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

func (p *parser) resolveRespArrayFromLine(r *bufio.Reader) (int, error) {
	// read a command
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}

		return 0, errors.Wrapf(ErrSourceFileReadFailed, "could not parse array from line: %s", err.Error())
	}

	// fixme
	if len(line) == 2 {
		line, _  = r.ReadBytes('\n')
	}

	// should be \*\d{1,}\\r
	// for now we only expects array like commands
	if len(line) < 2 || line[0] != '*' {
		return p.totalSize, errors.Wrapf(ErrCommandInvalid, "line %s should actually start with *", string(line))
	}

	cmdBuf := p.buf[:0]
	for _, r := range line[1:] {
		if r >= '0' && r <= '9' {
			cmdBuf = append(cmdBuf, r)
		}
	}

	n, err := strconv.Atoi(string(cmdBuf))
	if err != nil {
		return 0, errors.Wrapf(ErrCommandInvalid, "could not parse command size %s", err.Error())
	}

	p.currentCmdSize += len(line)

	return n, nil
}

func (p *parser) resolveRespCommandCode(r *bufio.Reader) (commandCode, error) {
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
		return invalidCode, errors.Wrap(ErrCommandInvalid, "any command should start with + symbol")
	}

	p.currentCmdSize += len(line)

	if line[1] == 's' && line[2] == 'e' && line[3] == 't' {
		return setCode, nil
	}

	if line[1] == 'd' && line[2] == 'e' && line[3] == 'l' {
		return delCode, nil
	}

	return invalidCode, errors.Wrapf(ErrCommandInvalid, "command [%s] is unknown", string(line))
}

func (p *parser) resolveRespSimpleString(r *bufio.Reader) (string, error) {
	strLine, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", io.ErrUnexpectedEOF
		}

		return "", errors.Wrap(ErrCommandInvalid, err.Error())
	}

	p.currentCmdSize += len(strLine)

	if len(strLine) < 4 || strLine[0] != '+' {
		return "", errors.Wrapf(ErrCommandInvalid, "line %s is invalid", string(strLine))
	}

	token := string(strLine[1:len(strLine) - 2])

	return token, nil
}

const (
	boolTagFn = "btg"
	strTagFn = "stg"
)

func (p *parser) resolveTagger(r *bufio.Reader) (Tagger, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if len(line) < 3 || line[0] != '@' || line[len(line) - 1] == ')' {
		return nil, errors.Wrapf(ErrCommandInvalid, "line %s does not contain a valid function", string(line))
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
	default:
		panic(fmt.Sprintf("tag function %s not supported", prefix))
	}
}

func resolveTagFnTypeAndArguments(expression string) (prefix string, args []string, err error) {
	for _, p := range []string{boolTagFn, strTagFn} {
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

func (p *parser) resolveRespBlobString(r *bufio.Reader) ([]byte, error) {
	strInfoLine, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrapf(ErrCommandInvalid, "could not resolve blob: %s", err.Error())
	}

	p.currentCmdSize += len(strInfoLine)

	if len(strInfoLine) == 0 || strInfoLine[0] != '$' {
		return nil, errors.Wrapf(ErrCommandInvalid, "line %s is invalid", string(strInfoLine))
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
		return nil, errors.Wrapf(ErrCommandInvalid, "line %s blob is invalid", string(strInfoLine))
	}

	return blob[:blobLen], nil
}

func writeRespArray(segments int, buf *bytes.Buffer) {
	buf.WriteRune('*')
	buf.WriteString(strconv.FormatInt(int64(segments), 10))
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}

func writeRespBoolTag(bt *boolTag, buf *bytes.Buffer) {
	writeRespFunc(fmt.Sprintf("btg(%s,%v)", bt.Name, bt.Value), buf)
}

func writeRespStrTag(st *strTag, buf *bytes.Buffer) {
	writeRespFunc(fmt.Sprintf("stg(%s,%s)", st.Name, st.Value), buf)
}

func writeRespSimpleString(s string, buf *bytes.Buffer) {
	buf.WriteRune('+')
	buf.WriteString(s)
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}

func writeRespFunc(fn string, buf *bytes.Buffer) {
	buf.WriteRune('@')
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
