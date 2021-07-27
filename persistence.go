package lemon

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"strconv"

	"os"
)

var ErrDbFileWriteFailed = errors.New("database write failed")
var ErrSourceFileReadFailed = errors.New("source file read failed")
var ErrCommandInvalid = errors.New("command invalid")
var ErrUnexpectedEof = errors.New("unexpected end of file")

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
	f *os.File
	flushes int
}

func newPersistence(filepath string, strategy persistenceStrategy) (*persistence, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &persistence{
		f: f,
		strategy: strategy,
	}, nil
}

func (p *persistence) load(cb func(d deserializer) error) error {
	_, err := p.f.Stat()
	if err != nil {
		return errors.Wrapf(err, "could not collect file %s stats", p.f.Name())
	}

	return nil
}

func readCommands(r *bufio.Reader, cb func(d deserializer) error) (int, error) {
	n := int64(0)
	totalSize := 0
	bufBytes := [1024]byte{}

	for {
		cmdByteSize := 0
		firstByte, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return totalSize, nil
			} else {
				return totalSize, errors.Wrap(ErrSourceFileReadFailed, err.Error())
			}
		}

		if firstByte == 0 {
			n += 1
			continue
		}

		if err := r.UnreadByte(); err != nil {
			return totalSize, errors.Wrap(ErrSourceFileReadFailed, err.Error())
		}

		// read a command
		line, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return totalSize, io.ErrUnexpectedEOF
			}

			return totalSize, errors.Wrap(ErrSourceFileReadFailed, err.Error())
		}

		// should be \*\d{1,}\\r
		// for now we only expects array like commands
		if len(line) < 3 || line[0] != '*' {
			return totalSize, ErrCommandInvalid
		}

		segments, bytesInLine, err := resolveRespArrayFromLine(bufBytes[:], line)
		if err != nil {
			return totalSize, err
		}

		cmdByteSize += bytesInLine

		cmdCode, bytesInLine, err := resolveRespCommandCode(r)
		if err != nil {
			return totalSize, err
		}

		cmdByteSize += bytesInLine

		switch cmdCode {
		case delCode:
			key, bytesInLine, err := resolveRespSimpleString(r)
			if err != nil {
				return totalSize, err
			}
			cmdByteSize += bytesInLine
			if err := cb(&deleteCmd{key: newPK(key)}); err != nil {
				return totalSize, err
			}
		case setCode:
			key, bytesInLine, err := resolveRespSimpleString(r)
			if err != nil {
				return totalSize, err
			}
			cmdByteSize += bytesInLine

			ent := &entry{key: newPK(key)}
			value, bytesInBlob, err := resolveRespBlobString(r)
			if err != nil {
				return totalSize, err
			}

			ent.value = value
			cmdByteSize += bytesInBlob

			// subtracting command, key and value
			segments -= 3
			for j := 0; j < segments; j++ {

			}

			if err := cb(ent); err != nil {
				return totalSize, err
			}
		}

		totalSize += cmdByteSize
	}
}

func (p *persistence) write(buf bytes.Buffer) error {
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

func resolveRespArrayFromLine(cmdBuf []byte, line []byte) (int, int, error) {
	for _, r := range line[1:] {
		if r >= '0' && r <= '9' {
			cmdBuf = append(cmdBuf, r)
		}
	}

	n, err := strconv.Atoi(string(cmdBuf))
	if err != nil {
		return 0, len(line), errors.Wrap(ErrCommandInvalid, err.Error())
	}

	return n, len(line), nil
}

func resolveRespCommandCode(r *bufio.Reader) (commandCode, int, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return invalidCode, 0, err
	}

	if len(line) < 3 {
		return invalidCode, 0, ErrCommandInvalid
	}

	if line[0] == 's' && line[1] == 'e' && line[2] == 't' {
		return setCode, len(line), nil
	}

	if line[0] == 'd' && line[1] == 'e' && line[2] == 'l' {
		return delCode, len(line), nil
	}

	return invalidCode, 0, errors.Wrapf(ErrCommandInvalid, "line %s is invalid", string(line))
}

func resolveRespSimpleString(r *bufio.Reader) (string, int, error) {
	strInfoLine, err := r.ReadBytes('\n')
	if err != nil {
		return "", 0, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if len(strInfoLine) == 0 || strInfoLine[0] != '+' {
		return "", len(strInfoLine), errors.Wrapf(ErrCommandInvalid, "line %s is invalid", string(strInfoLine))
	}

	strLen, err := strconv.Atoi(string(strInfoLine[1:len(strInfoLine) - 2]))
	if err != nil {
		return "", len(strInfoLine), errors.Wrap(ErrCommandInvalid, err.Error())
	}

	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", len(strInfoLine), io.ErrUnexpectedEOF
		}

		return "", len(strInfoLine), errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if len(line) - 2 != strLen {
		return "", len(strInfoLine), errors.Wrapf(ErrCommandInvalid, "line %s is invalid", string(strInfoLine))
	}

	return string(line[0:strLen]), len(line) + len(strInfoLine), nil
}

func resolveRespBlobString(r *bufio.Reader) ([]byte, int, error) {
	strInfoLine, err := r.ReadBytes('\n')
	if err != nil {
		return nil, 0, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if len(strInfoLine) == 0 || strInfoLine[0] != '$' {
		return nil, len(strInfoLine), errors.Wrapf(ErrCommandInvalid, "line %s is invalid", string(strInfoLine))
	}

	blobLen, err := strconv.Atoi(string(strInfoLine[1:len(strInfoLine) - 2]))
	if err != nil {
		return nil, len(strInfoLine), errors.Wrap(ErrCommandInvalid, err.Error())
	}

	blob := make([]byte, blobLen)
	n, err := io.ReadFull(r, blob)
	if err != nil {
		return nil, len(strInfoLine), errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if n < blobLen {
		return nil, len(strInfoLine), errors.Wrapf(ErrCommandInvalid, "line %s blob is invalid", string(strInfoLine))
	}

	return blob, n + len(strInfoLine), nil
}

func respArray(segments int, buf *bytes.Buffer) {
	buf.WriteRune('*')
	buf.WriteString(strconv.FormatInt(int64(segments), 10))
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}

func respBoolTag(bt *boolTag, buf *bytes.Buffer) {
	respSimpleString(fmt.Sprintf("btg(%s,%v)", bt.Name, bt.Value), buf)
}

func respStrTag(st *strTag, buf *bytes.Buffer) {
	respSimpleString(fmt.Sprintf("stg(%s,%s)", st.Name, st.Value), buf)
}

func respSimpleString(s string, buf *bytes.Buffer) {
	buf.WriteRune('+')
	buf.WriteString(strconv.FormatInt(int64(len(s)), 10))
	buf.WriteRune('\n')
	buf.WriteRune('\r')
	buf.WriteString(s)
	buf.WriteRune('\n')
	buf.WriteRune('\r')
}

func respBlob(blob []byte, buf *bytes.Buffer) {
	buf.WriteRune('$')
	buf.WriteString(strconv.FormatInt(int64(len(blob)), 10))
	buf.WriteRune('\n')
	buf.WriteRune('\r')
	buf.Write(blob)
	buf.WriteRune('\n')
	buf.WriteRune('\r')
}
