package lemon

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"strconv"
)

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
			if err := p.parseTagCommand(r, segments, cb); err != nil {
				return p.totalSize, err
			}
		case untagCode:
			if err := p.parseUntagCommand(r, segments, cb); err != nil {
				return p.totalSize, err
			}
		case delCode:
			if err := p.parseDelCommand(r, cb); err != nil {
				return p.totalSize, err
			}
		case setCode:
			if err := p.parseSetCommand(r, segments, cb); err != nil {
				return p.totalSize, err
			}
		case flushAllCode:
			if err := p.parseFlushAllCommand(cb); err != nil {
				return p.totalSize, err
			}
		}

		p.totalCommands++
		p.totalSize += p.currentCmdSize
	}
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


// parseSetCommand - parses `set` command from serialization protocol
func (p *parser) parseSetCommand(r *bufio.Reader, segments int, cb func(d deserializer) error) error {
	key, err := p.resolveRespKey(r)
	if err != nil {
		return err
	}

	value, err := p.resolveRespBlob(r)
	if err != nil {
		return err
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
			return err
		}
		tagger(ent.tags)
	}

	return cb(ent)
}

// parseDelCommand - parses delete entry command from serialization protocol
func (p *parser) parseDelCommand(r *bufio.Reader, cb func(d deserializer) error) error {
	key, err := p.resolveRespKey(r)
	if err != nil {
		return err
	}

	return cb(&deleteCmd{key: newPK(string(key))})
}

// parseUntagCommand - parses untag command from serialization protocol
func (p *parser) parseUntagCommand(r *bufio.Reader, segments int, cb func(d deserializer) error) error {
	key, err := p.resolveRespKey(r)
	if err != nil {
		return err
	}

	names, err := p.resolveNamesToUntag(segments-2, r)
	if err != nil {
		return err
	}

	return cb(&untagCmd{key: newPK(string(key)), names: names})
}

// parses a tag command from serialization protocol
func (p *parser) parseTagCommand(r *bufio.Reader, segments int, cb func(d deserializer) error) error {
	key, err := p.resolveRespKey(r)
	if err != nil {
		return err
	}

	tgs := newTags()
	for j := 0; j < segments; j++ {
		tagger, err := p.resolveTagger(r)
		if err != nil {
			return err
		}

		tagger(tgs)
	}

	return cb(&tagCmd{key: newPK(string(key)), tags: tgs})
}

// resolveRespBlob - resolves a blob from serialization protocol
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

func (p *parser) parseFlushAllCommand(cb func(d deserializer) error) error {
	return cb(&flushAllCmd{})
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

