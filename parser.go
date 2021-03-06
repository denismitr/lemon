package lemon

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"strconv"
)

type respParser struct {
	vls            ValueLoadStrategy
	totalSize      int
	buf            [1024]byte
	currentCmdSize int
	totalCommands  int
	cursor         int
	currentLine    uint8
}

func (p *respParser) parse(
	r *bufio.Reader,
	cache cache,
	cb func(d deserializable) error,
) (int, error) {
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
			p.cursor++
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
			if err := p.parseSetCommand(r, cache, segments, cb); err != nil {
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

func (p *respParser) resolveTagger(r *bufio.Reader) (Tagger, error) {
	p.currentLine++
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if len(line) < 3 || line[0] != '+' || line[len(line)-1] == ')' {
		return nil, errors.Wrapf(
			ErrCommandInvalid,
			"line #%d - %s does not contain a valid function",
			p.currentLine,
			string(line),
		)
	}

	fn := string(line[1 : len(line)-2])
	prefix, args, err := resolveTagFnTypeAndArguments(fn)
	if err != nil {
		return nil, err
	}

	p.currentCmdSize += len(line)
	p.cursor += len(line)

	switch prefix {
	case boolTagFn:
		return boolTagger(args[0], args[1] == "true"), nil
	case strTagFn:
		return strTagger(args[0], args[1]), nil
	case intTagFn:
		v, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, errors.Errorf(
				"tag function itg contains invalid integer %s at line %d - %s",
				args[1], p.currentLine, line,
			)
		}
		return intTagger(args[0], v), nil
	case floatTagFn:
		v, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return nil, errors.Errorf(
				"tag function ftg contains invalid float %s in line #%d - %s",
				args[1], p.currentLine, line)
		}
		return floatTagger(args[0], v), nil
	default:
		panic(fmt.Sprintf("at line #%d tag function %s not supported", p.currentLine, prefix))
	}
}

// parseSetCommand - parses `set` command from serialization protocol
func (p *respParser) parseSetCommand(
	r *bufio.Reader,
	cache cache,
	segments int,
	cb func(d deserializable) error,
) error {
	key, err := p.resolveRespKey(r)
	if err != nil {
		return err
	}

	value, blobOffset, err := p.resolveRespBlob(r)
	if err != nil {
		return err
	}

	pos := position{offset: uint64(blobOffset), size: uint64(len(value))}
	ent := newEntryWithTags(string(key), pos, nil)

	if p.vls == BufferedLoad {
		cache.Add(pos.offset, value)
	} else if p.vls == EagerLoad {
		ent.value = value
	}

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

	// todo: send ent to output channel
	return cb(ent)
}

// parseDelCommand - parses delete entry command from serialization protocol
func (p *respParser) parseDelCommand(
	r *bufio.Reader,
	cb func(d deserializable) error,
) error {
	key, err := p.resolveRespKey(r)
	if err != nil {
		return err
	}

	return cb(&deleteCmd{key: newPK(string(key))})
}

// parseUntagCommand - parses untag command from serialization protocol
func (p *respParser) parseUntagCommand(r *bufio.Reader, segments int, cb func(d deserializable) error) error {
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
func (p *respParser) parseTagCommand(r *bufio.Reader, segments int, cb func(d deserializable) error) error {
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
func (p *respParser) resolveRespBlob(r *bufio.Reader) ([]byte, int, error) {
	p.currentLine++
	strInfoLine, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, io.ErrUnexpectedEOF
		}

		return nil, 0, errors.Wrapf(
			ErrCommandInvalid,
			"could not resolve blob at line #%d: %v",
			p.currentLine, err)
	}

	if len(strInfoLine) == 0 || strInfoLine[0] != '$' {
		return nil, 0, errors.Wrapf(
			ErrCommandInvalid,
			"line #%d - %s is invalid", p.currentLine, string(strInfoLine),
		)
	}

	p.currentCmdSize += len(strInfoLine)
	p.cursor += len(strInfoLine)

	blobLen, err := strconv.Atoi(string(strInfoLine[1 : len(strInfoLine)-2]))
	if err != nil {
		return nil, 0, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	blob := make([]byte, blobLen+2)
	n, err := io.ReadFull(r, blob)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, io.ErrUnexpectedEOF
		}

		return nil, 0, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if n-2 != blobLen {
		return nil, 0, errors.Wrapf(
			ErrCommandInvalid,
			"line #%d - %s blob is invalid",
			p.currentLine,
			string(strInfoLine),
		)
	}

	blobOffset := p.cursor
	p.currentCmdSize += n
	p.cursor += n

	return blob[:blobLen], blobOffset, nil
}

func (p *respParser) resolveNamesToUntag(segments int, r *bufio.Reader) ([]string, error) {
	result := make([]string, segments)

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
			return nil, errors.Wrapf(
				ErrCommandInvalid,
				"line #%d - %s does not contain a valid function",
				p.currentLine,
				string(line),
			)
		}

		p.cursor += len(line)

		name := string(line[1 : len(line)-2])
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

func (p *respParser) parseFlushAllCommand(cb func(d deserializable) error) error {
	return cb(&flushAllCmd{})
}

func (p *respParser) resolveRespArrayFromLine(r *bufio.Reader) (int, error) {
	// read a command
	p.currentLine++
	line, err := r.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}

		return 0, errors.Wrapf(
			ErrSourceFileReadFailed,
			"could not parse array at line #%d: %s",
			p.currentLine,
			err.Error(),
		)
	}

	p.cursor += len(line)

	// fixme: investigate - seems we are getting phantoms from prev line
	if len(line) == 2 {
		p.currentLine++
		line, _ = r.ReadBytes('\n')
		p.cursor += len(line)
	}

	// should be \*\d{1,}\\r
	// for now we only expects array like commands
	if len(line) < 2 || line[0] != '*' {
		p.cursor -= len(line)

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

func (p *respParser) resolveRespCommandCode(r *bufio.Reader) (commandCode, error) {
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
		return invalidCode, errors.Wrapf(
			ErrCommandInvalid,
			"at line #%d, any command should start with + symbol",
			p.currentLine,
		)
	}

	p.cursor += len(line)
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

	p.cursor -= len(line)

	return invalidCode, errors.Wrapf(
		ErrCommandInvalid,
		"at line #%d command [%s] is unknown",
		p.currentLine,
		string(line),
	)
}

func (p *respParser) resolveRespKey(r *bufio.Reader) ([]byte, error) {
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

	if len(strInfoLine) == 0 || strInfoLine[0] != '$' {
		return nil, errors.Wrapf(
			ErrCommandInvalid,
			"line #%d - %s does not contain valid length",
			p.currentLine, string(strInfoLine))
	}

	p.currentCmdSize += len(strInfoLine)
	p.cursor += len(strInfoLine)

	keyLen, err := strconv.Atoi(string(strInfoLine[1 : len(strInfoLine)-2]))
	if err != nil {
		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	key := make([]byte, keyLen+2)
	n, err := io.ReadFull(r, key)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}

		return nil, errors.Wrap(ErrCommandInvalid, err.Error())
	}

	if n-2 != keyLen {
		return nil, errors.Wrapf(
			ErrCommandInvalid,
			"line #%d - %s has invalid key",
			p.currentLine, string(strInfoLine))
	}

	p.currentCmdSize += n
	p.cursor += n

	return key[:keyLen], nil
}
