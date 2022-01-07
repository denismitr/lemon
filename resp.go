package lemon

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"strconv"
)

const (
	setCommand      = "set"
	delCommand      = "del"
	untagCommand    = "untag"
	tagCommand      = "tag"
	flushAllCommand = "flushall"
)

type respSerializer struct {
	buf bytes.Buffer
	pos int
}

func (rs *respSerializer) reset() {
	rs.pos = 0
}

func (rs *respSerializer) serializeSetCommand(ent *entry) error {
	rs.pos += writeRespArray(3+ent.tagCount(), &rs.buf)
	rs.pos += writeRespSimpleString([]byte(setCommand), &rs.buf)
	rs.pos += writeRespKeyString(ent.key.Bytes(), &rs.buf)
	prefix, total := writeRespBlob(ent.value, &rs.buf)

	ent.pos = position{
		size:   uint64(len(ent.value)),
		offset: uint64(rs.pos + prefix + 1),
	}

	rs.pos += total

	if ent.tagCount() > 0 {
		sortedNames := sortNames(ent.tags)
		for _, name := range sortedNames {
			t := ent.tags[name]

			switch t.dt {
			case intDataType:
				rs.pos += writeRespIntTag(name, t.data.(int), &rs.buf)
			case boolDataType:
				rs.pos += writeRespBoolTag(name, t.data.(bool), &rs.buf)
			case strDataType:
				rs.pos += writeRespStrTag(name, t.data.(string), &rs.buf)
			case floatDataType:
				rs.pos += writeRespFloatTag(name, t.data.(float64), &rs.buf)
			default:
				return errors.Wrapf(ErrInvalidTagType, "unknown tag type %d", t.dt)
			}
		}
	}

	return nil
}

func (rs *respSerializer) serializeDelCommand(cmd *deleteCmd) error {
	rs.pos += writeRespArray(2, &rs.buf)
	rs.pos += writeRespSimpleString([]byte(delCommand), &rs.buf)
	rs.pos += writeRespKeyString(cmd.key.Bytes(), &rs.buf)
	rs.pos += 1
	return nil
}

func (rs *respSerializer) serializeUntagCommand(cmd *untagCmd) error {
	segments := len(cmd.names)
	rs.pos += writeRespArray(segments, &rs.buf)
	rs.pos += writeRespSimpleString([]byte(untagCommand), &rs.buf)
	rs.pos += writeRespKeyString(cmd.key.Bytes(), &rs.buf)

	for _, n := range cmd.names {
		rs.pos += writeRespSimpleString([]byte(n), &rs.buf)
	}

	return nil
}

func (rs *respSerializer) serializeTagCommand(cmd *tagCmd) error {
	segments := cmd.tags.count()
	rs.pos += writeRespArray(segments, &rs.buf)
	rs.pos += writeRespSimpleString([]byte(tagCommand), &rs.buf)
	rs.pos += writeRespKeyString(cmd.key.Bytes(), &rs.buf)

	sortedNames := sortNames(cmd.tags)

	for _, name := range sortedNames {
		t := cmd.tags[name]
		switch t.dt {
		case intDataType:
			rs.pos += writeRespIntTag(name, t.data.(int), &rs.buf)
		case floatDataType:
			rs.pos += writeRespFloatTag(name, t.data.(float64), &rs.buf)
		case boolDataType:
			rs.pos += writeRespBoolTag(name, t.data.(bool), &rs.buf)
		case strDataType:
			rs.pos += writeRespStrTag(name, t.data.(string), &rs.buf)
		default:
			return errors.Wrapf(ErrInvalidTagType, "invalid tag type %d", t.dt)
		}
	}

	return nil
}

func (rs *respSerializer) serializeFlushAllCommand() error {
	rs.pos += writeRespArray(1, &rs.buf)
	rs.pos += writeRespSimpleString([]byte(flushAllCommand), &rs.buf)
	return nil
}

func writeRespArray(segments int, buf *bytes.Buffer) int {
	buf.WriteRune('*')
	s := strconv.FormatInt(int64(segments), 10)
	buf.WriteString(s)
	buf.WriteRune('\r')
	buf.WriteRune('\n')

	return 3 + len(s)
}

func writeRespBoolTag(name string, v bool, buf *bytes.Buffer) int {
	return writeRespFunc([]byte(fmt.Sprintf("btg(%s,%v)", name, v)), buf)
}

func writeRespStrTag(name, v string, buf *bytes.Buffer) int {
	return writeRespFunc([]byte(fmt.Sprintf("stg(%s,%s)", name, v)), buf)
}

func writeRespIntTag(name string, v int, buf *bytes.Buffer) int {
	return writeRespFunc([]byte(fmt.Sprintf("%s(%s,%d)", intTagFn, name, v)), buf)
}

func writeRespFloatTag(name string, v float64, buf *bytes.Buffer) int {
	return writeRespFunc([]byte(fmt.Sprintf("%s(%s,%v)", floatTagFn, name, v)), buf)
}

func writeRespSimpleString(b []byte, buf *bytes.Buffer) int {
	buf.WriteRune('+')
	buf.Write(b)
	buf.WriteRune('\r')
	buf.WriteRune('\n')
	return 3 + len(b)
}

func writeRespKeyString(b []byte, buf *bytes.Buffer) int {
	buf.WriteRune('$')
	l, _ := buf.Write([]byte(strconv.FormatInt(int64(len(b)), 10)))
	buf.WriteRune('\r')
	buf.WriteRune('\n')
	n, _ := buf.Write(b)
	buf.WriteRune('\r')
	buf.WriteRune('\n')
	return 4 + l + n
}

func writeRespFunc(fn []byte, buf *bytes.Buffer) int {
	buf.WriteRune('+')
	buf.Write(fn)
	buf.WriteRune('\r')
	buf.WriteRune('\n')

	return 3 + len(fn)
}

func writeRespBlob(blob []byte, buf *bytes.Buffer) (int, int) {
	buf.WriteRune('$')
	l := []byte(strconv.FormatInt(int64(len(blob)), 10))
	buf.Write(l)
	buf.WriteRune('\r')
	buf.WriteRune('\n')
	buf.Write(blob)
	buf.WriteRune('\r')
	buf.WriteRune('\n')

	prefix := 1 + len(l) + 2
	total := prefix + len(blob) + 2
	return prefix, total
}
