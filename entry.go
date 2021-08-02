package lemon

import (
	"bytes"
	"github.com/pkg/errors"
)

var ErrInvalidTagType = errors.New("invalid tag type")

type MetaSetter func(e *entry) error

type entry struct {
	key PK
	value []byte
	tags *Tags
}

func (ent *entry) deserialize(e *engine) error {
	return e.insert(ent)
}

func newEntryWithTags(key string, value []byte, tags *Tags) *entry {
	return &entry{key: newPK(key), value: value, tags: tags}
}

func newEntry(key string, value []byte) *entry {
	return &entry{key: newPK(key), value: value}
}

func (ent *entry) serialize(buf *bytes.Buffer) {
	writeRespArray(3 + ent.tagCount(), buf)
	writeRespSimpleString("set", buf)
	writeRespSimpleString(ent.key.String(), buf)
	writeRespBlob(ent.value, buf)

	if ent.tagCount() > 0 {
		for n, v := range ent.tags.booleans {
			writeRespBoolTag(n, v, buf)
		}

		for n, v := range ent.tags.strings {
			writeRespStrTag(n, v, buf)
		}

		for n, v := range ent.tags.integers {
			writeRespIntTag(n, v, buf)
		}
	}
}

func (ent *entry) tagCount() int {
	if ent.tags == nil {
		return 0
	}

	var count int
	count += len(ent.tags.booleans)
	count += len(ent.tags.strings)
	count += len(ent.tags.floats)
	count += len(ent.tags.integers)
	return count
}

type deleteCmd struct {
	key PK
}

func (cmd *deleteCmd) serialize(buf *bytes.Buffer) {
	writeRespArray(2, buf)
	writeRespSimpleString("del", buf)
	writeRespSimpleString(cmd.key.String(), buf)
}

func (cmd *deleteCmd) deserialize(e *engine) error {
	ent, err := e.findByKey(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize delete key %s command", cmd.key.String())
	}

	if e.boolTags != nil {
		e.boolTags.removeEntry(ent)
	}

	if e.strTags != nil {
		e.strTags.removeEntry(ent)
	}

	if e.intTags != nil {
		e.intTags.removeEntry(ent)
	}

	e.pks.Delete(&entry{key: cmd.key})

	return nil
}