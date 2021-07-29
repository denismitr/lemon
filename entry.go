package lemon

import (
	"bytes"
	"github.com/pkg/errors"
)

type entry struct {
	key PK
	value []byte
	tags *Tags
}

func (ent *entry) deserialize(e *Engine) error {
	return e.insert(ent)
}

func newEntry(key string, value []byte, tags *Tags) *entry {
	return &entry{key: PK(key), value: value, tags: tags}
}

func (ent *entry) serialize(buf *bytes.Buffer) {
	writeRespArray(3 + ent.tagCount(), buf)
	writeRespSimpleString("set", buf)
	writeRespSimpleString(ent.key.String(), buf)
	writeRespBlob(ent.value, buf)

	if ent.tagCount() > 0 {
		for _, bt := range ent.tags.Booleans {
			writeRespBoolTag(&bt, buf)
		}

		for _, st := range ent.tags.Strings {
			writeRespStrTag(&st, buf)
		}
	}
}

func (ent *entry) tagCount() int {
	if ent.tags == nil {
		return 0
	}

	var count int
	count += len(ent.tags.Booleans)
	count += len(ent.tags.Strings)
	count += len(ent.tags.FloatTag)
	count += len(ent.tags.IntTag)
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

func (cmd *deleteCmd) deserialize(e *Engine) error {
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

	e.pks.Delete(&entry{key: cmd.key})

	return nil
}