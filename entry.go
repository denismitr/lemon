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

func newEntry(key string, value []byte, tags *Tags) *entry {
	return &entry{key: PK(key), value: value, tags: tags}
}

func (ent *entry) serialize(buf *bytes.Buffer) {
	respArray(3 + ent.tagCount(), buf)
	respSimpleString("set", buf)
	respSimpleString(ent.key.String(), buf)
	respBlob(ent.value, buf)

	if ent.tagCount() > 0 {
		for _, bt := range ent.tags.Booleans {
			respBoolTag(&bt, buf)
		}

		for _, st := range ent.tags.Strings {
			respStrTag(&st, buf)
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
	respArray(2, buf)
	respSimpleString("del", buf)
	respSimpleString(cmd.key.String(), buf)
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