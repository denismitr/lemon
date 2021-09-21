package lemon

import (
	"bytes"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
)

var ErrInvalidTagType = errors.New("invalid tag type")

type MetaSetter func(e *entry) error

type entry struct {
	key       PK
	value     []byte
	tags      *tags
	committed bool
}

func (ent *entry) clone() *entry {
	var cpEnt entry
	if err := copier.Copy(&cpEnt, ent); err != nil {
		panic("could not copy entry + " + err.Error())
	}

	return &cpEnt
}

func (ent *entry) deserialize(e engine) error {
	ent.committed = true
	return e.Put(ent, true)
}

func newEntryWithTags(key string, value []byte, tags *tags) *entry {
	return &entry{key: newPK(key), value: value, tags: tags}
}

func newEntry(key string, value []byte) *entry {
	return &entry{key: newPK(key), value: value}
}

func (ent *entry) serialize(buf *bytes.Buffer) {
	writeRespArray(3+ent.tagCount(), buf)
	writeRespSimpleString("set", buf)
	writeRespKeyString(ent.key.String(), buf)
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

		for n, v := range ent.tags.floats {
			writeRespFloatTag(n, v, buf)
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
	writeRespKeyString(cmd.key.String(), buf)
}

func (cmd *deleteCmd) deserialize(e engine) error {
	ent, err := e.FindByKey(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize delete key %s command", cmd.key.String())
	}

	e.RemoveEntry(ent)

	return nil
}
