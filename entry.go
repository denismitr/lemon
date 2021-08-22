package lemon

import (
	"bytes"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
)

var ErrInvalidTagType = errors.New("invalid tag type")

type MetaSetter func(e *entry) error

type entry struct {
	key PK
	value []byte
	tags *tags
}

func (ent *entry) clone() *entry {
	var cpEnt entry
	if err := copier.Copy(&cpEnt, ent); err != nil {
		panic("could not copy entry + " + err.Error())
	}

	return &cpEnt
}

func (ent *entry) deserialize(e *engine) error {
	return e.putUnderLock(ent, true) // todo: append under lock?
}

func newEntryWithTags(key string, value []byte, tags *tags) *entry {
	return &entry{key: newPK(key), value: value, tags: tags}
}

func newEntry(key string, value []byte) *entry {
	return &entry{key: newPK(key), value: value}
}

func (ent *entry) serialize(buf *bytes.Buffer) {
	writeRespArray(3 + ent.tagCount(), buf)
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

type untagCmd struct {
	key PK
	names []string
}

func (cmd *untagCmd) serialize(buf *bytes.Buffer) {
	segments := len(cmd.names)
	writeRespArray(segments, buf)
	writeRespSimpleString("untag", buf)
	writeRespKeyString(cmd.key.String(), buf)
	for _, n := range cmd.names {
		writeRespSimpleString(n, buf)
	}
}

func (cmd *untagCmd) deserialize(e *engine) error {
	ent, err := e.findByKeyUnderLock(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize tag command for key %s command", cmd.key.String())
	}

	for _, name := range cmd.names {
		dt, ok := ent.tags.getTypeByName(name)
		if ok {
			e.tags.removeEntryByNameAndType(name, dt, ent)
			ent.tags.removeByNameAndType(name, dt)
		}
	}

	return nil
}

type tagCmd struct {
	key PK
	tags *tags
}

func (cmd *tagCmd) serialize(buf *bytes.Buffer) {
	segments := cmd.tags.count()
	writeRespArray(segments, buf)
	writeRespSimpleString("tag", buf)
	writeRespKeyString(cmd.key.String(), buf)
	for n, v := range cmd.tags.integers {
		writeRespIntTag(n, v, buf)
	}
	for n, v := range cmd.tags.floats {
		writeRespFloatTag(n, v, buf)
	}
	for n, v := range cmd.tags.booleans {
		writeRespBoolTag(n, v, buf)
	}
	for n, v := range cmd.tags.strings {
		writeRespStrTag(n, v, buf)
	}
}

func (cmd *tagCmd) deserialize(e *engine) error {
	ent, err := e.findByKeyUnderLock(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize tag command for key %s command", cmd.key.String())
	}

	for n, v := range cmd.tags.integers {
		e.tags.removeEntryByNameAndType(n, intDataType, ent)
		ent.tags.set(n, v)
		if err := e.tags.add(n, v, ent); err != nil {
			return err
		}
	}

	for n, v := range cmd.tags.strings {
		e.tags.removeEntryByNameAndType(n, strDataType, ent)
		ent.tags.set(n, v)
		if err := e.tags.add(n, v, ent); err != nil {
			return err
		}
	}

	for n, v := range cmd.tags.booleans {
		e.tags.removeEntryByNameAndType(n, boolDataType, ent)
		ent.tags.set(n, v)
		if err := e.tags.add(n, v, ent); err != nil {
			return err
		}
	}

	for n, v := range cmd.tags.floats {
		e.tags.removeEntryByNameAndType(n, floatDataType, ent)
		ent.tags.set(n, v)
		if err := e.tags.add(n, v, ent); err != nil {
			return err
		}
	}

	return nil
}

type deleteCmd struct {
	key PK
}

func (cmd *deleteCmd) serialize(buf *bytes.Buffer) {
	writeRespArray(2, buf)
	writeRespSimpleString("del", buf)
	writeRespKeyString(cmd.key.String(), buf)
}

func (cmd *deleteCmd) deserialize(e *engine) error {
	ent, err := e.findByKeyUnderLock(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize delete key %s command", cmd.key.String())
	}

	e.tags.removeEntry(ent)

	e.pks.Delete(&entry{key: cmd.key})

	return nil
}