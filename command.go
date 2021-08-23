package lemon

import (
	"bytes"
	"github.com/pkg/errors"
)

type serializer interface {
	serialize(buf *bytes.Buffer)
}

type deserializer interface {
	deserialize(e *engine) error
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

type flushAllCmd struct {}

func (flushAllCmd) deserialize(e *engine) error {
	return e.flushAll(func (*entry) {})
}

