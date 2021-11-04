package lemon

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
)

type serializer interface {
	serialize(buf *bytes.Buffer)
}

type deserializer interface {
	deserialize(e executionEngine) error
}

type untagCmd struct {
	key   PK
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

func (cmd *untagCmd) deserialize(e executionEngine) error {
	ent, err := e.FindByKey(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize tag command for key %s command", cmd.key.String())
	}

	for _, name := range cmd.names {
		dt, ok := ent.tags.getTypeByName(name)
		if ok {
			e.RemoveEntryFromTagsByNameAndType(name, dt, ent)
			ent.tags.removeByNameAndType(name)
		}
	}

	return nil
}

type tagCmd struct {
	key  PK
	tags tags
}

func (cmd *tagCmd) serialize(buf *bytes.Buffer) {
	segments := cmd.tags.count()
	writeRespArray(segments, buf)
	writeRespSimpleString("tag", buf)
	writeRespKeyString(cmd.key.String(), buf)
	for n, t := range cmd.tags {
		switch t.dt {
		case intDataType:
			writeRespIntTag(n, t.data.(int), buf)
		case floatDataType:
			writeRespFloatTag(n, t.data.(float64), buf)
		case boolDataType:
			writeRespBoolTag(n, t.data.(bool), buf)
		case strDataType:
			writeRespStrTag(n, t.data.(string), buf)
		default:
			panic(fmt.Sprintf("invalid tag type %d", t.dt))
		}
	}
}

func (cmd *tagCmd) deserialize(e executionEngine) error {
	ent, err := e.FindByKey(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize tag command for key %s command", cmd.key.String())
	}

	for n, t := range cmd.tags {
		e.RemoveEntryFromTagsByNameAndType(n, t.dt, ent)
		ent.tags.set(n, t.data)
		if err := e.AddTag(n, t.data, ent); err != nil {
			return err
		}
	}

	return nil
}

type flushAllCmd struct{}

func (c flushAllCmd) serialize(buf *bytes.Buffer) {
	writeRespArray(1, buf)
	writeRespSimpleString("flushall", buf)
}

func (flushAllCmd) deserialize(e executionEngine) error {
	return e.FlushAll(func(*entry) {})
}
