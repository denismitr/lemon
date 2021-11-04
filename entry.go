package lemon

import (
	"bytes"
	"fmt"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
)

var ErrInvalidTagType = errors.New("invalid tag type")

type MetaSetter func(e *entry) error

type entry struct {
	key       PK
	value     []byte
	tags      tags
	committed bool
}

func (ent *entry) clone() *entry {
	var cpEnt entry
	if err := copier.Copy(&cpEnt, ent); err != nil {
		panic("could not copy entry + " + err.Error())
	}

	return &cpEnt
}

func (ent *entry) deserialize(e executionEngine) error {
	ent.committed = true
	return e.Put(ent, true)
}

func newEntryWithTags(key string, value []byte, tags tags) *entry {
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
		for n, v := range ent.tags {
			switch v.dt {
			case intDataType:
				writeRespIntTag(n, v.data.(int), buf)
			case boolDataType:
				writeRespBoolTag(n, v.data.(bool), buf)
			case strDataType:
				writeRespStrTag(n, v.data.(string), buf)
			case floatDataType:
				writeRespFloatTag(n, v.data.(float64), buf)
			default:
				panic(fmt.Sprintf("unknown tag type %d", v.dt))
			}
		}
	}
}

func (ent *entry) tagCount() int {
	if ent.tags == nil {
		return 0
	}

	return len(ent.tags)
}

type deleteCmd struct {
	key PK
}

func (cmd *deleteCmd) serialize(buf *bytes.Buffer) {
	writeRespArray(2, buf)
	writeRespSimpleString("del", buf)
	writeRespKeyString(cmd.key.String(), buf)
}

func (cmd *deleteCmd) deserialize(e executionEngine) error {
	ent, err := e.FindByKey(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize delete key %s command", cmd.key.String())
	}

	e.RemoveEntry(ent)

	return nil
}
