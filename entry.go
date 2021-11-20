package lemon

import (
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
)

var ErrInvalidTagType = errors.New("invalid tag type")

type MetaSetter func(e *entry) error

type position struct {
	offset uint
	length uint
}

type entry struct {
	key   PK
	pos   position
	value []byte
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

func newEntryWithTags(key string, pos position, tags tags) *entry {
	return &entry{
		key: newPK(key),
		pos: pos,
		tags: tags,
	}
}

func newEntry(key string, pos position) *entry {
	return &entry{key: newPK(key), pos: pos}
}

func (ent *entry) serialize(rs *respSerializer) error {
	return rs.serializeSetCommand(ent)
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

func (cmd *deleteCmd) serialize(rs *respSerializer) error {
	return rs.serializeDelCommand(cmd)
}

func (cmd *deleteCmd) deserialize(ee executionEngine) error {
	ent, err := ee.FindByKey(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize delete key %s command", cmd.key.String())
	}

	ee.RemoveEntryUnderLock(ent)

	return nil
}
