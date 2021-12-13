package lemon

import (
	"github.com/pkg/errors"
	"sort"
)

type serializable interface {
	serialize(rs *respSerializer) error
}

type deserializable interface {
	deserialize(e executionEngine) error
}

type untagCmd struct {
	key   PK
	names []string
}

func (cmd *untagCmd) serialize(rs *respSerializer) error {
	return rs.serializeUntagCommand(cmd)
}

func (cmd *untagCmd) deserialize(e executionEngine) error {
	ent, err := e.FindByKey(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize tag command for key %s command", cmd.key.String())
	}

	for _, name := range cmd.names {
		if ent.tags.exists(name) {
			if err := e.RemoveEntryFromTagsByName(name, ent); err != nil {
				return err
			}

			ent.tags.removeByName(name)
		}
	}

	return nil
}

type tagCmd struct {
	key  PK
	tags tags
}

func (cmd *tagCmd) serialize(rs *respSerializer) error {
	return rs.serializeTagCommand(cmd)
}

func sortNames(tgs tags) []string {
	names := make([]string, len(tgs))
	i := 0
	for name := range tgs {
		names[i] = name
		i++
	}

	sort.Strings(names)
	return names
}

func (cmd *tagCmd) deserialize(e executionEngine) error {
	ent, err := e.FindByKey(cmd.key.String())
	if err != nil {
		return errors.Wrapf(err, "could not deserialize tag command for key %s command", cmd.key.String())
	}

	for n, t := range cmd.tags {
		_ = e.RemoveEntryFromTagsByName(n, ent)

		if err := ent.tags.set(n, t.data); err != nil {
			return err
		}

		if err := e.AddTag(n, t.data, ent); err != nil {
			return err
		}
	}

	return nil
}

type flushAllCmd struct{}

func (flushAllCmd) serialize(rs *respSerializer) error {
	return rs.serializeFlushAllCommand()
}

func (flushAllCmd) deserialize(e executionEngine) error {
	return e.FlushAll(func(*entry) {})
}
