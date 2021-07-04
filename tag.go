package lemon

import (
	"github.com/denismitr/lemon/internal/engine"
)

type TagType uint8

const (
	BoolTagType TagType = iota
)

type Tag interface {
	Name() string
	Type() TagType
	TagIndex() engine.TagIndex
}

func BoolTag(name string, value bool) Tag {
	return boolTag{name: name, value: value}
}

type boolTag struct {
	name string
	value bool
}

func (b boolTag) Name() string {
	return b.name
}

func (b boolTag) Type() TagType {
	return BoolTagType
}

func (b boolTag) TagIndex() engine.TagIndex {
	return engine.NewBoolTagIndex(b.name, b.value)
}

