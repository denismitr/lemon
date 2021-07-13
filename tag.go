package lemon

import (
	"github.com/denismitr/lemon/internal/engine"
)

type TagType uint8

const (
	BoolTagType TagType = iota
)

type Tagger func(t *engine.Tags)

type Tag interface {
	Name() string
	Type() TagType
	TagIndex() engine.TagIndex
}

func BoolTag(name string, value bool) Tagger {
	return func(t *engine.Tags) {
		t.Booleans = append(t.Booleans, engine.BoolTag{K: name, V: value})
	}
}


