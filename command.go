package lemon

import "bytes"

type serializer interface {
	serialize(buf *bytes.Buffer)
}

type deserializer interface {
	deserialize(e *Engine) error
}


