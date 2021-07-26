package lemon

import "bytes"

type serializable interface {
	serialize(buf *bytes.Buffer)
}
