package lemon

import (
	"bytes"
	"fmt"
	"strconv"
)

type entry struct {
	key PK
	value []byte
	tags *Tags
}

func newEntry(key string, value []byte, tags *Tags) *entry {
	return &entry{key: PK(key), value: value, tags: tags}
}

func (e *entry) serialize(buf *bytes.Buffer) {
	appendSegmentsCountTo(3, buf)
	appendStringTo("put", buf)
	appendStringTo(e.key.String(), buf)
	appendBlobTo(e.value, buf)

	if e.tagCount() > 0 {
		appendSegmentsCountTo(2 + e.tagCount(), buf)
		appendStringTo("tag", buf)
		appendStringTo(e.key.String(), buf)
		for _, bt := range e.tags.Booleans {
			appendBoolTagTo(&bt, buf)
		}

		for _, st := range e.tags.Strings {
			appendStrTagTo(&st, buf)
		}
	}
}

func (e *entry) tagCount() int {
	if e.tags == nil {
		return 0
	}

	var count int
	count += len(e.tags.Booleans)
	count += len(e.tags.Strings)
	count += len(e.tags.FloatTag)
	count += len(e.tags.IntTag)
	return count
}

func appendSegmentsCountTo(segments int, buf *bytes.Buffer) {
	buf.WriteRune('*')
	buf.WriteString(strconv.FormatInt(int64(segments), 10))
	buf.WriteRune('\r')
	buf.WriteRune('\n')
}

func appendBoolTagTo(bt *boolTag, buf *bytes.Buffer) {
	appendStringTo(fmt.Sprintf("btg(%s,%v)", bt.Name, bt.Value), buf)
}

func appendStrTagTo(st *strTag, buf *bytes.Buffer) {
	appendStringTo(fmt.Sprintf("stg(%s,%s)", st.Name, st.Value), buf)
}

func appendStringTo(s string, buf *bytes.Buffer) {
	buf.WriteRune('$')
	buf.WriteString(strconv.FormatInt(int64(len(s)), 10))
	buf.WriteRune('\n')
	buf.WriteRune('\r')
	buf.WriteString(s)
	buf.WriteRune('\n')
	buf.WriteRune('\r')
}

func appendBlobTo(blob []byte, buf *bytes.Buffer) {
	buf.WriteRune('@')
	buf.WriteString(strconv.FormatInt(int64(len(blob)), 10))
	buf.WriteRune('\n')
	buf.WriteRune('\r')
	buf.Write(blob)
	buf.WriteRune('\n')
	buf.WriteRune('\r')
}

type deleteCmd struct {
	key PK
}

func (e *deleteCmd) serialize(buf *bytes.Buffer) {
	appendSegmentsCountTo(2, buf)
	appendStringTo("del", buf)
	appendStringTo(e.key.String(), buf)
}