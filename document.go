package lemon

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

var ErrJsonCouldNotBeUnmarshalled = errors.New("json contents could not be unmarshalled, probably is invalid")
var ErrJsonPathInvalid = errors.New("json path is invalid")

type JsonValue struct {
	b []byte
}

type Document struct {
	key string
	tags M
	value []byte
}

func (d *Document) Key() string {
	return d.key
}

func newDocumentFromEntry(ent *entry) *Document {
	d :=  &Document{
		key: ent.key.String(),
		value: ent.value, // fixme: maybe copy
		tags: createMapFromTags(ent.tags),
	}

	return d
}

func (d *Document) Value() []byte {
	return d.value
}

func (d *Document) Json() *JsonValue {
	return &JsonValue{b: d.value}
}

func (d *Document) RawString() string {
	return string(d.value)
}

func createMapFromTags(t *tags) M {
	result := make(M)
	if t == nil {
		return result
	}

	for k, v := range t.integers {
		result[k] = v
	}

	for k, v := range t.strings {
		result[k] = v
	}

	for k, v := range t.floats {
		result[k] = v
	}

	for k, v := range t.booleans {
		result[k] = v
	}

	return result
}

func (d *Document) Tags() M {
	return d.tags
}

func (js *JsonValue) Unmarshal(dest interface{}) error {
	err := json.Unmarshal(js.b, &dest)
	if err != nil {
		return errors.Wrap(ErrJsonCouldNotBeUnmarshalled, err.Error())
	}

	return nil
}

func (js *JsonValue) String(path string) (string, error) {
	raw := gjson.GetBytes(js.b, path)
	if !raw.Exists() {
		return "", ErrJsonPathInvalid
	}
	return raw.String(), nil
}

func (js *JsonValue) StringOrDefault(path, def string) string {
	if v, err := js.String(path); err != nil {
		return def
	} else {
		return v
	}
}

func (js *JsonValue) Float(path string) (float64, error) {
	get := gjson.GetBytes(js.b, path)
	if !get.Exists() {
		return 0, ErrJsonPathInvalid
	}
	return get.Float(), nil
}

func (js *JsonValue) FloatOrDefault(path string, def float64) float64 {
	if v, err := js.Float(path); err != nil {
		return def
	} else {
		return v
	}
}

func (js *JsonValue) Int(path string) (int, error) {
	get := gjson.GetBytes(js.b, path)
	if !get.Exists() {
		return 0, ErrJsonPathInvalid
	}

	return int(get.Int()), nil
}

func (js *JsonValue) IntOrDefault(path string, def int) int {
	if v, err := js.Int(path); err != nil {
		return def
	} else {
		return v
	}
}