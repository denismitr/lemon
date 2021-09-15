package lemon

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

var ErrJSONCouldNotBeUnmarshalled = errors.New("json contents could not be unmarshalled, probably is invalid")
var ErrJSONPathInvalid = errors.New("json path is invalid")

type JSONValue struct {
	b []byte
}

type Document struct {
	key   string
	tags  M
	value []byte
}

func (d *Document) Key() string {
	return d.key
}

func newDocumentFromEntry(ent *entry) *Document {
	d := &Document{
		key:   ent.key.String(),
		value: ent.value, // fixme: maybe copy
		tags:  createMapFromTags(ent.tags),
	}

	return d
}

func (d *Document) Value() []byte {
	return d.value
}

func (d *Document) JSON() *JSONValue {
	return &JSONValue{b: d.value}
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

func (js *JSONValue) Unmarshal(dest interface{}) error {
	err := json.Unmarshal(js.b, &dest)
	if err != nil {
		return errors.Wrap(ErrJSONCouldNotBeUnmarshalled, err.Error())
	}

	return nil
}

func (js *JSONValue) String(path string) (string, error) {
	raw := gjson.GetBytes(js.b, path)
	if !raw.Exists() {
		return "", ErrJSONPathInvalid
	}
	return raw.String(), nil
}

func (js *JSONValue) StringOrDefault(path, def string) string {
	v, err := js.String(path)
	if err != nil {
		return def
	}
	return v
}

func (js *JSONValue) Float(path string) (float64, error) {
	get := gjson.GetBytes(js.b, path)
	if !get.Exists() {
		return 0, ErrJSONPathInvalid
	}
	return get.Float(), nil
}

func (js *JSONValue) FloatOrDefault(path string, def float64) float64 {
	v, err := js.Float(path)
	if err != nil {
		return def
	}
	return v
}

func (js *JSONValue) Int(path string) (int, error) {
	get := gjson.GetBytes(js.b, path)
	if !get.Exists() {
		return 0, ErrJSONPathInvalid
	}
	return int(get.Int()), nil
}

func (js *JSONValue) Bool(path string) (bool, error) {
	get := gjson.GetBytes(js.b, path)
	if !get.Exists() {
		return false, ErrJSONPathInvalid
	}
	return get.Bool(), nil
}

func (js *JSONValue) BoolOrDefault(path string, def bool) bool {
	get := gjson.GetBytes(js.b, path)
	if !get.Exists() {
		return def
	}
	return get.Bool()
}

func (js *JSONValue) IntOrDefault(path string, def int) int {
	v, err := js.Int(path)
	if err != nil {
		return def
	}
	return v
}
