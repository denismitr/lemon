package lemon

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

var ErrResultCouldNotBeUnmarshalled = errors.New("result could not be unmarshalled into the destination")
var ErrJsonPathInvalid = errors.New("json path is invalid")

type Document struct {
	key   string
	value []byte
	tags Tags
}

func (d *Document) Key() string {
	return d.key
}

func (d *Document) Tags() Tags {
	return d.tags
}

func newDocument(k string, v []byte, tags *Tags) *Document {
	return &Document{key: k, value: v, tags: *tags}
}

func newDocumentFromEntry(ent *entry) *Document {
	return &Document{key: ent.key.String(), value: ent.value, tags: *ent.tags}
}

func createDocument(k string, v []byte, tags *Tags) Document {
	return Document{key: k, value: v, tags: *tags}
}

func (d *Document) Err() error {
	return d.Err()
}

func (d *Document) RawString() string {
	return string(d.value)
}

func (d *Document) Unmarshal(dest interface{}) error {
	err := json.Unmarshal(d.value, &dest)
	if err != nil {
		return errors.Wrap(ErrResultCouldNotBeUnmarshalled, err.Error())
	}

	return nil
}

func (d *Document) String(path string) (string, error) {
	raw := gjson.GetBytes(d.value, path)
	if !raw.Exists() {
		return "", ErrJsonPathInvalid
	}
	return raw.String(), nil
}

func (d *Document) StringOrDefault(path, def string) string {
	if v, err := d.String(path); err != nil {
		return def
	} else {
		return v
	}
}

func (d *Document) Float(path string) (float64, error) {
	get := gjson.GetBytes(d.value, path)
	if !get.Exists() {
		return 0, ErrJsonPathInvalid
	}
	return get.Float(), nil
}

func (d *Document) FloatOrDefault(path string, def float64) float64 {
	if v, err := d.Float(path); err != nil {
		return def
	} else {
		return v
	}
}

func (d *Document) Int(path string) (int, error) {
	get := gjson.GetBytes(d.value, path)
	if !get.Exists() {
		return 0, ErrJsonPathInvalid
	}

	return int(get.Int()), nil
}

func (d *Document) IntOrDefault(path string, def int) int {
	if v, err := d.Int64(path); err != nil {
		return def
	} else {
		return int(v)
	}
}

func (d *Document) Int64(path string) (int64, error) {
	get := gjson.GetBytes(d.value, path)
	if !get.Exists() {
		return 0, ErrJsonPathInvalid
	}
	return get.Int(), nil
}

func (d *Document) Int64OrDefault(path string, def int64) int64 {
	if v, err := d.Int64(path); err != nil {
		return def
	} else {
		return v
	}
}