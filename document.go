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
	value string
}

func newDocument(k, v string) *Document {
	return &Document{key: k, value: v}
}

func (d *Document) Err() error {
	return d.Err()
}

func (d *Document) Unwrap() string {
	return d.value
}

func (d *Document) Unmarshal(dest interface{}) error {
	err := json.Unmarshal([]byte(d.value), &dest)
	if err != nil {
		return errors.Wrap(ErrResultCouldNotBeUnmarshalled, err.Error())
	}

	return nil
}

func (d *Document) String(path string) (string, error) {
	get := gjson.Get(d.value, path)
	if !get.Exists() {
		return "", ErrJsonPathInvalid
	}
	return get.String(), nil
}

func (d *Document) StringOrDefault(path, def string) string {
	if v, err := d.String(path); err != nil {
		return def
	} else {
		return v
	}
}

func (d *Document) Float(path string) (float64, error) {
	get := gjson.Get(d.value, path)
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
	get := gjson.Get(d.value, path)
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
	get := gjson.Get(d.value, path)
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