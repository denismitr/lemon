package lemondb

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

var ErrResultCouldNotBeUnmarshalled = errors.New("result could not be unmarshalled into the destination")
var ErrJsonPathInvalid = errors.New("json path is invalid")

type Result struct {
	key   string
	value string
	err   error
}

func newErrorResult(key string, err error) *Result {
	return &Result{err: err, key: key}
}

func newSuccessResult(k, v string) *Result {
	return &Result{key: k, value: v}
}

type ResultCollection []Result

func (r *Result) Err() error {
	return r.Err()
}

func (r *Result) Unwrap() (string, error) {
	return r.value, r.err
}

func (r *Result) Unmarshal(dest interface{}) error {
	err := json.Unmarshal([]byte(r.value), &dest)
	if err != nil {
		return errors.Wrap(ErrResultCouldNotBeUnmarshalled, err.Error())
	}

	return nil
}

func (r *Result) String(path string) (string, error) {
	if r.err != nil {
		return "", r.err
	}

	get := gjson.Get(r.value, path)

	if !get.Exists() {
		return "", ErrJsonPathInvalid
	}
	return get.String(), nil
}

func (r *Result) StringOrDefault(path, def string) string {
	if v, err := r.String(path); err != nil {
		return def
	} else {
		return v
	}
}

func (r *Result) Float(path string) (float64, error) {
	if r.err != nil {
		return 0, r.err
	}

	get := gjson.Get(r.value, path)
	if !get.Exists() {
		return 0, ErrJsonPathInvalid
	}
	return get.Float(), nil
}

func (r *Result) FloatOrDefault(path string, def float64) float64 {
	if v, err := r.Float(path); err != nil {
		return def
	} else {
		return v
	}
}

func (r *Result) Int(path string) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	get := gjson.Get(r.value, path)
	if !get.Exists() {
		return 0, ErrJsonPathInvalid
	}

	return int(get.Int()), nil
}

func (r *Result) IntOrDefault(path string, def int) int {
	if v, err := r.Int64(path); err != nil {
		return def
	} else {
		return int(v)
	}
}

func (r *Result) Int64(path string) (int64, error) {
	if r.err != nil {
		return 0, r.err
	}

	get := gjson.Get(r.value, path)
	if !get.Exists() {
		return 0, ErrJsonPathInvalid
	}
	return get.Int(), nil
}

func (r *Result) Int64OrDefault(path string, def int64) int64 {
	if v, err := r.Int64(path); err != nil {
		return def
	} else {
		return v
	}
}