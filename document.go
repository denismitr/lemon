package lemon

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"strconv"
	"strings"
	"time"
)

var ErrJSONCouldNotBeUnmarshalled = errors.New("json contents could not be unmarshalled, probably is invalid")
var ErrJSONPathInvalid = errors.New("json path is invalid")

type JSONValue struct {
	b []byte
}

type Document struct {
	key      string
	userTags M
	metaTags M
	value    []byte
}

func newDocumentFromEntry(ent *entry) *Document {
	userTags, metaTags := createMapFromTags(ent.tags)

	d := &Document{
		key:      ent.key.String(),
		userTags: userTags,
		value:    make([]byte, len(ent.value)),
		metaTags: metaTags,
	}

	copy(d.value, ent.value)

	return d
}

func (d *Document) Key() string {
	return d.key
}

func (d *Document) ContentType() ContentTypeIdentifier {
	return ContentTypeIdentifier(d.metaTags.String(ContentType))
}

func (d *Document) HasTimestamps() bool {
	return d.metaTags.Int(CreatedAt) > 0 && d.metaTags.Int(UpdatedAt) > 0
}

func (d *Document) CreatedAt() time.Time {
	ct := d.metaTags.Int(CreatedAt)
	return time.UnixMilli(int64(ct))
}

func (d *Document) UpdatedAt() time.Time {
	ct := d.metaTags.Int(UpdatedAt)
	return time.UnixMilli(int64(ct))
}

func (d *Document) IsJSON() bool {
	return d.metaTags.String(ContentType) == string(JSON)
}

func (d *Document) IsInteger() bool {
	return d.metaTags.String(ContentType) == string(Integer)
}

func (d *Document) IsString() bool {
	return d.metaTags.String(ContentType) == string(String)
}

func (d *Document) IsBytes() bool {
	return d.metaTags.String(ContentType) == string(Bytes)
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

func createMapFromTags(tgs tags) (M, M) {
	userTags := make(M)
	metaTags := make(M)
	if tgs == nil {
		return userTags, metaTags
	}

	for name, t := range tgs {
		if !strings.HasPrefix(name, "_") {
			userTags[name] = t.data
		} else {
			metaTags[name] = t.data
		}
	}

	return userTags, metaTags
}

func (d *Document) Tags() M {
	return d.userTags
}

func (d *Document) M() (M, error) {
	var m M
	if err := d.JSON().Unmarshal(&m); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal to lemon.M")
	}
	return m, nil
}

func (d *Document) MustIntegerValue() int {
	n, err := strconv.Atoi(string(d.value))
	if err != nil {
		panic(fmt.Errorf("could not convert value %s to integer", string(d.value)))
	}
	return n
}

func (d *Document) IntegerValue() int {
	n, _ := strconv.Atoi(string(d.value))
	return n
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
