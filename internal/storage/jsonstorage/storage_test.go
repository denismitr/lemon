package jsonstorage

import (
	"github.com/denismitr/lemon/internal/data"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestJSONStorage_Read(t *testing.T) {
	f, err := os.Open("./__fixtures__/db1.json")
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	s := JSONStorage{f: f}

	t.Run("it can be read from", func(t *testing.T) {
		var dst data.Model
		if err := s.Read(&dst); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, data.PrimaryKeys{"key1": 0, "key2": 1}, dst.PKs)
		assert.Equal(t, []data.Value{"{\"foo\":\"bar\",\"bar\":123}", "{\"123\":\"foobar\",\"baz\":\"foo\"}"}, dst.Values)
		assert.Len(t, dst.Values, 2)
	})
}

func TestJSONStorage_Write(t *testing.T) {
	t.Run("it can write anything marshalable to a file", func(t *testing.T) {
		f, err := os.Create("./__fixtures__/db2.json")
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			b, err := ioutil.ReadFile("./__fixtures__/db2.json")
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, `{"pks":{"keys1":0,"keys2":1,"keys3":2},"documents":["{\"foo\":\"bar\"}","{\"baz\":123}","{\"123\":345.45}"]}`, string(b))

			os.Remove("./__fixtures__/db2.json")
		}()

		s := JSONStorage{f: f}

		db := data.Model{
			PKs:      map[string]int{"keys1": 0, "keys2": 1, "keys3": 2},
			Values: []data.Value{
				`{"foo":"bar"}`,
				`{"baz":123}`,
				`{"123":345.45}`,
			},
		}

		if err := s.Write(db); err != nil {
			t.Fatal(err)
		}

		assert.FileExists(t, "./__fixtures__/db2.json")
	})
}
