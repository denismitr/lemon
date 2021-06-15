package jsonstorage

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

type db struct {
	LastID    int            `json:"lastId"`
	Keys      map[string]int `json:"keys"`
	Documents []interface{}
}

func TestJSONStorage_Read(t *testing.T) {
	f, err := os.Open("./__fixtures__/db1.json")
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	s := JSONStorage{f: f}

	t.Run("it can be read from", func(t *testing.T) {
		var dst db
		if err := s.Read(&dst); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 5, dst.LastID)
		assert.Equal(t, map[string]int{"key1": 0, "key2": 1}, dst.Keys)
		assert.Len(t, dst.Documents, 2)
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

			assert.Equal(t, `{"lastId":10,"keys":{"keys1":0,"keys2":1,"keys3":2},"Documents":[{"id":123,"value":"foo"},{"id":200,"value":"bar"}]}`, string(b))

			os.Remove("./__fixtures__/db2.json")
		}()

		s := JSONStorage{f: f}

		docs := []struct {
			Id    int    `json:"id"`
			Value string `json:"value"`
		}{
			{Id: 123, Value: "foo"},
			{Id: 200, Value: "bar"},
		}

		var documents []interface{}
		for i := range docs {
			documents = append(documents, docs[i])
		}

		db := db{
			LastID:    10,
			Keys:      map[string]int{"keys1": 0, "keys2": 1, "keys3": 2},
			Documents: documents,
		}

		if err := s.Write(db); err != nil {
			t.Fatal(err)
		}

		assert.FileExists(t, "./__fixtures__/db2.json")
	})
}
