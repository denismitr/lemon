package lemon_test

import (
	"context"
	"github.com/denismitr/lemon"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"testing"
)

func TestLemonDB_Read(t *testing.T) {
	db, closer, err := lemon.New("./__fixtures__/db1.json")
	if err != nil {
		t.Fatal(err)
	}

	defer closer()

	t.Run("get existing keys", func(t *testing.T) {
		var result1 *lemon.Document
		var result2 *lemon.Document
		if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
			doc1, err := tx.Get("user:123")
			if err != nil {
				return err
			}

			doc2, err := tx.Get("user:678")
			if err != nil {
				return err
			}

			result1 = doc1
			result2 = doc2
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		json1 := result1.Unwrap()
		assert.Equal(t, `{"foo":"bar"}`, json1)
		foo, err := result1.String("foo")
		require.NoError(t, err)
		assert.Equal(t, "bar", foo)

		json2  := result2.Unwrap()
		assert.Equal(t, `{"bar":56745,"baz":123.6}`, json2)
		bar, err := result2.Int("bar")
		require.NoError(t, err)
		assert.Equal(t, 56745, bar)
		baz, err := result2.Float("baz")
		require.NoError(t, err)
		assert.Equal(t, 123.6, baz)
	})
}

func TestLemonDB_Write(t *testing.T) {
	db, closer, err := lemon.New("./__fixtures__/db2.json")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := closer(); err != nil {
			t.Error(err)
		}

		if err := os.Remove("./__fixtures__/db2.json"); err != nil {
			t.Error(err)
		}
	}()

	var result1 *lemon.Document
	var result2 *lemon.Document
	t.Run("add new documents and confirm with read", func(t *testing.T) {
		err := db.UpdateTx(context.Background(), func(tx *lemon.Tx) error {
			if err := tx.Insert("product:8976", lemon.D{
				"foo": "bar",
				"baz": 8989764,
				"100": "foobar",
			}); err != nil {
				return err
			}

			if err := tx.Insert("product:1145", map[string]interface{}{
				"foo": "bar5674",
				"baz12": 123.879,
				"999": nil,
			}); err != nil {
				return err
			}

			doc1, err := tx.Get("product:8976")
			if err != nil {
				return err
			}

			doc2, err := tx.Get("product:1145")
			if err != nil {
				return err
			}

			result1 = doc1
			result2 = doc2

			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, "bar", result1.StringOrDefault("foo", ""))
		assert.Equal(t, 8989764, result1.IntOrDefault("baz", 0))
		assert.Equal(t, "foobar", result1.StringOrDefault("100", ""))
		assert.Equal(t, "bar5674", result2.StringOrDefault("foo", ""))
		assert.Equal(t, 123.879, result2.FloatOrDefault("baz12", 0))
		/*assert.Equal(t, nil, docs[1]["999"])*/

		var readResult1 *lemon.Document
		var readResult2 *lemon.Document
		// Confirm that those keys are accessible after previous transaction has committed
		// and results should be identical
		if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
			doc1, err := tx.Get("product:8976")
			if err != nil {
				return err
			}

			doc2, err := tx.Get("product:1145")
			if err != nil {
				return err
			}

			readResult1 = doc1
			readResult2 = doc2

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		readJson1 := readResult1.Unwrap()
		assert.Equal(t, `{"100":"foobar","baz":8989764,"foo":"bar"}`, readJson1)
		assert.Equal(t, result1.Unwrap(), readJson1)

		readJson2 := readResult2.Unwrap()
		assert.Equal(t, `{"999":null,"baz12":123.879,"foo":"bar5674"}`, readJson2)
		assert.Equal(t, result2.Unwrap(), readJson2)
	})
}

type removeTestSuite struct {
	suite.Suite
	db *lemon.LemonDB
	fileName string
	closer func() error
}

func (rts *removeTestSuite) SetupTest() {
	db, closer, err := lemon.New("./__fixtures__/db3.json")
	rts.Require().NoError(err)
	rts.closer = closer
	rts.db = db

	if err := db.UpdateTx(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("item:8976", lemon.D{
			"foo": "bar",
			"baz": 8989764,
			"100": "foobar",
		}); err != nil {
			return err
		}

		if err := tx.Insert("item:1145", lemon.D{
			"foo": "bar5674",
			"baz12": 123.879,
			"999": nil,
		}); err != nil {
			return err
		}

		if err := tx.Insert("users", lemon.D{
			"user1": "abc123",
			"user2": "John Smith",
			"user3": "anyone",
			"user4": "someone",
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		rts.Require().NoError(err)
	}
}

func (rts *removeTestSuite) TearDownTest() {
	if rts.closer == nil {
		return
	}

	err := rts.closer()
	rts.Require().NoError(err)

	if err := os.Remove("./__fixtures__/db3.json"); err != nil {
		rts.Require().NoError(err)
	}
}

func (rts *removeTestSuite) TestLemonDB_RemoveItemInTheMiddle() {
	if err := rts.db.UpdateTx(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Remove("item:1145"); err != nil {
			return err
		}

		return nil
	}); err != nil {
		rts.Require().NoError(err)
	}

	expectedContents := `{"pks":{"item:8976":0,"users":1},"documents":["{\"100\":\"foobar\",\"baz\":8989764,\"foo\":\"bar\"}","{\"user1\":\"abc123\",\"user2\":\"John Smith\",\"user3\":\"anyone\",\"user4\":\"someone\"}"]}`
	AssertFileContents(rts.T(), "./__fixtures__/db3.json", expectedContents)

	if err := rts.db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
		doc, err := tx.Get("item:1145")
		rts.Require().Error(err)
		rts.Assert().Nil(doc)
		rts.Assert().True(errors.Is(err, lemon.ErrKeyDoesNotExist))

		return nil
	}); err != nil {
		rts.Require().NoError(err)
	}
}

func TestTx_Remove(t *testing.T) {
	suite.Run(t, &removeTestSuite{})
}

func AssertFileContents(t *testing.T, path string, expectedContents string) {
	t.Helper()

	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Errorf("file %s could not be opened\nbecause:  %v", path, err)
	}

	if string(b) != expectedContents {
		t.Errorf("file %s contents\n%s\ndoes not match expected\n%s", path, string(b), expectedContents)
	}

	t.Log("contents match")
}