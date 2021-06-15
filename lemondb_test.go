package lemondb

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestLemonDB_Read(t *testing.T) {
	db, closer, err := New("./__fixtures__/db1.json")
	if err != nil {
		t.Fatal(err)
	}

	defer closer()

	t.Run("get existing key", func(t *testing.T) {
		var result1 *Result
		if err := db.ReadTx(context.Background(), func(tx *Tx) error {
			result1 = tx.Get("user:123")
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		json1, err := result1.Unwrap()
		require.NoError(t, err)
		assert.Equal(t, `{"foo":"bar"}`, json1)

		var result2 *Result
		if err := db.ReadTx(context.Background(), func(tx *Tx) error {
			result2 = tx.Get("user:678")
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		json2, err := result2.Unwrap()
		require.NoError(t, err)
		assert.Equal(t, `{"bar":56745,"baz":123}`, json2)
	})
}

func TestLemonDB_Write(t *testing.T) {
	db, closer, err := New("./__fixtures__/db2.json")
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

	var result1 *Result
	var result2 *Result
	t.Run("add new documents and confirm with read", func(t *testing.T) {
		err := db.UpdateTx(context.Background(), func(tx *Tx) error {
			if err := tx.Insert("product:8976", D{
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

			result1 = tx.Get("product:8976")
			result2 = tx.Get("product:1145")

			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, "bar", result1.StringOrDefault("foo", ""))
		assert.Equal(t, 8989764, result1.IntOrDefault("baz", 0))
		assert.Equal(t, "foobar", result1.StringOrDefault("100", ""))
		assert.Equal(t, "bar5674", result2.StringOrDefault("foo", ""))
		assert.Equal(t, 123.879, result2.FloatOrDefault("baz12", 0))
		/*assert.Equal(t, nil, docs[1]["999"])*/

		var readResult1 *Result
		var readResult2 *Result
		// Confirm that those keys are accessible after previous transaction has committed
		// and results should be identical
		if err := db.ReadTx(context.Background(), func(tx *Tx) error {
			readResult1 = tx.Get("product:8976")
			readResult2 = tx.Get("product:1145")
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		readJson1, err := readResult1.Unwrap()
		require.NoError(t, err)
		assert.Equal(t, `{"100":"foobar","baz":8989764,"foo":"bar"}`, readJson1)

		readJson2, err := readResult2.Unwrap()
		require.NoError(t, err)
		assert.Equal(t, `{"999":null,"baz12":123.879,"foo":"bar5674"}`, readJson2)
	})
}
