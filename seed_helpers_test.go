package lemon_test

import (
	"context"
	"github.com/denismitr/lemon"
	"os"
	"testing"
)

func seedSomeProducts(t *testing.T, path string, removeIfExists bool) {
	t.Helper()

	if removeIfExists {
		_ = os.Remove(path)
	}

	db, closer, err := lemon.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := closer(); err != nil {
			t.Errorf("ERROR: %v", err)
		}
	}()

	t.Run("seed products without tags", func(t *testing.T) {
		if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
			if err := tx.Insert("product:2", lemon.M{
				"100": "foobar2",
				"baz": 2,
				"foo": "bar",
			}); err != nil {
				return err
			}

			if err := tx.Insert("product:88", lemon.M{
				"100": "foobar-88",
				"baz": 88,
				"foo": "bar/88",
			}); err != nil {
				return err
			}

			if err := tx.Insert("product:10", lemon.M{
				"999": nil,
				"baz12": 123.879,
				"foo": "bar5674",
			}); err != nil {
				return err
			}

			if err := tx.Insert("product:100", lemon.M{
				"999": nil,
				"baz12": 123.879,
				"foo": "bar5674",
			}); err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}
