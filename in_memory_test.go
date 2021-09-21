package lemon_test

import (
	"context"
	"github.com/denismitr/lemon"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_InMemory_InsertRead(t *testing.T) {
	t.Logf("Testing InMemory Insert And Read")

	db, closer, err := lemon.Open(lemon.InMemory)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := closer(); err != nil {
			t.Errorf("ERROR: %v", err)
		}
	}()

	if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("users:123", lemon.M{"foo": "bar"}); err != nil {
			return err
		}

		if err := tx.Insert("users:5789", lemon.M{"baz": 456.988}); err != nil {
			return err
		}

		if err := tx.Insert("users:578894:data", lemon.M{"data": lemon.M{"foo": "bar"}}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	u123, err := db.Get("users:123")
	if err != nil {
		t.Fatal(err)
	}

	u123Map, err := u123.M()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, lemon.M{"foo": "bar"}, u123Map)
	assert.Equal(t, "users:123", u123.Key())

	u5789, err := db.Get("users:5789")
	if err != nil {
		t.Fatal(err)
	}

	u5789Map, err := u5789.M()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, lemon.M{"baz": 456.988}, u5789Map)
	assert.Equal(t, "users:5789", u5789.Key())

	t.Logf("Tested InMemory Insert And Read")
}


