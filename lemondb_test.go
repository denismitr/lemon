package lemon_test

import (
	"context"
	"fmt"
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
	db, closer, err := lemon.New("./__fixtures__/read_db1.ldb")
	if err != nil {
		t.Fatal(err)
	}

	defer closer()

	t.Run("get existing keys", func(t *testing.T) {
		var result1 *lemon.Document
		var result2 *lemon.Document
		if err := db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
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
		}); err != nil {
			t.Fatal(err)
		}

		json1 := result1.RawString()
		assert.Equal(t, `{"100":"foobar","baz":8989764,"foo":"bar"}`, json1)
		foo, err := result1.String("foo")
		require.NoError(t, err)
		assert.Equal(t, "bar", foo)

		json2 := result2.RawString()
		assert.Equal(t, `{"999":null,"baz12":123.879,"foo":"bar5674"}`, json2)
		bar5674, err := result2.String("foo")
		require.NoError(t, err)
		assert.Equal(t, "bar5674", bar5674)
		baz12, err := result2.Float("baz12")
		require.NoError(t, err)
		assert.Equal(t, 123.879, baz12)
	})
}

func TestLemonDB_Write(t *testing.T) {
	db, closer, err := lemon.New("./__fixtures__/write_db1.ldb")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := closer(); err != nil {
			t.Error(err)
		}

		if err := os.Remove("./__fixtures__/write_db1.ldb"); err != nil {
			t.Error(err)
		}
	}()

	var result1 *lemon.Document
	var result2 *lemon.Document
	t.Run("add new documents and confirm with read", func(t *testing.T) {
		err := db.MultiUpdate(context.Background(), func(tx *lemon.Tx) error {
			if err := tx.Insert("product:8976", lemon.D{
				"foo": "bar",
				"baz": 8989764,
				"100": "username",
			}); err != nil {
				return err
			}

			if err := tx.Insert("product:1145", map[string]interface{}{
				"foo":   "bar5674",
				"baz12": 123.879,
				"999":   nil,
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
		assert.Equal(t, "username", result1.StringOrDefault("100", ""))
		assert.Equal(t, "bar5674", result2.StringOrDefault("foo", ""))
		assert.Equal(t, 123.879, result2.FloatOrDefault("baz12", 0))
		/*assert.Equal(t, nil, docs[1]["999"])*/

		var readResult1 *lemon.Document
		var readResult2 *lemon.Document
		// Confirm that those keys are accessible after previous transaction has committed
		// and results should be identical
		if err := db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
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

		readJson1 := readResult1.RawString()
		assert.Equal(t, `{"100":"username","baz":8989764,"foo":"bar"}`, readJson1)
		assert.Equal(t, result1.RawString(), readJson1)

		readJson2 := readResult2.RawString()
		assert.Equal(t, `{"999":null,"baz12":123.879,"foo":"bar5674"}`, readJson2)
		assert.Equal(t, result2.RawString(), readJson2)
	})
}

type removeTestSuite struct {
	suite.Suite
	db       *lemon.LemonDB
	fileName string
	closer   func() error
}

func (rts *removeTestSuite) SetupTest() {
	db, closer, err := lemon.New("./__fixtures__/db3.ldb")
	rts.Require().NoError(err)
	rts.closer = closer
	rts.db = db

	if err := db.MultiUpdate(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("item:8976", lemon.D{
			"foo": "bar",
			"baz": 8989764,
			"100": "username",
		}); err != nil {
			return err
		}

		if err := tx.Insert("item:1145", lemon.D{
			"foo":   "bar5674",
			"baz12": 123.879,
			"999":   nil,
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

	if err := os.Remove("./__fixtures__/db3.ldb"); err != nil {
		rts.Require().NoError(err)
	}
}

func (rts *removeTestSuite) TestLemonDB_RemoveItemInTheMiddle() {
	if err := rts.db.MultiUpdate(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Remove("item:1145"); err != nil {
			return err
		}

		return nil
	}); err != nil {
		rts.Require().NoError(err)
	}

	if err := rts.db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
		doc, err := tx.Get("item:1145")
		rts.Require().Error(err)
		rts.Assert().Nil(doc)
		rts.Assert().True(errors.Is(err, lemon.ErrKeyDoesNotExist))

		return nil
	}); err != nil {
		rts.Require().NoError(err)
	}
}

func seedUserData(t *testing.T, db *lemon.LemonDB, n int) {
	t.Helper()

	type userData struct {
		Username string  `json:"username"`
		Phone    string  `json:"phone"`
		Address  string  `json:"address"`
		Balance  float64 `json:"balance"`
		Logins   int     `json:"logins"`
	}

	baseUser := userData{
		Username: "username",
		Phone:    "999444555",
		Address:  "Some street ap.",
	}

	if err := db.MultiUpdate(context.Background(), func(tx *lemon.Tx) error {
		for i := 1; i < n+1; i++ {
			user := userData{
				Username: fmt.Sprintf("%s_%d", baseUser.Username, i),
				Phone:    fmt.Sprintf("%s%d", baseUser.Phone, i),
				Address:  fmt.Sprintf("%s %d", baseUser.Address, i),
				Balance:  float64(i),
				Logins:   i,
			}

			if err := tx.Insert(fmt.Sprintf("user:%d", i), user); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func seedUserPets(t *testing.T, db *lemon.LemonDB, firstUserId, lastUserId, pets int) {
	t.Helper()

	type petData struct {
		Name   string `json:"name"`
		Age    int    `json:"age"`
		Weight float64 `json:"weight"`
		Kind   string `json:"kind"`
	}

	if err := db.MultiUpdate(context.Background(), func(tx *lemon.Tx) error {
		for i := firstUserId; i <= lastUserId; i++ {
			for j := 0; j < pets; j++ {
				pet := petData{
					Name: fmt.Sprintf("pet_%d", j + 1),
					Age:    j + 1,
					Weight:  float64(j) + 1.5,
					Kind:   fmt.Sprintf("animal kind %d", j + 1),
				}

				if err := tx.Insert(fmt.Sprintf("user:%d:pet:%d", i, j + 1), pet); err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func seedProductData(t *testing.T, db *lemon.LemonDB, n int) {
	t.Helper()

	type productData struct {
		Name     string  `json:"name"`
		Buyers   []int   `json:"buyers"`
		ID       int     `json:"id"`
		OwnerID  int     `json:"ownerId"`
		Price    float64 `json:"price"`
		Quantity int     `json:"quantity"`
	}

	baseProduct := productData{
		Name: "product",
		ID:   0,
	}

	if err := db.MultiUpdate(context.Background(), func(tx *lemon.Tx) error {
		for i := 0; i < n; i++ {
			user := productData{
				Name:     fmt.Sprintf("%s_%d", baseProduct.Name, i+1),
				Buyers:   []int{1 + i, 2 + i, 3 + i, 4 + i},
				ID:       i + 1,
				OwnerID:  n - i,
				Price:    float64(i + 1),
				Quantity: i,
			}

			if err := tx.Insert(fmt.Sprintf("product:%d", i+1), user); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
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
