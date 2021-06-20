package lemon_test

import (
	"context"
	"fmt"
	"github.com/denismitr/lemon"
	"github.com/denismitr/lemon/options"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"testing"
	"time"
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
		if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
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
		err := db.UpdateTx(context.Background(), func(tx *lemon.Tx) error {
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

	if err := db.UpdateTx(context.Background(), func(tx *lemon.Tx) error {
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
	if err := rts.db.UpdateTx(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Remove("item:1145"); err != nil {
			return err
		}

		return nil
	}); err != nil {
		rts.Require().NoError(err)
	}

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

type findTestSuite struct {
	suite.Suite
	fixture string
}

func (fts *findTestSuite) SetupSuite() {
	fts.fixture = "./__fixtures__/find_db1.ldb"
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.Require().NoError(err)
		}
	}()

	seedUserData(fts.T(), db, 1_000)
	seedProductData(fts.T(), db, 1_000)
}

func (fts *findTestSuite) TearDownSuite() {
	if err := os.Remove(fts.fixture); err != nil {
		fts.Require().NoError(err)
	}
}

func (fts *findTestSuite) TestLemonDB_FindRangeOfUsers_Descend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.Require().NoError(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
		opts := options.Find().SetOrder(options.Descend).KeyRange("user:100", "user:110")
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err)
	}

	fts.Assert().Len(docs, 10)
}

func (fts *findTestSuite) TestLemonDB_FindRangeOfUsers_Ascend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.Require().NoError(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
		opts := options.Find().SetOrder(options.Ascend).KeyRange("product:750", "product:500")
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Assert().Len(docs, 250)

	for i := 500; i < 750; i++ {
		idx := i - 500
		fts.Assert().Equal(fmt.Sprintf("product_%d", i), docs[idx].StringOrDefault("name", ""))
		fts.Assert().Equal(i, docs[idx].IntOrDefault("id", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllUsers_Ascend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.Require().NoError(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
		opts := options.Find().SetOrder(options.Ascend).Prefix("user")
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Require().Lenf(docs, 1_000, "users total count mismatch, got %d", len(docs))

	for i := 1; i < 1_001; i++ {
		fts.Assert().Equal(fmt.Sprintf("username_%d", i), docs[i - 1].StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", i), docs[i - 1].StringOrDefault("phone", ""))
		fts.Assert().Equal(i, docs[i - 1].IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(i), docs[i - 1].FloatOrDefault("balance", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllUsers_Descend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.Require().NoError(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
		opts := options.Find().SetOrder(options.Descend).Prefix("user")
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Require().Lenf(docs, 1_000, "users total count mismatch, got %d", len(docs))

	total := 1_000
	for i := 1_000; i > 0; i-- {
		fts.Assert().Equal(fmt.Sprintf("username_%d", i), docs[total - i].StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", i), docs[total - i].StringOrDefault("phone", ""))
		fts.Assert().Equal(i, docs[total - i].IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(i), docs[total - i].FloatOrDefault("balance", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllDocs_Descend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.Require().NoError(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
		opts := options.Find().SetOrder(options.Descend)
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Require().Lenf(docs, 2_000, "users and products total count mismatch, got %d", len(docs))

	totalUsers := 1_000
	for i := 0; i < totalUsers; i++ {
		fts.Assert().Equal(fmt.Sprintf("username_%d", totalUsers - i), docs[i].StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", totalUsers - i), docs[i].StringOrDefault("phone", ""))
		fts.Assert().Equal(totalUsers - i, docs[i].IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(totalUsers - i), docs[i].FloatOrDefault("balance", 0))
	}

	totalProducts := 1_000
	for i := 0; i < totalProducts; i++ {
		fts.Assert().Equal(fmt.Sprintf("product_%d", totalProducts - i), docs[totalUsers + i].StringOrDefault("name", ""))
		fts.Assert().Equal(totalProducts - i, docs[totalUsers + i].IntOrDefault("id", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllDocs_Ascend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.Require().NoError(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.ReadTx(context.Background(), func(tx *lemon.Tx) error {
		opts := options.Find().SetOrder(options.Ascend)
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Require().Lenf(docs, 2_000, "users and products total count mismatch, got %d", len(docs))

	totalProducts := 1_000
	for i := 0; i < totalProducts; i++ {
		fts.Assert().Equal(fmt.Sprintf("product_%d", i + 1), docs[i].StringOrDefault("name", ""))
		fts.Assert().Equal(i + 1, docs[i].IntOrDefault("id", 0))
	}

	totalUsers := 1_000
	for i := 0; i < totalUsers; i++ {
		fts.Assert().Equal(fmt.Sprintf("username_%d", i + 1), docs[totalProducts + i].StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", i + 1), docs[totalProducts + i].StringOrDefault("phone", ""))
		fts.Assert().Equal(i + 1, docs[totalProducts + i].IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(i + 1), docs[totalProducts + i].FloatOrDefault("balance", 0))
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

	if err := db.UpdateTx(context.Background(), func(tx *lemon.Tx) error {
		for i := 1; i < n + 1; i++ {
			user := userData{
				Username: fmt.Sprintf("%s_%d", baseUser.Username, i),
				Phone:    fmt.Sprintf("%s%d", baseUser.Phone, i),
				Address:  fmt.Sprintf("%s %d", baseUser.Address, i),
				Balance:  float64(i),
				Logins:  i,
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
		ID:  0,
	}

	if err := db.UpdateTx(context.Background(), func(tx *lemon.Tx) error {
		for i := 0; i < n; i++ {
			user := productData{
				Name: fmt.Sprintf("%s_%d", baseProduct.Name, i + 1),
				Buyers:    []int{1 + i, 2 + i, 3 + i, 4 + i},
				ID:  i + 1,
				OwnerID:  n - i,
				Price:   float64(i + 1),
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

func TestTx_Find(t *testing.T) {
	suite.Run(t, &findTestSuite{})
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
