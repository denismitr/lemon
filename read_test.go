package lemon_test

import (
	"context"
	"fmt"
	"github.com/denismitr/lemon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLemonDB_Read(t *testing.T) {
	db, closer, err := lemon.New("./__fixtures__/read_db1.ldb")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := closer(); err != nil {
			t.Errorf("ERROR: %v", err)
		}
	}()

	//t.Run("seed", func(t *testing.T) {
	//	if err := db.MultiUpdate(context.Background(), func(tx *lemon.Tx) error {
	//		if err := tx.Insert("product:8976", lemon.D{
	//			"100": "foobar",
	//			"baz":8989764,
	//			"foo":"bar",
	//		}); err != nil {
	//			return err
	//		}
	//
	//		if err := tx.Insert("product:1145", lemon.D{
	//			"999":nil,
	//			"baz12":123.879,
	//			"foo":"bar5674",
	//		}); err != nil {
	//			return err
	//		}
	//
	//		return nil
	//	}); err != nil {
	//		t.Fatal(err)
	//	}
	//})

	t.Run("get existing keys", func(t *testing.T) {
		var result1 *lemon.Document
		var result2 *lemon.Document
		if err := db.View(context.Background(), func(tx *lemon.Tx) error {
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

type findByTagsTestSuite struct {
	suite.Suite
	fixture string
}

func (fts *findByTagsTestSuite) SetupSuite() {
	fts.fixture = "./__fixtures__/find_by_tags_db1.ldb"
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	seedUserData(fts.T(), db, 1_000, seedTags{hashes: true})
	seedProductData(fts.T(), db, 1_000)
}

func (fts *findByTagsTestSuite) TearDownSuite() {
	if err := os.Remove(fts.fixture); err != nil {
		fts.Require().NoError(err)
	}
}

func (fts *findByTagsTestSuite) TestLemonDB_FindByBoolTag() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().Order(lemon.DescOrder).AllTags(lemon.BoolTag("foo", true))
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err)
	}

	//expectedDocs := 181
	fts.Assert().True(len(docs) > 0)
	fts.Assert().True(len(docs) < db.Count())

	//for i := 0; i < 9; i++ {
	//	fts.Require().Equal(fmt.Sprintf("user:10%d", expectedDocs - i), docs[i].Key())
	//	fts.Require().Equal(fmt.Sprintf("username_10%d", expectedDocs - i), docs[i].StringOrDefault("username", ""))
	//}
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
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	seedUserData(fts.T(), db, 1_000, seedTags{})
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
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().Order(lemon.DescOrder).KeyRange("user:100", "user:109")
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err)
	}

	expectedDocs := 9
	fts.Assert().Len(docs, expectedDocs)

	for i := 0; i < 9; i++ {
		fts.Require().Equal(fmt.Sprintf("user:10%d", expectedDocs - i), docs[i].Key())
		fts.Require().Equal(fmt.Sprintf("username_10%d", expectedDocs - i), docs[i].StringOrDefault("username", ""))
	}
}

func (fts *findTestSuite) TestLemonDB_FindRangeOfUsers_Ascend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().Order(lemon.AscOrder).KeyRange("product:500", "product:750")
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
		fts.Assert().Equal(fmt.Sprintf("product_%d", i), docs[idx].StringOrDefault("Name", ""))
		fts.Assert().Equal(i, docs[idx].IntOrDefault("id", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllUsers_Ascend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().Order(lemon.AscOrder).Prefix("user")
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
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		q := lemon.Q().Order(lemon.DescOrder).Prefix("user")
		if err := tx.Find(ctx, q, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Require().Lenf(docs, 1_000, "users total count mismatch, got %d", len(docs))

	total := 1_000
	for i := 0; i < total - 999; i++ {
		//fts.Assert().Equal("", docs[999].RawString())
		fts.Assert().Equal(fmt.Sprintf("username_%d", total - i), docs[i].StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", total - i), docs[i].StringOrDefault("phone", ""))
		fts.Assert().Equal(total - i, docs[i].IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(total - i), docs[i].FloatOrDefault("balance", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllDocs_Descend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().Order(lemon.DescOrder)
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
		fts.Assert().Equal(fmt.Sprintf("product_%d", totalProducts - i), docs[totalUsers + i].StringOrDefault("Name", ""))
		fts.Assert().Equal(totalProducts - i, docs[totalUsers + i].IntOrDefault("id", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllDocs_Ascend() {
	db, closer, err := lemon.New(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().Order(lemon.AscOrder)
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
		fts.Assert().Equal(fmt.Sprintf("product_%d", i + 1), docs[i].StringOrDefault("Name", ""))
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

type scanTestSuite struct {
	suite.Suite
	fixture string
}

func (sts *scanTestSuite) SetupSuite() {
	sts.fixture = "./__fixtures__/scan_db1.ldb"
	db, closer, err := lemon.New(sts.fixture)
	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	seedUserData(sts.T(), db, 1_000, seedTags{})
	seedProductData(sts.T(), db, 1_000)
	seedUserPets(sts.T(), db,10, 50, 3)
	seedUserPets(sts.T(), db,134, 140, 5)
}

func (sts *scanTestSuite) TearDownSuite() {
	if err := os.Remove(sts.fixture); err != nil {
		sts.Require().NoError(err)
	}
}

func (sts *scanTestSuite) Test_ScanUserPets() {
	db, closer, err := lemon.New(sts.fixture)
	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	sts.Require().Equal(2158, db.Count())

	var docs []lemon.Document
	if err := db.View(ctx, func(tx *lemon.Tx) error {
		opts := lemon.Q().Order(lemon.AscOrder).Prefix("user")
		if scanErr := tx.Scan(ctx, opts, func (d lemon.Document) bool {
			if strings.Contains(d.Key(), ":pet:") {
				docs = append(docs, d)
			}

			return true
		}); scanErr != nil {
			return scanErr
		}

		return nil
	}); err != nil {
		sts.Require().NoError(err)
	}

	sts.Require().Lenf(docs, 158, "docs count mismatch: got %d", len(docs))
}

func (sts *scanTestSuite) Test_ScanUserPetsWithManualLimit() {
	db, closer, err := lemon.New(sts.fixture)
	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	sts.Require().Equal(2158, db.Count())

	var docs []lemon.Document
	if err := db.View(ctx, func(tx *lemon.Tx) error {
		opts := lemon.Q().Order(lemon.AscOrder).Prefix("user")
		if scanErr := tx.Scan(ctx, opts, func (d lemon.Document) bool {
			if strings.Contains(d.Key(), ":pet:") {
				docs = append(docs, d)
			}

			if len(docs) > 20 {
				return false
			}

			return true
		}); scanErr != nil {
			return scanErr
		}

		return nil
	}); err != nil {
		sts.Require().NoError(err)
	}

	sts.Require().Lenf(docs, 21, "docs count mismatch: got %d", len(docs))
}

func TestTx_Find(t *testing.T) {
	suite.Run(t, &findTestSuite{})
}

func TestTx_FindByTags(t *testing.T) {
	suite.Run(t, &findByTagsTestSuite{})
}

func TestTx_Scan(t *testing.T) {
	suite.Run(t, &scanTestSuite{})
}


