package lemon_test

import (
	"context"
	"fmt"
	"github.com/denismitr/lemon"
	"github.com/denismitr/lemon/options"
	"github.com/stretchr/testify/suite"
	"os"
	"strings"
	"testing"
	"time"
)

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
	if err := db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
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
	if err := db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
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
	if err := db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
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
	if err := db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
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
	if err := db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
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
	if err := db.MultiRead(context.Background(), func(tx *lemon.Tx) error {
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
			sts.Require().NoError(err)
		}
	}()

	seedUserData(sts.T(), db, 1_000)
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
			sts.Require().NoError(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.MultiRead(ctx, func(tx *lemon.Tx) error {
		opts := options.Find().SetOrder(options.Ascend).Prefix("user")
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

	sts.Require().Lenf(docs, 158, "docs count mismatch: got %d", len(docs))
}

func TestTx_Find(t *testing.T) {
	suite.Run(t, &findTestSuite{})
}

func TestTx_Scan(t *testing.T) {
	suite.Run(t, &scanTestSuite{})
}
