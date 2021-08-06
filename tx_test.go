package lemon_test

import (
	"context"
	"errors"
	"github.com/denismitr/lemon"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

func TestTx_Rollback(t *testing.T) {
	suite.Run(t, &rollbackTestSuite{})
}

type rollbackTestSuite struct {
	suite.Suite
	fixture string
}

func (rts *rollbackTestSuite) SetupSuite() {
	rts.fixture = "./__fixtures__/insert_rollback_db1.ldb"
	db, closer, err := lemon.Open(rts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	rts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			rts.T().Errorf("ERROR: %v", err)
		}
	}()

	forceSeedBooks(rts.T(), db)
}

func (rts *rollbackTestSuite) TearDownSuite() {
	if err := os.Remove(rts.fixture); err != nil {
		rts.Require().NoError(err)
	}
}

func (rts *rollbackTestSuite) TestInsertRollbackWithoutTags() {
	rts.fixture = "./__fixtures__/insert_rollback_db1.ldb"
	db, closer, err := lemon.Open(rts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	rts.Require().NoError(err)


	err = db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("book:21", lemon.M{
			"author": "Jeff Smith",
			"year": 2009,
			"edition": 1.3,
		}); err != nil {
			rts.T().Fatal(err)
		}

		if err := tx.Insert("book:22", lemon.M{
			"author": "Paul Figler",
			"year": 2011,
			"edition": 3,
		}); err != nil {
			rts.T().Fatal(err)
		}

		return errors.New("should roll back")
	})

	rts.Require().Error(err)

	// expect rolled back items not to be found
	err = db.View(context.Background(), func(tx *lemon.Tx) error {
		doc1, err := tx.Get("book:21")
		rts.Require().Error(err)
		rts.Require().True(errors.Is(err, lemon.ErrKeyDoesNotExist))
		rts.Require().Nil(doc1)

		doc2, err := tx.Get("book:22")
		rts.Require().Error(err)
		rts.Require().True(errors.Is(err, lemon.ErrKeyDoesNotExist))
		rts.Require().Nil(doc2)

		return nil
	})

	AssertTwoFilesHaveEqualContents(rts.T(), rts.fixture, "./__fixtures__/correct/insert_rollback_db1.ldb")

	if err := closer(); err != nil {
		rts.T().Errorf("ERROR: %v", err)
	}

	AssertTwoFilesHaveEqualContents(rts.T(), rts.fixture, "./__fixtures__/correct/insert_rollback_db1.ldb")
}

func forceSeedBooks(t *testing.T, db *lemon.DB) {
	err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.InsertOrReplace(
			"book:4",
			lemon.M{"author": "Brian", "year": 2008, "edition": 1.0},
			lemon.WithTags().
				Float("price", 30.45).
				Int("inStock", 2).
				Str("type", "paper"),
		); err != nil {
			return err
		}

		if err := tx.InsertOrReplace(
			"book:41",
			lemon.M{"author": "Valeria Pucci", "year": 2011, "edition": 2.0},
			lemon.WithTags().
				Float("price", 33.45).
				Int("inStock", 29).
				Str("type", "digital"),
		); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}
