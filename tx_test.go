package lemon_test

import (
	"context"
	"errors"
	"github.com/denismitr/lemon"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
	"time"
)

func TestTx_Rollback(t *testing.T) {
	suite.Run(t, &rollbackTestSuite{})
}

func TestTx_AutoVacuum(t *testing.T) {
	suite.Run(t, &autoVacuumTestSuite{})
}

func TestTx_ManualVacuum(t *testing.T) {
	suite.Run(t, &manualVacuumTestSuite{})
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

	assertTwoFilesHaveEqualContents(rts.T(), rts.fixture, "./__fixtures__/correct/insert_rollback_db1.ldb")

	if err := closer(); err != nil {
		rts.T().Errorf("ERROR: %v", err)
	}

	assertTwoFilesHaveEqualContents(rts.T(), rts.fixture, "./__fixtures__/correct/insert_rollback_db1.ldb")
}

func (rts *rollbackTestSuite) TestInsertRollbackWithTags() {
	db, closer, err := lemon.Open(rts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	rts.Require().NoError(err)

	err = db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("book:21", lemon.M{
				"author": "Jeff Smith",
				"year": 2009,
				"edition": 1.3,
			},
			lemon.WithTags().Int("foo", 10).Bool("bar", true),
		); err != nil {
			rts.T().Fatal(err)
		}

		if err := tx.InsertOrReplace(
			"book:4",
			lemon.M{"author": "Brian Jefferson", "year": 2009, "edition": 1.1},
			lemon.WithTags().
				Float("price", 20.45).
				Int("inStock", 0),
		); err != nil {
			return err
		}

		if err := tx.Remove("book:41"); err != nil {
			return err
		}

		return errors.New("should roll back")
	})

	rts.Require().Error(err)

	// expect inserted item not to be found
	// expect updated item to stay as it was before an update
	err = db.View(context.Background(), func(tx *lemon.Tx) error {
		book21, err := tx.Get("book:21")
		rts.Require().Error(err)
		rts.Require().True(errors.Is(err, lemon.ErrKeyDoesNotExist))
		rts.Require().Nil(book21)

		book4, err := tx.Get("book:4")
		rts.Require().NoError(err)
		rts.Require().Equal("book:4", book4.Key())
		rts.Require().Equal(`{"author":"Brian","edition":1,"year":2008}`, book4.RawString())
		rts.Require().Equal(`paper`, book4.Tags().String("type"))
		rts.Require().Equal(2, book4.Tags().Int("inStock"))
		rts.Require().Equal(30.45, book4.Tags().Float("price"))

		book41, err := tx.Get("book:41")
		rts.Require().NoError(err)
		rts.Require().Equal("book:41", book41.Key())
		rts.Require().Equal(`{"author":"Valeria Pucci","edition":2,"year":2011}`, book41.RawString())
		rts.Require().Equal(2, book41.IntOrDefault("edition", 0))
		rts.Require().Equal(`digital`, book41.Tags().String("type"))
		rts.Require().Equal(29, book41.Tags().Int("inStock"))
		rts.Require().Equal(30.33, book41.Tags().Float("price"))

		return nil
	})

	assertTwoFilesHaveEqualContents(rts.T(), rts.fixture, "./__fixtures__/correct/insert_rollback_db1.ldb")

	if err := closer(); err != nil {
		rts.T().Errorf("ERROR: %v", err)
	}

	assertTwoFilesHaveEqualContents(rts.T(), rts.fixture, "./__fixtures__/correct/insert_rollback_db1.ldb")
}

type autoVacuumTestSuite struct {
	suite.Suite
	fixture string
}

func (vts *autoVacuumTestSuite) SetupSuite() {
	vts.fixture = "./__fixtures__/vacuum_db1.ldb"
	db, closer, err := lemon.Open(vts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	vts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			vts.T().Errorf("ERROR: %v", err)
		}
	}()

	forceSeedDataForVacuum(vts.T(), db)
	assertTwoFilesHaveEqualContents(vts.T(), vts.fixture, "./__fixtures__/correct/vacuum_db1.ldb")
}

func (vts *autoVacuumTestSuite) TearDownSuite() {
	if err := os.Remove(vts.fixture); err != nil {
		vts.Require().NoError(err)
	}
}

func (vts *autoVacuumTestSuite) Test_AutoVacuumWithIntervals() {
	db, closer, err := lemon.Open(vts.fixture, &lemon.Config{
		DisableAutoVacuum: false,
		AutoVacuumMinSize: 2,
		AutoVacuumOnlyOnClose: false,
		AutoVacuumIntervals: 1 * time.Second,
	})

	vts.Require().NoError(err)
	vts.Assert().Equal(2, db.Count())

	defer func() {
		if err := closer(); err != nil {
			vts.T().Errorf("ERROR: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	assertTwoFilesHaveEqualContents(vts.T(), vts.fixture, "./__fixtures__/correct/after_vacuum_db1.ldb")
}

type manualVacuumTestSuite struct {
	suite.Suite
	fixture string
}

func (vts *manualVacuumTestSuite) SetupSuite() {
	vts.fixture = "./__fixtures__/manual_vacuum_db2.ldb"
	db, closer, err := lemon.Open(vts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	vts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			vts.T().Errorf("ERROR: %v", err)
		}
	}()

	forceSeedDataForVacuum(vts.T(), db)
	assertTwoFilesHaveEqualContents(vts.T(), vts.fixture, "./__fixtures__/correct/before_manual_vacuum_db2.ldb")
}

func (vts *manualVacuumTestSuite) TearDownSuite() {
	if err := os.Remove(vts.fixture); err != nil {
		vts.Require().NoError(err)
	}
}

func (vts *manualVacuumTestSuite) Test_ManualVacuum() {
	db, closer, err := lemon.Open(vts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	vts.Require().NoError(err)
	vts.Assert().Equal(2, db.Count())

	defer func() {
		if err := closer(); err != nil {
			vts.T().Errorf("ERROR: %v", err)
		}
	}()

	vts.Require().NoError(db.Vacuum())
	assertTwoFilesHaveEqualContents(vts.T(), vts.fixture, "./__fixtures__/correct/after_manual_vacuum_db2.ldb")
}

func forceSeedDataForVacuum(t *testing.T, db *lemon.DB) {
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
				Float("price", 30.33).
				Int("inStock", 29).
				Str("type", "digital"),
		); err != nil {
			return err
		}

		if err := tx.InsertOrReplace(
			"book:400",
			lemon.M{"author": "Valerio Strozzi", "year": 2021, "edition": 1},
			lemon.WithTags().
				Float("price", 24.444).
				Int("inStock", 290).
				Str("type", "digital"),
		); err != nil {
			return err
		}

		if err := tx.InsertOrReplace(
			"book:1000",
			lemon.M{"author": "Marco Grigoli", "year": 1989, "edition": 2.8},
			lemon.WithTags().
				Float("price", 19.99).
				Int("inStock", 1).
				Str("type", "paper"),
		); err != nil {
			return err
		}

		if err := tx.Remove("book:4"); err != nil {
			return err
		}

		if err := tx.Remove("book:41"); err != nil {
			return err
		}

		if err := tx.Remove("book:1000"); err != nil {
			return err
		}

		if err := tx.InsertOrReplace(
			"book:1001",
			lemon.M{"author": "Alessia Valle", "year": 1978, "edition": 3.0},
			lemon.WithTags().
				Float("price", 21.99).
				Int("inStock", 2).
				Bool("delivery", true).
				Str("type", "paper"),
		); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
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
				Float("price", 30.33).
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