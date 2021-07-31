package lemon_test

import (
	"context"
	"github.com/denismitr/lemon"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

type matchTestSuite struct {
	suite.Suite
	fixture string
}

func (mts *matchTestSuite) SetupSuite() {
	mts.fixture = "./__fixtures__/match_db1.ldb"
	db, closer, err := lemon.New(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	seedGranularUsers(mts.T(), db)
}

func (mts *matchTestSuite) TearDownSuite() {
	if err := os.Remove(mts.fixture); err != nil {
		mts.Require().NoError(err)
	}
}

func (mts *matchTestSuite) MatchSingleUserByPatternAndTag() {
	mts.fixture = "./__fixtures__/match_db1.ldb"
	db, closer, err := lemon.New(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	docs := make([]lemon.Document, 0)
	ctx := context.Background()
	err = db.View(context.Background(), func(tx *lemon.Tx) error {
		return tx.Find(ctx, lemon.Q().Match("user:*").StrTag("content", "list"), &docs)
	})

	mts.Require().NoError(err)
	mts.Require().Len(docs, 1)
	mts.Require().Equal("user:12:animals", docs[0].Key())
	mts.Require().Equal(`[123, 987, 6789]`, docs[0].RawString())
	mts.Require().Equal(map[string]string{"content": "list"}, docs[0].Tags().Strings())
}

func seedGranularUsers(t *testing.T, db *lemon.DB) {
	err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		err := tx.Insert("user:12", lemon.M{
			"foo": 567,
			"bar": lemon.M{
				"a": 1234567,
				"b": "baz22",
			},
			"1900-10-20": 10.345,
		}, lemon.StrTag("content", "doc"), lemon.BoolTag("valid", true))
		if err != nil {
			return err
		}

		err = tx.Insert("user:123", lemon.M{
			"foo": 123,
			"bar": lemon.M{
				"a": 987,
				"b": "baz",
			},
			"1900-10-20": 678.345,
		}, lemon.StrTag("content", "doc"))
		if err != nil {
			return err
		}

		err = tx.Insert("user:12:animals", `[123, 987, 6789]`, lemon.StrTag("content", "list"))
		if err != nil {
			return err
		}

		err = tx.Insert("animal:12", `{"species": "turtle"}`, lemon.StrTag("content", "list"))
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestTx_Match(t *testing.T) {
	suite.Run(t, &matchTestSuite{})
}
