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

func (mts *matchTestSuite) TestMatchSingleUserByPatternAndTag() {
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
		q := lemon.Q().Match("user:*").
			HasAllTags(lemon.QT().StrTagEq("content", "list"))

		return tx.Find(ctx, q, &docs)
	})

	mts.Require().NoError(err)
	mts.Require().Len(docs, 1)
	mts.Require().Equal("user:12:animals", docs[0].Key())
	mts.Require().Equal(`[123, 987, 6789]`, docs[0].RawString())
	mts.Require().Equal(map[string]string{"content": "list"}, docs[0].Tags().Strings())
	mts.Require().Equal("list", docs[0].Tags().GetString("content"))
}

func (mts *matchTestSuite) TestMatchMultipleUsersByPatternAndGtIntTag() {
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
		q := lemon.Q().Match("user:*").
			HasAllTags(lemon.QT().IntTagGt("age", 55))

		return tx.Find(ctx, q, &docs)
	})

	mts.Require().NoError(err)
	mts.Require().Len(docs, 2)
	mts.Require().Equal("user:124", docs[0].Key())
	mts.Require().Equal(`{"1900-10-20":null,"bar":{"a":666,"b":"baz223"},"foo":124}`, docs[0].RawString())
	mts.Require().Equal(map[string]string{"auth":"basic", "content":"doc"}, docs[0].Tags().Strings())
	mts.Require().Equal("doc", docs[0].Tags().GetString("content"))

	mts.Require().Equal("user:125", docs[1].Key())
	mts.Require().Equal(`{"1900-10-20":0,"bar":{"a":667,"b":"baz123223"},"foo":125}`, docs[1].RawString())
	mts.Require().Equal(map[string]string{"auth":"basic", "content":"doc"}, docs[1].Tags().Strings())
	mts.Require().Equal("doc", docs[1].Tags().GetString("content"))
	mts.Require().Equal(59, docs[1].Tags().GetInt("age"))
	mts.Require().Equal(true, docs[1].Tags().GetBool("valid"))
}

func (mts *matchTestSuite) TestMatchMultipleUsersByPatternAndTagWithDescSorting() {
	mts.fixture = "./__fixtures__/match_db1.ldb"
	db, closer, err := lemon.New(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	var docs []lemon.Document
	ctx := context.Background()
	err = db.View(context.Background(), func(tx *lemon.Tx) error {
		q := lemon.Q().
			Match("user:*").
			HasAllTags(lemon.QT().StrTagEq("content", "doc").BoolTagEq("valid", true)).
			Order(lemon.DescOrder)

		return tx.Find(ctx, q, &docs)
	})

	mts.Require().NoError(err)
	mts.Require().Len(docs, 4)

	mts.Require().Equal("user:125", docs[0].Key())
	mts.Require().Equal(`{"1900-10-20":0,"bar":{"a":667,"b":"baz123223"},"foo":125}`, docs[0].RawString())
	mts.Require().Equal(map[string]string{"auth":"basic", "content":"doc"}, docs[0].Tags().Strings())
	mts.Require().Equal("doc", docs[0].Tags().GetString("content"))
	mts.Require().Equal(true, docs[0].Tags().GetBool("valid"))

	mts.Require().Equal("user:123", docs[1].Key())
	mts.Require().Equal(`{"1900-10-20":678.345,"bar":{"a":987,"b":"baz"},"id":123}`, docs[1].RawString())
	mts.Require().Equal(map[string]string{"content":"doc"}, docs[1].Tags().Strings())
	mts.Require().Equal("doc", docs[1].Tags().GetString("content"))
	mts.Require().Equal(true, docs[1].Tags().GetBool("valid"))

	mts.Require().Equal("user:12", docs[2].Key())
	mts.Require().Equal(`{"1900-10-20":10.345,"bar":{"a":1234567,"b":"baz22"},"id":12}`, docs[2].RawString())
	mts.Require().Equal(map[string]string{"content":"doc"}, docs[2].Tags().Strings())
	mts.Require().Equal("doc", docs[2].Tags().GetString("content"))
	mts.Require().Equal(true, docs[2].Tags().GetBool("valid"))

	mts.Require().Equal("user:9", docs[3].Key())
	mts.Require().Equal(`{"1900-11-20":0.04,"bar":{"a":555,"b":"foo1234"},"id":9}`, docs[3].RawString())
	mts.Require().Equal(map[string]string{"content":"doc", "foo":"bar"}, docs[3].Tags().Strings())
	mts.Require().Equal("doc", docs[3].Tags().GetString("content"))
	mts.Require().Equal("bar", docs[3].Tags().GetString("foo"))
	mts.Require().Equal(55, docs[3].Tags().GetInt("age"))
	mts.Require().Equal(true, docs[3].Tags().GetBool("valid"))
}

func (mts *matchTestSuite) TestMatchSingleUsersByPreciseAge() {
	mts.fixture = "./__fixtures__/match_db1.ldb"
	db, closer, err := lemon.New(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	var docs []lemon.Document
	ctx := context.Background()
	err = db.View(context.Background(), func(tx *lemon.Tx) error {
		q := lemon.Q().
			Match("user:*").
			HasAllTags(lemon.QT().IntTagEq("age", 55))

		return tx.Find(ctx, q, &docs)
	})

	mts.Require().NoError(err)
	mts.Require().Len(docs, 1)

	mts.Require().Equal("user:9", docs[0].Key())
	mts.Require().Equal(`{"1900-11-20":0.04,"bar":{"a":555,"b":"foo1234"},"id":9}`, docs[0].RawString())
	mts.Require().Equal(map[string]string{"content":"doc", "foo":"bar"}, docs[0].Tags().Strings())
	mts.Require().Equal("doc", docs[0].Tags().GetString("content"))
	mts.Require().Equal("bar", docs[0].Tags().GetString("foo"))
	mts.Require().Equal(55, docs[0].Tags().GetInt("age"))
	mts.Require().Equal(true, docs[0].Tags().GetBool("valid"))
}

func seedGranularUsers(t *testing.T, db *lemon.DB) {
	err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("user:12", lemon.M{
				"id": 12,
				"bar": lemon.M{
					"a": 1234567,
					"b": "baz22",
				},
				"1900-10-20": 10.345,
			}, lemon.WithTags().Map(lemon.M{
				"content": "doc",
				"valid":   true,
			}),
		); err != nil {
			return err
		}

		if err := tx.Insert("user:9", lemon.M{
				"id": 9,
				"bar": lemon.M{
					"a": 555,
					"b": "foo1234",
				},
				"1900-11-20": 0.04,
			}, lemon.WithTags().Map(lemon.M{
				"content": "doc",
				"foo":     "bar",
				"age": 55,
				"valid":   true,
			}),
		); err != nil {
			return err
		}

		if err := tx.Insert("user:123", lemon.M{
			"id": 123,
			"bar": lemon.M{
				"a": 987,
				"b": "baz",
			},
			"1900-10-20": 678.345,
		}, lemon.WithTags().Str("content", "doc").Bool("valid", true)); err != nil {
			return err
		}

		if err := tx.Insert(
			"user:124",
			lemon.M{
				"foo": 124,
				"bar": lemon.M{
					"a": 666,
					"b": "baz223",
				},
				"1900-10-20": nil,
			},
			lemon.WithTags().Map(lemon.M{
				"content": "doc",
				"auth": "basic",
				"valid": false,
				"age": 58,
			}),
		); err != nil {
			return err
		}

		if err := tx.Insert("user:125", lemon.M{
				"foo": 125,
				"bar": lemon.M{
					"a": 667,
					"b": "baz123223",
				},
				"1900-10-20": 0.0,
			}, lemon.WithTags().Map(lemon.M{
				"content": "doc",
				"auth": "basic",
				"valid": true,
				"age": 59,
			}),
		); err != nil {
			return err
		}

		if err := tx.Insert("user:12:animals", `[123, 987, 6789]`, lemon.WithTags().Str("content", "list")); err != nil {
			return err
		}

		if err := tx.Insert(
			"user:2:animals",
			`{"turtle":1,"kangaroo":34}`,
			lemon.WithTags().Str("content", "json").Int("count", 2),
		); err != nil {
			return err
		}

		if err := tx.Insert("animal:12", `{"species": "turtle"}`,
			lemon.WithTags().Str("content", "json")); err != nil {
			return err
		}

		if err := tx.Insert("animal:1", `{"species": "kangaroo"}`,
			lemon.WithTags().Str("content", "json").Int("age", 20)); err != nil {
			return err
		}

		if err := tx.Insert("animal:3", `{"species": "penguin"}`,
			lemon.WithTags().Str("content", "json").Int("age", 22)); err != nil {
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
