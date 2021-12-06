package lemon_test

import (
	"context"
	"github.com/denismitr/lemon"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"os"
	"sync"
	"testing"
)

func TestTx_Match(t *testing.T) {
	t.Parallel()
	suite.Run(t, &matchTestSuite{})
}

func TestTx_Untag(t *testing.T) {
	t.Parallel()
	suite.Run(t, &untagTestSuite{})
}

type untagTestSuite struct {
	suite.Suite
	fixture string
}

func (uts *untagTestSuite) SetupSuite() {
	uts.fixture = "./__fixtures__/untag_db1.ldb"
	db, closer, err := lemon.Open(uts.fixture, &lemon.Config{
		AutoVacuumOnlyOnClose: true,
	})

	uts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			uts.T().Errorf("ERROR: %v", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go seedAnimals(uts.T(), db, &wg)
	go seedTvProducts(uts.T(), db, &wg)
	wg.Wait()
}

func (uts *untagTestSuite) TearDownSuite() {
	if err := os.Remove(uts.fixture); err != nil {
		uts.Require().NoError(err)
	}
}

func (uts *untagTestSuite) TestUntagSingleProduct() {
	db, closer, err := lemon.Open(uts.fixture)
	uts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			uts.T().Errorf("DB CLOSE ERROR: %v", err)
		}
	}()

	productBeforeUntag, err := db.Get("product:34")
	uts.Require().NoError(err)
	uts.Require().NotNil(productBeforeUntag)

	tagsBeforeUntag := productBeforeUntag.Tags()
	uts.Assert().Equal(23.45, tagsBeforeUntag["price"])
	uts.Assert().Equal("tv", tagsBeforeUntag["type"])

	uts.Require().NoError(db.Untag("product:34", "price", "type"))

	productAfterUntag, err := db.Get("product:34")
	uts.Require().NoError(err)
	uts.Require().NotNil(productAfterUntag)

	tagsAfterUntag := productAfterUntag.Tags()
	uts.Assert().Nil(tagsAfterUntag["price"])
	uts.Assert().Nil(tagsAfterUntag["type"])
}

func (uts *untagTestSuite) Test_Attempt_Untag_NonExistingKey() {
	db, closer, err := lemon.Open(uts.fixture)
	uts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			uts.T().Errorf("DB CLOSE ERROR: %v", err)
		}
	}()

	utErr := db.Untag("non:existing:key", "foo", "bar", "baz")
	uts.Require().Error(utErr)
	uts.Require().True(errors.Is(utErr, lemon.ErrKeyDoesNotExist))
}

func (uts *untagTestSuite) Test_Untag_MultipleProduct() {
	db, closer, err := lemon.Open(uts.fixture)
	uts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			uts.T().Errorf("DB CLOSE ERROR: %v", err)
		}
	}()

	p11BeforeUntag, err := db.Get("product:11")
	uts.Require().NoError(err)
	uts.Require().NotNil(p11BeforeUntag)
	uts.Require().Equal(lemon.M{"inStock": 8, "price": 9.45, "type": "tv"}, p11BeforeUntag.Tags())

	p10BeforeUntag, err := db.Get("product:10")
	uts.Require().NoError(err)
	uts.Require().NotNil(p10BeforeUntag)
	uts.Require().Equal(lemon.M{"inStock": 2, "price": 10.45, "type": "tv"}, p10BeforeUntag.Tags())

	var p11TagsBeforeCommit lemon.M
	var p10TagsBeforeCommit lemon.M
	if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Untag("product:11", "inStock", "price", "type"); err != nil {
			return err
		}

		if err := tx.Untag("product:10", "price"); err != nil {
			return err
		}

		p11, _ := tx.Get("product:11")
		p11TagsBeforeCommit = p11.Tags()

		p10, _ := tx.Get("product:10")
		p10TagsBeforeCommit = p10.Tags()

		return nil
	}); err != nil {
		uts.Require().NoError(err)
	}

	// expect tags before commit and after commit be equal
	uts.Assert().Equal(lemon.M{}, p11TagsBeforeCommit)
	uts.Assert().Equal(lemon.M{"inStock": 2, "type": "tv"}, p10TagsBeforeCommit)

	p10AfterUntag, err := db.Get("product:10")
	uts.Require().NoError(err)
	uts.Require().NotNil(p10AfterUntag)
	uts.Assert().Equal(lemon.M{"inStock": 2, "type": "tv"}, p10AfterUntag.Tags())

	p11AfterUntag, err := db.Get("product:11")
	uts.Require().NoError(err)
	uts.Require().NotNil(p11AfterUntag)
	uts.Assert().Equal(lemon.M{}, p11AfterUntag.Tags())
}

func (uts *untagTestSuite) Test_Untag_TagAndUntagInOneTx() {
	db, closer, err := lemon.Open(uts.fixture)
	uts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			uts.T().Errorf("DB CLOSE ERROR: %v", err)
		}
	}()

	u2aBeforeUntag, err := db.Get("user:2:animals")
	uts.Require().NoError(err)
	uts.Require().NotNil(u2aBeforeUntag)
	uts.Require().Equal(lemon.M{"content": "json", "count": 2, "dailyExpenses":45.6}, u2aBeforeUntag.Tags())

	var u2aTagsBeforeCommit lemon.M
	if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Untag("user:2:animals", "count", "content"); err != nil {
			return err
		}

		if err := tx.Tag("user:2:animals", lemon.M{
			"foo": "bar",
			"continents": 3,
			"extinct": false,
		}); err != nil {
			return err
		}

		u2a, _ := tx.Get("user:2:animals")
		u2aTagsBeforeCommit = u2a.Tags()

		return nil
	}); err != nil {
		uts.Require().NoError(err)
	}

	// expect tags before commit and after commit be equal
	uts.Assert().Equal(lemon.M{
		"foo": "bar",
		"continents": 3,
		"dailyExpenses":45.6,
		"extinct": false,
	}, u2aTagsBeforeCommit)

	u2aAfterCommit, err := db.Get("user:2:animals")
	uts.Require().NoError(err)
	uts.Require().NotNil(u2aAfterCommit)
	uts.Assert().Equal(lemon.M{
		"foo": "bar",
		"continents": 3,
		"dailyExpenses":45.6,
		"extinct": false,
	}, u2aAfterCommit.Tags())
}

type matchTestSuite struct {
	suite.Suite
	fixture string
}

func (mts *matchTestSuite) SetupSuite() {
	mts.fixture = "./__fixtures__/match_db1.ldb"
	db, closer, err := lemon.Open(mts.fixture, &lemon.Config{
		AutoVacuumOnlyOnClose: true,
	})

	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(4)
	seedGranularUsers(mts.T(), db, &wg)
	seedAnimals(mts.T(), db, &wg)
	seedTvProducts(mts.T(), db, &wg)
	seedWebPages(mts.T(), db, &wg)
	wg.Wait()
}

func (mts *matchTestSuite) TearDownSuite() {
	if err := os.Remove(mts.fixture); err != nil {
		mts.Require().NoError(err)
	}
}

func (mts *matchTestSuite) TestMatchSingleUserByPatternAndTag() {
	db, closer, err := lemon.Open(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	q := lemon.Q().Match("user:*").
		HasAllTags(lemon.QT().StrTagEq("content", "list"))
	docs, err := db.FindContext(context.Background(), q)

	mts.Require().NoError(err)
	mts.Require().Len(docs, 1)
	mts.Require().Equal("user:12:animals", docs[0].Key())
	mts.Require().Equal(`[123, 987, 6789]`, docs[0].RawString())
	mts.Require().Equal(123, docs[0].JSON().IntOrDefault(`0`, 0))
	mts.Require().Equal(lemon.M{"content": "list"}, docs[0].Tags())
	mts.Require().Equal("list", docs[0].Tags().String("content"))
}

func (mts *matchTestSuite) TestMatchSingleUserByPatternAndTag_Lazyload() {
	db, closer, err := lemon.Open(mts.fixture, &lemon.Config{
		ValueLoadStrategy: lemon.LazyLoad,
	})

	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	q := lemon.Q().Match("user:*").
		HasAllTags(lemon.QT().StrTagEq("content", "list"))
	docs, err := db.FindContext(context.Background(), q)

	mts.Require().NoError(err)
	mts.Require().Len(docs, 1)
	mts.Require().Equal("user:12:animals", docs[0].Key())
	mts.Require().Equal(`[123, 987, 6789]`, docs[0].RawString())
	mts.Require().Equal(123, docs[0].JSON().IntOrDefault(`0`, 0))
	mts.Require().Equal(lemon.M{"content": "list"}, docs[0].Tags())
	mts.Require().Equal("list", docs[0].Tags().String("content"))
}

func (mts *matchTestSuite) TestMatchMultipleUsersByPatternAndGtIntTag() {
	db, closer, err := lemon.Open(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	q := lemon.Q().Match("user:*").
		HasAllTags(lemon.QT().IntTagGt("age", 55))

	docs, err := db.FindContext(context.Background(), q)
	mts.Require().NoError(err)
	mts.Require().Len(docs, 2)

	mts.Require().Equal("user:124", docs[0].Key())
	mts.Require().Equal(`{"1900-10-20":null,"bar":{"a":666,"b":"baz223"},"foo":124}`, docs[0].RawString())
	mts.Require().Equal(lemon.M{"age": 58, "auth": "basic", "content": "other", "valid": false}, docs[0].Tags())
	mts.Require().Equal("other", docs[0].Tags().String("content"))

	mts.Require().Equal("user:125", docs[1].Key())
	mts.Require().Equal(`{"1900-10-20":0,"bar":{"a":667,"b":"baz123223"},"foo":125}`, docs[1].RawString())
	mts.Require().Equal(lemon.M{"age": 59, "auth": "basic", "content": "doc", "valid": true}, docs[1].Tags())
	mts.Require().Equal("doc", docs[1].Tags().String("content"))
	mts.Require().Equal(59, docs[1].Tags().Int("age"))
	mts.Require().Equal(true, docs[1].Tags().Bool("valid"))
}

func (mts *matchTestSuite) TestMatchMultipleUsersByPatternAndTagWithDescSorting() {
	db, closer, err := lemon.Open(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	q := lemon.Q().
		Match("user:*").
		HasAllTags(lemon.QT().StrTagEq("content", "doc").BoolTagEq("valid", true)).
		KeyOrder(lemon.DescOrder)

	docs, err := db.FindContext(context.Background(), q)
	mts.Require().NoError(err)
	//mts.Require().Lenf(docs, 4, "got %d instead of 4", len(docs))

	mts.Require().Equal("user:125", docs[0].Key())
	mts.Require().Equal(`{"1900-10-20":0,"bar":{"a":667,"b":"baz123223"},"foo":125}`, docs[0].RawString())
	mts.Require().Equal(lemon.M{"age": 59, "auth": "basic", "content": "doc", "valid": true}, docs[0].Tags())
	mts.Require().Equal("doc", docs[0].Tags().String("content"))
	mts.Require().Equal(true, docs[0].Tags().Bool("valid"))

	mts.Require().Equal("user:123", docs[1].Key())
	mts.Require().Equal(`{"1900-10-20":678.345,"bar":{"a":987,"b":"baz"},"id":123}`, docs[1].RawString())
	mts.Require().Equal(lemon.M{"auth": "token", "content": "doc", "valid": true}, docs[1].Tags())
	mts.Require().Equal("doc", docs[1].Tags().String("content"))
	mts.Require().Equal(true, docs[1].Tags().Bool("valid"))

	mts.Require().Equal("user:12", docs[2].Key())
	mts.Require().Equal(`{"1900-10-20":10.345,"bar":{"a":1234567,"b":"baz22"},"id":12}`, docs[2].RawString())
	mts.Assert().Equal(lemon.M{"auth": "token", "content": "doc", "valid": true}, docs[2].Tags())
	mts.Require().Equal("doc", docs[2].Tags().String("content"))
	mts.Require().Equal(true, docs[2].Tags().Bool("valid"))

	mts.Require().Equal("user:9", docs[3].Key())
	mts.Require().Equal(`{"1900-11-20":0.04,"bar":{"a":555,"b":"foo1234"},"id":9}`, docs[3].RawString())
	mts.Require().Equal(lemon.M{"age": 55, "auth": "basic", "content": "doc", "foo": "bar", "valid": true}, docs[3].Tags())
	mts.Require().Equal("doc", docs[3].Tags().String("content"))
	mts.Require().Equal("bar", docs[3].Tags().String("foo"))
	mts.Require().Equal(55, docs[3].Tags().Int("age"))
	mts.Require().Equal(true, docs[3].Tags().Bool("valid"))
}

func (mts *matchTestSuite) TestMatchMultipleUsersByPatternAndTagWithAscSorting() {
	db, closer, err := lemon.Open(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	q := lemon.Q().
		Match("user:*").
		HasAllTags(lemon.QT().StrTagEq("auth", "basic")).
		KeyOrder(lemon.AscOrder)

	docs, err := db.FindContext(context.Background(), q)

	mts.Require().NoError(err)
	mts.Require().Len(docs, 3)

	mts.Require().Equal("user:125", docs[2].Key())
	mts.Require().Equal(`{"1900-10-20":0,"bar":{"a":667,"b":"baz123223"},"foo":125}`, docs[2].RawString())
	mts.Require().Equal(lemon.M{"age": 59, "auth": "basic", "content": "doc", "valid": true}, docs[2].Tags())
	mts.Require().Equal("doc", docs[2].Tags().String("content"))
	mts.Require().Equal(true, docs[2].Tags().Bool("valid"))

	mts.Require().Equal("user:124", docs[1].Key())
	mts.Require().Equal(`{"1900-10-20":null,"bar":{"a":666,"b":"baz223"},"foo":124}`, docs[1].RawString())
	mts.Assert().Equal(lemon.M{"age": 58, "auth": "basic", "content": "other", "valid": false}, docs[1].Tags())
	mts.Require().Equal("other", docs[1].Tags().String("content"))
	mts.Require().Equal(false, docs[1].Tags().Bool("valid"))

	mts.Require().Equal("user:9", docs[0].Key())
	mts.Require().Equal(`{"1900-11-20":0.04,"bar":{"a":555,"b":"foo1234"},"id":9}`, docs[0].RawString())
	mts.Assert().Equal(lemon.M{"age": 55, "auth": "basic", "content": "doc", "foo": "bar", "valid": true}, docs[0].Tags())
	mts.Require().Equal("doc", docs[0].Tags().String("content"))
	mts.Require().Equal("bar", docs[0].Tags().String("foo"))
	mts.Require().Equal(55, docs[0].Tags().Int("age"))
	mts.Require().Equal(true, docs[0].Tags().Bool("valid"))
}

func (mts *matchTestSuite) TestMatchSingleUsersByPreciseAge() {
	db, closer, err := lemon.Open(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	q := lemon.Q().
		Match("user:*").
		HasAllTags(lemon.QT().IntTagEq("age", 55))

	docs, err := db.FindContext(context.Background(), q)

	mts.Require().NoError(err)
	mts.Require().Len(docs, 1)

	mts.Require().Equal("user:9", docs[0].Key())
	mts.Require().Equal(`{"1900-11-20":0.04,"bar":{"a":555,"b":"foo1234"},"id":9}`, docs[0].RawString())
	mts.Require().Equal(lemon.M{"age": 55, "auth": "basic", "content": "doc", "foo": "bar", "valid": true}, docs[0].Tags())
	mts.Require().Equal("doc", docs[0].Tags().String("content"))
	mts.Require().Equal("bar", docs[0].Tags().String("foo"))
	mts.Require().Equal(55, docs[0].Tags().Int("age"))
	mts.Require().Equal(true, docs[0].Tags().Bool("valid"))
}

func (mts *matchTestSuite) TestMatchSingleUrlKey() {
	db, closer, err := lemon.Open(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	doc, err := db.Get("https://www.php.net/manual/en/function.str-replace")
	mts.Require().NoError(err)

	mts.Assert().Equal("https://www.php.net/manual/en/function.str-replace", doc.Key())
	b := doc.Value()

	assertFileContentsEquals(mts.T(), "./__fixtures__/web4.html", b)
}

func (mts *matchTestSuite) TestMatchMultipleTvsByGtFloatTag() {
	db, closer, err := lemon.Open(mts.fixture)
	mts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			mts.T().Errorf("ERROR: %v", err)
		}
	}()

	q := lemon.Q().
		HasAllTags(lemon.QT().FloatTagGt("price", 4.1)).
		KeyOrder(lemon.AscOrder)

	docs, err := db.FindContext(context.Background(), q)

	mts.Require().NoError(err)
	mts.Require().Len(docs, 5)
	mts.Assert().Equal("product:1", docs[0].Key())
	mts.Assert().Equal(`{"model":"XDF897","vendor":"Samsung","version":1.2}`, docs[0].RawString())

	mts.Assert().Equal("product:7", docs[1].Key())
	mts.Assert().Equal(`{"model":"AFK2","vendor":"LG","version":4.3}`, docs[1].RawString())

	mts.Assert().Equal("product:10", docs[2].Key())
	mts.Assert().Equal(`{"model":"AFK1","vendor":"LG","version":4.2}`, docs[2].RawString())

	mts.Assert().Equal("product:11", docs[3].Key())
	mts.Assert().Equal(`{"model":"Bravia-22","vendor":"Sony","version":4.3}`, docs[3].RawString())

	mts.Assert().Equal("product:34", docs[4].Key())
	mts.Assert().Equal(`{"model":"XDF897","vendor":"Samsung","version":1.2}`, docs[4].RawString())
}

func seedGranularUsers(t *testing.T, db *lemon.DB, wg *sync.WaitGroup) {
	defer wg.Done()

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
			"auth":    "token",
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
			"auth":    "basic",
			"age":     55,
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
		}, lemon.WithTags().
			Str("content", "doc").
			Str("auth", "token").
			Bool("valid", true),
		); err != nil {
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
				"content": "other",
				"auth":    "basic",
				"valid":   false,
				"age":     58,
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
			"auth":    "basic",
			"valid":   true,
			"age":     59,
		}),
		); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

func seedAnimals(t *testing.T, db *lemon.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	err := db.Update(context.Background(), func(tx *lemon.Tx) error {
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
			lemon.WithTags().
			Str("content", "json").
			Int("age", 22),
		); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if err := db.Tag("user:2:animals", lemon.M{"dailyExpenses": 45.6}); err != nil {
		t.Fatal(err)
	}
}

func seedTvProducts(t *testing.T, db *lemon.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert(
			"product:4",
			lemon.M{"vendor": "Samsung", "model": "XDF555", "version": 1.0},
			lemon.WithTags().
				Float("price", 3.45).
				Int("inStock", 2).
				Str("type", "tv"),
		); err != nil {
			return err
		}

		if err := tx.Insert(
			"product:1",
			lemon.M{"vendor": "Samsung", "model": "XDF897", "version": 1.2},
			lemon.WithTags().
				Float("price", 23.45).
				Int("inStock", 20).
				Str("type", "tv"),
		); err != nil {
			return err
		}

		if err := tx.Insert(
			"product:34",
			lemon.M{"vendor": "Samsung", "model": "XDF897", "version": 1.2},
			lemon.WithTags().
				Float("price", 23.45).
				Int("inStock", 20).
				Str("type", "tv"),
		); err != nil {
			return err
		}

		if err := tx.Insert(
			"product:10",
			lemon.M{
				"vendor":  "LG",
				"model":   "AFK1",
				"version": 4.2,
			},
			lemon.WithTags().
				Float("price", 10.45).
				Int("inStock", 2).
				Str("type", "tv"),
		); err != nil {
			return err
		}

		if err := tx.Insert(
			"product:7",
			lemon.M{
				"vendor":  "LG",
				"model":   "AFK2",
				"version": 4.3,
			},
			lemon.WithTags().
				Float("price", 43.45).
				Int("inStock", 11).
				Str("type", "tv"),
		); err != nil {
			return err
		}

		if err := tx.Insert(
			"product:11",
			lemon.M{
				"vendor":  "Sony",
				"model":   "Bravia-22",
				"version": 4.3,
			},
			lemon.WithTags().
				Float("price", 9.45).
				Int("inStock", 8).
				Str("type", "tv"),
		); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

func seedWebPages(t *testing.T, db *lemon.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert(
			"https://www.php.net/manual/en/function.str-replace",
			loadFixtureContents(t, "./__fixtures__/web4.html"),
			lemon.WithTags().Map(lemon.M{
				"content": "html",
				"auth":    "none",
				"active":  true,
			}),
		); err != nil {
			return err
		}

		if err := tx.Insert(
			"https://www.php.net/manual/en/function.str-repeat.php",
			loadFixtureContents(t, "./__fixtures__/web3.html"),
			lemon.WithTags().Map(lemon.M{
				"content": "html",
				"auth":    "none",
				"active":  true,
			}),
		); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}
