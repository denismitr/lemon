package lemon_test

import (
	"context"
	"github.com/denismitr/lemon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"os"
	"sync"
	"testing"
	"time"
)

func Test_ImplicitTags_WriteRead(t *testing.T) {
	fixture := "./__fixtures__/implicit_tag_1.ldb"

	db, closer, err := lemon.Open(fixture)
	require.NoError(t, err)

	defer func() {
		if err := closer(); err != nil {
			t.Fatal(err)
		}

		if err := os.Remove(fixture); err != nil {
			t.Fatal(err)
		}
	}()

	start := time.Now()
	assert.NoError(t, db.Insert("key:001", lemon.M{"key": 1}, lemon.WithTimestamps()))
	time.Sleep(1 * time.Second)

	assert.NoError(t, db.Insert("key:002", lemon.M{"key": 2}, lemon.WithTimestamps()))
	time.Sleep(1 * time.Second)

	assert.NoError(t,
		db.Insert("key:003",
			`key: 003`,
			lemon.WithTimestamps()))
	time.Sleep(1 * time.Second)

	assert.NoError(t,
		db.Insert("key:004",
			[]byte(`key: 004`),
			lemon.WithTimestamps()))
	time.Sleep(1 * time.Second)

	assert.NoError(t, db.Insert("key:005", lemon.M{"key": 5}, lemon.WithTimestamps()))
	time.Sleep(1 * time.Second)

	require.NoError(t, db.Insert("key:006", lemon.M{"key": 5}, lemon.WithTimestamps()))

	qt := lemon.QT().IntTagGt(lemon.CreatedAt, int(start.Add(2500 * time.Millisecond).Unix()))
	docs, err := db.Find(context.Background(), lemon.Q().HasAllTags(qt))
	require.NoError(t, err)

	require.Equal(t, 3, len(docs))
}

func Test_ScanByTagName(t *testing.T) {
	suite.Run(t, &scanByTagNameSuite{})
}

type scanByTagNameSuite struct {
	suite.Suite
	fixture string
}

func (s *scanByTagNameSuite) SetupSuite() {
	s.fixture = "./__fixtures__/scan_tag_1.ldb"

	db, closer, err := lemon.Open(s.fixture)
	s.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			s.Fail(err.Error())
		}
	}()

	var wg sync.WaitGroup
	wg.Add(4)

	go seedAnimals(s.T(), db, &wg)
	go seedTvProducts(s.T(), db, &wg)
	go seedGranularUsers(s.T(), db, &wg)
	go seedTaggedCars(s.T(), db, &wg)

	wg.Wait()
}

func (s *scanByTagNameSuite) TearDownSuite() {
	if err := os.Remove(s.fixture); err != nil {
		s.Fail(err.Error())
	}
}

func (s *scanByTagNameSuite) TestScanByBoolTag_Asc() {
	docs := testScan(s.T(), s.fixture, lemon.Q().ByTagName("maxSpeed").KeyOrder(lemon.AscOrder))
	s.Require().Equal(4, len(docs))

	s.Assert().Equal("car:12", docs[0].Key())
	s.Assert().Equal(160, docs[0].Tags().Int("maxSpeed"))

	s.Assert().Equal("car:10", docs[1].Key())
	s.Assert().Equal(200, docs[1].Tags().Int("maxSpeed"))

	s.Assert().Equal("car:88", docs[2].Key())
	s.Assert().Equal(240, docs[2].Tags().Int("maxSpeed"))

	s.Assert().Equal("car:104", docs[3].Key())
	s.Assert().Equal(322, docs[3].Tags().Int("maxSpeed"))
}

func testScan(t *testing.T, dbPath string, qo *lemon.QueryOptions) (docs []*lemon.Document) {
	t.Helper()

	db, closer, err := lemon.Open(dbPath)
	require.NoError(t, err)

	defer func() {
		if err := closer(); err != nil {
			t.Fatal(err)
		}
	}()

	err = db.Scan(context.Background(), qo, func(d *lemon.Document) bool {
		docs = append(docs, d)
		return true
	})

	require.NoError(t, err)
	return
}

func seedTaggedCars(t *testing.T, db *lemon.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.InsertOrReplace(
			"car:12",
			lemon.M{
				"id": 12,
				"maker": "ford",
				"model": "focus",
			},
			lemon.WithTags().Map(lemon.M{
				"transmission": "automatic",
				"maxSpeed": 160,
				"price": 98773.98,
			}),
		); err != nil {
			return err
		}

		if err := tx.InsertOrReplace(
			"car:10",
			lemon.M{
				"id": 10,
				"maker": "Tesla",
				"model": "mx900",
				"currency": []string{"EUR", "USD", "GBP"},
			},
			lemon.WithTags().Map(lemon.M{
				"transmission": "automatic",
				"maxSpeed": 200,
				"price": 298473.80,
			}),
		); err != nil {
			return err
		}

		if err := tx.InsertOrReplace(
			"car:104",
			lemon.M{
				"id": 104,
				"maker": "Ferrari",
				"model": "FX999",
				"currency": []string{"EUR", "USD"},
			},
			lemon.WithTags().Map(lemon.M{
				"transmission": "manual",
				"maxSpeed":322,
				"price": 458473.80,
			}),
		); err != nil {
			return err
		}

		if err := tx.InsertOrReplace(
			"car:88",
			lemon.M{
				"id": 88,
				"maker": "Nissan",
				"model": "Murano",
				"currency": []string{"EUR", "USD", "RUR"},
			},
			lemon.WithTags().Map(lemon.M{
				"transmission": "automatic",
				"maxSpeed": 240,
				"price": 33900.66,
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

