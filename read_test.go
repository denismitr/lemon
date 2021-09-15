package lemon_test

import (
	"context"
	"fmt"
	"github.com/denismitr/lemon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTx_Find(t *testing.T) {
	t.Parallel()
	suite.Run(t, &findTestSuite{})
}

func TestTx_FindByTags(t *testing.T) {
	t.Parallel()
	suite.Run(t, &findByTagsTestSuite{})
}

func TestTx_Scan(t *testing.T) {
	t.Parallel()
	suite.Run(t, &scanTestSuite{})
}

func TestTx_Structs(t *testing.T) {
	t.Parallel()
	suite.Run(t, &structsTestSuite{})
}

func TestLemonDB_Read(t *testing.T) {
	fixture := "./__fixtures__/read_db1.ldb"
	seedSomeProducts(t, fixture, true)

	db, closer, err := lemon.Open(fixture)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := closer(); err != nil {
			t.Errorf("ERROR: %v", err)
		}
	}()

	assert.True(t, db.Has("product:88"))
	assert.True(t, db.Has("product:100"))

	t.Run("count existing products", func(t *testing.T) {
		assert.Equal(t, 4, db.Count())

		q1 := lemon.Q().KeyRange("product:88", "product:100")
		count1, err := db.CountByQuery(context.Background(), q1)
		require.NoError(t, err)
		assert.Equal(t, 2, count1)

		q2 := lemon.Q().KeyOrder(lemon.DescOrder).KeyRange("product:88", "product:100")
		count2, err := db.CountByQuery(context.Background(), q2)
		require.NoError(t, err)
		assert.Equal(t, 2, count2)
	})

	t.Run("get existing keys", func(t *testing.T) {
		var result1 *lemon.Document
		var result2 *lemon.Document
		if err := db.View(context.Background(), func(tx *lemon.Tx) error {
			doc1, err := tx.Get("product:88")
			if err != nil {
				return err
			}

			doc2, err := tx.Get("product:100")
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
		assert.Equal(t, `{"100":"foobar-88","baz":88,"foo":"bar/88"}`, json1)
		foo, err := result1.JSON().String("foo")
		require.NoError(t, err)
		assert.Equal(t, "bar/88", foo)

		json2 := result2.RawString()
		assert.Equal(t, `{"999":null,"baz12":123.879,"foo":"bar5674"}`, json2)
		bar5674, err := result2.JSON().String("foo")
		require.NoError(t, err)
		assert.Equal(t, "bar5674", bar5674)
		baz12, err := result2.JSON().Float("baz12")
		require.NoError(t, err)
		assert.Equal(t, 123.879, baz12)
	})

	t.Run("get many existing keys ignoring non existent", func(t *testing.T) {
		var result1 *lemon.Document
		var result2 *lemon.Document

		docs, err := db.MGet("product:88", "product:100", "non:existing:key")
		require.NoError(t, err)

		require.Len(t, docs, 2)

		result1 = docs["product:88"]
		require.NotNil(t, result1)
		result2 = docs["product:100"]
		require.NotNil(t, result2)

		rs1 := result1.RawString()
		assert.Equal(t, `{"100":"foobar-88","baz":88,"foo":"bar/88"}`, rs1)
		json1 := result1.JSON()
		foo, err := json1.String("foo")
		require.NoError(t, err)
		assert.Equal(t, "bar/88", foo)
		baz, err := json1.Int("baz")
		require.NoError(t, err)
		assert.Equal(t, 88, baz)
		assert.Equal(t, 88, json1.IntOrDefault("baz", 0))

		json2 := result2.RawString()
		assert.Equal(t, `{"999":null,"baz12":123.879,"foo":"bar5674"}`, json2)
		bar5674, err := result2.JSON().String("foo")
		require.NoError(t, err)
		assert.Equal(t, "bar5674", bar5674)
		baz12, err := result2.JSON().Float("baz12")
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
	db, closer, err := lemon.Open(fts.fixture)
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
	db, closer, err := lemon.Open(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().KeyOrder(lemon.DescOrder).HasAllTags(lemon.QT().BoolTagEq("foo", true))
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
	db, closer, err := lemon.Open(fts.fixture)
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
	db, closer, err := lemon.Open(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().KeyOrder(lemon.DescOrder).KeyRange("user:100", "user:109")
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err)
	}

	expectedDocs := 10
	fts.Assert().Len(docs, expectedDocs)

	for i := 0; i <= 9; i++ {
		fts.Require().Equal(fmt.Sprintf("user:10%d", 9-i), docs[i].Key())
		fts.Require().Equal(fmt.Sprintf("username_10%d", 9-i), docs[i].JSON().StringOrDefault("username", ""))
	}
}

func (fts *findTestSuite) TestLemonDB_FindRangeOfUsers_Ascend() {
	db, closer, err := lemon.Open(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().KeyOrder(lemon.AscOrder).KeyRange("product:500", "product:750")
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Assert().Len(docs, 251)

	for i := 500; i <= 750; i++ {
		idx := i - 500
		fts.Assert().Equal(fmt.Sprintf("product_%d", i), docs[idx].JSON().StringOrDefault("Name", ""))
		fts.Assert().Equal(i, docs[idx].JSON().IntOrDefault("id", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllUsers_Ascend() {
	db, closer, err := lemon.Open(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().KeyOrder(lemon.AscOrder).Prefix("user")
		if err := tx.Find(ctx, opts, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Require().Lenf(docs, 1_000, "users total count mismatch, got %d", len(docs))

	for i := 1; i < 1_001; i++ {
		fts.Assert().Equal(fmt.Sprintf("username_%d", i), docs[i-1].JSON().StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", i), docs[i-1].JSON().StringOrDefault("phone", ""))
		fts.Assert().Equal(i, docs[i-1].JSON().IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(i), docs[i-1].JSON().FloatOrDefault("balance", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllUsers_Descend() {
	db, closer, err := lemon.Open(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		q := lemon.Q().KeyOrder(lemon.DescOrder).Prefix("user")
		if err := tx.Find(ctx, q, &docs); err != nil {
			return err
		}

		return nil
	}); err != nil {
		fts.Require().NoError(err, "should be no error")
	}

	fts.Require().Lenf(docs, 1_000, "users total count mismatch, got %d", len(docs))

	total := 1_000
	for i := 0; i < total-999; i++ {
		//fts.Assert().Equal("", docs[999].RawString())
		fts.Assert().Equal(fmt.Sprintf("username_%d", total-i), docs[i].JSON().StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", total-i), docs[i].JSON().StringOrDefault("phone", ""))
		fts.Assert().Equal(total-i, docs[i].JSON().IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(total-i), docs[i].JSON().FloatOrDefault("balance", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllDocs_Descend() {
	db, closer, err := lemon.Open(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().KeyOrder(lemon.DescOrder)
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
		fts.Assert().Equal(fmt.Sprintf("username_%d", totalUsers-i), docs[i].JSON().StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", totalUsers-i), docs[i].JSON().StringOrDefault("phone", ""))
		fts.Assert().Equal(totalUsers-i, docs[i].JSON().IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(totalUsers-i), docs[i].JSON().FloatOrDefault("balance", 0))
	}

	totalProducts := 1_000
	for i := 0; i < totalProducts; i++ {
		fts.Assert().Equal(fmt.Sprintf("product_%d", totalProducts-i), docs[totalUsers+i].JSON().StringOrDefault("Name", ""))
		fts.Assert().Equal(totalProducts-i, docs[totalUsers+i].JSON().IntOrDefault("id", 0))
	}
}

func (fts *findTestSuite) TestLemonDB_FindAllDocs_Ascend() {
	db, closer, err := lemon.Open(fts.fixture)
	fts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			fts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var docs []lemon.Document
	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		opts := lemon.Q().KeyOrder(lemon.AscOrder)
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
		fts.Assert().Equal(fmt.Sprintf("product_%d", i+1), docs[i].JSON().StringOrDefault("Name", ""))
		fts.Assert().Equal(i+1, docs[i].JSON().IntOrDefault("id", 0))
	}

	totalUsers := 1_000
	for i := 0; i < totalUsers; i++ {
		fts.Assert().Equal(fmt.Sprintf("username_%d", i+1), docs[totalProducts+i].JSON().StringOrDefault("username", ""))
		fts.Assert().Equal(fmt.Sprintf("999444555%d", i+1), docs[totalProducts+i].JSON().StringOrDefault("phone", ""))
		fts.Assert().Equal(i+1, docs[totalProducts+i].JSON().IntOrDefault("logins", 0))
		fts.Assert().Equal(float64(i+1), docs[totalProducts+i].JSON().FloatOrDefault("balance", 0))
	}
}

type structsTestSuite struct {
	hasTags bool
	modAddress int
	totalPersons int
	suite.Suite
	fixture string
}

func (sts *structsTestSuite) SetupSuite() {
	sts.fixture = "./__fixtures__/structs_db1.ldb"
	sts.hasTags = true
	sts.modAddress = 4
	sts.totalPersons = 1_000

	db, closer, err := lemon.Open(sts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})
	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	seedPersonStructs(sts.T(), db, 10_000, sts.hasTags, sts.modAddress)
}

func (sts *structsTestSuite) TearDownSuite() {
	if err := os.Remove(sts.fixture); err != nil {
		sts.Require().NoError(err)
	}
}

func (sts *structsTestSuite) TestCheckTagsAsync() {
	if !sts.hasTags {
		sts.Fail("cannot check tags")
	}

	db, closer, err := lemon.Open(sts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	var wg sync.WaitGroup

	for i := 1; i <= sts.totalPersons; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			if sts.totalPersons == index {
				return
			}

			id := sts.totalPersons - index
			key := personKey(id)
			doc, err := db.Get(key)
			sts.Require().NoError(err)

			sts.Assert().Equal(id % sts.modAddress == 0, doc.Tags().Bool("has-address"))
			sts.Assert().Equal("application/json", doc.Tags().String("content-type"))
			sts.Assert().Equal(0, doc.Tags().Int("non-existent"))
		}(i)
	}

	wg.Wait()
}

func (sts *structsTestSuite) TestScanAll() {
	db, closer, err := lemon.Open(sts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx := context.Background()

	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		i := 1

		err := tx.Scan(ctx, nil, func(d *lemon.Document) bool {
			var p person
			sts.Assert().Equal(fmt.Sprintf("person:%d", i), d.Key())
			sts.Require().NoError(d.JSON().Unmarshal(&p))
			sts.Assert().Equal(uint32(i), p.ID)
			sts.Assert().True(p.Sex == "male" || p.Sex == "female")

			if p.Address != nil {
				sts.Assert().True(strings.HasPrefix(p.Address.Street, "New York"))
			} else {
				sts.Assert().Truef(p.ID % uint32(sts.modAddress) != 0, "there should be no address here")
			}

			sts.Assert().True(p.Salary > 0)
			i++
			return true
		})

		sts.Require().NoError(err)

		return nil
	}); err != nil {
		sts.T().Fatal(err)
	}
}

func (sts *structsTestSuite) TestScanCanBeInterrupted() {
	db, closer, err := lemon.Open(sts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx := context.Background()

	if err := db.View(context.Background(), func(tx *lemon.Tx) error {
		i := 1

		err := tx.Scan(ctx, nil, func(d *lemon.Document) bool {
			if i >= 3 {
				return false
			}

			var p person
			sts.Assert().Equal(fmt.Sprintf("person:%d", i), d.Key())
			sts.Require().NoError(d.JSON().Unmarshal(&p))
			sts.Assert().Equal(uint32(i), p.ID)
			sts.Assert().True(p.Sex == "male" || p.Sex == "female")

			if p.Address != nil {
				sts.Assert().True(strings.HasPrefix(p.Address.Street, "New York"))
			} else {
				sts.Assert().Truef(p.ID % uint32(sts.modAddress) != 0, "there should be no address here")
			}

			sts.Assert().True(p.Salary > 0)
			i++
			return true
		})

		sts.Require().Equal(3, i)
		sts.Require().NoError(err)

		return nil
	}); err != nil {
		sts.T().Fatal(err)
	}
}

func (sts *structsTestSuite) Test_ItCanReadOwnWrites() {
	db, closer, err := lemon.Open(sts.fixture, &lemon.Config{
		DisableAutoVacuum: true,
	})

	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx := context.Background()

	if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		i := 0

		sts.NoError(tx.Insert("foo:bar:baz", lemon.M{"foo": []string{"bar", "baz"}}))
		sts.NoError(tx.Insert("123:bar:baz", lemon.M{"123": []string{"bar123","bar123"}}))

		var docs []*lemon.Document
		q := lemon.Q().Match("*:bar:baz").KeyOrder(lemon.AscOrder)
		err := tx.Scan(ctx, q, func(d *lemon.Document) bool {
			docs = append(docs, d)
			i++
			return true
		})

		sts.Require().Equal(2, i)
		sts.Require().NoError(err)

		return nil
	}); err != nil {
		sts.T().Fatal(err)
	}
}

type address struct {
	Phone  string `json:"phone"`
	Street string `json:"street"`
	Zip    int    `json:"zip"`
}
type person struct {
	ID      uint32  `json:"id"`
	Name    string  `json:"name"`
	Age     int     `json:"age"`
	Salary  float64 `json:"salary"`
	Sex     string  `json:"sex"`
	Address *address `json:"address"`
}

func seedPersonStructs(t *testing.T, db *lemon.DB, num int, tag bool, modAddress int) {
	t.Helper()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Whoops %+v", r)
		}
	}()

	tx, err := db.Begin(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= num; i++ {
		p := person{
			ID:     uint32(i),
			Name:   "name_" + RandomString(10),
			Age:    rand.Int(),
			Salary: rand.Float64(),
			Sex:    RandomBoolString("male", "female"),
		}

		if i % modAddress == 0 {
			p.Address = &address{
				Phone:  fmt.Sprintf("+%d", rand.Uint64()),
				Street: fmt.Sprintf("New York, %s, avenue, %d", RandomString(10), rand.Uint32()),
				Zip:    int(rand.Uint32()),
			}
		}

		key := personKey(i)
		if err := tx.InsertOrReplace(key, p); err != nil {
			require.NoError(t, tx.Rollback())
			t.Fatal(err)
		}

		if tag {
			if err := tx.Tag(key, lemon.M{
				"has-address": p.Address != nil,
			}); err != nil {
				require.NoError(t, tx.Rollback())
				t.Fatal(err)
			}
		}
	}

	require.NoError(t, tx.Commit())

	if tag {
		err = db.Update(context.Background(), func(tx *lemon.Tx) error {
			for i := 1; i <= num; i++ {
				key := personKey(i)
				if err := tx.Tag(key, lemon.M{
					"content-type": "application/json",
				}); err != nil {
					require.NoError(t, tx.Rollback())
					t.Fatal(err)
				}
			}
			return nil
		})
	}
}

type scanTestSuite struct {
	suite.Suite
	fixture string
}

func (sts *scanTestSuite) SetupSuite() {
	sts.fixture = "./__fixtures__/scan_db1.ldb"
	db, closer, err := lemon.Open(sts.fixture)
	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	seedUserData(sts.T(), db, 1_000, seedTags{})
	seedProductData(sts.T(), db, 1_000)
	seedUserPets(sts.T(), db, 10, 50, 3)
	seedUserPets(sts.T(), db, 134, 140, 5)
}

func (sts *scanTestSuite) TearDownSuite() {
	if err := os.Remove(sts.fixture); err != nil {
		sts.Require().NoError(err)
	}
}

func (sts *scanTestSuite) Test_ScanUserPets() {
	db, closer, err := lemon.Open(sts.fixture)
	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sts.Require().Equal(2158, db.Count())

	var docs []*lemon.Document
	if err := db.View(ctx, func(tx *lemon.Tx) error {
		opts := lemon.Q().KeyOrder(lemon.AscOrder).Prefix("user")
		if scanErr := tx.Scan(ctx, opts, func(d *lemon.Document) bool {
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
	db, closer, err := lemon.Open(sts.fixture)
	sts.Require().NoError(err)

	defer func() {
		if err := closer(); err != nil {
			sts.T().Errorf("ERROR: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sts.Require().Equal(2158, db.Count())

	var docs []*lemon.Document
	if err := db.View(ctx, func(tx *lemon.Tx) error {
		opts := lemon.Q().KeyOrder(lemon.AscOrder).Prefix("user")
		if scanErr := tx.Scan(ctx, opts, func(d *lemon.Document) bool {
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

func personKey(i int) string {
	return fmt.Sprintf("person:%d", i)
}