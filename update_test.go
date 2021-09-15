package lemon_test

import (
	"context"
	"fmt"
	"github.com/denismitr/lemon"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestTx_Remove(t *testing.T) {
	t.Parallel()
	suite.Run(t, &removeTestSuite{})
}

func Test_Write(t *testing.T) {
	t.Parallel()
	suite.Run(t, &writeTestSuite{})
}

func Test_TruncateExistingDatabase(t *testing.T) {
	t.Parallel()

	path := "./__fixtures__/truncate_db1.ldb"
	seedSomeProducts(t, path, true)

	t.Run("existing database should be truncated on open", func(t *testing.T) {
		db, closer, err := lemon.Open(path, &lemon.Config{
			TruncateFileWhenOpen: true,
		})

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := closer(); err != nil {
				t.Errorf("ERROR: %v", err)
			}

			_ = os.Remove(path)
		}()

		require.NoError(t, db.Insert("product:2", lemon.M{
			"100": "foobar2",
			"baz": 2,
			"foo": "bar",
		}))
	})
}

func TestTx_FlushAll(t *testing.T) {
	t.Run("database can be opened, seeded and than flushed completely", func(t *testing.T) {
		fixture := createDbForFlushAllTest(t, "flush1")

		t.Logf("Database %s created and seeded", fixture)

		db, closer, err := lemon.Open(fixture)
		if err != nil {
			require.NoError(t, err)
		}

		defer closeDbAndRemoveFile(t, closer, fixture)

		assert.Equal(t, 10000, db.Count())

		if err := db.FlushAll(context.Background()); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, 0, db.Count())
		assert.NoError(t, db.Vacuum())

		assertFileContentsEquals(t, fixture, []byte(""))
	})

	t.Run("database can be opened, seeded flushed and rolled back immediately", func(t *testing.T) {
		fixture := createDbForFlushAllTest(t, "flush2")

		t.Logf("Database %s created and seeded", fixture)

		db, closer, err := lemon.Open(fixture)
		if err != nil {
			require.NoError(t, err)
		}

		defer closeDbAndRemoveFile(t, closer, fixture)

		assert.Equal(t, 10000, db.Count())

		assert.Error(t, db.Update(context.Background(), func(tx *lemon.Tx) error {
			t.Logf("Now we insert a record, update a record, flush all and than rollback everyting")

			assert.NoError(t, tx.Insert("foo:bar:baz", lemon.M{
				"abc": true,
				"bar": "baz",
				"lemon": "database",
			}))

			assert.NoError(t, tx.InsertOrReplace("person:100", lemon.M{
				"def": 1000.987,
			}))

			assert.NoError(t, tx.FlushAll())

			return errors.New("rollback")
		}))

		t.Logf("Now we check that everythin was rolled back with success")
		assert.Equal(t, 10000, db.Count())
		t.Logf("\t\tRecoord count did not change")

		t.Logf("Checking that update was rolled back")
		p100, err := db.Get("person:100")
		require.NoError(t, err)
		assert.Equal(t, "person:100", p100.Key())
		assert.Equal(t, 100, p100.JSON().IntOrDefault("id", 0))
		t.Logf("\t\tUpdate was rolled back")

		t.Logf("Checking that insert was rolled back")
		fbz, err := db.Get("foo:bar:baz")
		require.Error(t, err)
		require.Nil(t, fbz)
		t.Logf("\t\tInsert was rolled back")
	})
}

func createDbForFlushAllTest(t *testing.T, name string) string {
	t.Helper()

	fixture := fmt.Sprintf("./__fixtures__/%s.ldb", name)

	// only init new database
	db, closer, err := lemon.Open(fixture)
	if err != nil {
		require.NoError(t, err)
	}

	defer closeDB(t, closer, fixture)

	assert.FileExists(t, fixture)
	seedPersonStructs(t, db,10_000, true, 3)

	return fixture
}

func closeDB(t *testing.T, closer lemon.Closer, fixture string) {
	t.Helper()

	if err := closer(); err != nil {
		t.Errorf("ERROR: %+v", err)
	}
}

func closeDbAndRemoveFile(t *testing.T, closer lemon.Closer, fixture string) {
	t.Helper()

	if err := closer(); err != nil {
		t.Errorf("ERROR: %+v", err)
	}

	if err := os.Remove(fixture); err != nil {
		require.NoError(t, err)
	}
}

type writeTestSuite struct {
	suite.Suite
	fixture string
}

func (wts *writeTestSuite) SetupSuite() {
	wts.fixture = "./__fixtures__/write_db1.ldb"

	// only init new database
	_, closer, err := lemon.Open(wts.fixture)
	if err != nil {
		wts.Require().NoError(err)
	}

	defer func() {
		if err := closer(); err != nil {
			wts.T().Errorf("ERROR: %v", err)
		}
	}()

	assert.FileExists(wts.T(), wts.fixture)
}

func (wts *writeTestSuite) TearDownSuite() {
	if err := os.Remove(wts.fixture); err != nil {
		wts.Require().NoError(err)
	}
}

func (wts *writeTestSuite) Test_WriteAndRead_InTwoTransactions() {
	db, closer, err := lemon.Open(wts.fixture)
	if err != nil {
		wts.Require().NoError(err)
	}

	defer func() {
		if err := closer(); err != nil {
			wts.T().Errorf("ERROR: %v", err)
		}
	}()

	var result1 *lemon.Document
	var result2 *lemon.Document
	if txErr := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("product:8976", lemon.M{
			"foo": "bar",
			"baz": 8989764,
			"100": "username",
			"abc": true,
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
	}); txErr != nil {
		wts.Require().NoError(txErr)
	}

	wts.Assert().Equal("bar", result1.JSON().StringOrDefault("foo", ""))
	wts.Assert().Equal(8989764, result1.JSON().IntOrDefault("baz", 0))
	wts.Assert().Equal("username", result1.JSON().StringOrDefault("100", ""))
	wts.Assert().Equal("bar5674", result2.JSON().StringOrDefault("foo", ""))
	wts.Assert().Equal(123.879, result2.JSON().FloatOrDefault("baz12", 0))
	/*assert.Equal(t, nil, docs[1]["999"])*/

	var readResult1 *lemon.Document
	var readResult2 *lemon.Document
	// Confirm that those keys are accessible after previous transaction has committed
	// and results should be identical
	if txErr := db.View(context.Background(), func(tx *lemon.Tx) error {
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
	}); txErr != nil {
		wts.Require().NoError(txErr)
	}

	readJson1 := readResult1.RawString()
	wts.Assert().Equal(`{"100":"username","abc":true,"baz":8989764,"foo":"bar"}`, readJson1)
	wts.Assert().Equal(result1.RawString(), readJson1)
	wts.Assert().Equal(true, result1.JSON().BoolOrDefault("abc", false))
	wts.Assert().Equal("username", result1.JSON().StringOrDefault("100", ""))

	readJson2 := readResult2.RawString()
	wts.Assert().Equal(`{"999":null,"baz12":123.879,"foo":"bar5674"}`, readJson2)
	wts.Assert().Equal(result2.RawString(), readJson2)
}

func (wts *writeTestSuite) Test_ReplaceInsertedDocs() {
	db, closer, err := lemon.Open(wts.fixture)
	if err != nil {
		wts.Require().NoError(err)
	}

	defer func() {
		if err := closer(); err != nil {
			wts.T().Errorf("ERROR: %v", err)
		}
	}()

	if txErr := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("item:77", lemon.M{
			"foo": "bar",
			"baz": 8989764,
			"100": "username",
		}); err != nil {
			return err
		}

		if err := tx.Insert("item:1145", lemon.M{
			"foo":   "bar5674",
			"baz12": 123.879,
			"999":   nil,
		}); err != nil {
			return err
		}

		return nil
	}); txErr != nil {
		wts.Require().NoError(txErr)
	}

	wts.Assert().Equal(2, db.Count())

	if txErr := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.InsertOrReplace("item:77", lemon.M{
			"foo": "bar22",
			"baz": 1,
			"bar": nil,
		}); err != nil {
			return err
		}

		if err := tx.InsertOrReplace("item:1145", lemon.M{
			"foo1":   "0",
			"baz": 123.879,
			"999":   "bar",
		}, lemon.WithTags().Bool("valid", true)); err != nil {
			return err
		}


		return nil
	}); txErr != nil {
		wts.Require().NoError(txErr)
	}

	wts.Assert().Equal(2, db.Count())

	var readResult1 *lemon.Document
	var readResult2 *lemon.Document
	// Confirm that those keys are accessible after previous transaction has committed
	// and results should be identical
	if txErr := db.View(context.Background(), func(tx *lemon.Tx) error {
		doc1, err := tx.Get("item:77")
		if err != nil {
			return err
		}

		doc2, err := tx.Get("item:1145")
		if err != nil {
			return err
		}

		readResult1 = doc1
		readResult2 = doc2

		return nil
	}); txErr != nil {
		wts.Require().NoError(txErr)
	}

	readJson1 := readResult1.RawString()
	wts.Assert().Equal(`{"bar":null,"baz":1,"foo":"bar22"}`, readJson1)
	wts.Assert().Equal("bar22", readResult1.JSON().StringOrDefault("foo", ""))
	wts.Assert().Equal(1, readResult1.JSON().IntOrDefault("baz", 0))

	readJson2 := readResult2.RawString()
	wts.Assert().Equal(`{"999":"bar","baz":123.879,"foo1":"0"}`, readJson2)
	wts.Assert().Equal(123.879, readResult2.JSON().FloatOrDefault("baz", 0))
	wts.Assert().Equal("bar", readResult2.JSON().StringOrDefault("999", ""))

	//expectedContent := ``
	//AssertFileContents(wts.T(), wts.fixture, expectedContent)
}

func (wts *writeTestSuite) Test_RollbackReplaceOfInsertedDocs() {
	db, closer, err := lemon.Open(wts.fixture)
	if err != nil {
		wts.Require().NoError(err)
	}

	defer func() {
		if err := closer(); err != nil {
			wts.T().Errorf("ERROR: %v", err)
		}
	}()

	if txErr := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("item:567", lemon.M{
			"foo": "bar",
			"baz": 8989764,
			"100": "username",
		}); err != nil {
			return err
		}

		if err := tx.Insert("item:2233", lemon.M{
			"foo":   "bar5674",
			"baz12": 123.879,
			"999":   nil,
		}); err != nil {
			return err
		}

		return nil
	}); txErr != nil {
		wts.Require().NoError(txErr)
	}

	wts.Assert().Equal(4, db.Count())

	if txErr := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.InsertOrReplace("item:567", lemon.M{
			"foo": "bar22",
			"baz": 1,
			"bar": nil,
		}); err != nil {
			return err
		}

		if err := tx.InsertOrReplace("item:2233", lemon.M{
			"foo1":   "0",
			"baz": 123.879,
			"999":   "bar",
		}, lemon.WithTags().Bool("valid", true)); err != nil {
			return err
		}

		return errors.New("roll me back")
	}); txErr != nil {
		// we expect the rollback
		wts.Require().Error(txErr)
	}

	wts.Assert().Equal(4, db.Count())

	// Confirm that those keys are accessible after previous transaction has rolled back
	// and results should be as after the first insert
	readResult1, err := db.Get("item:567")
	wts.Require().NoError(err)
	readResult2, err := db.Get("item:2233")
	wts.Require().NoError(err)

	readJson1 := readResult1.RawString()
	wts.Assert().Equal(`{"100":"username","baz":8989764,"foo":"bar"}`, readJson1)
	wts.Assert().Equal("bar", readResult1.JSON().StringOrDefault("foo", ""))
	wts.Assert().Equal(8989764, readResult1.JSON().IntOrDefault("baz", 0))

	readJson2 := readResult2.RawString()
	wts.Assert().Equal(`{"999":null,"baz12":123.879,"foo":"bar5674"}`, readJson2)
	wts.Assert().Equal(123.879, readResult2.JSON().FloatOrDefault("baz12", 0))
	wts.Assert().Equal("", readResult2.JSON().StringOrDefault("999", ""))
}

type removeTestSuite struct {
	suite.Suite
	db      *lemon.DB
	closer  lemon.Closer
	fixture string
}

func (rts *removeTestSuite) SetupTest() {
	rts.fixture = "./__fixtures__/remove_db3.ldb"
	db, closer, err := lemon.Open(rts.fixture)
	rts.Require().NoError(err)

	rts.db = db
	rts.closer = closer

	if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Insert("item:8976", lemon.M{
			"foo": "bar",
			"baz": 8989764,
			"100": "username",
		}); err != nil {
			return err
		}

		if err := tx.Insert("item:1145", lemon.M{
			"foo":   "bar5674",
			"baz12": 123.879,
			"999":   nil,
		}); err != nil {
			return err
		}

		if err := tx.Insert("users", lemon.M{
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
	assertTwoFilesHaveEqualContents(rts.T(), rts.fixture, "./__fixtures__/correct/before_vacuum_remove_db3.ldb")

	if err := rts.closer(); err != nil {
		rts.T().Errorf("ERROR: %v", err)
	}

	assertTwoFilesHaveEqualContents(rts.T(), rts.fixture, "./__fixtures__/correct/after_vacuum_remove_db3.ldb")

	if err := os.Remove(rts.fixture); err != nil {
		rts.Require().NoError(err)
	}
}

func (rts *removeTestSuite) TestLemonDB_RemoveItemInTheMiddle() {
	if err := rts.db.Update(context.Background(), func(tx *lemon.Tx) error {
		if err := tx.Remove("item:1145"); err != nil {
			return err
		}

		return nil
	}); err != nil {
		rts.Require().NoError(err)
	}

	if err := rts.db.View(context.Background(), func(tx *lemon.Tx) error {
		doc, err := tx.Get("item:1145")
		rts.Require().Error(err)
		rts.Assert().Nil(doc)
		rts.Assert().True(errors.Is(err, lemon.ErrKeyDoesNotExist))

		return nil
	}); err != nil {
		rts.Require().NoError(err)
	}
}

type seedTags struct {
	hashes bool
}

func seedUserData(t *testing.T, db *lemon.DB, n int, tags seedTags) {
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

	if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		for i := 1; i < n+1; i++ {
			user := userData{
				Username: fmt.Sprintf("%s_%d", baseUser.Username, i),
				Phone:    fmt.Sprintf("%s%d", baseUser.Phone, i),
				Address:  fmt.Sprintf("%s %d", baseUser.Address, i),
				Balance:  float64(i),
				Logins:   i,
			}

			if tags.hashes {
				var metaSetter []lemon.MetaApplier
				if i % 4 == 0 {

					metaSetter = append(metaSetter, lemon.WithTags().Map(lemon.M{
						"foo": i % 2 == 0,
						"bar": i % 2 != 0,
						"baz": "abc123",
						"foobar": fmt.Sprintf("country_%d", i % 2),
					}))
				}

				if err := tx.Insert(fmt.Sprintf("user:%d", i), user, metaSetter...); err != nil {
					return err
				}
			} else {
				if err := tx.Insert(fmt.Sprintf("user:%d", i), user); err != nil {
					return err
				}
			}

		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func seedUserPets(t *testing.T, db *lemon.DB, firstUserId, lastUserId, pets int) {
	t.Helper()

	type petData struct {
		Name   string `json:"Name"`
		Age    int    `json:"age"`
		Weight float64 `json:"weight"`
		Kind   string `json:"kind"`
	}

	if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		for i := firstUserId; i <= lastUserId; i++ {
			for j := 0; j < pets; j++ {
				pet := petData{
					Name: fmt.Sprintf("pet_%d", j + 1),
					Age:    j + 1,
					Weight:  float64(j) + 1.5,
					Kind:   fmt.Sprintf("animal kind %d", j + 1),
				}

				if err := tx.Insert(fmt.Sprintf("user:%d:pet:%d", i, j + 1), pet); err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func seedProductData(t *testing.T, db *lemon.DB, n int) {
	t.Helper()

	type productData struct {
		Name     string  `json:"Name"`
		Buyers   []int   `json:"buyers"`
		ID       int     `json:"id"`
		OwnerID  int     `json:"ownerId"`
		Price    float64 `json:"price"`
		Quantity int     `json:"quantity"`
	}

	baseProduct := productData{
		Name: "product",
		ID:   0,
	}

	if err := db.Update(context.Background(), func(tx *lemon.Tx) error {
		for i := 0; i < n; i++ {
			user := productData{
				Name:     fmt.Sprintf("%s_%d", baseProduct.Name, i+1),
				Buyers:   []int{1 + i, 2 + i, 3 + i, 4 + i},
				ID:       i + 1,
				OwnerID:  n - i,
				Price:    float64(i + 1),
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

func loadFixtureContents(t *testing.T, path string) []byte {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("could not load file %s: %s", path, err.Error())
	}
	return b
}

func assertTwoFilesHaveEqualContents(t *testing.T, pathA, pathB string) {
	t.Helper()

	b1, err := ioutil.ReadFile(pathA)
	if err != nil {
		t.Errorf("file %s could not be opened\nbecause:  %v", pathA, err)
	}

	b2, err := ioutil.ReadFile(pathB)
	if err != nil {
		t.Errorf("file %s could not be opened\nbecause:  %v", pathB, err)
	}

	strA := strings.Trim(string(b1), " \n")
	strB := strings.Trim(string(b2), " \n")
	if strA != strB {
		t.Log("\n================================================================================")
		t.Errorf("file %s contents\n%s\n\ndoes not match expected file %s contents \n\n%s", pathA, strA, pathB, strB)
		t.Log("\n================================================================================")
	} else {
		t.Log("contents match")
	}
}

func assertFileContentsEquals(t *testing.T, path string, expectedContents []byte) {
	t.Helper()

	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("file %s could not be opened\nbecause:  %v", path, err)
	}

	expectedContentsString := strings.Trim(string(expectedContents), " \n")
	str := strings.Trim(string(b), " \n")
	if str != expectedContentsString {
		t.Errorf("\nATTENTION! Contents mismatch!!!!")
		t.Errorf("\nExpected contents length is %d.\nActual length of file %s contents is %d", len(expectedContentsString), path, len(str))
		t.Errorf("\nfile %s contents\n%s\n\ndoes not match expected value\n\n%s", path, string(b), expectedContents)
	} else {
		t.Log("contents match")
	}
}