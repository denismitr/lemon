package jsonstorage

//func TestJSONStorage_Read(t *testing.T) {
//	f, err := os.Open("./__fixtures__/db1.json")
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	defer f.Close()
//
//	s := JSONStorage{f: f}
//
//	t.Run("it can be read from", func(t *testing.T) {
//		var dst data.Model
//		if err := s.read(&dst); err != nil {
//			t.Fatal(err)
//		}
//
//		assert.Equal(t, data.PrimaryKeys{"key1", "key2"}, dst.PKs)
//		assert.Equal(t, []data.Value{"{\"foo\":\"bar\",\"bar\":123}", "{\"123\":\"foobar\",\"baz\":\"foo\"}"}, dst.Values)
//		assert.Len(t, dst.Values, 2)
//	})
//}
//
//func TestJSONStorage_Write(t *testing.T) {
//	t.Run("it can write anything marshalable to a file", func(t *testing.T) {
//		f, err := os.Create("./__fixtures__/db2.json")
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		defer func() {
//			if err := f.Close(); err != nil {
//				t.Fatal(err)
//			}
//
//			b, err := ioutil.ReadFile("./__fixtures__/db2.json")
//			if err != nil {
//				t.Error(err)
//			}
//
//			assert.Equal(t, `{"pks":["keys1","keys2","keys3"],"documents":["{\"foo\":\"bar\"}","{\"baz\":123}","{\"123\":345.45}"]}`, string(b))
//
//			os.Remove("./__fixtures__/db2.json")
//		}()
//
//		s := JSONStorage{f: f}
//
//		db := data.Model{
//			PKs:      data.PrimaryKeys{"keys1", "keys2", "keys3"},
//			Values: []data.Value{
//				`{"foo":"bar"}`,
//				`{"baz":123}`,
//				`{"123":345.45}`,
//			},
//		}
//
//		if err := s.write(db); err != nil {
//			t.Fatal(err)
//		}
//
//		assert.FileExists(t, "./__fixtures__/db2.json")
//	})
//}
