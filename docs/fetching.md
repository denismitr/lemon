# Fetching documents by keys

## Get single JSON document by key
```go
doc, err := db.Get("product:88")
if err != nil {
	panic(err)
}

doc.Key() // product:88
doc.RawString() // e.g. {"abc":"foobar-88","baz":88,"foo":1234.567,"bar":true}

// if value is valid Json you can use some helper methods to retrieve parts of that Json
doc.JSON().StringOrDefault("abc", "") // foobar-88
doc.JSON().IntOrDefault("baz", 0) // 88
doc.JSON().FloatOrDefault("foo", 0) // 1234.567
doc.JSON().BoolOrDefault("bar", false) // true
```

### MGet - fetches multiple documents by variadic keys
MGet returns the result as a map `map[string]*lemon.Document` where key
is a specified key and value is a pointer to a document `*lemon.Document`. 
Even when some keys do not exist, error is not returned, these keys 
will be absent from the resulting map.

```go
docs, err := db.MGet("product:88", "product:100", "non:existing:key")
// err == nil even though one key is not found

len(docs) // 2 - since 2 keys were found
docs["product:88"] // *lemon.Document with product:88 key
docs["product:100"] // *lemon.Document with product:100 key
docs["non:existing:key"] // nil
```