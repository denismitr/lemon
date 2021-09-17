# LemonDB WIP
A document oriented database which can store strings, BLOBs and JSON documents (as tagged structs or bytes or JSON strings) 
and is meant for local (non distributed) usage as it stores everything in one file or can work fully in memory. Ideally
suits cli or desktop applications, non distributed pipelines or testing frameworks.

All data is stored on disk in [RESP](https://redis.io/topics/protocol) **like** encoding.

LemonDB supports transactions. Reads are concurrent and writes are executed under exclusive lock.

## Primary keys
Keys can contain any alpha-numeric characters, dashes and underscored. `:` can be used as key segments separators
to be used in pattern matching - like `user:*`. 
Also, it helps correct sorting when some segments of a key are integer numbers.
Like `user:123:products`

## Tags (secondary indexes)
Any document can be tagged by an arbitrary number of tags. Queries can use these tags for filtering and sorting of
results.

Tags in LemonDB are basically secondary indexes of 4 basic types `float64`, `int`, `string`, `boolean`.

## Usage
Create/open a lemonDB database file
```go
db, closer, err := lemon.Open(mts.fixture)
if err != nil {
	// handle
}

defer func() {
    if err := closer(); err != nil {
        log.Println(err)
    }
}()
```

### Insert several entries in a transaction
tags can be provided as a third argument and will service as secondary index
```go
err := db.Update(context.Background(), func(tx *lemon.Tx) error {
    if err := tx.Insert("item:8976", lemon.M{
        "foo": "bar",
        "baz": 8989764,
        "someList": []int{100, 200, 345},
    }); err != nil {
        return err
    }

    if err := tx.Insert("product:1145", lemon.M{
        "foo":   "bar5674",
        "baz12": 123.879,
        "anotherMap": lemon.M{"abc": 123},
    }, lemon.WithTags().Bool("valid", true).Str("city", "Budapest")); err != nil {
        return err	
    }
    
    return nil
})
```
`lemon.M` is actually a `type M map[string]interface{}`

alternatively you can do a single operation, without manually opening a transaction, instead
a single operation will be wrapped in a transaction automatically, the only difference is that you have to
pass a context as the first argument.

```go
err := db.Insert(context.Background(), "item:1145", lemon.M{
        "foo1":   "0",
        "baz": 123.879,
        "999":   "bar",
    }, lemon.WithTags().Bool("valid", true).Str("city", "Budapest"))
```

### InsertOrReplace one or several entries in a transaction
in case an entry already exists it is overwritten, including all of it tags (secondary indexes), otherwise
a new entry is created with tags, if provided
```go
err := db.Update(context.Background(), func(tx *lemon.Tx) error {
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
		}, lemon.WithTags().Bool("valid", true).Str("city", "Budapest")); err != nil {
			return err
		}
		
		return nil
	})
```
alternatively, like with regular inserts, you can do a single operation, without manually opening a transaction, instead
a single operation will be wrapped in a transaction automatically, the only difference is that you have to
pass a context as the first argument.
```go
err := db.InsertOrReplace(context.Background(), "item:1145", lemon.M{
        "foo1":   "0",
        "baz": 123.879,
        "999":   "bar",
    }, lemon.WithTags().Bool("valid", true).Str("city", "Budapest"))
```

## Reading from the database
#### Get
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
#### MGet
MGet fetches specified keys, and returns the result as a map `map[string]*lemon.Document` where key
is a specified key and value is a `*lemon.Document`. Even some keys do not exist, error is not returned
```go
docs, err := db.MGet("product:88", "product:100", "non:existing:key")
// err == nil even though one key is not found

len(docs) // 2 - since 2 keys were found
docs["product:88"] // *lemon.Document with product:88 key
docs["product:100"] // *lemon.Document with product:100 key
docs["non:existing:key"] // nil
```

### Contributing
Make sure to use ac=nc compare benchmarks with `go get golang.org/x/tools/cmd/benchcmp`