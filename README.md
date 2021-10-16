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

if you pass `lemon.InMemory` as the first argument to `lemon.Open` database will
work purely as in memory database and will not persist anything to disk.

[Fetching documents by keys and extracting values](/docs/fetching.md)
[Iterating, searching and filtering](/docs/searching.md)

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
alternatively, like with regular inserts, you can do a single operation, without manually opening a transaction, 
instead  a single operation will be wrapped in a transaction automatically and background context will be used. 
```go
err := db.InsertOrReplace("item:1145", lemon.M{
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

### Tags
Tags are a crucial piece of functionality in `LemonDB` they allow to take any data of any type and
assign one or many tags to it. Those tags will act as secondary indexes for your data. Allowing you to make 
queries by tags, from simple equality to less than and greater than queries. Sorting by tags may be supported in the 
future, but right now it is not. 

You can assign a single or multiple tags on an entry on `Insert`/`InsertOrReplace` operation or at any time 
by using `Tag` method on `lemon.Tx` object. Also `Untag` method is available to remove one or more tags at once. 

Example with tagging on insertion
```go
err := db.Update(ctx, func(tx *lemon.Tx) error {
    tagsForProject := lemon.WithTags().Map(lemon.M{
        "tag1": true,
        "tag2": 567.3,
        "tag3": "foobar",
        "tag4": 45,
    })

    if err := tx.Insert("project:3456", lemon.M{
        "foo": "bar",
        "baz": 123,
    }, tagsForProject); err != nil {
        return err
    }

    tagsForUser := lemon.WithTags().Bool("someTag", true).Float("someFloat", 887.2)

    if err := tx.InsertOrReplace(
        "user:9876",
        `<someXml><userId>123</userId><someXml>`,
        tagsForUser,
    ); err != nil {
        return err
    }

    return nil
})
```

Example with tagging and untagging
```go
err := db.Update(ctx, func(tx *lemon.Tx) error {
    if err := tx.Tag("user:9876", lemon.M{
        "newTag1": 5667,
        "newTag2": false,
        "newTag3": "foo-bar-baz",
    }); err != nil {
        return err
    }
    
    if err := tx.Untag("project:3456", "tag1", "tag3"); err != nil {
        return err
    }
    
    return nil
})

if err != nil {
    panic(err)
}
```

### Contributing
Make sure to use ac=nc compare benchmarks with `go install golang.org/x/tools/cmd/benchcmp@latest`