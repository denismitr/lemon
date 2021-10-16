# Inserting and updating documents in LemonDB

## Insert
tags can be provided as a third argument and will act as secondary index

### Example of inserting several JSON based documents in a single transaction
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

`lemon.M` is essentially a helper type to make it easier to work with JSON based documents
```

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

## Update
Updates in LemonDB are actually replacements of documents 

### Example of inserting or replacing 
when a document already exists it is overwritten, including all of it tags (secondary indexes), otherwise
a new document is created with tags, if provided

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