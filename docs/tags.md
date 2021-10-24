# Tags in LemonDB

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

    tagsForUser := lemon.WithTags().Bool("someBoolTag", true).Float("someFloatTag", 887.2)

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

Example with tagging one project and untagging another in one transaction
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

## Implicit Tags or Meta Tags
Implicit tags are set by the LemonDB itself, sometimes with a hint from the user.

#### Timestamps
When you insert ot replace a document you can add timestamps to it by using `WithTimestamps()` helpers.
LemonDB will add `createdAt` and `ùpdatedAt` hidden meta tags with timestamps in milliseconds.

Example of inserting a document with timestamps:
```go
db.Insert("key:001", lemon.M{"key": 1}, lemon.WithTimestamps())
````
Example of insert or replace with timestamps and user defined tags. 
```go
db.InsertOrReplace(
    "key:002",
    lemon.M{"key": 20002},
    lemon.WithTimestamps(),
    lemon.M{"strTag": "foo"},
)
```
The last two arguments are **meta appliers** a variadic arguments of type `...MetaApplier`.

When using `InsertOrReplace` and key already exists and already has timestamps, `createdAt`is obviously
preserved.

#### Content Type of documents
Content type is set automatically by LemonDB when you insert or replace a document. The content type will 
exist as a hidden meta tag. The following ContentTypes exists at the moment:

* lemon.JSON
* lemon.String
* lemon.Bytes
* lemon.Integer

You can use helpers described below to find out the content type of retrieved document.

#### Reading hidden meta tags from documents
When you retrieve a document you cannot directly access any hidden meta tags, but there are helper functions.

Example of retrieving timestamps:
```go
doc, err := db.Get("key:123")
if err != nil {
	panic(err)
}

doc.HasTimestamps() // true if document was created or updated with timestamps
doc.CreatedAt() // returns time.Time
doc.UpdatedAt() // returns time.Time
```

Example of retrieving content type:
````go
doc.ContentType() // lemon.Integer | lemon.JSON | lemon.String | lemon.Bytes
doc.IsJSON() // true or false
doc.IsInteger() // true or false
doc.IsString() // true or false
doc.IsBytes() // true or false
````

Then you can use document converters like `JSON()`, `RawString()` to get converted values. More in the
[LemonDB documents](/docs/documents.md) section of documentation.

