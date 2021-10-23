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