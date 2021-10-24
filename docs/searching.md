# Iterating, searching, filtering and counting documents in LemonDB

## Scan iterates over records with or without filter
### Example of scanning all entries
```go
var docs []*lemon.Document
if err := db.ScanContext(ctx, nil, func(d *lemon.Document) bool {
    docs = append(docs, d)
    return true
}); err != nil {
    log.Fatal(err)
}
```

### Example of iteration documents filtered by key prefix
```go
var docs []*lemon.Document
if err := db.View(ctx, func(tx *lemon.Tx) error {
    opts := lemon.Q().KeyOrder(lemon.AscOrder).Prefix("user")
    if scanErr := tx.Scan(opts, func(d *lemon.Document) bool {
        // you can do various checks on contents here
        // not only parts of keys but also values or parts of them can be checked
        // it is not very fast, but if you need to filter by something
        // that cannot be indexed - that is the way to go
        if strings.HasSuffix(d.Key(), "pet") {
            docs = append(docs, d)
        }

        return true
    }); scanErr != nil {
        return scanErr
    }

    return nil
}); err != nil {
    log.Fatal(err)
}
```

## Find searches documents by query filter

### Example of finding documents filtered by primary key range 
```go
opts := lemon.Q().KeyOrder(lemon.DescOrder).KeyRange("user:100", "user:109")
docs, err := db.Find(ctx, opts);
if err != nil {
    log.Fatal(err)
}
```

## Count documents
LemonDB offers two methods to count documents - one always count the total and another allows to pass
query options in order to count documents that match certain criteria.

#### Example of the total count
```go
totalCount := db.Count()
```

#### Example of counting documents within a given range
```go
q := lemon.Q().KeyRange("product:88", "product:100")
rangeCount, err := db.CountByQueryContext(context.Background(), q)
if err != nil {
	panic(err)
}
```

alternatively there is `db.CountByQuery(q)`that does not require context.
