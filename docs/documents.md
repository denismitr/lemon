# LemonDB documents
### WIP

## Get JSON value from document
```go
doc, err := db.Get("product:88")
if err != nil {
	panic(err)
}

doc.RawString() // e.g. {"abc":"foobar-88","baz":88,"foo":1234.567,"bar":true}

doc.IsJSON() // true

// if value is valid Json you can use some helper methods to retrieve parts of that Json
doc.JSON().StringOrDefault("abc", "") // foobar-88
doc.JSON().IntOrDefault("baz", 0) // 88
doc.JSON().FloatOrDefault("foo", 0) // 1234.567
doc.JSON().BoolOrDefault("bar", false) // true
```