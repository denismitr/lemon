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