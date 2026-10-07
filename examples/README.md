# Examples

Runnable tours of the library. Each directory is a `package main`, so it can be
read top to bottom and run as one program.

| Example | Shows |
|---|---|
| [quickstart](quickstart) | `zdb.Open`: write a batch, query it with ZQL text, close |
| [reads](reads) | `writer` and `reader` apart: `Cursor`, `Bounds`, `Count`, `Latest`, and when `Refresh` is needed |
| [query](query) | one lookup as ZQL text and through the Go builder, plus `String`, `Collect` and `First` |
| [tick](tick) | `record.Tick` in a store, and why `Store.Query` refuses it |

Run one from the module root:

```bash
go run ./examples/quickstart
```

Each example writes to a temporary directory and removes it on the way out, so
running them leaves nothing behind. The other entry points — the read shapes, the
query language, the durability policy — are documented with checked output blocks
in the package docs: `go doc github.com/aldok10/zdb`, `go doc
github.com/aldok10/zdb/reader`, and `go doc github.com/aldok10/zdb/zql`.
