# zdb

A zero-allocation, mmap-backed time-series store for OHLC candles and ticks.
Append one record per symbol per tick, then scan one symbol's window straight
out of the mapping. Standard library only. Go 1.24+.

```sql
SELECT datetime, close FROM "binance/spot/BTCUSDT"
WHERE close >= 103 ORDER BY datetime ASC LIMIT 10
```

| query on 200k bars | time | allocs |
|---|---|---|
| ASC, filtered, LIMIT 10 | ~3.8 µs | 8 |
| DESC, filtered, LIMIT 10 | ~0.8 µs | 9 |
| cold single-shot (fresh process, 5M bars) | ~776 µs ASC / ~19 µs DESC | — |

Details and the rest of the suite: [`BENCHMARK.md`](BENCHMARK.md).

## 60 seconds

```go
s, err := zdb.Open[record.Bar](zdb.Options{Dir: "data"})
if err != nil { return err }
defer s.Close()

// one process, writer + reader + query runner on the same directory
s.Writer.Append("binance/spot/BTCUSDT", record.Bar{
    Datetime: time.Now().UnixMilli(),
    Open: 100.50, High: 101.25, Low: 100.00, Close: 101.00, Volume: 12345,
})

for bar, err := range s.Query().Run(`SELECT datetime, close
    FROM "binance/spot/BTCUSDT" WHERE close > 100.5 ORDER BY datetime DESC LIMIT 500`) {
    if err != nil { return err }
    fmt.Println(bar.Datetime, bar.Close)
}
```

More worked examples run as tests in `example_test.go`.

## Why it is fast

- **Zero-copy on the hot path.** Records live in a fixed-width format inside
  one shared `mmap`. `reader.Cursor` allocates nothing; `Scan`/`All`/`RangeInto`
  cost exactly one allocation per query, enforced by tests.
- **A cursor that walks backwards.** `ORDER BY ... DESC LIMIT n` stops at the
  n-th match from the newest bar; it never scans the window.
- **Pruning, not decoding.** Per-column min/max per segment, plus a zone map
  (one min/max row per column per 256-record block) the cursor checks before
  decoding, so a sparse predicate costs a block skip, not 256 decodes.
- **Bounded open cost.** Seal-time CRC64 covers every published byte; open-time
  verification is capped at 256 KiB, larger segments audit on demand via
  `Segment.Verify()`.
- **Durability you dial, not guess.** `Append` never calls `Sync`;
  `writer.Policy` decides flush cadence from RAM budget and measured append
  rate. No WAL — the page cache is the write buffer.
- **One writer per shard, one record type per store.** The constraints are what
  make the format simple enough to hit the numbers above.

## Be honest with yourself first

This package is deliberately narrow:

- one symbol per query, no `JOIN`, no `GROUP BY`, no second key
- reads are 12x slower than SQLite's indexed B-tree at point lookup — close
  enough that a chart renderer wins by streaming without copying
- no compression: 13 MiB on disk where ClickHouse fits 5 MiB
- pre-1.0: the on-disk format can change between builds; old stores are
  disposable

If your workload is ad-hoc relational queries, use SQLite. If it is "append
prints and tick bars all day, render windows of five thousand candles
immediately," that is the case this engine is measured for.

## Layout

```
record/    fixed-width Bar/Tick types, tagged-field schema
internal/  format (header, segment, index) + platform (mmap per OS)
writer/    append, batch, rollover, seal, SyncIfDue
reader/    mmap reader, Cursor, zone-map skip, retention window
zql/       SQL-ish query language over one key
examples/  quickstart and tours
```

## Develop

```bash
go test -race ./...
go vet ./... && gofmt -l . && golangci-lint run ./...
go test -run XXX -bench . -benchmem ./ ./zql/
```

`AGENTS.md` is the constitution: the zero-allocation rules, the benchmark
gate, and the measured reasoning behind every rejection. Read it before
touching a hot path.

## License

No license yet. Nothing is open source until there is one — add it before
publishing.
