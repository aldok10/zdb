# Benchmarks

Machinery: Apple M1 Pro, darwin/arm64, go1.27.1. Every statement below is a
`go test -run XXX -bench . -benchmem` measurement; numbers are medians of the
benchdefault sample count, in one run of the suite. Reproduce with
`go test -run XXX -bench . -benchmem ./ ./zql/`.

## How the target query is measured

The benchmark that matters:

```sql
SELECT datetime, close FROM "binance/spot/BTCUSDT"
WHERE close >= 103 ORDER BY datetime ASC  LIMIT 10
SELECT datetime, close FROM "binance/spot/BTCUSDT"
WHERE close >= 103 ORDER BY datetime DESC LIMIT 10
```

- Store: one key, 200000 bars, 64 KiB segments, warm page cache.
- `BenchmarkQueryFilteredLimitAsc` / `...Desc`: the same query through
  `zql.DB.Query`, which streams ascending either way; the DESC path resolves
  the answer by walking the cursor backwards and then replays it in order.

| query | ns/op | allocs | B/op |
|---|---|---|---|
| ASC LIMIT 10, `close >= 103` | 3 655 | 8 | 472 |
| DESC LIMIT 10, `close >= 103` | 792 | 9 | 1112 |

In-process repeated runs on a 5 000 000-bar store settle to the same shape:
~5 µs ascending, ~5 µs descending, whatever the bar count, because both paths
stop at the limit-th match instead of scanning the window. A single cold query
after `writer.Close` in a fresh process costs ~800 µs ascending (segment
mapping + open checks) and ~20 µs descending, both dominated by address-space
setup, not by decoding.

One cold-shot run of `examples/quickstart` (5M bars) on that binary reports:

```
Finish Query ASC: 775.792µs
Finish Query DESC: 18.916µs
```

## Cursor / reader read cost

```
BenchmarkCursorWindow-8          123081      9359 ns/op        0 B/op    0 allocs/op
BenchmarkScanWindow-8            123475     10006 ns/op       64 B/op    1 allocs/op
BenchmarkRangeIntoWindow-8       126152      9494 ns/op        0 B/op    0 allocs/op
BenchmarkAllWindow-8              97606     11770 ns/op       64 B/op    1 allocs/op
BenchmarkRangeWindow-8            97776     12388 ns/op    32768 B/op    1 allocs/op
BenchmarkAllFullSeries-8           1192   1028570 ns/op       65 B/op    1 allocs/op
BenchmarkRangeFullSeries-8         1026   1249781 ns/op  3203077 B/op    1 allocs/op
```

- The window benchmarks read 501 bars; the full-series benchmarks read 50000.
- `Cursor` allocates nothing, and neither does a raw `Scan` into
  caller-owned slices. `Scan` reports one 64-byte allocation per query; that
  is the record slot the decoder writes into, forced on the heap by the
  generic `Record[T]` method dispatch, which no local destination can avoid.
  `All` carries one allocation per query for its iterator closure, and
  `RangeInto` with a caller-owned buffer allocates nothing at all.
  `Range` materializes the result slice; its cost scales with the result size.
  It sizes the slice with one exact `Window` pass up front, so growing by
  doubling no longer shows up as ten allocations, and it decodes straight
  into the slice storage — exactly one slice allocation per query.
- Throughput over mapping reads: `BenchmarkWindowManualReadAt-8` moves
  8.9 GB/s with zero allocation, the stdlib baseline the cursor keeps up with.

## Ingest

```
BenchmarkAppendThroughput-8            16384      63704 ns/op    0 B/op    0 allocs/op
BenchmarkAppendBatchSizes/1-8        3705022        307 ns/op    0 B/op    0 allocs/op
BenchmarkAppendBatchSizes/10-8        507728       3105 ns/op    0 B/op    0 allocs/op
BenchmarkAppendBatchSizes/100-8        50748      31401 ns/op    0 B/op    0 allocs/op
BenchmarkAppendBatchSizes/1000-8        5545     267922 ns/op    1 B/op    0 allocs/op
BenchmarkAppendBatchSizes/10000-8        502    2777743 ns/op   11 B/op    0 allocs/op
BenchmarkAppendWithPolicy-8            20058      70148 ns/op    0 B/op    0 allocs/op
BenchmarkAppendSyncPerTick-8             187    5432742 ns/op   27 B/op    0 allocs/op
BenchmarkAppendSyncBatched-8           20204      72738 ns/op    0 B/op    0 allocs/op
```

`Append` never calls `Sync`; it is a fixed-width encode plus a seqlock count
store, ~290 ns for one bar through the fixed-width encode. A flush per tick
costs ~5.2 ms because an `msync` is a file-system barrier, while batching the
flush into one call after the batch costs ~67 µs: the lever is flush frequency,
not bytes written. `BenchmarkAppendWithPolicy` is the same throughput as a bare
append, two atomic adds of accounting per batch.

## Ticks

```
BenchmarkTickCursorWindow-8    154300      8042 ns/op   3488 MB/s    0 B/op   0 allocs/op
BenchmarkTickScanWindow-8      141072      8327 ns/op   3369 MB/s   64 B/op   1 allocs/op
BenchmarkTickAllWindow-8       138582      8693 ns/op   3227 MB/s   64 B/op   1 allocs/op
BenchmarkTickFullSeries-8         355   3629347 ns/op   3085 MB/s   69 B/op   1 allocs/op
BenchmarkTickAppendThroughput-8 39406     38010 ns/op   1.47 MB/s    0 B/op   0 allocs/op
```

## Pruning structures, in numbers

- Whole-segment pruning via per-column min/max skips every segment whose stored
  range lies outside the predicate; when a query touches no qualifying record it
  costs one index lookup, zero record reads.
- A zone-map row per column per 256-record block is consulted before the block is
  decoded; the cost of the target query moves from "records in the window" to
  "records between the window edge and the limit-th match", which is constant
  for a selective or raising predicate.
- Bounded checksum verification keeps segment open at ~O(256 KiB): a segment
  larger than the bound is validated by its seal-time CRC, and `Segment.Verify`
  re-checks on demand. The bound is in `internal/format/segment.go`.

## What these numbers do and do not promise

These are darwin/arm64 warm-cache numbers. A plain B-tree beats `Cursor` on
point lookups and short ranges; there is no compression (records are stored
fixed-width and prices as 64-bit fixed point), no join, no aggregate, and a
crash window exists between page-cache writeback and header sync. The engine's
case is: zero-allocation streaming reads at window scale, constant-allocation
target queries that terminate after the limit, append throughput that never
pays a sync, a shared mapping that another process can read without
coordination, and no dependencies outside the standard library.
