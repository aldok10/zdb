# AGENTS.md — ZDB

ZDB is a MaxMind-DB-inspired OHLC chart database: mmap, sharding per symbol, a
format tuned for write-and-read-heavy candle data. Format, writer and reader are
done. Read `internal/format/zdb.go` and `internal/format/header.go` before touching anything.

These are rules, not suggestions. When two conflict, pick the smaller blast
radius and write the reasoning in the commit.

## 1. The goal that dominates

Two hot paths shape everything: **append** one bar per symbol per tick, and
**range scan** one symbol over one time window. Anything that can be moved off
either path, move it off. Slow but simple beats fast and dragging the GC along.

## 2. Zero-allocation

### 2.1 Read path

The hot path allocates nothing except a result slice genuinely handed to the
caller, and that must stay fixed in size. Forbidden: `make`/`new`/`append` to any
other slice; `string(bytes)` over a mapping; rebuilding `[]string` for `Keys`/`List`
per call; `fmt.Sprintf` or `errors.New` with formatting; boxing into `interface{}`
when the concrete type is known; escaping closures.

Correct: read out of the mapping (`decodeBar(mm[off:]).float()`); hand results
through a caller-owned slice — `Scan` and `Cursor` allocate nothing and are the
path a chart renders through, `Range` is only a convenience wrapper; read index
keys via `unsafe.String` under the lifetime contract (a mapping is unmapped only in
`Close`); when the caller has a buffer, take the variant that accepts it.

### 2.2 Write path

`Append`/`AppendBatch` do not allocate in steady state: the batch slice is the
caller's, and the header is written through a pointer already inside the mapping
(no per-append `SegmentHeader`). `sync.Pool` only if the buffer is returned on
**every** error path — one forgotten path is hidden allocation.

### 2.3 The line that does not move

> **It is not permitted, in any form, to write code that makes the Go garbage
> collector work hard.**

GC here is an architectural failure: concurrent mark stalls the goroutine running
a query that must finish in milliseconds.

- no `interface{}` when the concrete type is known; no boxing
- no `defer` in a loop that runs millions of iterations
- no goroutines that allocate in steady state
- no `[]byte` → `string` on the hot path, including for formatting
- no `map[string]*T` on the hot path; use a sorted slice or an array
- no reflection outside `record.Schema` (§2.4)

`go build -gcflags=-m` showing a stack variable moved to the heap is a bug — fix it
where it is.

### 2.4 Reflection: schema construction only

`record.Schema[T]` is the one place reflection is allowed, and it is never on the
path: `buildSchema` walks `reflect.Type` once into
`{Offset, Kind, Width, Time, Price, Primary, Indexed}`, then a filter resolves a
column name at plan time and touches records with `unsafe.Add`. `SchemaOf` resolves
generated (`ZDBSchema()`) → cached (`sync.Map` by `reflect.Type`, caching successes
**and failures**) → reflective walk, at most once per type per process, allocating
once on both paths.

Measured (generated 13 ns, cached 21 ns, reflective walk 1226 ns with 13 allocs):
generation buys startup and binary size, not query latency, so do not add a
per-record schema lookup to save the walk. `ponytail:` if `SchemaOf` ever reaches a
per-record path, switch the generated method to a pointer receiver and assert on
`(*T)(nil)`.

**`Offset` is never computed outside `buildSchema`** (it comes from reflect on that
exact struct type, so alignment is guaranteed; nothing accepts a caller-supplied
offset), and **only tagged fields are columns** (an untagged `uint8` flag or
`string` beside them is fine). `record.SchemaBuilds` counts the walks; the tests
that read it are the enforcement, not a comment.

Tag option words: `time`, `unit=`, `price`, `primary`, `index`, `alias=`. No
synonyms for the bare four — they collide with column names (a column named `ts`
would be unspellable). A column named exactly `time`, `price`, `primary` or `index`
takes its name from the Go field; use `alias=` otherwise. `price` means the column's
float is fixed-point at `PriceScale`: the query layer scales the condition by `1e8`
and the record side the same way (`zql.stored()`), so both meet in integer space and
no price becomes a double on the scan path. `record.ScalePrice`/`ScaleSignedPrice`
are exported for it, the second for a signed price like `Tick.Bid`. Tags live on
exported `Bar`/`Tick` (`float64`), never on the unexported fixed-point `bar`/`tick`.

### 2.4.1 Generating a schema instead of reflecting it

`cmd/gen-schema-record` writes a type's schema from tags alone (no reflect, no
`go/types`), driven by `go:generate`; `record`'s own generated files are checked in.

```bash
go generate ./record/   # bar_schema_gen.go, tick_schema_gen.go
```

- Offsets are `unsafe.Offsetof(Trade{}.Price)`, so a field insertion breaks the
  build instead of silently preserving a literal.
- **Every generated file ends with one type guard per column**
  (`_ [12]byte = Trade{}.Venue`). Without it, retyping a field keeps the offset,
  still validates the tags, compiles, and reads the wrong bytes — the checks pass and
  the data is wrong. Held by `TestGeneratedFileEmitsOneGuardPerColumn` and by
  compiling against a modified struct (`TestGeneratedFileCatchesARetypedField`,
  `...CatchesAReshapedByteColumn`).
- Output calls `record.MustSchema`, never a literal (derived time index; a two-value
  return cannot be a package-level `var`), so it panics only for a hand-edited file.
  `ast.Expr` gives the generator a **second kind table**
  (`TestGeneratorKindTableMatchesRecordKindOf`,
  `TestGeneratorNamesEveryKindRecordDefines`), flags default in `run` not `config`,
  `-qualify` defaults to `record.`, and `writeFile` refuses to overwrite a file
  without the generated header.
- Output is idempotent and asserted: `TestGeneratedSchemasMatchReflection` compares
  checked-in output against a fresh walk, and `TestBuiltinTypesTakeTheGeneratedPath`
  requires `SchemaOf[Bar]` to return `barSchema` by pointer with zero reflections.

### 2.5 What a column may be

`kindOf` accepts every integer width, both float widths, `bool`, `[N]byte`. It
refuses `string`, `[]byte`, `time.Time`, pointers, slices, maps, funcs and
interfaces, and each refusal names the way to store what it refused. Variable width
is refused by the format: record `i` lives at `i * width`, which is what lets a
cursor walk by arithmetic, the seqlock publish at a fixed position, and the binary
search index by offset without decoding ahead.

- `[N]byte` for a venue code, symbol or order id; `Column.CompareBytes` orders it
  lexicographically without materialising a slice.
- The shard key for the string dimension — the shard is already
  `binance/spot/BTCUSDT`, so a per-record `Symbol` costs 8 bytes per append to
  duplicate it.
- Dictionary encoding for a few dozen distinct values is right but unbuilt: the
  dictionary belongs to the store and `Record[T]` has no parameter for one.
  `ponytail:` it needs a per-store type or a lookup through the encode path.
- `time.Time` is refused separately: 24 bytes holding a `*time.Location` into a
  timezone database that may not be loaded later. Use `int64` + `time` + `unit=`.

## 3. Zero-copy, one exported type per record

`Bar` is the API (doubles); `bar` is the file (unsigned, `uint32`, fixed-point) and
never escapes. Width comes from the type: `SegmentHeader.recSize` carries it, so a
reader opened as the wrong type is refused instead of decoding one layout as
another, and `SegmentHeaderAt` validates it (`0` or `> MaxRecordSize` is
`ErrCorrupt`).

`Record[T].DecodeInto(src []byte, dst *T)` keeps its **value receiver**: a
value-returning decode moves the result to the heap per record, `DecodeFrom` over
`*Bar` makes `Range`/`All`/`Latest` hand back one reused pointer (a collected slice
aliases the last record), and a second type parameter or a func value is 16x slower.
`Cursor` therefore owns its destination as a field and copies out (0 allocs).

`record.SizeOf[T]()` exists because `T{}.RecordSize()` does not compile and
`(*new(T)).RecordSize()` faults on arm64 for a value-receiver method.
`record.Le64`/`PutLe64`/`Le32`/`PutLe32` are a seam so little-endianness is decided in
one file, not restated per record type — a big-endian write produces a file only its
own reader parses, with wrong values and no error
(`TestEndiannessSeamMatchesTheStandardLibrary` compares byte for byte). There is
deliberately no `Reader.AllKeys` (a sorted result must be materialised, buckets are
hash-scattered) and no streaming `Keys` (built on `AllEntries`).

**Zero-copy:** bytes into storage are the bytes out, never copied in between. Bars
are read straight from the mapping; readers use `Scan`/`Cursor`/`All`; when a value
must leave the mapping, copy it once at the documented boundary and never again;
`string(bytes)` is a copy — index keys use `unsafe.String` under the lifetime
contract, or are narrowed to a hash.

## 4. Lean on the operating system

### 4.1 mmap is the default

Read-only mappings for reads; no `read`/`write` on the hot path. A mapping is
created once and never remapped — segments are pre-allocated to full capacity, so
the file size never changes, which is why pre-allocation is mandatory. Use
`MAP_SHARED`: private writes are invisible to other processes. The page cache does
read-ahead and write-back; do not add your own buffer. `msync`/`fsync` only from
`Sync()`, never per append.

### 4.2 A missing primitive is never a silent downgrade

1. Find the native equivalent. Windows has no `syscall.Mmap`, so
   `CreateFileMappingW` + `MapViewOfFile` go through `syscall.NewLazyDLL`, adding no
   dependency; netbsd, solaris, illumos and aix have no `SYS_MSYNC`, so
   `flushMapping` returns `ErrUnsupported` honestly and `Sync` still runs `fsync`.
2. Otherwise split per platform inside `internal/platform` — every OS-dependent file
   in the repository lives there: one directory, not build tags through the root.
3. **Never** fall back to buffered reads or `io.Copy` and call it a fallback: it
   silently changes the crash and concurrency guarantees, so failing `Open` with
   `ErrUnsupported` beats running on a false promise.

| Platform | `Map` | Page flush | Durability | `TotalRAM` |
|---|---|---|---|---|
| linux, darwin, freebsd, openbsd, dragonfly | `syscall.Mmap` | `msync(MS_SYNC)` | flush + `fsync` | native |
| netbsd, solaris, illumos, aix | `syscall.Mmap` | `ErrUnsupported` | `fsync` alone | none yet |
| windows | `kernel32` mapping | `FlushViewOfFile` | flush + `FlushFileBuffers` | `kernel32` |
| everything else | `ErrUnsupported` | `ErrUnsupported` | refused at `Open` | none yet |

`ponytail:` add `x/sys/unix` only if one of those platforms needs real msync —
hardcoding a syscall number is worse than losing the optimisation. **A mapping
longer than the file is refused in the shared layer**: unix raises `SIGBUS` past
EOF, Windows returns zero-filled pages, and for a store the zeros are worse than
the crash, because the header validates and the prices are invented. Every caller
maps a file it already pre-allocated.

```bash
go build ./... ; GOOS=windows go build ./... ; GOOS=linux go build ./...
GOOS=js GOARCH=wasm go build ./...
grep -l "go:build" *.go reader/*.go writer/*.go zql/*.go   # must be empty
```

### 4.3 The page cache is the write buffer. No WAL.

`MAP_SHARED` appends land in the page cache (190 ns/record, 0 allocs, nothing on disk);
a userspace WAL would add a memcpy per record and duplicate the cache's flush decision.

### 4.4 Durability costs the call, not the volume

`Sync` is the only thing that reaches disk and its cost does not follow volume:
50000 records at 4/16/64/256 MB mapping sizes cost 10.955/9.909/9.870/10.015 ms.
The barrier is the cost, so the IOPS lever is **how often `Sync` is called** —
5194818 ns per tick against 43560 ns batched, **119x**. `ponytail:` group commit needs
a per-shard written/flushed pair and a `-race` test, or a caller can be told its data
is durable when the flush started before its append.

### 4.5 Sizing a flush from the machine

```
budget   = RAMFraction x total_ram
rate     = bytes_appended / seconds_elapsed
derived  = max(MinSyncInterval, budget / rate)
interval = min(derived, MaxInterval)
due      = at_risk >= budget OR seconds_since_flush >= interval
```

The budget branch fires when the dirty set reaches a fixed fraction of RAM (the
throttling condition); the interval branch fires on time, including before any rate
has been observed. **It does not compute device throughput** — that is not knowable
from RAM, CPU count or page size, and measuring by writing perturbs what it
measures. Calling `Sync` more often **increases** IOPS, so this is a policy, not an
interval: flush as rarely as the exposure allows. A policy does not flush by itself
(flushing inside `Append` would make 176 ns occasionally cost 10 ms), so `SyncIfDue`
is polled from the caller's loop; it costs two atomic adds per batch behind one nil
check, with `TestPolicyHotPathIsZeroAllocation` guarding the 0 allocs.

Never feed a duration field seconds (`time.Duration(10)` is 10 ns, not 10 s), and note
`syscall.Sysctl("hw.memsize")` drops the top byte on darwin because Go strips a
terminating NUL (`internal/platform/ram_darwin.go` restores it).

### 4.6 What is not protected

No checksum anywhere: `commit` is a sequence number, not a digest, so a crash between
the `MAP_SHARED` write and the `count` store leaves a header claiming records over
stale page cache — **corruption, not a short tail**. `ponytail:` range CRC verified in
`Open` (not per query), mismatch treated as a truncated tail.

## 5. `unsafe` and assembly: allowed, under conditions

Both are permitted; the format needs them to reach the performance target.

1. **Mature.** Already used widely in the Go runtime, in kernels, or in databases
   with a proven track record. Copied from a tested source, not invented here.
2. **Already benchmarked.** Without a number showing what it buys, `unsafe` is a
   cost that was not paid for.
3. `go test -race ./...` green, no allocation regression, no new race.
4. **A comment explains the invariant** — why this is safe, in one paragraph. If it
   cannot be written in a paragraph, do not use it.

Not allowed: an `unsafe.Pointer` holding a GC-managed object across an unguaranteed
boundary; undocumented register or stack rewriting; reimplementing a standard-library
primitive without a measured reason; `//go:linkname` into runtime internals without a
written lifecycle argument; a pointer into a mapping returned in an escaping struct.
If ordinary Go reaches it, use ordinary Go — `unsafe` is the last tool, not a style.

## 6. Benchmarks are the gate

**Target: 0 allocations.** Every benchmark calls `b.ReportAllocs()`, runs with
`-benchmem`, and a nonzero `allocs/op` is a failure.

| Call | Cost | Use when |
|---|---|---|
| `Cursor` | 0 | a chart reads record by record |
| `RangeInto` | 1, given a reused buffer | caller has a buffer and wants a slice |
| `Scan` | 1 | callback, no cursor |
| `All` | 1, lazy | range loop that may stop early |
| `Range` | one slice, ~30 MB | caller genuinely wants to hold the records |

The ones are per query, never per record: `Scan`/`RangeInto`/`All` build an internal
cursor whose destination cannot be proven non-escaping through a type parameter, so
it moves to the heap once (identical at 10 through 100k records).
`TestQueryPathIsZeroAllocation` fails the build if `Cursor` regresses to 1 alloc;
`TestQueryWrappersAllocateOncePerQuery` fails it if a wrapper's cost scales with
record count. `bench_baseline_test.go` is the stdlib comparison and
`TestManualBaselineMatchesReader` requires the same bars. `BENCHMARKS.md` records
that a warm range scan is **12x faster in SQLite** — widen the gap, update it.

```bash
go test -run XXX -bench=. -benchmem ./...
go test -race ./...        # must be green every time
go vet ./...
gofmt -l .                 # must be empty
golangci-lint run ./...    # must report 0 issues
```

The linter is a readability gate (`wsl_v5`, `nlreturn`, `whitespace`) plus hygiene,
with no opinion on allocation — `allocs/op` is still the gate. Keep
`max-same-issues: 0` (the default of 3 hid six unchecked unmaps).

Hot-path changes report before and after side by side:

```
BenchmarkAllFullSeries-8      1192   1028570 ns/op    65 B/op    1 allocs/op
BenchmarkCursorWindow-8     123081      9359 ns/op     0 B/op    0 allocs/op
BenchmarkRangeWindow-8       97776     12388 ns/op 32768 B/op    1 allocs/op
```

Width is measured: 60 bytes, not the 48 a six-`uint64` bar needs; exposed `float64`
prices cost ~2.3x on a 501-bar window (25% wider record, four divisions per bar), so if
the float API stops paying, store doubles and drop the scaling. **No before number
means unverified.** The query path is 4 allocs per query at any record count.

## 7. Builder pattern and zero allocation

- **`Options` struct, not a setter chain.** Chaining allocates a closure per step
  and makes intermediates escape; a struct passed by value does neither.
- **Pointer receivers that do not escape.** `func (b *Builder) Init()` borrows the
  receiver instead of allocating one.
- **No functional options.** One closure capturing state per option per call site.
- **An explicit zero-allocation path** when the caller already owns a buffer.
- **`sync.Pool` only for state that lives across requests**, never for state born
  and dying inside one operation.

A builder earns its place by saving allocations or by hiding a 12-parameter
constructor. Otherwise an `Options` struct is enough.

## 8. Project layout

```
store.go, errors.go   the store API: Open, Store[T], Options, Close, plus the
                      sentinel errors and suffixes re-exported so a caller
                      never imports an internal package
internal/format/      the on-disk format (was the root package zdb)
  zdb.go              package doc, KeyID/KeyHash/Bucket, magic, layout constants
  header.go           on-disk layout: SegmentHeader, index region, offsets
  segment.go          Segment[T] read path: binary search, Each, Scan, Range
  index.go            bucket index: key -> segment chain, copy-on-write region
record/               Record[T], SizeOf, TypeName, PriceScale, endianness seam
  bar.go, tick.go     the tagged types; bar_schema_gen.go, tick_schema_gen.go
  schema.go           Schema[T] from tags: generated, cached, reflected
cmd/gen-schema-record/  schema source from tags alone (ast only, stdlib only)
internal/platform/    every OS dependency in the repository, and nothing else
writer/               DB[T] append/rollover/seal/sync/index; tune.go = Policy
reader/               Reader[T], Cursor[T], All, Range, Scan, Latest, Keys, List
zql/                  query builder, ZQL parser, query runner, retention
examples/             usage tours to run, one package main per example
zdb_test.go, bench_baseline_test.go, bench_concurrency_test.go, example_test.go
.golangci.yml         lint gate: block spacing, spacing at return, hygiene
```

`Segment[T]`, `Reader[T]`, `Cursor[T]`, `DB[T]` are generic; the format is not,
because width is a header field, and `zql` is the deliberate exception (§10). The
dependency chain is one-way, `zdb(root) → zql → reader → internal/format → record`,
with `internal/format → internal/platform` as the OS leaf: the format knows nothing
about queries, `reader` cannot mutate file contents, and the format lives in
`internal/format` so the store API can occupy the root package. A caller imports
`github.com/aldok10/zdb` for `Open`/`Store` and never reaches past it; the mmap
primitives in `internal/platform` are gone rather than re-exported.

`internal/platform` is the only package with a build tag, and a leaf. **No
build-tagged file outside it**; **nothing above it may re-export it** — the mmap
primitives are gone rather than aliased, and `ErrUnsupported` is the one exception a
caller must be able to test.

## 9. Rules that apply to every change

1. **Search first, then write.** The features already exist; do not rewrite them.
   Delete first if something genuinely has to go.
2. **YAGNI.** No interface with one implementation, no factory for one product, no
   configuration for a value that never changes.
3. **No scaffolding "for later".** If it is genuinely unused, it does not exist.
4. **The fewest files possible.** The shortest correct diff wins.
5. **Boring over clever.**
6. **When two standard-library options are the same size, take the one that is
   correct at the edge case.**
7. **Mark deliberate simplifications.** When there is a known limit that is not
   handled yet, write a `ponytail:` comment naming the ceiling and the upgrade path.
   Do not leave an undocumented limit behind.
8. **Never compromise these, under any circumstance:**
   - input validation at trust boundaries (file names, keys, data from a corrupted
     file)
   - error handling that prevents data loss
   - security
   - accessibility, for any part with a UI
   - anything explicitly requested

## 10. Deliberate limits

Not built, on purpose.
- **Per-session tick aggregates.** ~272 bytes per symbol per session, almost
  entirely derivable from the stored ticks, at ~25x their storage. `ponytail:` a
  `TickStat` type implementing `Record`, one store per symbol per session, keyed
  `"CL/2024.03.15"` — no format or read-path change.
- **ZQL is bar-shaped.** `zql.Open` takes `*reader.Reader[zdb.Bar]` and its `Field`
  enum names OHLC fields, so a tick store is read through `reader` directly.
  `ponytail:` a tick query language needs its own field registry — separate language,
  not a second enum here.
- **One record type per store.** No mixed store: that is the point of the width in
  the header, so a tick segment read as bars is `ErrCorrupt`. Do not relax it.
- **Retention is a view, not reclamation.** `zql.Options{MaxBars, MaxAge}` caps and
  clamps what a query sees; it reclaims no disk, and the index region is rewritten
  whole, so a store that ages out still grows. `ponytail:` a maintenance pass that
  unlinks whole segments and rewrites the affected bucket regions off the write path.
- **Time partitioning.** Segments roll over on size, not a time bucket. A bucket
  would give retention a deletable unit almost for free (the index already holds an
  ordered segment chain per key), but file count is expensive, since the reader maps
  every segment it touches. `ponytail:` bucket duration is an `Options` field.
- **Columnar / SoA.** Records stay 60-byte AoS; use `Scan` with a callback that
  ignores the fields it does not need.
- **Multiple writers per shard.** One is an explicit assumption; two corrupt `count`,
  and nothing hides that.
- **Index region growth.** Pre-allocated once, so a reader never remaps; when it
  fills, an append fails with `ErrIndexFull` naming the option. Raise
  `IndexRegionBytes` before first use.
- **Columnar with frame-of-reference bit packing.** Measured, not built; see
  `BENCHMARKS.md`. The negative result matters most: columnar alone is **0.94x, a
  loss** — the saving is all in packing each column to the width its values need.
  Design: a per-segment bit width per column from that column's min and max, record
  *i* of column *c* at bit offset `i * width[c]`; constant width keeps position
  arithmetic, which is what rules Parquet out (variable width cannot address record
  *i* without decoding ahead). `ponytail:` sealed segments only, minima and maxima
  computed at seal time, encoding chosen from one header field.

A limit becoming a real requirement is a format change. Pre-1.0, no on-disk store is
promised to survive a version, so the old store is disposable — an argument for
changing the format deliberately, with a fault injection test and a reader benchmark,
not quickly.

## 11. No memory leaks

> **No memory leak is permitted — virtual address space, file descriptors and
> handles, not just the Go heap.**

The GC cannot see mapped regions (`mmap`/`munmap`), descriptors (`Close`) or section
handles (`CloseHandle`), so every acquisition needs a matching release on every path,
error paths included.

| Resource | Acquired by | Released by |
|---|---|---|
| Mapped bytes | `platform.Map` | `platform.Unmap`, and only there |
| File descriptor | `os.Open` / `os.OpenFile` | `Close`, and only there |
| Windows section handle | `CreateFileMappingW` | `CloseHandle` |
| Global registry entry | `sync.Map.Store` | `LoadAndDelete` |

- One release site per resource, reached by every path; never unmap outside `Close`
  (a double unmap faults, a missed one leaks).
- A constructor that fails after an acquisition releases what it holds first.
- `Close` is idempotent (mapping field nil after it) — a no-op, not a double free.
  Never retain a mapping pointer past it.
- **Copy every value out of a mapping before releasing it, including for an error
  message** — a `*SegmentHeader` points into `mm`, so reading it after `Munmap(mm)` is
  a fault, not a stale value. Order: read, release, format the message from locals.
- No goroutine may own a mapping, file or handle unless it is guaranteed joined.

A cache that only grows is a leak with a delay: every cached segment holds an open
descriptor plus a live mapping, so many symbols exhaust descriptors, not the heap.
Every cache needs a documented bound and eviction policy ("the caller will call `Close`
eventually" is not one), and a key that can go in must be able to come out.

A leak claim is proved by measurement, not by reading code:

```bash
go test -race ./...
go test -run XXX -bench=. -benchmem -memprofile=/tmp/mem.out ./...
```

Touch many distinct keys and compare counts: `delta` must be zero after
`Reader.Close` and must not grow unbounded while it stays open.

Known, unfixed — do not build on these:

1. **`reader.Reader` caches mappings without bound** (`r.segs`, one entry per segment
   ever touched, each with an open descriptor: 500 keys → 500 held). `ponytail:` an
   LRU closed and unmapped on eviction, with a documented maximum.
2. **Windows holds a package-global registry** of section handles
   (`mapping_windows.go`), correct only if every caller unmaps exactly once;
   `release_unix_test.go` measures the pair-release rule on unix through `/dev/fd`.

Lesson from one fixed leak: `Refresh` opened a second mapping per file because two
maps disagreed about what was mapped. Test leaks through an operation a service
performs, not only a unit call.

## 12. Rules for the assistant

- **Reply in English** unless asked otherwise.
- **Never claim completion without verification.** Run `go vet ./...`, `gofmt -l .`,
  `go test -race ./...`, and the benchmarks yourself, then **quote the actual
  output**. If something cannot be run, say so explicitly.
- **Never say "this is optimal" without a number.** Any performance claim comes with
  benchmark output attached.
- **Do not add a new dependency** for something a few lines of code or the standard
  library already handles. If a dependency really is needed, state what cannot be
  done without it.
- **Corrections are data, not ego.** If the user is wrong, say so. If the user is
  right, say they are right.
- **Separate what was verified from what was not.** If only part of the suite ran,
  name that part. Do not write "verified" for something that was not tested.
- **Do not write code that was not asked for.** If there is an additional need,
  mention it in a single line at the end as an option — do not implement it on the
  spot.
