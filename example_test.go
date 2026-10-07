package zdb_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
	"github.com/aldok10/zdb/zql"
)

// Every example below runs as a test, so each `// Output:` block is checked. If
// the API drifts, an example fails rather than quietly lying on a documentation
// page.
//
// Nothing is written inside the package directory. A `Dir: "data"` in an example
// would create a real directory every time the suite runs, so every example gets
// a scratch directory under one temp root, removed when the test binary exits.

var (
	exampleOnce sync.Once
	exampleRoot string
)

// exampleTemp names a scratch directory for one example. Each example takes its
// own because a store is keyed by directory, and two examples writing the same
// key into a shared directory would collide on the strictly-increasing timestamp
// rule.
func exampleTemp(name string) string {
	exampleOnce.Do(func() {
		dir, err := os.MkdirTemp("", "zdb-examples")
		if err != nil {
			panic(err)
		}

		exampleRoot = dir
	})

	return filepath.Join(exampleRoot, name)
}

func TestMain(m *testing.M) {
	code := m.Run()

	if exampleRoot != "" {
		os.RemoveAll(exampleRoot)
	}

	os.Exit(code)
}

// exampleClose is 100 + i/100, so the printed prices stay short and exact at two
// decimals. A price derived as 100 + i/1000 prints as 100.99899999999999 and
// makes an Output block unreadable.
func exampleClose(i int) float64 { return 100 + float64(i)/100 }

// newExampleStore writes n bars one minute apart for one key and returns a Reader
// on them. Each example takes its own directory and its own store: timestamps
// restart from zero in a fresh store, and re-opening an existing one would fail
// the strictly-increasing rule rather than append.
func newExampleStore(dir string, n int) (*reader.Reader[record.Bar], error) {
	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: 4 << 20})
	if err != nil {
		return nil, err
	}

	for i := range n {
		c := exampleClose(i)

		err = w.Append("binance/spot/BTCUSDT", record.Bar{
			Datetime: int64(stamp(i)),
			Open:     c - 0.25,
			High:     c + 0.50,
			Low:      c - 0.50,
			Close:    c,
			Volume:   uint64(1000 + i),
		})
		if err != nil {
			w.Close()

			return nil, err
		}
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return reader.Open[record.Bar](dir)
}

const exampleKey = "binance/spot/BTCUSDT"

// A store is a directory of segment and index files. There is no transaction to
// bracket, because a record is self-contained and the header count is what makes
// it visible.
func Example() {
	w, err := writer.Open[record.Bar](writer.Options{Dir: exampleTemp("write-basic")})
	if err != nil {
		panic(err)
	}
	defer w.Close()

	now := time.Now().UnixMilli()

	err = w.AppendBatch("binance/spot/ETHUSDT", []record.Bar{
		{Datetime: now, Open: 3.10, High: 3.30, Low: 3.00, Close: 3.20},
		{Datetime: now + 60_000, Open: 3.20, High: 3.40, Low: 3.10, Close: 3.30},
	})
	if err != nil {
		panic(err)
	}

	err = w.Append(exampleKey, record.Bar{
		Datetime: now,
		Open:     100.50,
		High:     101.25,
		Low:      100.00,
		Close:    101.00,
		Volume:   12345,
	})
	if err != nil {
		panic(err)
	}

	keys := w.Keys()
	fmt.Println("wrote", len(keys), "symbols")
	// Output: wrote 2 symbols
}

// Datetime is unix milliseconds and must strictly increase per key. A timestamp
// that is not newer is rejected rather than stored, because the header keeps
// timestamps unsigned and a wrapped value would sort after every real bar.
func ExampleDB_Append_outOfOrder() {
	w, err := writer.Open[record.Bar](writer.Options{Dir: exampleTemp("write-order")})
	if err != nil {
		panic(err)
	}
	defer w.Close()

	_ = w.Append(exampleKey, record.Bar{Datetime: 1_704_067_260_000, Close: 100})
	err = w.Append(exampleKey, record.Bar{Datetime: 1_704_067_200_000, Close: 100})
	fmt.Println("rejected:", err != nil)
	// Output: rejected: true
}

// All streams bars without building a slice, so a caller that stops early stops
// the walk. This is the shape a chart redrawing at 60 fps wants.
func ExampleReader_All() {
	r, err := newExampleStore(exampleTemp("read-all"), 1000)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	first, last, err := r.Bounds(exampleKey)
	if err != nil {
		panic(err)
	}

	// The newest 3 bars of the whole window, handed back oldest first.
	count := 0

	for bar, err := range r.All(exampleKey, first, last, 3) {
		if err != nil {
			panic(err)
		}

		fmt.Printf("%d %.2f\n", bar.Datetime, bar.Close)

		count++
	}

	fmt.Println("streamed", count)
	// Output:
	// 1704127020000 109.97
	// 1704127080000 109.98
	// 1704127140000 109.99
	// streamed 3
}

// Range returns a slice and therefore allocates. It is the right call when the
// caller genuinely wants to hold the bars, and the wrong one inside a redraw
// loop. The README has the measurement: 15.9 MB against one byte for the same
// 50000 bars.
func ExampleReader_Range() {
	r, err := newExampleStore(exampleTemp("read-range"), 1000)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	bars, err := r.Range(exampleKey, 0, 0, 0)
	if err != nil {
		panic(err)
	}

	fmt.Println("count:", len(bars), "first:", bars[0].Datetime, "last:", bars[len(bars)-1].Datetime)
	// Output: count: 1000 first: 1704067200000 last: 1704127140000
}

// Scan takes a callback and allocates nothing. Returning false stops the walk, so
// a caller that has seen enough does not pay for the rest.
func ExampleReader_Scan() {
	r, err := newExampleStore(exampleTemp("read-scan"), 1000)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	total := 0.0
	matched := 0

	err = r.Scan(exampleKey, 0, 0, 0, func(bar record.Bar) bool {
		if bar.Close > 109.0 {
			total += bar.Close
			matched++
		}

		return true
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("matched:", matched, "total:", total)
	// Output: matched: 99 total: 10840.5
}

// Cursor is the lowest-level shape and allocates nothing at all. With a limit it
// walks newest first, because that is the end a chart starts from.
func ExampleReader_Cursor() {
	r, err := newExampleStore(exampleTemp("read-cursor"), 1000)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	var c reader.Cursor[record.Bar]

	err = r.Cursor(exampleKey, 0, 0, 3, &c)
	if err != nil {
		panic(err)
	}

	var bar record.Bar
	for c.Next(&bar) {
		fmt.Printf("%d %d\n", bar.Datetime, bar.Volume)
	}

	if err := c.Err(); err != nil {
		panic(err)
	}
	// Output:
	// 1704127140000 1999
	// 1704127080000 1998
	// 1704127020000 1997
}

// A raw query. A condition on datetime is folded into the binary search rather
// than tested per record, and a condition on a price compares the stored
// fixed-point integer, so the scan never turns a price back into a double.
func ExampleDB_Run() {
	r, err := newExampleStore(exampleTemp("read-run"), 1000)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	db := zql.Open(r, zql.Options{})

	q, err := zql.Parse(`SELECT datetime, close FROM "binance/spot/BTCUSDT"
	                     WHERE close >= 109.95
	                     LIMIT 5`)
	if err != nil {
		panic(err)
	}

	// The parsed query is just a value, so it can be built in Go instead.
	for bar, err := range db.Query(q) {
		if err != nil {
			panic(err)
		}

		fmt.Printf("%d %.2f\n", bar.Datetime, bar.Close)
	}
	// Output:
	// 1704126900000 109.95
	// 1704126960000 109.96
	// 1704127020000 109.97
	// 1704127080000 109.98
	// 1704127140000 109.99
}

// The builder and the text form are the same query, and one prints to the other.
// A query can live in a config file or be assembled in code without either losing
// meaning.
func ExampleQuery() {
	r, err := newExampleStore(exampleTemp("read-query"), 1000)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	db := zql.Open(r, zql.Options{})

	// Where is the one fallible builder method, so it ends the chain. Everything
	// after it is a value method and can go on.
	q, err := zql.NewQuery(exampleKey).
		Between(0, 0).
		Where(zql.FieldClose, zql.Gt, 109.9)
	if err != nil {
		panic(err)
	}

	q = q.NewestFirst().Take(3)

	fmt.Println(q)

	// Query streams oldest first whatever the order says, because the mapping is
	// read forward. Collect applies Desc, so this one comes back newest first.
	// The read APIs agree on which bars you get and differ on which end they
	// start from: Latest(key, 3) hands back the newest three oldest first.
	bars, err := db.Collect(q)
	if err != nil {
		panic(err)
	}

	for _, b := range bars {
		fmt.Printf("%d %.2f\n", b.Datetime, b.Close)
	}
	// Output:
	// SELECT * FROM binance/spot/BTCUSDT WHERE close > 109.9 ORDER BY datetime DESC LIMIT 3
	// 1704127140000 109.99
	// 1704127080000 109.98
	// 1704127020000 109.97
}

// Retention is a ceiling on what a query can see, not on what is on disk. It
// takes the newest bars, and it measures the age limit against the newest stored
// bar rather than the wall clock, so a store that stopped receiving data still
// has something to draw.
func ExampleOptions() {
	r, err := newExampleStore(exampleTemp("read-options"), 1000)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	db := zql.Open(r, zql.Options{MaxBars: 3, MaxAge: 10 * time.Minute})

	q, err := zql.Parse(`SELECT * FROM "binance/spot/BTCUSDT"`)
	if err != nil {
		panic(err)
	}

	bars, err := db.Collect(q)
	if err != nil {
		panic(err)
	}

	for _, b := range bars {
		fmt.Println(b.Datetime)
	}
	// Output:
	// 1704127020000
	// 1704127080000
	// 1704127140000
}

// Sync is the only call that reaches the disk. Append never calls it, because a
// write through a shared mapping lands in the page cache and the kernel's
// writeback is the batching layer. Its cost follows the number of calls rather
// than the volume written, so call it when the data has to survive a power loss,
// not once per bar.
func ExampleDB_Sync() {
	w, err := writer.Open[record.Bar](writer.Options{Dir: exampleTemp("write-sync")})
	if err != nil {
		panic(err)
	}
	defer w.Close()

	base := time.Now().UnixMilli()
	for i := range 500 {
		err = w.Append(exampleKey, record.Bar{
			Datetime: base + int64(i)*1000,
			Open:     100, High: 101, Low: 99, Close: 100.5,
		})
		if err != nil {
			panic(err)
		}
	}
	// One flush for 500 bars. A flush per bar instead costs about 119x as much,
	// measured, and buys nothing a later flush would not.
	if err := w.Sync(); err != nil {
		panic(err)
	}

	fmt.Println("flushed")
	// Output: flushed
}

// A tick store is the same database with a different record type. Nothing about
// the file format, the index, the seqlock or the flush policy changes: the record
// width lives in every segment header, so a tick store is just a directory whose
// segments say "56 bytes per record".
func ExampleTick() {
	w, err := writer.Open[record.Tick](writer.Options{Dir: exampleTemp("write-tick")})
	if err != nil {
		fmt.Println(err)

		return
	}

	// Datetime is unix microseconds, so a liquid symbol's ticks do not collide
	// inside one millisecond.
	base := int64(1_704_067_200_000_000)

	ticks := []record.Tick{
		{
			Datetime: base, Bid: 1.0842, Ask: 1.0843, Last: 1.08425, Volume: 12,
			Flags: record.TickFlagBid | record.TickFlagAsk | record.TickFlagLast | record.TickFlagVolume,
		},
		{
			Datetime: base + 250_000, Bid: 1.0841, Ask: 1.0842, Last: 1.08415, Volume: 3,
			Flags: record.TickFlagBid | record.TickFlagAsk,
		},
		{
			Datetime: base + 900_000, Bid: 1.0845, Ask: 1.0846, Last: 1.08455,
			Flags: record.TickFlagBid | record.TickFlagAsk | record.TickFlagLast,
		},
	}
	if err := w.AppendBatch("tick/EURUSD", ticks); err != nil {
		fmt.Println(err)

		return
	}

	if err := w.Close(); err != nil {
		fmt.Println(err)

		return
	}

	r, err := reader.Open[record.Tick](exampleTemp("write-tick"))
	if err != nil {
		fmt.Println(err)

		return
	}
	defer r.Close()

	// Cursor allocates nothing, which is what a feed reading every tick needs.
	var (
		cur  reader.Cursor[record.Tick]
		tick record.Tick
	)

	if err := r.Cursor("tick/EURUSD", uint64(base), uint64(base+1_000_000), 0, &cur); err != nil {
		fmt.Println(err)

		return
	}

	for cur.Next(&tick) {
		// A flag says which fields this tick actually carries, so a stale bid is
		// not mistaken for a current one.
		mid := tick.Bid + (tick.Ask-tick.Bid)/2
		fmt.Printf("%.2fs mid %.5f volume %d\n",
			float64(tick.Datetime-base)/1e6, mid, tick.Volume)
	}

	if err := cur.Err(); err != nil {
		fmt.Println(err)
	}

	// Output:
	// 0.00s mid 1.08425 volume 12
	// 0.25s mid 1.08415 volume 3
	// 0.90s mid 1.08455 volume 0
}
