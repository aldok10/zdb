package zql_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
	"github.com/aldok10/zdb/zql"
)

const segBytes = 64 << 10

// stamp is one minute apart, in unix milliseconds, which is what a Bar carries.
func stamp(i int) int64 { return 1_704_067_200_000 + int64(i)*60_000 }

func bar(i int) record.Bar {
	return record.Bar{
		Datetime: stamp(i),
		Open:     float64(i) + 1,
		High:     float64(i) + 2,
		Low:      float64(i),
		Close:    float64(i) + 0.5,
		Volume:   uint64(i * 10),
	}
}

// store writes n bars one minute apart and returns an open reader.
func store(t testing.TB, n int) *reader.Reader[record.Bar] {
	t.Helper()
	dir := t.TempDir()

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	for i := range n {
		if err := w.Append("binance/spot/BTCUSDT", bar(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}

	t.Cleanup(func() { r.Close() })

	return r
}

// mustParse keeps the tests readable: a query literal that does not parse is a
// bug in the test, not a case to assert on.
func mustParse(t testing.TB, src string) zql.Query {
	t.Helper()

	q, err := zql.Parse(src)
	if err != nil {
		t.Fatalf("Parse(%q): %v", src, err)
	}

	return q
}

func collect(t testing.TB, seq func(func(record.Bar, error) bool)) []record.Bar {
	t.Helper()

	var out []record.Bar

	for b, err := range seq {
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		out = append(out, b)
	}

	return out
}

// The text form and the builder must produce the same query, or one of them is
// lying about what the language means.
func TestParseMatchesBuilder(t *testing.T) {
	q, err := zql.NewQuery("binance/spot/BTCUSDT").
		Between(uint64(stamp(10)), uint64(stamp(900))).
		Where(zql.FieldClose, zql.Gt, 100.5)
	if err != nil {
		t.Fatalf("builder: %v", err)
	}

	q, err = q.Where(zql.FieldVolume, zql.Lte, 5000)
	if err != nil {
		t.Fatalf("builder: %v", err)
	}

	q = q.NewestFirst().Take(250).Select(zql.FDatetime | zql.FClose | zql.FVolume)

	parsed, err := zql.Parse(q.String())
	if err != nil {
		t.Fatalf("Parse(%q): %v", q.String(), err)
	}

	if !reflect.DeepEqual(q, parsed) {
		t.Fatalf("round trip differs\n built: %+v\nparsed: %+v", q, parsed)
	}
}

// Every builder-produced query has to survive its own text form, including the
// price formatting, which is the part that could quietly lose precision.
func TestStringRoundTrips(t *testing.T) {
	for _, v := range []float64{0, 1, 100.5, 0.00000001, 12345.6789, 1e6} {
		q, err := zql.NewQuery("k").Where(zql.FieldOpen, zql.Gt, v)
		if err != nil {
			t.Fatalf("Where(%v): %v", v, err)
		}

		parsed, err := zql.Parse(q.String())
		if err != nil {
			t.Fatalf("Parse(%q): %v", q.String(), err)
		}

		if !reflect.DeepEqual(q.Conds, parsed.Conds) {
			t.Fatalf("price %v did not round trip: %v vs %v", v, q.Conds, parsed.Conds)
		}
	}
}

func TestParseRejects(t *testing.T) {
	for _, src := range []string{
		`SELECT nope FROM "k"`,
		`SELECT * FROM`,
		`SELECT * FROM "k" WHERE close ~ 3`,
		`SELECT * FROM "k" WHERE close >`,
		`SELECT * FROM "k" WHERE close > -1`,
		`SELECT * FROM "k" LIMIT -5`,
		`SELECT * FROM "k" RANGE 5 TO 1`,
		`SELECT * FROM "k" RANGE 5`,
		`SELECT * FROM "k" LIMIT 1.5`,
		`SELECT * FROM "k" WHERE close > 1e9`,
		`SELECT * FROM "k" ORDER BY close ASC`,
		`SELECT * FROM "k" ORDER BY datetime SIDEWAYS`,
		`SELECT * FROM "k" WHERE datetime >= "x"`,
		`SELECT * FROM "k" extra`,
		`SELECT * FROM "unterminated`,
		``,
	} {
		if _, err := zql.Parse(src); err == nil {
			t.Fatalf("Parse(%q) accepted a bad query", src)
		}
	}
}

// A condition on time has to narrow the binary search, not filter afterwards.
// The difference is visible as the result being correct either way but the wrong
// window silently including a boundary bar, so this pins the boundary.
func TestDatetimeConditionNarrowsWindow(t *testing.T) {
	db := zql.Open(store(t, 100), zql.Options{})

	got, err := db.Collect(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT" WHERE datetime >= 1704067800000`))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	// stamp(10) is the first bar at or after 1704067800000, so 10..99 remain.
	if len(got) != 90 {
		t.Fatalf("got %d bars, want 90", len(got))
	}

	if got[0].Datetime != 1704067800000 {
		t.Fatalf("first bar at %d, want 1704067800000", got[0].Datetime)
	}

	// `>` must be exclusive and `>=` inclusive; an off-by-one here is invisible
	// in a round trip because the text form still parses.
	gt, err := db.Collect(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT" WHERE datetime > 1704067800000`))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(gt) != 89 {
		t.Fatalf("strict > got %d bars, want 89", len(gt))
	}
}

func TestFiltersAreAnded(t *testing.T) {
	db := zql.Open(store(t, 100), zql.Options{})

	got := collect(t, db.Query(mustParse(t,
		`SELECT * FROM "binance/spot/BTCUSDT" WHERE close >= 20.5 AND volume < 500`)))
	for _, b := range got {
		if b.Close < 20.5 || b.Volume >= 500 {
			t.Fatalf("bar %+v should have been filtered out", b)
		}
	}

	if len(got) == 0 || len(got) == 100 {
		t.Fatalf("filter matched %d bars, want a strict subset", len(got))
	}
}

func TestProjectionZerosUnselected(t *testing.T) {
	db := zql.Open(store(t, 10), zql.Options{})

	got := collect(t, db.Query(mustParse(t, `SELECT datetime, close FROM "binance/spot/BTCUSDT"`)))
	if len(got) != 10 {
		t.Fatalf("got %d bars, want 10", len(got))
	}

	b := got[3]
	if b.Datetime != stamp(3) || b.Close != 3.5 {
		t.Fatalf("selected fields wrong: %+v", b)
	}

	if b.Open != 0 || b.High != 0 || b.Low != 0 || b.Volume != 0 {
		t.Fatalf("unselected fields were populated: %+v", b)
	}
}

func TestFirstStopsAndReportsNoMatch(t *testing.T) {
	db := zql.Open(store(t, 1000), zql.Options{})

	got, err := db.First(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT" WHERE close > 100`))
	if err != nil {
		t.Fatalf("First: %v", err)
	}
	// Close is i+0.5, so the first bar whose close clears 100 is i=100.
	if got.Datetime != stamp(100) {
		t.Fatalf("First returned %d, want %d", got.Datetime, stamp(100))
	}

	if _, err := db.First(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT" WHERE close > 1000000000`)); !errors.Is(err, zql.ErrNoMatch) {
		t.Fatalf("First on no match gave %v, want ErrNoMatch", err)
	}
	// A missing key is an error, not an empty answer.
	if _, err := db.First(mustParse(t, `SELECT * FROM "no/such/key"`)); err == nil {
		t.Fatal("First on a missing key returned no error")
	}
}

func TestDescIsCollectOnly(t *testing.T) {
	db := zql.Open(store(t, 50), zql.Options{})

	q := mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT" ORDER BY datetime DESC LIMIT 3`)
	if got := collect(t, db.Query(q)); got[0].Datetime > got[len(got)-1].Datetime {
		t.Fatalf("Query must stream ascending, got %d then %d", got[0].Datetime, got[len(got)-1].Datetime)
	}

	got, err := db.Collect(q)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(got) != 3 || got[0].Datetime != stamp(49) || got[2].Datetime != stamp(47) {
		t.Fatalf("Desc Collect gave %+v", got)
	}
}

// LIMIT caps the rows that survive WHERE, not the rows the scan walks through,
// and ascending takes the oldest matches: a filter must never be able to shrink
// a limit below the rows that actually match.
func TestLimitCountsTheRowsThatMatch(t *testing.T) {
	db := zql.Open(store(t, 200), zql.Options{})

	// Only bars 0..19 pass this filter, so a limit fed by the scan would come
	// back empty: the newest 200-window bars are not the ones that match.
	got := collect(t, db.Query(mustParse(t,
		`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close < 20.5
		 ORDER BY datetime ASC LIMIT 5`)))
	if len(got) != 5 {
		t.Fatalf("ASC under a filter gave %d bars, want 5", len(got))
	}

	for i, b := range got {
		if b.Close != float64(i)+0.5 || b.Datetime != stamp(i) {
			t.Fatalf("bar %d = %d close %.2f, want %d close %.2f", i, b.Datetime, b.Close, stamp(i), float64(i)+0.5)
		}
	}
}

// Ascending LIMIT takes the oldest matches, which is the end a chart starts at,
// not the newest bars in the window.
func TestLimitAscendingTakesTheOldestMatches(t *testing.T) {
	db := zql.Open(store(t, 200), zql.Options{})

	got := collect(t, db.Query(mustParse(t,
		`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close >= 103
		 ORDER BY datetime ASC LIMIT 10`)))
	if len(got) != 10 {
		t.Fatalf("got %d bars, want 10", len(got))
	}

	// close is i+0.5, so the first bar clearing 103 is i=103 at 103.5.
	for i, b := range got {
		want := 103.5 + float64(i)
		if b.Close != want || b.Datetime != stamp(103+i) {
			t.Fatalf("bar %d = %d close %.2f, want %d close %.2f", i, b.Datetime, b.Close, stamp(103+i), want)
		}
	}
}

// Descending LIMIT takes the newest matches, and the filter still decides which
// rows exist to take: the newest bars of the window are not automatically the
// newest rows that match.
func TestLimitDescendingTakesTheNewestMatches(t *testing.T) {
	db := zql.Open(store(t, 200), zql.Options{})

	got, err := db.Collect(mustParse(t,
		`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close < 20.5
		 ORDER BY datetime DESC LIMIT 5`))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(got) != 5 {
		t.Fatalf("DESC under a filter gave %d bars, want 5", len(got))
	}

	for i, b := range got {
		want := 19 - i
		if b.Close != float64(want)+0.5 || b.Datetime != stamp(want) {
			t.Fatalf("bar %d = %d close %.2f, want %d close %.2f", i, b.Datetime, b.Close, stamp(want), float64(want)+0.5)
		}
	}
}

// A descending limit that finds fewer matches than it asked for yields every
// match it found: the ring holds bars, not slots, and a partial one walks out
// from the front rather than from the head of a full ring.
func TestLimitDescendingYieldsEveryMatchItFound(t *testing.T) {
	db := zql.Open(store(t, 200), zql.Options{})

	got, err := db.Collect(mustParse(t,
		`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close < 5.5
		 ORDER BY datetime DESC LIMIT 10`))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(got) != 5 {
		t.Fatalf("got %d bars, want the 5 that matched", len(got))
	}

	// close is i+0.5 and `<` is strict, so the matches are i=0..4 and nothing
	// above: 4.5 down to 0.5.
	for i, b := range got {
		want := 4 - i
		if b.Close != float64(want)+0.5 || b.Datetime != stamp(want) {
			t.Fatalf("bar %d = %d close %.2f, want %d close %.2f", i, b.Datetime, b.Close, stamp(want), float64(want)+0.5)
		}
	}
}

// Retention is a view over the store, so the limit reads through it: only the
// newest MaxBars bars exist for a query, and a descending limit takes from what
// is visible, not from matches older than the ceiling.
func TestLimitDescendingReadsThroughTheRetentionView(t *testing.T) {
	db := zql.Open(store(t, 100), zql.Options{MaxBars: 10})

	// Bars 90..99 are the view, and only 90..94 of those match. Reading past
	// the ceiling would add 89 down to 87.
	got, err := db.Collect(mustParse(t,
		`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close < 95
		 ORDER BY datetime DESC LIMIT 8`))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(got) != 5 {
		t.Fatalf("got %d bars, want the 5 inside the view", len(got))
	}

	for i, b := range got {
		want := 94 - i
		if b.Close != float64(want)+0.5 || b.Datetime != stamp(want) {
			t.Fatalf("bar %d = %d close %.2f, want %d close %.2f", i, b.Datetime, b.Close, stamp(want), float64(want)+0.5)
		}
	}
}

// Retention can only narrow a result, never widen one, and the smaller limit wins.
func TestRetentionMaxBars(t *testing.T) {
	r := store(t, 100)

	wide := zql.Open(r, zql.Options{})
	if got := collect(t, wide.Query(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT"`))); len(got) != 100 {
		t.Fatalf("no retention gave %d bars, want 100", len(got))
	}

	capped := zql.Open(r, zql.Options{MaxBars: 10})

	got, err := capped.Collect(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT"`))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(got) != 10 || got[0].Datetime != stamp(90) {
		t.Fatalf("MaxBars 10 gave %d bars starting %d, want 10 starting %d", len(got), got[0].Datetime, stamp(90))
	}

	// A tighter query limit is kept; a looser one is cut to the ceiling.
	if got, _ := capped.Collect(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT" LIMIT 3`)); len(got) != 3 {
		t.Fatalf("LIMIT 3 under MaxBars 10 gave %d bars, want 3", len(got))
	}

	if got, _ := capped.Collect(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT" LIMIT 99`)); len(got) != 10 {
		t.Fatalf("LIMIT 99 under MaxBars 10 gave %d bars, want 10", len(got))
	}
}

// MaxAge is measured from the newest stored bar, not from the wall clock, so a
// store that stopped receiving data still has something to draw.
func TestRetentionMaxAge(t *testing.T) {
	r := store(t, 100)
	db := zql.Open(r, zql.Options{MaxAge: 10 * time.Minute})

	got, err := db.Collect(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT"`))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	// 10 minutes back from stamp(99) is stamp(89), inclusive, so 11 bars.
	if len(got) != 11 || got[0].Datetime != stamp(89) {
		t.Fatalf("MaxAge 10m gave %d bars from %d, want 11 from %d", len(got), got[0].Datetime, stamp(89))
	}
	// A window that starts before the cut is clamped up to it.
	got, err = db.Collect(mustParse(t,
		`SELECT * FROM "binance/spot/BTCUSDT" WHERE datetime >= 1704067200000`))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if got[0].Datetime != stamp(89) {
		t.Fatalf("window start was not clamped: first bar %d", got[0].Datetime)
	}
}

// Stopping the loop has to stop the walk: a filter that matches one bar in a
// hundred thousand must not cost a hundred thousand decodes to find it.
func TestEarlyBreakStopsWork(t *testing.T) {
	db := zql.Open(store(t, 1000), zql.Options{})

	n := 0
	for range db.Query(mustParse(t, `SELECT * FROM "binance/spot/BTCUSDT"`)) {
		n++
		if n == 5 {
			break
		}
	}

	if n != 5 {
		t.Fatalf("counted %d, want 5", n)
	}
}

func TestRunReportsBadTextThroughTheSequence(t *testing.T) {
	db := zql.Open(store(t, 10), zql.Options{})

	// A syntax error must arrive before any bar, so a caller cannot mistake a
	// rejected query for a query that matched nothing.
	var got error

	n := 0

	for _, err := range db.Run(`SELECT * FROM`) {
		got = err
		n++
	}

	if got == nil {
		t.Fatal("Run accepted a bad query")
	}

	if n != 1 {
		t.Fatalf("bad query yielded %d steps, want exactly 1", n)
	}

	if got := collect(t, db.Run(`SELECT * FROM "binance/spot/BTCUSDT" LIMIT 2`)); len(got) != 2 {
		t.Fatalf("Run gave %d bars, want 2", len(got))
	}
}

// A condition value that float64 cannot hold exactly is refused rather than
// rounded, because a filter that silently compares a rounded number is worse than
// one that fails loudly.
func TestCondRejectsInexactValues(t *testing.T) {
	if _, err := zql.NewCond(zql.FieldVolume, zql.Gt, 1<<53); err == nil {
		t.Fatal("NewCond accepted a value past the exactly comparable range")
	}

	if _, err := zql.NewCond(zql.FieldClose, zql.Gt, -1); err == nil {
		t.Fatal("NewCond accepted a negative price")
	}

	if _, err := zql.NewCond(zql.Field(99), zql.Gt, 1); err == nil {
		t.Fatal("NewCond accepted an unknown field")
	}
	// A real millisecond timestamp is well under the limit and must be accepted.
	if _, err := zql.NewCond(zql.FieldDatetime, zql.Gt, 1_704_067_200_000); err != nil {
		t.Fatalf("NewCond rejected a real timestamp: %v", err)
	}
}

func TestQueryWithoutKeyIsAnError(t *testing.T) {
	db := zql.Open(store(t, 10), zql.Options{})

	var got error
	for _, err := range db.Query(zql.Query{}) {
		got = err
	}

	if got == nil || !strings.Contains(got.Error(), "no key") {
		t.Fatalf("empty query gave %v, want a no-key error", got)
	}
}

// A query that filters nothing folded into the window must not copy the caller's
// conditions, and a window query must not allocate at all.
func BenchmarkQueryWindow(b *testing.B) {
	db := benchDB(b, 200_000)
	q := mustParseB(b, `SELECT * FROM "bench/BTCUSDT" RANGE 1704067200000 TO 1704096000000`)
	b.ReportAllocs()

	for b.Loop() {
		n := 0

		for _, err := range db.Query(q) {
			if err != nil {
				b.Fatal(err)
			}

			n++
		}

		if n == 0 {
			b.Fatal("no bars")
		}
	}
}

// The same window with a price filter: the filter is per-record, so this is the
// shape that does the work the format cannot index away.
func BenchmarkQueryFiltered(b *testing.B) {
	db := benchDB(b, 200_000)
	q := mustParseB(b, `SELECT datetime, close FROM "bench/BTCUSDT" RANGE 1704067200000 TO 1704096000000 WHERE close > 100`)
	b.ReportAllocs()

	for b.Loop() {
		for _, err := range db.Query(q) {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// The target query in ascending order: oldest matches win, so the walk stops
// at the limit-th match and never sees the rest of the store.
func BenchmarkQueryFilteredLimitAsc(b *testing.B) {
	db := benchDB(b, 200_000)
	q := mustParseB(b, `SELECT datetime, close FROM "bench/BTCUSDT" WHERE close >= 103 ORDER BY datetime ASC LIMIT 10`)
	b.ReportAllocs()

	for b.Loop() {
		n := 0

		for _, err := range db.Query(q) {
			if err != nil {
				b.Fatal(err)
			}

			n++
		}

		if n != 10 {
			b.Fatalf("got %d rows", n)
		}
	}
}

// The same query descending: the ring-buffer walk used to decode every bar;
// the reverse cursor stops at the limit-th match from the newest end.
func BenchmarkQueryFilteredLimitDesc(b *testing.B) {
	db := benchDB(b, 200_000)
	q := mustParseB(b, `SELECT datetime, close FROM "bench/BTCUSDT" WHERE close >= 103 ORDER BY datetime DESC LIMIT 10`)
	b.ReportAllocs()

	for b.Loop() {
		n := 0

		for _, err := range db.Query(q) {
			if err != nil {
				b.Fatal(err)
			}

			n++
		}

		if n != 10 {
			b.Fatalf("got %d rows", n)
		}
	}
}

func benchDB(b *testing.B, n int) *zql.DB {
	b.Helper()
	dir := b.TempDir()

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: 64 << 10})
	if err != nil {
		b.Fatal(err)
	}

	batch := make([]record.Bar, 1000)
	for i := range n {
		batch[i%1000] = bar(i)
		if i%1000 == 999 {
			if err := w.AppendBatch("bench/BTCUSDT", batch); err != nil {
				b.Fatal(err)
			}
		}
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { r.Close() })

	return zql.Open(r, zql.Options{})
}

func mustParseB(b *testing.B, src string) zql.Query {
	b.Helper()

	q, err := zql.Parse(src)
	if err != nil {
		b.Fatal(err)
	}

	return q
}
