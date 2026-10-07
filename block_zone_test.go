package zdb_test

import (
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/aldok10/zdb"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/zql"
)

// The zone-map skip in the cursor must never invent or drop rows: with a
// scrambled close, a block-granular predicate has to produce exactly what a
// brute-force filter sees, ascending and descending, with and without a limit.
func TestBlockZoneSkipMatchesBruteForce(t *testing.T) {
	dir := t.TempDir()

	w, err := zdb.Open[record.Bar](zdb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	const n = 8192

	rng := rand.New(rand.NewPCG(42, 7))
	bars := make([]record.Bar, n)

	for i := range bars {
		bars[i] = record.Bar{
			Datetime: int64(stamp(i)),
			Close:    float64(rng.IntN(1000)) + 0.5,
			Volume:   1,
		}
	}

	if err := w.Writer.AppendBatch("binance/spot/BTCUSDT", bars); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	q, err := zdb.Open[record.Bar](zdb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	defer q.Close()

	zdbq, err := q.Query()
	if err != nil {
		t.Fatal(err)
	}

	type run struct {
		src     string
		matches func(record.Bar) bool
		order   string // ASC or DESC, appended to src
		limit   int    // 0 for unbounded
	}

	for _, r := range []run{
		{`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close >= 500`, func(b record.Bar) bool { return b.Close >= 500 }, "ASC", 0},
		{`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close >= 500`, func(b record.Bar) bool { return b.Close >= 500 }, "DESC", 50},
		{`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close < 100`, func(b record.Bar) bool { return b.Close < 100 }, "ASC", 20},
		{`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close >= 0`, func(b record.Bar) bool { return b.Close >= 0 }, "DESC", 3},
	} {
		var want []record.Bar

		for _, b := range bars {
			if r.matches(b) {
				want = append(want, b)
			}
		}

		if r.order == "DESC" && r.limit > 0 && len(want) > r.limit {
			want = want[len(want)-r.limit:]
		}

		if r.order == "ASC" && r.limit > 0 && len(want) > r.limit {
			want = want[:r.limit]
		}

		src := r.src + " ORDER BY datetime " + r.order
		if r.limit > 0 {
			src += " LIMIT " + strconv.Itoa(r.limit)
		}

		var got []record.Bar

		for bar, err := range zdbq.Run(src) {
			if err != nil {
				t.Fatal(err)
			}

			got = append(got, bar)
		}

		if len(got) != len(want) {
			t.Fatalf("%s: got %d rows, want %d", src, len(got), len(want))
		}

		for i := range got {
			if got[i].Datetime != want[i].Datetime || got[i].Close != want[i].Close {
				t.Fatalf("%s: row %d = (%d, %.1f), want (%d, %.1f)",
					src, i, got[i].Datetime, got[i].Close, want[i].Datetime, want[i].Close)
			}
		}
	}

	// Collect through the whole-suite reader to double-check the descending
	// stream order.
	qry, err := zql.Parse(`SELECT datetime, close FROM "binance/spot/BTCUSDT" WHERE close >= 0 ORDER BY datetime DESC LIMIT 4`)
	if err != nil {
		t.Fatal(err)
	}

	got, err := zdbq.Collect(qry)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 4 {
		t.Fatalf("got %d", len(got))
	}

	for i := 1; i < len(got); i++ {
		if got[i-1].Datetime <= got[i].Datetime {
			t.Fatalf("DESC order broken: %d then %d", got[i-1].Datetime, got[i].Datetime)
		}
	}
}
