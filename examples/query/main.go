// Command query runs one lookup two ways: as ZQL text and through the Go
// builder. The builder is the source of truth, and Query.String round trips back
// to the text form.
//
//	go run ./examples/query
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aldok10/zdb"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/zql"
)

const key = "binance/spot/BTCUSDT"

// base is 2024-01-01T00:00:00Z in unix milliseconds.
const base = int64(1704067200000)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run returns its error instead of calling log.Fatal, so that the deferred
// cleanup below still runs when something fails.
func run() error {
	dir, err := os.MkdirTemp("", "zdb-query-")
	if err != nil {
		return err
	}

	defer os.RemoveAll(dir)

	s, err := zdb.Open[record.Bar](zdb.Options{Dir: dir})
	if err != nil {
		return err
	}

	defer s.Close()

	// 24 hourly candles, closes 100.00 through 123.00.
	bars := make([]record.Bar, 24)

	for i := range bars {
		closing := 100.00 + float64(i)

		bars[i] = record.Bar{
			Datetime: base + int64(i)*3_600_000,
			Open:     closing - 0.50,
			High:     closing + 0.25,
			Low:      closing - 0.75,
			Close:    closing,
			Volume:   uint64(100 + i),
		}
	}

	if err := s.Writer.AppendBatch(key, bars); err != nil {
		return err
	}

	q, err := s.Query()
	if err != nil {
		return err
	}

	from, to := uint64(base), uint64(base+12*3_600_000)

	// The text form. RANGE bounds the window, WHERE filters, ORDER BY picks the
	// direction, LIMIT caps the result.
	fmt.Println("text:")

	src := fmt.Sprintf(`SELECT datetime, close FROM %q
	                    RANGE %d TO %d
	                    WHERE close > 108
	                    ORDER BY datetime DESC
	                    LIMIT 5`, key, from, to)

	for bar, err := range q.Run(src) {
		if err != nil {
			return err
		}

		fmt.Printf("  %d close=%.2f\n", bar.Datetime, bar.Close)
	}

	// The same query in Go. A condition on datetime is folded into the binary
	// search, so it costs O(log n) rather than a test per record; every other
	// field is compared as the stored fixed-point integer.
	bq, err := zql.NewQuery(key).
		Between(from, to).
		Where(zql.FieldClose, zql.Gt, 108)
	if err != nil {
		return err
	}

	// Select narrows the projection, NewestFirst picks the direction and Take
	// caps the result.
	bq = bq.Select(zql.FDatetime | zql.FClose).NewestFirst().Take(5)

	fmt.Println("builder:")
	fmt.Printf("  %s\n", bq.String())

	found, err := q.Collect(bq)
	if err != nil {
		return err
	}

	for _, bar := range found {
		fmt.Printf("  %d close=%.2f\n", bar.Datetime, bar.Close)
	}

	// First is the single-record convenience: it reports ErrNoMatch rather than
	// a zero bar when the query is valid and matched nothing.
	newest, err := zql.NewQuery(key).
		Between(from, to).
		Where(zql.FieldClose, zql.Gte, 108)
	if err != nil {
		return err
	}

	bar, err := q.First(newest.NewestFirst())
	if err != nil {
		return err
	}

	fmt.Printf("newest over 108 in the window: %d close=%.2f\n", bar.Datetime, bar.Close)

	return nil
}
