// Command quickstart opens a store in one call, writes a few candles, and reads
// them back with a ZQL query.
//
//	go run ./examples/quickstart
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aldok10/zdb"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/zql"
)

// One key is one shard: its own segment chain, its own lock.
const key = "binance/spot/BTCUSDT"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run returns its error instead of calling log.Fatal, so that the deferred
// cleanup below still runs when something fails.
func run() error {
	dir, err := os.MkdirTemp("", "zdb-quickstart-")
	if err != nil {
		return err
	}

	defer os.RemoveAll(dir)

	// zdb.Open is the writer, the reader and the query runner in one call,
	// which is the shape for a process that writes and reads one directory.
	s, err := zdb.Open[record.Bar](zdb.Options{
		Dir:   dir,
		Query: zql.Options{MaxBars: 5_000_000},
	})
	if err != nil {
		return err
	}

	defer s.Close()

	// Datetime is unix milliseconds and must strictly increase per key.
	base := int64(1704067200000) // 2024-01-01T00:00:00Z

	bars := make([]record.Bar, 5_000_000)

	for i := range bars {
		bars[i] = record.Bar{
			Datetime: base + int64(i)*60_000,
			Open:     100.00 + float64(i),
			High:     100.75 + float64(i),
			Low:      99.75 + float64(i),
			Close:    100.50 + float64(i),
			Volume:   uint64(1000 + i),
		}
	}

	// AppendBatch encodes the batch once and splits it across segments when it
	// is larger than one, so a million bars go in with no tuning at all.
	if err := s.Writer.AppendBatch(key, bars); err != nil {
		return err
	}

	q, err := s.Query()
	if err != nil {
		return err
	}

	start := time.Now()
	fmt.Printf("Start Query ASC: %v\n", start.UnixMilli())

	// A condition on datetime is folded into the binary search; every other
	// field is compared as the stored fixed-point integer.
	for bar, err := range q.Run(`SELECT datetime, close FROM "binance/spot/BTCUSDT"
	                             WHERE close >= 41243
								 ORDER BY datetime ASC
	                             LIMIT 10`) {
		if err != nil {
			return err
		}

		fmt.Printf("%d close=%.2f\n", bar.Datetime, bar.Close)
	}

	fmt.Printf("Finish Query ASC: %v, %v\n", time.Now().UnixMilli(), time.Since(start))

	start = time.Now()
	fmt.Printf("Start Query DESC: %v\n", start.UnixMilli())

	// A condition on datetime is folded into the binary search; every other
	// field is compared as the stored fixed-point integer.
	for bar, err := range q.Run(`SELECT datetime, close FROM "binance/spot/BTCUSDT"
	                             WHERE close <= 34111
								 ORDER BY datetime DESC
	                             LIMIT 10`) {
		if err != nil {
			return err
		}

		fmt.Printf("%d close=%.2f\n", bar.Datetime, bar.Close)
	}

	fmt.Printf("Finish Query DESC: %v, %v\n", time.Now().UnixMilli(), time.Since(start))

	return nil
}
