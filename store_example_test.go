package zdb_test

import (
	"fmt"
	"os"

	"github.com/aldok10/zdb"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/zql"
)

// ExampleOpen is the whole initiation in one place: one Open for the writer,
// the reader and the query runner, and one Close for all three. The query runs
// straight after the append with no Refresh, because the first read of a key
// resolves the index and maps the segment itself.
func ExampleOpen() {
	dir, err := os.MkdirTemp("", "zdb-store-example")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	s, err := zdb.Open[record.Bar](zdb.Options{
		Dir:   dir,
		Query: zql.Options{MaxBars: 100},
	})
	if err != nil {
		panic(err)
	}

	defer s.Close()

	bars := []record.Bar{
		{Datetime: 1704067200000, Open: 100.00, High: 101.00, Low: 99.50, Close: 100.00, Volume: 10},
		{Datetime: 1704067200010, Open: 100.00, High: 101.50, Low: 99.75, Close: 101.00, Volume: 11},
		{Datetime: 1704067200020, Open: 101.00, High: 102.00, Low: 100.25, Close: 102.00, Volume: 12},
	}

	if err := s.Writer.AppendBatch("binance/spot/BTCUSDT", bars); err != nil {
		panic(err)
	}

	q, err := s.Query()
	if err != nil {
		panic(err)
	}

	for bar, err := range q.Run(`SELECT datetime, close FROM "binance/spot/BTCUSDT"
	                             WHERE close > 100.5
	                             LIMIT 2`) {
		if err != nil {
			panic(err)
		}

		fmt.Printf("%d %.2f\n", bar.Datetime, bar.Close)
	}

	// Output:
	// 1704067200010 101.00
	// 1704067200020 102.00
}
