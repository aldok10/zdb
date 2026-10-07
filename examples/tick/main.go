// Command tick stores ticks with record.Tick and reads them through the reader.
// ZQL is bar-shaped, so a tick store gets no query runner and Store.Query says
// so rather than returning a nil one.
//
//	go run ./examples/tick
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aldok10/zdb"
	"github.com/aldok10/zdb/record"
)

const key = "binance/spot/BTCUSDT"

// base is 2024-01-01T00:00:00Z in unix microseconds, the unit a tick uses.
const base = int64(1704067200000000)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run returns its error instead of calling log.Fatal, so that the deferred
// cleanup below still runs when something fails.
func run() error {
	dir, err := os.MkdirTemp("", "zdb-tick-")
	if err != nil {
		return err
	}

	defer os.RemoveAll(dir)

	s, err := zdb.Open[record.Tick](zdb.Options{Dir: dir})
	if err != nil {
		return err
	}

	defer s.Close()

	ticks := make([]record.Tick, 3)

	for i := range ticks {
		ticks[i] = record.Tick{
			Datetime: base + int64(i)*1000,
			Bid:      42000.50 + float64(i)/100,
			Ask:      42000.75 + float64(i)/100,
			Last:     42000.60 + float64(i)/100,
			Volume:   uint64(1 + i),
			// Flags say which of the three prices and the volume are valid on
			// this tick, so a consumer never reads a stale bid as a current one.
			Flags: record.TickFlagBid | record.TickFlagAsk | record.TickFlagLast | record.TickFlagVolume,
		}
	}

	if err := s.Writer.AppendBatch(key, ticks); err != nil {
		return err
	}

	// ZQL's field enum names OHLC columns, so a runner over ticks would answer
	// about columns this record does not have. This is the expected error, and it
	// is an error rather than a nil runner on purpose.
	if _, err := s.Query(); err != nil {
		fmt.Println(err)
	}

	// A tick store reads through the reader directly. Latest takes the newest n,
	// ascending.
	latest, err := s.Reader.Latest(key, 3)
	if err != nil {
		return err
	}

	for _, t := range latest {
		fmt.Printf("%d bid=%.2f ask=%.2f last=%.2f volume=%d\n",
			t.Datetime, t.Bid, t.Ask, t.Last, t.Volume)
	}

	return nil
}
