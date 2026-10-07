package writer_test

import (
	"fmt"
	"os"
	"time"

	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
)

// A Policy sizes flushes from the machine it runs on and the rate it is actually
// being written at, so SyncIfDue flushes as often as the exposure warrants rather
// than as often as a timer fired.
//
// The default budget is one percent of system RAM, which is what keeps the dirty
// page set clear of the kernel's own throttle, and that path depends on the host.
// This example pins the time trigger instead and drives the clock itself, so the
// output is identical on every platform.
func ExamplePolicy() {
	now := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)

	pol, err := writer.NewPolicy(writer.TuneOptions{
		RAMFraction: -1, // no RAM budget, so MaxInterval alone decides
		MaxInterval: 5 * time.Second,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		panic(err)
	}
	// RAMKnown is deliberately not printed. It is true on linux, darwin and
	// windows and false everywhere else, so an Output block containing it would
	// fail on every other target.

	dir, err := os.MkdirTemp("", "zdb-policy")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := writer.Open[record.Bar](writer.Options{Dir: dir, Sync: pol})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Three bars written in the same instant cannot have reached a five second
	// tolerance, so nothing flushes and each check is two atomic loads.
	const key = "binance/spot/BTCUSDT"

	flushed := 0

	for i := range 3 {
		err = db.Append(key, record.Bar{
			Datetime: 1_704_067_200_000 + int64(i)*1000,
			Open:     100, High: 101, Low: 99, Close: 100.5,
		})
		if err != nil {
			panic(err)
		}

		did, err := db.SyncIfDue()
		if err != nil {
			panic(err)
		}

		if did {
			flushed++
		}
	}

	fmt.Println("flushes after 3 appends:", flushed)
	fmt.Println("bytes at risk:", pol.AtRiskBytes())

	// Six seconds later the same call finds the tolerance exceeded, and flushes.
	now = now.Add(6 * time.Second)

	did, err := db.SyncIfDue()
	if err != nil {
		panic(err)
	}

	fmt.Println("flushed 6s later:", did)
	fmt.Println("bytes at risk after the flush:", pol.AtRiskBytes())
	// Output:
	// flushes after 3 appends: 0
	// bytes at risk: 180
	// flushed 6s later: true
	// bytes at risk after the flush: 0
}
