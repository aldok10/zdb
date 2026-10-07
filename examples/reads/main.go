// Command reads writes candles with package writer and reads them back with
// package reader. It shows the read shapes, and the one case that needs Refresh:
// a segment that appeared after the reader first resolved the key.
//
//	go run ./examples/reads
package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
)

const (
	key = "binance/spot/BTCUSDT"

	// base is 2024-01-01T00:00:00Z in unix milliseconds.
	base = int64(1704067200000)

	// segmentBytes is deliberately small so that the writes below roll over into
	// more than one segment and the Refresh case is visible. The default is
	// 16 MB, which holds far more candles than an example writes.
	segmentBytes = 4 << 10

	// batchSize keeps each AppendBatch small so the writes below cross segment
	// boundaries gradually and the Refresh case stays visible. A batch larger
	// than one segment would be split across segments automatically.
	batchSize = 50
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run returns its error instead of calling log.Fatal, so that the deferred
// cleanup below still runs when something fails.
func run() error {
	dir, err := os.MkdirTemp("", "zdb-reads-")
	if err != nil {
		return err
	}

	defer os.RemoveAll(dir)

	if err := appendBars(dir, 0, 10); err != nil {
		return err
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		return err
	}

	defer r.Close()

	// Cursor is the zero-allocation shape: the caller owns the destination
	// record and drives the loop. With no limit the walk is ascending.
	var c reader.Cursor[record.Bar]

	if err := r.Cursor(key, 0, 0, 0, &c); err != nil {
		return err
	}

	var bar record.Bar

	for c.Next(&bar) {
		fmt.Printf("%d close=%.2f\n", bar.Datetime, bar.Close)
	}

	if err := c.Err(); err != nil {
		return err
	}

	// Bounds and Count answer from the segment headers, without decoding a
	// record.
	first, last, err := r.Bounds(key)
	if err != nil {
		return err
	}

	n, err := r.Count(key)
	if err != nil {
		return err
	}

	fmt.Printf("count=%d first=%d last=%d\n", n, first, last)

	// The next session writes past the first segment's capacity, so the shard
	// rolls over. This reader resolved the key before that, and it caches the
	// key's chain of files, so the segment that appeared since is not in the
	// chain it is holding: what is on disk and what this reader can see have
	// disagreed since the moment of the roll.
	if err := appendBars(dir, 10, 100); err != nil {
		return err
	}

	stale, err := r.Count(key)
	if err != nil {
		return err
	}

	// Refresh maps segments created since the last read and drops the resolution
	// cache, so the next query re-reads the index. The mappings themselves are
	// left alone, because a segment's size never changes.
	if err := r.Refresh(); err != nil {
		return err
	}

	fresh, err := r.Count(key)
	if err != nil {
		return err
	}

	fmt.Printf("count before Refresh=%d, after Refresh=%d\n", stale, fresh)

	// Latest takes the newest n, ascending.
	latest, err := r.Latest(key, 3)
	if err != nil {
		return err
	}

	for _, bar := range latest {
		fmt.Printf("%d close=%.2f\n", bar.Datetime, bar.Close)
	}

	return nil
}

// appendBars appends count candles starting at offset in batches of batchSize,
// and closes the store.
func appendBars(dir string, offset, count int) error {
	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segmentBytes})
	if err != nil {
		return err
	}

	for done := 0; done < count; done += batchSize {
		n := min(batchSize, count-done)

		bars := make([]record.Bar, n)

		for i := range bars {
			j := offset + done + i

			bars[i] = record.Bar{
				Datetime: base + int64(j)*60_000,
				Open:     100.00 + float64(j),
				High:     100.75 + float64(j),
				Low:      99.75 + float64(j),
				Close:    100.50 + float64(j),
				Volume:   uint64(1000 + j),
			}
		}

		if err := w.AppendBatch(key, bars); err != nil {
			return errors.Join(err, w.Close())
		}
	}

	return w.Close()
}
