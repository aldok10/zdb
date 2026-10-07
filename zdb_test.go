package zdb_test

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aldok10/zdb/internal/format"
	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
)

// segBytes is small enough that a few hundred candles roll into several
// segments, so every test also exercises the rollover and cross-segment reads.
const segBytes = 8192

// minute is the candle spacing used throughout, in unix milliseconds.
const minute = 60_000

func newBar(t *testing.T, ts uint64, base float64) record.Bar {
	t.Helper()

	return newBarB(t, ts, base)
}

// stamp turns a minute offset from 2024-01-01 into unix milliseconds.
func stamp(minutes int) uint64 { return uint64(1_704_067_200_000 + int64(minutes)*60_000) }

func stampText(ms int64) string {
	return time.UnixMilli(ms).Format(time.RFC3339)
}

func TestAppendAndReadBack(t *testing.T) {
	dir := t.TempDir()

	const key = "binance/spot/BTCUSDT"

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	// Append in chunks; the small segment is what forces rollover mid-stream.
	const n, chunk = 500, 50
	for done := 0; done < n; done += chunk {
		batch := make([]record.Bar, min(chunk, n-done))
		for i := range batch {
			batch[i] = newBar(t, stamp(done+i), float64(100+done+i))
		}

		if err := w.AppendBatch(key, batch); err != nil {
			t.Fatalf("AppendBatch at %d: %v", done, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	got, err := r.Range(key, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != n {
		t.Fatalf("got %d candles, want %d", len(got), n)
	}

	for i, c := range got {
		want := newBar(t, stamp(i), float64(100+i))
		if c != want {
			t.Fatalf("candle %d: got %+v, want %+v", i, c, want)
		}
	}
}

// TestAppendBatchSpansSegments pins the bulk-load path: a batch larger than one
// segment is split across segments rather than refused, and the records come
// back in order with the count intact.
func TestAppendBatchSpansSegments(t *testing.T) {
	dir := t.TempDir()

	const key = "binance/spot/BTCUSDT"

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	// One segment holds segCap bars exactly, so a batch of 3*segCap+7 fills
	// three segments and starts a fourth.
	segCap := (segBytes - format.RecordOffset(len(key)) - format.TrailerBytes[record.Bar](segBytes)) / record.BarSize

	const extra = 7

	n := 3*segCap + extra

	batch := make([]record.Bar, n)
	for i := range batch {
		batch[i] = newBar(t, stamp(i), float64(100+i))
	}

	if err := w.AppendBatch(key, batch); err != nil {
		t.Fatalf("AppendBatch of %d records: %v", n, err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	if segs, err := r.Segments(key); err != nil || segs != 4 {
		t.Fatalf("Segments = %d, %v; want exactly 4", segs, err)
	}

	got, err := r.Range(key, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != n {
		t.Fatalf("got %d candles, want %d", len(got), n)
	}

	for i, c := range got {
		want := newBar(t, stamp(i), float64(100+i))
		if c != want {
			t.Fatalf("candle %d: got %+v, want %+v", i, c, want)
		}
	}
}

// TestAppendBatchSpanValidationCommitsNothing pins the failure half of the
// spanning path: an unencodable record is caught before the first chunk is
// committed, whether it sits in the first segment or in a later one. The proof
// is a clean retry of the same batch: order checking would refuse it if any
// prefix had been left stored.
func TestAppendBatchSpanValidationCommitsNothing(t *testing.T) {
	const key = "binance/spot/BTCUSDT"

	segCap := (segBytes - format.RecordOffset(len(key)) - format.TrailerBytes[record.Bar](segBytes)) / record.BarSize

	for _, tc := range []struct {
		name  string
		badAt int
	}{
		{"first segment", 5},
		{"later segment", segCap + 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
			if err != nil {
				t.Fatalf("writer.Open: %v", err)
			}

			n := 3*segCap + 7
			if tc.badAt >= n {
				t.Fatalf("badAt %d is outside a batch of %d", tc.badAt, n)
			}

			batch := make([]record.Bar, n)
			for i := range batch {
				batch[i] = newBar(t, stamp(i), float64(100+i))
			}

			batch[tc.badAt].Close = math.NaN()

			err = w.AppendBatch(key, batch)
			if err == nil {
				t.Fatal("want an error for the unencodable record, got nil")
			}

			if !strings.Contains(err.Error(), "non-finite price") {
				t.Fatalf("want the unencodable-record rejection, got: %v", err)
			}

			batch[tc.badAt].Close = float64(100 + tc.badAt)

			if err := w.AppendBatch(key, batch); err != nil {
				t.Fatalf("clean retry after the rejection: %v", err)
			}

			if err := w.Close(); err != nil {
				t.Fatalf("writer.Close: %v", err)
			}

			r, err := reader.Open[record.Bar](dir)
			if err != nil {
				t.Fatalf("reader.Open: %v", err)
			}
			defer r.Close()

			got, err := r.Count(key)
			if err != nil {
				t.Fatalf("Count: %v", err)
			}

			if got != uint64(n) {
				t.Fatalf("Count = %d, want %d", got, n)
			}
		})
	}
}

func TestSegmentRolloverKeepsWriteOrder(t *testing.T) {
	dir := t.TempDir()

	const key = "ETHUSDT"

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}
	// Append one at a time so rollover happens mid-stream, not on a boundary.
	const n = 400
	for i := range n {
		if err := w.Append(key, newBar(t, stamp(i), float64(i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	segs, _ := filepath.Glob(filepath.Join(dir, "*"+format.SegmentSuffix))
	if len(segs) < 2 {
		t.Fatalf("want multiple segments after rollover, got %d", len(segs))
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	// The index must reach every rolled-over segment, or the earlier candles
	// become invisible.
	if n, err := r.Segments(key); err != nil || n < 2 {
		t.Fatalf("Segments = %d, %v; want at least 2", n, err)
	}

	got, err := r.Range(key, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != n {
		t.Fatalf("got %d candles, want %d", len(got), n)
	}

	for i := 1; i < len(got); i++ {
		if got[i-1].Datetime >= got[i].Datetime {
			t.Fatalf("timestamps out of order at %d: %s then %s",
				i, stampText(got[i-1].Datetime), stampText(got[i].Datetime))
		}
	}
}

func TestRangeWindowAndLimit(t *testing.T) {
	dir := t.TempDir()

	const key = "SOLUSDT"

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	const n = 300
	for i := range n {
		if err := w.Append(key, newBar(t, stamp(i), float64(i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	tests := []struct {
		name      string
		from, to  uint64
		limit     int
		wantFirst int
		wantLast  int
		wantLen   int
	}{
		{name: "full history", wantFirst: 0, wantLast: n - 1, wantLen: n},
		{
			name: "bounded window", from: stamp(100), to: stamp(199),
			wantFirst: 100, wantLast: 199, wantLen: 100,
		},
		{name: "limit keeps newest", limit: 10, wantFirst: n - 10, wantLast: n - 1, wantLen: 10},
		{
			name: "window plus limit keeps newest in window",
			from: stamp(100), to: stamp(199), limit: 5,
			wantFirst: 195, wantLast: 199, wantLen: 5,
		},
		{name: "window past the end", from: stamp(250), wantFirst: 250, wantLast: n - 1, wantLen: n - 250},
		{name: "empty window", from: stamp(500), to: stamp(600), wantLen: 0},
		{name: "single candle", from: stamp(42), to: stamp(42), wantFirst: 42, wantLast: 42, wantLen: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.Range(key, tc.from, tc.to, tc.limit)
			if err != nil {
				t.Fatalf("Range: %v", err)
			}

			if len(got) != tc.wantLen {
				t.Fatalf("got %d candles, want %d", len(got), tc.wantLen)
			}

			if len(got) == 0 {
				return
			}

			if got[0].Datetime != int64(stamp(tc.wantFirst)) {
				t.Fatalf("first candle %s, want %s", stampText(got[0].Datetime), stampText(int64(stamp(tc.wantFirst))))
			}

			if got[len(got)-1].Datetime != int64(stamp(tc.wantLast)) {
				t.Fatalf("last candle %s, want %s", stampText(got[len(got)-1].Datetime), stampText(int64(stamp(tc.wantLast))))
			}
		})
	}
}

func TestLatest(t *testing.T) {
	dir := t.TempDir()

	const key = "XRPUSDT"

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	for i := range 250 {
		if err := w.Append(key, newBar(t, stamp(i), float64(i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	w.Close()

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	got, err := r.Latest(key, 3)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("got %d candles, want 3", len(got))
	}

	for i, wantOffset := range []int{247, 248, 249} {
		if got[i].Datetime != int64(stamp(wantOffset)) {
			t.Fatalf("got[%d] %s, want %s", i, stampText(got[i].Datetime), stampText(int64(stamp(wantOffset))))
		}
	}
}

func TestRejectsNonIncreasingTimestamps(t *testing.T) {
	dir := t.TempDir()

	const key = "ADAUSDT"

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	defer w.Close()

	if err := w.Append(key, newBar(t, stamp(0), 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	t.Run("older than last", func(t *testing.T) {
		err := w.Append(key, newBar(t, stamp(0)-minute, 1))
		if !errors.Is(err, format.ErrOutOfOrder) {
			t.Fatalf("got %v, want ErrOutOfOrder", err)
		}
	})
	t.Run("equal to last", func(t *testing.T) {
		err := w.Append(key, newBar(t, stamp(0), 1))
		if !errors.Is(err, format.ErrOutOfOrder) {
			t.Fatalf("got %v, want ErrOutOfOrder", err)
		}
	})
	t.Run("inside batch", func(t *testing.T) {
		err := w.AppendBatch(key, []record.Bar{
			newBar(t, stamp(1), 1),
			newBar(t, stamp(3), 1),
			newBar(t, stamp(2), 1),
		})
		if !errors.Is(err, format.ErrOutOfOrder) {
			t.Fatalf("got %v, want ErrOutOfOrder", err)
		}
	})
}

func TestShardsAreIsolated(t *testing.T) {
	dir := t.TempDir()
	keys := []string{"binance/spot/BTCUSDT", "binance/spot/ETHUSDT", "bybit/BTCUSDT", "BTCUSDT"}

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	defer w.Close()

	for i, key := range keys {
		for j := range 100 {
			if err := w.Append(key, newBar(t, stamp(j), float64(i*1000+j))); err != nil {
				t.Fatalf("Append %s: %v", key, err)
			}
		}
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	// Same basename, different shard path: values must not bleed across keys.
	for i, key := range keys {
		got, err := r.Range(key, 0, 0, 0)
		if err != nil {
			t.Fatalf("Range %s: %v", key, err)
		}

		if len(got) != 100 {
			t.Fatalf("%s: got %d candles, want 100", key, len(got))
		}

		for j, c := range got {
			// base = i*1000+j, so a close from another shard is unmistakable.
			if cl := c.Close; cl != float64(i*1000+j)+0.5 {
				t.Fatalf("%s candle %d: close %v belongs to another shard", key, j, cl)
			}
		}
	}

	if got := r.List("binance/spot/"); !slices.Equal(got, keys[:2]) {
		t.Fatalf("List prefix got %v, want %v", got, keys[:2])
	}

	if got := r.Keys(); !slices.Equal(got, slices.Sorted(slices.Values(keys))) {
		t.Fatalf("Keys got %v, want %v", got, keys)
	}
}

func TestReopenResumesAppending(t *testing.T) {
	dir := t.TempDir()

	const key = "DOGEUSDT"

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	for i := range 100 {
		if err := w.Append(key, newBar(t, stamp(i), 1)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second writer handle over the same directory continues the same shard.
	w2, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("reopen writer: %v", err)
	}

	for i := 100; i < 200; i++ {
		if err := w2.Append(key, newBar(t, stamp(i), 2)); err != nil {
			t.Fatalf("Append after reopen: %v", err)
		}
	}

	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, _ := reader.Open[record.Bar](dir)
	defer r.Close()

	got, err := r.Range(key, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != 200 {
		t.Fatalf("got %d candles, want 200", len(got))
	}

	if cl := got[99].Close; cl != 1.5 {
		t.Fatalf("candle 99 was overwritten by the second writer: %v", cl)
	}
}

func TestRefreshPicksUpNewSegments(t *testing.T) {
	dir := t.TempDir()

	const key = "AVAXUSDT"

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	defer w.Close()

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	if _, err := r.Range(key, 0, 0, 0); !errors.Is(err, reader.ErrKeyNotFound) {
		t.Fatalf("got %v, want ErrKeyNotFound", err)
	}

	for i := range 200 {
		if err := w.Append(key, newBar(t, stamp(i), 1)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	got, err := r.Range(key, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != 200 {
		t.Fatalf("got %d candles, want 200", len(got))
	}
}

// TestConcurrentWriteAndRead is the point of the seqlock: a reader must never
// observe a half-written record while a writer appends.
func TestConcurrentWriteAndRead(t *testing.T) {
	dir := t.TempDir()

	const (
		key = "LINKUSDT"
		n   = 4000
	)

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	defer w.Close()

	// Create the shard before the readers start: a reader maps what exists, so
	// Refresh is the hand-off point for a shard created later.
	if err := w.Append(key, newBar(t, stamp(0), 0)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 1; i < n; i++ {
			if err := w.Append(key, newBar(t, stamp(i), float64(i))); err != nil {
				t.Errorf("Append %d: %v", i, err)

				return
			}
		}
	}()

	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 200 {
				got, err := r.Range(key, 0, 0, 0)
				if err != nil {
					t.Errorf("Range during writes: %v", err)

					return
				}

				for i := 1; i < len(got); i++ {
					if got[i-1].Datetime >= got[i].Datetime {
						t.Errorf("torn read: %s then %s",
							stampText(got[i-1].Datetime), stampText(got[i].Datetime))

						return
					}
				}

				for _, c := range got {
					// A partially written record would decode to garbage here.
					if c.Close == 0 || c.High < c.Low {
						t.Errorf("torn record at %s: %+v", stampText(c.Datetime), c)

						return
					}
				}
			}
		}()
	}

	wg.Wait()
}

// roundTrip writes one bar and reads it back through the store. Everything below
// the store boundary is unexported on purpose, so an end-to-end pass is the only
// honest proof that encoding, offsets and decoding all agree.
func roundTrip(t *testing.T, in record.Bar) (record.Bar, error) {
	t.Helper()
	dir := t.TempDir()

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	if err = w.Append("roundtrip", in); err != nil {
		w.Close()

		return record.Bar{}, err
	}

	if err = w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	got, err := r.Range("roundtrip", 0, 0, 0)
	if err != nil {
		return record.Bar{}, err
	}

	if len(got) != 1 {
		t.Fatalf("got %d bars, want 1", len(got))
	}

	return got[0], nil
}

func TestBarRoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		o, h, l, c float64
	}{
		{name: "integers", o: 100, h: 101, l: 99, c: 100},
		{name: "eight decimals", o: 0.00000001, h: 0.00000002, l: 0.00000001, c: 0.00000004},
		{name: "large price", o: 123456.789, h: 123460, l: 123450.5, c: 123455.25},
		{name: "zero", o: 0, h: 0, l: 0, c: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := record.Bar{
				Datetime:   int64(stamp(0)),
				Open:       tc.o,
				High:       tc.h,
				Low:        tc.l,
				Close:      tc.c,
				TickVolume: 42,
				Spread:     7,
				Volume:     12345,
			}

			got, err := roundTrip(t, in)
			if err != nil {
				t.Fatalf("round trip: %v", err)
			}
			// Every field, not just the prices: a volume or spread that silently
			// lost its value would still pass a price-only check.
			if got != in {
				t.Fatalf("round trip: got %+v, want %+v", got, in)
			}
		})
	}
}

// PriceScale fixes the precision ceiling: a price finer than one resolution step
// rounds rather than being stored, so a round trip's error is bounded by half a
// step. Raising PriceScale trades range for precision, so this pins the bound
// rather than one rounded literal, which would only be testing how 1.5e-8
// happens to be represented as a float.
func TestPriceScaleRoundsBelowResolution(t *testing.T) {
	half := 1 / (2 * record.PriceScale)

	for _, price := range []float64{1.5e-8, 2.4e-8, 7.7e-8, 1e-9, 123456.789_123_456_7} {
		got, err := roundTrip(t, record.Bar{Datetime: int64(stamp(0)), Open: price, High: 1, Low: 1, Close: 1})
		if err != nil {
			t.Fatalf("round trip(%v): %v", price, err)
		}

		if math.Abs(got.Open-price) > half {
			t.Fatalf("price %v came back as %v, error exceeds half a step (%v)", price, got.Open, half)
		}
	}
}

// Append is the trust boundary, so an unrepresentable bar must be refused there
// and must leave nothing behind.
func TestAppendRejectsInvalid(t *testing.T) {
	dir := t.TempDir()

	const key = "invalid/BTCUSDT"

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	if err := w.Append(key, record.Bar{Datetime: int64(stamp(0)), Open: 1, High: 1, Low: 1, Close: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	// The count after each rejection must not move: a rejected bar that still
	// published a slot would leave a hole the reader would decode as a bar.
	for _, tc := range []struct {
		name string
		in   record.Bar
	}{
		{"NaN", record.Bar{Datetime: int64(stamp(1)), Open: math.NaN(), High: 1, Low: 1, Close: 1}},
		{"Inf", record.Bar{Datetime: int64(stamp(1)), Open: 1, High: math.Inf(1), Low: 1, Close: 1}},
		{"negative price", record.Bar{Datetime: int64(stamp(1)), Open: 1, High: 1, Low: -0.5, Close: 1}},
		{"overflow", record.Bar{Datetime: int64(stamp(1)), Open: 1, High: 1, Low: 1, Close: 1e30}},
		{"negative datetime", record.Bar{Datetime: -1, Open: 1, High: 1, Low: 1, Close: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
			if err != nil {
				t.Fatalf("writer.Open: %v", err)
			}
			defer w.Close()

			if err := w.Append(key, tc.in); err == nil {
				t.Fatal("want an error, got nil")
			}
		})
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	n, err := r.Count(key)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}

	if n != 1 {
		t.Fatalf("rejected bars left %d stored, want 1", n)
	}
}

func TestCountAndBounds(t *testing.T) {
	dir := t.TempDir()

	const key = "MATICUSDT"

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	for i := range 350 {
		if err := w.Append(key, newBar(t, stamp(i), 1)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	w.Close()

	r, _ := reader.Open[record.Bar](dir)
	defer r.Close()

	count, err := r.Count(key)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}

	if count != 350 {
		t.Fatalf("Count got %d, want 350", count)
	}

	first, last, err := r.Bounds(key)
	if err != nil {
		t.Fatalf("Bounds: %v", err)
	}

	if first != stamp(0) {
		t.Fatalf("first %s, want %s", stampText(int64(first)), stampText(int64(stamp(0))))
	}

	if want := stamp(349); last != want {
		t.Fatalf("last %s, want %s", stampText(int64(last)), stampText(int64(want)))
	}
}

func TestScanStopsEarly(t *testing.T) {
	dir := t.TempDir()

	const key = "LTCUSDT"

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	defer w.Close()

	for i := range 300 {
		if err := w.Append(key, newBar(t, stamp(i), 1)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	r, _ := reader.Open[record.Bar](dir)
	defer r.Close()

	var seen int

	err := r.Scan(key, 0, 0, 0, func(record.Bar) bool {
		seen++

		return seen < 17
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if seen != 17 {
		t.Fatalf("Scan visited %d candles, want 17", seen)
	}
}

func TestSealSplitsSegments(t *testing.T) {
	dir := t.TempDir()

	const key = "BNBUSDT"

	w, _ := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	defer w.Close()

	for i := range 50 {
		if err := w.Append(key, newBar(t, stamp(i), 1)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := w.Seal(key); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	for i := 50; i < 100; i++ {
		if err := w.Append(key, newBar(t, stamp(i), 1)); err != nil {
			t.Fatalf("Append after seal: %v", err)
		}
	}

	r, _ := reader.Open[record.Bar](dir)
	defer r.Close()

	if n, err := r.Segments(key); err != nil || n != 2 {
		t.Fatalf("Segments = %d, %v; want 2", n, err)
	}

	got, err := r.Range(key, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != 100 {
		t.Fatalf("got %d candles, want 100", len(got))
	}
}

// A bucket is a wall-clock boundary the writer rolls on, so a segment becomes a
// deletable unit of time instead of a span that straddles the cut. The stamp
// unit comes from the record type: a candle is milliseconds, so one hour is
// 3_600_000 stamps and not 3_600_000_000_000.
func TestBucketDurationRollsAtTheBoundary(t *testing.T) {
	const (
		key  = "binance/spot/BTCUSDT"
		hour = int64(time.Hour / time.Millisecond)
	)

	// 2023-11-14T22:00:00Z is exactly divisible by the hour, so adding to it
	// stays inside one bucket until the hour is added in full.
	const edge = 472_222 * hour

	cases := []struct {
		name   string
		bucket time.Duration
		stamps []int64
		segs   int
	}{
		{
			name:   "inside one bucket",
			bucket: time.Hour,
			stamps: []int64{edge + 400_000, edge + 460_000},
			segs:   1,
		},
		{
			name:   "crossing the boundary",
			bucket: time.Hour,
			stamps: []int64{edge + 400_000, edge + hour},
			segs:   2,
		},
		{
			name:   "off by default",
			bucket: 0,
			stamps: []int64{edge + 400_000, edge + hour},
			segs:   1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			w, err := writer.Open[record.Bar](writer.Options{
				Dir:            dir,
				SegmentBytes:   segBytes,
				BucketDuration: tc.bucket,
			})
			if err != nil {
				t.Fatalf("writer.Open: %v", err)
			}

			for i, ts := range tc.stamps {
				if err := w.Append(key, newBar(t, uint64(ts), float64(i))); err != nil {
					t.Fatalf("Append %d: %v", i, err)
				}
			}

			if err := w.Close(); err != nil {
				t.Fatalf("writer.Close: %v", err)
			}

			r, err := reader.Open[record.Bar](dir)
			if err != nil {
				t.Fatalf("reader.Open: %v", err)
			}

			defer r.Close()

			if n, err := r.Segments(key); err != nil || n != tc.segs {
				t.Fatalf("Segments = %d, %v; want %d", n, err, tc.segs)
			}

			if len(tc.stamps) < 2 {
				return
			}

			got, err := r.Range(key, 0, 0, 0)
			if err != nil {
				t.Fatalf("Range: %v", err)
			}

			if len(got) != len(tc.stamps) {
				t.Fatalf("got %d candles, want %d", len(got), len(tc.stamps))
			}
		})
	}
}

// A bucket shorter than the record's own time unit would floor to zero stamps
// and every stamp would land in one bucket, so it is refused at Open rather than
// silently doing nothing.
func TestBucketDurationShorterThanTheUnitIsRefused(t *testing.T) {
	_, err := writer.Open[record.Bar](writer.Options{
		Dir:            t.TempDir(),
		BucketDuration: 500 * time.Microsecond,
	})
	if err == nil {
		t.Fatal("Open with a sub-millisecond bucket succeeded; want an error")
	}
}

// benchStore writes n bars for one key and returns an open Reader over it. Every
// read benchmark shares it so they all measure the same on-disk shape: one key,
// one segment, 200k records.
func benchStore(b *testing.B, key string, n int) *reader.Reader[record.Bar] {
	b.Helper()
	dir := b.TempDir()

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: 32 << 20})
	if err != nil {
		b.Fatalf("writer.Open: %v", err)
	}

	const chunk = 1000

	batch := make([]record.Bar, chunk)
	for done := 0; done < n; done += chunk {
		for i := range min(chunk, n-done) {
			batch[i] = newBarB(b, stamp(done+i), float64(done+i))
		}

		if err := w.AppendBatch(key, batch[:min(chunk, n-done)]); err != nil {
			b.Fatalf("AppendBatch: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		b.Fatalf("writer.Close: %v", err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		b.Fatalf("reader.Open: %v", err)
	}

	b.Cleanup(func() { r.Close() })

	return r
}

// The window every read benchmark uses: 501 candles out of 200k records.
func benchWindow() (from, to uint64) { return stamp(100_000), stamp(100_500) }

// All must be indistinguishable from Range except that it does not build the
// slice, and stopping the loop has to stop the walk.
func TestAllMatchesRangeAndStops(t *testing.T) {
	dir := t.TempDir()

	const key = "all/BTCUSDT"

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	for i := range 1000 {
		if err := w.Append(key, newBarB(t, stamp(i), float64(i))); err != nil {
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
	defer r.Close()

	want, err := r.Range(key, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	var got []record.Bar

	for bar, err := range r.All(key, 0, 0, 0) {
		if err != nil {
			t.Fatalf("All: %v", err)
		}

		got = append(got, bar)
	}

	if len(got) != len(want) {
		t.Fatalf("All yielded %d bars, Range returned %d", len(got), len(want))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("bar %d: All gave %+v, Range gave %+v", i, got[i], want[i])
		}
	}

	// Breaking early must not cost the whole series, and must not report an error.
	n := 0

	for bar, err := range r.All(key, 0, 0, 0) {
		if err != nil {
			t.Fatalf("All: %v", err)
		}

		if n == 0 && bar.Datetime != int64(stamp(0)) {
			t.Fatalf("bar 0 at %d, want %d", bar.Datetime, stamp(0))
		}

		if n >= 3 {
			t.Fatalf("All kept going after the break would have fired, at bar %d", n)
		}

		n++
		if n == 3 {
			break
		}
	}

	if n != 3 {
		t.Fatalf("break after %d bars, want 3", n)
	}

	// A limit must reach All the same way it reaches Range: the newest bars win.
	tail, err := r.Latest(key, 5)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}

	var streamed []record.Bar

	for bar, err := range r.All(key, 0, 0, 5) {
		if err != nil {
			t.Fatalf("All: %v", err)
		}

		streamed = append(streamed, bar)
	}

	if len(streamed) != len(tail) {
		t.Fatalf("limited All yielded %d bars, Latest returned %d", len(streamed), len(tail))
	}

	for i := range tail {
		if streamed[i] != tail[i] {
			t.Fatalf("limited bar %d: All gave %+v, Latest gave %+v", i, streamed[i], tail[i])
		}
	}

	// An error has to reach the loop, not be swallowed by an empty iteration.
	sawErr := false

	for _, err := range r.All("no/such/key", 0, 0, 0) {
		if errors.Is(err, reader.ErrKeyNotFound) {
			sawErr = true
		}
	}

	if !sawErr {
		t.Fatal("All on a missing key yielded no error")
	}
}

// The whole point of All: a large series streamed costs one small allocation
// instead of a slice holding every bar. Compare with BenchmarkRangeFullSeries.
func BenchmarkAllFullSeries(b *testing.B) {
	const key = "bench/BTCUSDT"

	r := benchStore(b, key, 50_000)

	b.ReportAllocs()

	for b.Loop() {
		n := 0

		for _, err := range r.All(key, 0, 0, 0) {
			if err != nil {
				b.Fatal(err)
			}

			n++
		}

		if n != 50_000 {
			b.Fatalf("yielded %d, want 50000", n)
		}
	}
}

func BenchmarkRangeFullSeries(b *testing.B) {
	const key = "bench/BTCUSDT"

	r := benchStore(b, key, 50_000)

	b.ReportAllocs()

	for b.Loop() {
		got, err := r.Range(key, 0, 0, 0)
		if err != nil {
			b.Fatal(err)
		}

		if len(got) != 50_000 {
			b.Fatalf("got %d, want 50000", len(got))
		}
	}
}

func BenchmarkAllWindow(b *testing.B) {
	const key = "bench/BTCUSDT"

	r := benchStore(b, key, 200_000)
	from, to := benchWindow()

	b.ReportAllocs()

	for b.Loop() {
		n := 0

		for _, err := range r.All(key, from, to, 0) {
			if err != nil {
				b.Fatal(err)
			}

			n++
		}

		if n != 501 {
			b.Fatalf("yielded %d, want 501", n)
		}
	}
}

func BenchmarkRangeWindow(b *testing.B) {
	const key = "bench/BTCUSDT"

	r := benchStore(b, key, 200_000)
	from, to := benchWindow()

	b.ReportAllocs()

	for b.Loop() {
		got, err := r.Range(key, from, to, 0)
		if err != nil {
			b.Fatal(err)
		}

		if len(got) != 501 {
			b.Fatalf("got %d, want 501", len(got))
		}
	}
}

func BenchmarkRangeIntoWindow(b *testing.B) {
	const key = "bench/BTCUSDT"

	r := benchStore(b, key, 200_000)
	from, to := benchWindow()
	buf := make([]record.Bar, 0, 1024)

	b.ReportAllocs()

	for b.Loop() {
		got, err := r.RangeInto(key, from, to, 0, buf)
		if err != nil {
			b.Fatal(err)
		}

		if len(got) != 501 {
			b.Fatalf("got %d, want 501", len(got))
		}
	}
}

func BenchmarkScanWindow(b *testing.B) {
	const key = "bench/BTCUSDT"

	r := benchStore(b, key, 200_000)
	from, to := benchWindow()

	b.ReportAllocs()

	for b.Loop() {
		n := 0

		err := r.Scan(key, from, to, 0, func(record.Bar) bool {
			n++

			return true
		})
		if err != nil {
			b.Fatal(err)
		}

		if n != 501 {
			b.Fatalf("visited %d, want 501", n)
		}
	}
}

func BenchmarkCursorWindow(b *testing.B) {
	const key = "bench/BTCUSDT"

	r := benchStore(b, key, 200_000)
	from, to := benchWindow()

	var (
		cur reader.Cursor[record.Bar]
		out record.Bar
	)

	b.ReportAllocs()

	for b.Loop() {
		if err := r.Cursor(key, from, to, 0, &cur); err != nil {
			b.Fatal(err)
		}

		n := 0
		for cur.Next(&out) {
			n++
		}

		if err := cur.Err(); err != nil {
			b.Fatal(err)
		}

		if n != 501 {
			b.Fatalf("visited %d, want 501", n)
		}
	}
}

// A cold key builds a path string and a segment slice once, which is the whole
// cost of the resolution cache. Every query after that must be free.
func TestQueryPathIsZeroAllocation(t *testing.T) {
	const key = "alloc/BTCUSDT"

	dir := t.TempDir()

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: 8 << 20})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	for done := 0; done < 20_000; done += 1000 {
		batch := make([]record.Bar, 1000)
		for i := range batch {
			batch[i] = newBarB(t, stamp(done+i), float64(done+i))
		}

		if err := w.AppendBatch(key, batch); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	from, to := stamp(5_000), stamp(6_000)

	warm := func(record.Bar) bool { return true }
	if err := r.Scan(key, from, to, 0, warm); err != nil {
		t.Fatalf("warm Scan: %v", err)
	}

	var (
		cur reader.Cursor[record.Bar]
		out record.Bar
	)

	// Cursor is the zero-allocation path and stays that way: the caller owns the
	// cursor, so the decode destination is a field of something already on the
	// heap and Next copies out of it. A chart reads bar by bar through here.
	if n := testing.AllocsPerRun(200, func() {
		if err := r.Cursor(key, from, to, 0, &cur); err != nil {
			t.Fatal(err)
		}

		for cur.Next(&out) {
		}

		if err := cur.Err(); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("Cursor allocated %.1f times per call, want 0", n)
	}
}

// TestQueryWrappersAllocateOncePerQuery pins the cost of the convenience
// wrappers, which build a cursor the caller did not supply.
//
// They cannot be zero: the decode destination is a field of that cursor, its
// address is handed to a method reached through a type parameter, and the
// compiler cannot prove such a parameter does not escape. So the local cursor
// moves to the heap, once. The number is the thing that matters and it is
// measured at 10, 100, 1k, 10k and 100k records: always 1, for every shape. That
// is the same bargain Range already makes and the same one zql documents in
// AGENTS.md 6.5, three allocations to answer one query whether it returns 501
// bars or 500,000.
//
// What this test protects is the fixity. A per-record allocation would pass a
// "less than 2 per query" check at 1k records and fail everything a chart
// actually does, so the assertion compares two record counts rather than trusting
// the absolute number.
func TestQueryWrappersAllocateOncePerQuery(t *testing.T) {
	const key = "alloc/BTCUSDT"

	counts := map[string]float64{}

	for _, total := range []int{1_000, 100_000} {
		dir := t.TempDir()

		w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: 32 << 20})
		if err != nil {
			t.Fatal(err)
		}

		for done := 0; done < total; done += 1000 {
			end := min(done+1000, total)

			batch := make([]record.Bar, 0, end-done)
			for i := done; i < end; i++ {
				batch = append(batch, newBarB(t, stamp(i), float64(i)))
			}

			if err := w.AppendBatch(key, batch); err != nil {
				t.Fatal(err)
			}
		}

		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		r, err := reader.Open[record.Bar](dir)
		if err != nil {
			t.Fatal(err)
		}

		from, to := stamp(0), stamp(total-1)

		warm := func(record.Bar) bool { return true }
		if err := r.Scan(key, from, to, 0, warm); err != nil {
			t.Fatal(err) // warm the mapping and the page cache
		}

		buf := make([]record.Bar, 0, total)

		for _, tc := range []struct {
			name string
			run  func()
		}{
			{"Scan", func() {
				if err := r.Scan(key, from, to, 0, warm); err != nil {
					t.Fatal(err)
				}
			}},
			{"RangeInto", func() {
				if _, err := r.RangeInto(key, from, to, 0, buf); err != nil {
					t.Fatal(err)
				}
			}},
			{"All", func() {
				for range r.All(key, from, to, 0) {
				}
			}},
		} {
			n := testing.AllocsPerRun(200, tc.run)
			if n > 1 {
				t.Errorf("%s over %d records allocated %.1f times per call, want at most 1",
					tc.name, total, n)
			}

			counts[fmt.Sprintf("%s@%d", tc.name, total)] = n
		}

		r.Close()
	}

	for name, n := range counts {
		t.Logf("%-18s %.0f allocations per call", name, n)
	}

	for _, name := range []string{"Scan", "RangeInto", "All"} {
		small := counts[fmt.Sprintf("%s@1000", name)]

		big := counts[fmt.Sprintf("%s@100000", name)]
		if small != big {
			t.Errorf("%s allocations scale with record count: %.0f at 1000 records, %.0f at 100000",
				name, small, big)
		}
	}
}

func newBarB(tb testing.TB, ts uint64, base float64) record.Bar {
	tb.Helper()
	// Low sits one tick under open, so base must leave room for it.
	return record.Bar{
		Datetime: int64(ts),
		Open:     base + 1,
		High:     base + 2,
		Low:      base,
		Close:    base + 0.5,
		Volume:   uint64(base * 10),
	}
}

// benchAppender appends batches that always move time forward. That part is not
// optional: timestamps must strictly increase, so a benchmark that appends a
// fixed batch can only ever append it once and then fail.
type benchAppender struct {
	w   *writer.DB[record.Bar]
	key string
	buf []record.Bar
	n   int
}

func (a *benchAppender) run(b *testing.B) {
	b.Helper()

	for i := range a.buf {
		a.buf[i] = newBarB(b, stamp(a.n+i), float64(a.n+i))
	}

	a.n += len(a.buf)
	if err := a.w.AppendBatch(a.key, a.buf); err != nil {
		b.Fatal(err)
	}
}

func newBenchAppender(b *testing.B, key string, pol ...*writer.Policy) *benchAppender {
	b.Helper()
	// A 256 MB segment keeps a run from spending its time in rollover, which is
	// file creation and would drown out the thing being measured.
	var sync *writer.Policy
	if len(pol) > 0 {
		sync = pol[0]
	}

	w, err := writer.Open[record.Bar](writer.Options{Dir: b.TempDir(), SegmentBytes: 256 << 20, Sync: sync})
	if err != nil {
		b.Fatalf("writer.Open: %v", err)
	}

	b.Cleanup(func() { w.Close() })

	return &benchAppender{w: w, key: key, buf: make([]record.Bar, 256)}
}

// Appending must cost memory speed, not disk speed. A MAP_SHARED write lands in
// the page cache and the kernel's writeback is what batches it, so this is the
// number that says whether the database also buffers in userspace. It is where a
// write-ahead log would show up: an extra copy per record, on top of the mapping
// write, before a single byte reaches the kernel.
func BenchmarkAppendThroughput(b *testing.B) {
	a := newBenchAppender(b, "bench/BTCUSDT")
	b.ReportAllocs()

	for b.Loop() {
		a.run(b)
	}
}

// BenchmarkAppendBatchSizes prices the batching: the lock, seqlock window and
// header publish are paid once per batch, so ns/record must fall as the batch
// grows. It is the measurement behind "one write barrier per symbol per tick".
func BenchmarkAppendBatchSizes(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			w, err := writer.Open[record.Bar](writer.Options{Dir: b.TempDir(), SegmentBytes: 256 << 20})
			if err != nil {
				b.Fatal(err)
			}

			b.Cleanup(func() { w.Close() })

			buf := make([]record.Bar, n)
			n0 := 0

			b.ReportAllocs()

			for b.Loop() {
				for i := range buf {
					buf[i] = newBarB(b, stamp(n0+i), float64(n0+i))
				}

				n0 += n

				if err := w.AppendBatch("bench/BTCUSDT", buf); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportMetric(float64(b.N*n)/b.Elapsed().Seconds(), "recs/s")
		})
	}
}

// BenchmarkAppendWithPolicy prices the auto-sync accounting on the write path. A
// store with a Policy installed does two atomic adds per batch, and the only
// honest way to know what that costs is to measure it against a store without one
// rather than to argue that atomics are cheap.
//
// The policy is never allowed to flush here: the budget is sized so that the whole
// run stays under it, which is what isolates the cost of counting from the cost of
// the barrier, which is 10 ms and would swamp everything.
func BenchmarkAppendWithPolicy(b *testing.B) {
	pol, err := writer.NewPolicy(writer.TuneOptions{RAMFraction: 0.5})
	if err != nil {
		b.Fatal(err)
	}

	a := newBenchAppender(b, "bench/BTCUSDT", pol)
	b.ReportAllocs()

	for b.Loop() {
		a.run(b)
	}

	if pol.Due() {
		b.Fatal("the run crossed the policy budget, so this measured a flush")
	}
}

// Sync is what costs IOPS, and the cost is the call rather than the volume: a
// flush is a filesystem commit, so a 60 KB flush costs about what a 3 MB flush
// does. AGENTS.md section 4.4 has the measurement: 50000 records flush in about
// 10 ms whether the mapping is 4 MB or 256 MB.
//
// The lever is how often Sync is called, not how much is written. This benchmark
// and the batched one below exist to keep that visible: a change that makes Sync
// cheaper must not be one that starts calling it more often behind the caller's
// back.
func BenchmarkAppendSyncPerTick(b *testing.B) {
	a := newBenchAppender(b, "bench/BTCUSDT")
	b.ReportAllocs()

	for b.Loop() {
		a.run(b)

		if err := a.w.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

// The same work with one flush at the end. The gap against the benchmark above is
// the whole cost of durability, and it is what batching is for.
func BenchmarkAppendSyncBatched(b *testing.B) {
	a := newBenchAppender(b, "bench/BTCUSDT")
	b.ReportAllocs()

	for b.Loop() {
		a.run(b)
	}

	if err := a.w.Sync(); err != nil {
		b.Fatal(err)
	}
}

// A segment file whose header names a different key than the shard it was opened
// for is a store that has been moved, renamed or copied around. It must be
// refused, and the refusal must be reportable: reading the key out of the header
// after the mapping has been released is a fault rather than an error message, so
// this exists to keep that read on the safe side of the Munmap.
func TestWriterRefusesASegmentHoldingAnotherKey(t *testing.T) {
	dir := t.TempDir()

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendBatch("a/AAA", []record.Bar{newBarB(t, stamp(0), 100)}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Rename the file onto another key's segment name. Its header still says
	// "a/AAA", so opening it as "b/BBB" finds a key that is not the one asked for.
	from := filepath.Join(dir, fmt.Sprintf("%s-0%s", format.KeyID("a/AAA"), format.SegmentSuffix))

	to := filepath.Join(dir, fmt.Sprintf("%s-0%s", format.KeyID("b/BBB"), format.SegmentSuffix))
	if err := os.Rename(from, to); err != nil {
		t.Fatal(err)
	}

	w2, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	err = w2.AppendBatch("b/BBB", []record.Bar{newBarB(t, stamp(1), 101)})
	if err == nil {
		t.Fatal("appending into a segment holding another key was accepted")
	}

	if !strings.Contains(err.Error(), "a/AAA") {
		t.Fatalf("error %q does not name the key the file actually holds", err)
	}
}

// micro is unix microseconds for the i'th tick, one millisecond apart, which is
// the resolution a millisecond key cannot express. Query bounds are uint64
// because a segment compares timestamps unsigned, so umicro is the same value in
// the type the reader takes.
func micro(i int) int64 { return 1_704_067_200_000_000 + int64(i)*1000 }

func umicro(i int) uint64 { return uint64(micro(i)) }

func newTick(i int) record.Tick {
	p := 100 + float64(i)/100

	return record.Tick{
		Datetime: micro(i),
		Bid:      p,
		Ask:      p + 0.02,
		Last:     p + 0.01,
		Volume:   uint64(i),
		Flags:    record.TickFlagBid | record.TickFlagAsk | record.TickFlagLast | record.TickFlagVolume,
	}
}

func tickStore(tb testing.TB, n int) string {
	tb.Helper()
	dir := tb.TempDir()

	w, err := writer.Open[record.Tick](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		tb.Fatalf("writer.Open: %v", err)
	}

	for done := 0; done < n; done += 100 {
		end := min(done+100, n)

		ticks := make([]record.Tick, 0, end-done)
		for i := done; i < end; i++ {
			ticks = append(ticks, newTick(i))
		}

		if err := w.AppendBatch("tick/EURUSD", ticks); err != nil {
			tb.Fatalf("AppendBatch: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		tb.Fatalf("writer.Close: %v", err)
	}

	return dir
}

// A tick survives the round trip field for field. Prices come back through the
// fixed-point divide, so they are compared with an exact bound rather than ==:
// PriceScale is 1e8, so a stored price is always a multiple of 1e-8 and the
// round trip is exact to within half of that.
func TestTickRoundTrip(t *testing.T) {
	const n = 500

	dir := tickStore(t, n)

	r, err := reader.Open[record.Tick](dir)
	if err != nil {
		t.Fatalf("reader.Open: %v", err)
	}
	defer r.Close()

	got, err := r.Range("tick/EURUSD", umicro(100), umicro(199), 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != 100 {
		t.Fatalf("got %d ticks, want 100", len(got))
	}

	for i, tick := range got {
		want := newTick(100 + i)
		if tick.Datetime != want.Datetime {
			t.Fatalf("tick %d datetime %d, want %d", i, tick.Datetime, want.Datetime)
		}

		if math.Abs(tick.Bid-want.Bid) > 1.0/record.PriceScale {
			t.Fatalf("tick %d bid %v, want %v", i, tick.Bid, want.Bid)
		}

		if math.Abs(tick.Ask-want.Ask) > 1.0/record.PriceScale {
			t.Fatalf("tick %d ask %v, want %v", i, tick.Ask, want.Ask)
		}

		if tick.Volume != want.Volume || tick.Flags != want.Flags {
			t.Fatalf("tick %d volume %d flags %#x, want %d %#x",
				i, tick.Volume, tick.Flags, want.Volume, want.Flags)
		}
	}
}

// Prices are signed. Crude oil printed negative in April 2020 and a store that
// refuses to keep it has a hole exactly when it matters, which is why a tick
// price carries its sign inside the stored word while a Bar rejects a negative
// price outright. That word is unsigned and the sign is reapplied on decode, so
// this test is the one that catches the conversion going the wrong way.
func TestTickStoresNegativePrices(t *testing.T) {
	dir := t.TempDir()

	w, err := writer.Open[record.Tick](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatal(err)
	}

	want := []record.Tick{
		{Datetime: micro(0), Bid: -37.63, Ask: -37.61, Last: -37.62},
		{Datetime: micro(1), Bid: -0.00000134, Ask: 0.00000122, Last: -0.00000001},
		{Datetime: micro(2), Bid: 0, Ask: 0, Last: 0},
	}
	if err := w.AppendBatch("tick/CL", want); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open[record.Tick](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.Range("tick/CL", 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d ticks, want %d", len(got), len(want))
	}

	for i, tick := range got {
		if math.Abs(tick.Bid-want[i].Bid) > 1.0/record.PriceScale {
			t.Fatalf("tick %d bid %v, want %v", i, tick.Bid, want[i].Bid)
		}
	}
}

// Ticks are microseconds because a shard requires strictly increasing stamps and
// a liquid symbol produces several ticks per millisecond. Under a millisecond
// key the second of these two is rejected.
func TestTickSubMillisecondOrdering(t *testing.T) {
	dir := t.TempDir()

	w, err := writer.Open[record.Tick](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatal(err)
	}

	base := micro(0)

	batch := make([]record.Tick, 0, 64)
	for i := range 64 {
		batch = append(batch, record.Tick{Datetime: base + int64(i)*10, Bid: 1, Ask: 1}) // 10us apart
	}

	if err := w.AppendBatch("tick/USDJPY", batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open[record.Tick](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.Range("tick/USDJPY", 0, 0, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != 64 {
		t.Fatalf("got %d ticks, want 64", len(got))
	}

	if got[1].Datetime-base != 10 {
		t.Fatalf("second tick is %d microseconds after the first, want 10",
			got[1].Datetime-base)
	}
}

func TestTickRejectsNonIncreasingStamp(t *testing.T) {
	dir := t.TempDir()

	w, err := writer.Open[record.Tick](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append("tick/EURUSD", record.Tick{Datetime: micro(10), Bid: 1, Ask: 1}); err != nil {
		t.Fatal(err)
	}

	err = w.Append("tick/EURUSD", record.Tick{Datetime: micro(10), Bid: 1, Ask: 1})
	if !errors.Is(err, format.ErrOutOfOrder) {
		t.Fatalf("same microsecond gave %v, want ErrOutOfOrder", err)
	}

	err = w.Append("tick/EURUSD", record.Tick{Datetime: micro(9), Bid: 1, Ask: 1})
	if !errors.Is(err, format.ErrOutOfOrder) {
		t.Fatalf("an earlier microsecond gave %v, want ErrOutOfOrder", err)
	}
}

// A store is one record type for its whole life, and the width in every segment
// header is what enforces it. Reading a tick store as bars, or writing bars into
// one, must fail rather than decode one layout as the other and hand back
// plausible garbage: a bar's Open and a tick's Bid are the same eight bytes.
//
// The refusal comes at the first read, not at Open, because the reader maps a
// segment on first use and validating at Open would mean walking every segment in
// the store. That is late but not loose: OpenSegment compares the header width
// against T before it maps a single record, so no query ever returns a wrong
// layout, it returns ErrCorrupt.
func TestStoreRefusesTheWrongRecordType(t *testing.T) {
	t.Run("tick store read as bars", func(t *testing.T) {
		dir := tickStore(t, 100)

		r, err := reader.Open[record.Bar](dir)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if _, err := r.Range("tick/EURUSD", 0, 0, 1); !errors.Is(err, format.ErrCorrupt) {
			t.Fatalf("reading a tick store as bars gave %v, want ErrCorrupt", err)
		}
	})
	t.Run("bar store read as ticks", func(t *testing.T) {
		dir := t.TempDir()

		w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
		if err != nil {
			t.Fatal(err)
		}

		if err := w.AppendBatch("alloc/BTCUSDT", []record.Bar{newBarB(t, stamp(0), 100)}); err != nil {
			t.Fatal(err)
		}

		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		r, err := reader.Open[record.Tick](dir)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if _, err := r.Range("alloc/BTCUSDT", 0, 0, 1); !errors.Is(err, format.ErrCorrupt) {
			t.Fatalf("reading a bar store as ticks gave %v, want ErrCorrupt", err)
		}
	})
	t.Run("tick store written as bars", func(t *testing.T) {
		dir := tickStore(t, 100)

		w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
		if err == nil {
			err = w.AppendBatch("tick/EURUSD", []record.Bar{newBarB(t, stamp(0), 100)})
			if err == nil {
				err = w.Close()
			}
		}

		if !errors.Is(err, format.ErrCorrupt) {
			t.Fatalf("appending bars into a tick store gave %v, want ErrCorrupt", err)
		}
	})
}

// TickSize is 56 and stays 56, because a fixed width is what makes a record
// addressable arithmetic and what lets the seqlock publish a count and the
// records behind it in one step. This test fails loudly if a field is added
// without the format being redone on purpose.
func TestTickSizeIsSevenWords(t *testing.T) {
	if record.TickSize != 56 {
		t.Fatalf("TickSize is %d, want 56", record.TickSize)
	}

	if got := (record.Tick{}).RecordSize(); got != record.TickSize {
		t.Fatalf("Tick.RecordSize is %d, want %d", got, record.TickSize)
	}

	if got := (record.Tick{Datetime: 5}).Stamp(); got != 5 {
		t.Fatalf("Tick.Stamp is %d, want 5", got)
	}
}

func TestTickTime(t *testing.T) {
	tick := record.Tick{Datetime: 1_704_067_200_000_000}

	want := time.Unix(1_704_067_200, 0).UTC()
	if got := tick.Time(); !got.Equal(want) {
		t.Fatalf("Time is %v, want %v", got, want)
	}
}

func TestTickRejectsNonFinitePrice(t *testing.T) {
	dir := t.TempDir()

	w, err := writer.Open[record.Tick](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		err := w.Append("tick/EURUSD", record.Tick{Datetime: micro(0), Bid: bad, Ask: 1})
		if err == nil {
			t.Fatalf("appending bid %v was accepted", bad)
		}
	}

	err = w.Append("tick/EURUSD", record.Tick{Datetime: -1, Bid: 1, Ask: 1})
	if !errors.Is(err, format.ErrInvalid) {
		t.Fatalf("negative datetime gave %v, want ErrInvalid", err)
	}
}

// The tick read path holds the same line as the bar read path: Cursor is zero
// allocations, and the convenience wrappers are the fixed one per query that
// TestQueryWrappersAllocateOncePerQuery documents. A per-record allocation here
// would be worse than on bars, because a tick store holds far more records than a
// candle store for the same symbol and the same span.
func TestTickQueryPathAllocations(t *testing.T) {
	dir := tickStore(t, 2000)

	r, err := reader.Open[record.Tick](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	from, to := umicro(500), umicro(1499)

	warm := func(record.Tick) bool { return true }
	if err := r.Scan("tick/EURUSD", from, to, 0, warm); err != nil {
		t.Fatal(err)
	}

	var (
		cur reader.Cursor[record.Tick]
		out record.Tick
	)

	if n := testing.AllocsPerRun(200, func() {
		if err := r.Cursor("tick/EURUSD", from, to, 0, &cur); err != nil {
			t.Fatal(err)
		}

		for cur.Next(&out) {
		}

		if err := cur.Err(); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("Tick Cursor allocated %.1f times per call, want 0", n)
	}

	for name, run := range map[string]func(){
		"Scan": func() { _ = r.Scan("tick/EURUSD", from, to, 0, warm) },
		"All": func() {
			for range r.All("tick/EURUSD", from, to, 0) {
			}
		},
	} {
		if n := testing.AllocsPerRun(200, run); n > 1 {
			t.Errorf("Tick %s allocated %.1f times per call, want at most 1", name, n)
		}
	}
}

// benchTickStore writes n ticks one millisecond apart for one key and returns an
// open Reader on them, so the tick benchmarks measure the same on-disk shape the
// bar benchmarks do.
func benchTickStore(b *testing.B, n int) *reader.Reader[record.Tick] {
	b.Helper()
	dir := b.TempDir()

	w, err := writer.Open[record.Tick](writer.Options{Dir: dir, SegmentBytes: 32 << 20})
	if err != nil {
		b.Fatal(err)
	}

	for done := 0; done < n; done += 1000 {
		end := min(done+1000, n)

		batch := make([]record.Tick, 0, end-done)
		for i := done; i < end; i++ {
			p := 100 + float64(i%1000)/100
			batch = append(batch, record.Tick{
				Datetime: micro(i),
				Bid:      p,
				Ask:      p + 0.02,
				Last:     p + 0.01,
				Volume:   uint64(i),
				Flags:    record.TickFlagBid | record.TickFlagAsk,
			})
		}

		if err := w.AppendBatch("bench/EURUSD", batch); err != nil {
			b.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	r, err := reader.Open[record.Tick](dir)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() { r.Close() })

	return r
}

const (
	tickBenchKey    = "bench/EURUSD"
	tickBenchWindow = 501
	tickBenchTotal  = 200_000
)

// BenchmarkTickCursorWindow is the tick equivalent of BenchmarkCursorWindow and
// the one a chart of ticks renders through. Zero allocations is the requirement;
// 56 bytes per record against a bar's 60 means a tick store is the denser of the
// two, so anything slower here would be the format and not the record count.
func BenchmarkTickCursorWindow(b *testing.B) {
	r := benchTickStore(b, tickBenchTotal)
	to := umicro(tickBenchTotal - 1)
	from := to - uint64(tickBenchWindow)*1000

	var (
		cur reader.Cursor[record.Tick]
		out record.Tick
	)

	b.ReportAllocs()
	b.SetBytes(record.TickSize * tickBenchWindow)

	for b.Loop() {
		if err := r.Cursor(tickBenchKey, from, to, 0, &cur); err != nil {
			b.Fatal(err)
		}

		for cur.Next(&out) {
		}

		if err := cur.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTickScanWindow is the callback shape, and BenchmarkTickAllWindow the
// lazy one. Both cost one fixed allocation for the cursor they build internally;
// BenchmarkTickCursorWindow is the one that costs none.
func BenchmarkTickScanWindow(b *testing.B) {
	r := benchTickStore(b, tickBenchTotal)
	to := umicro(tickBenchTotal - 1)
	from := to - uint64(tickBenchWindow)*1000
	warm := func(record.Tick) bool { return true }

	b.ReportAllocs()
	b.SetBytes(record.TickSize * tickBenchWindow)

	for b.Loop() {
		if err := r.Scan(tickBenchKey, from, to, 0, warm); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTickAllWindow(b *testing.B) {
	r := benchTickStore(b, tickBenchTotal)
	to := umicro(tickBenchTotal - 1)
	from := to - uint64(tickBenchWindow)*1000

	b.ReportAllocs()
	b.SetBytes(record.TickSize * tickBenchWindow)

	for b.Loop() {
		for range r.All(tickBenchKey, from, to, 0) {
		}
	}
}

// BenchmarkTickFullSeries streams the whole series in one byte, against
// BenchmarkRangeFullSeries which materialises it.
func BenchmarkTickFullSeries(b *testing.B) {
	r := benchTickStore(b, tickBenchTotal)

	b.ReportAllocs()
	b.SetBytes(record.TickSize * tickBenchTotal)

	for b.Loop() {
		for range r.All(tickBenchKey, 0, 0, 0) {
		}
	}
}

// benchTickAppender writes one tick per op. It cannot append a fixed batch twice,
// because stamps must strictly increase, so the stamp advances with the run.
type benchTickAppender struct {
	w   *writer.DB[record.Tick]
	key string
	n   int
	buf []record.Tick
}

func BenchmarkTickAppendThroughput(b *testing.B) {
	dir := b.TempDir()

	w, err := writer.Open[record.Tick](writer.Options{Dir: dir, SegmentBytes: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	a := benchTickAppender{w: w, key: "bench/EURUSD", buf: make([]record.Tick, 256)}

	b.ReportAllocs()
	b.SetBytes(record.TickSize)

	for b.Loop() {
		for i := range a.buf {
			p := 100 + float64(a.n%1000)/100
			a.buf[i] = record.Tick{
				Datetime: micro(a.n), Bid: p, Ask: p + 0.02, Volume: uint64(a.n),
			}
			a.n++
		}

		if err := w.AppendBatch(a.key, a.buf); err != nil {
			b.Fatal(err)
		}
	}
}
