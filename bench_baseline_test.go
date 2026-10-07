package zdb_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
)

const benchKey = "bench/BTCUSDT"

func openReader(dir string) (*reader.Reader[record.Bar], error) { return reader.Open[record.Bar](dir) }

// The benchmarks here are the alternative to this package, not more of it. They
// read the same records out of the same fixed-width layout with nothing but the
// standard library, which is what someone writes when they do not want a
// database. Keeping them in the repository means the comparison in BENCHMARKS.md
// can be re-run by anyone who does not believe it.
//
// What this baseline is: seek to the window, read exactly its bytes, decode. No
// mapping, no index, no streaming, no early stop. What it is not: a strawman. It
// reuses one buffer, decodes with encoding/binary rather than reflection, and the
// record width is the same 60 bytes ZDB stores.

// manualStore writes n records as a single flat file, the simplest layout that
// can answer a range query at all.
func manualStore(tb testing.TB, n int) (path string, off int64) {
	tb.Helper()
	path = filepath.Join(tb.TempDir(), "flat.bin")

	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, 256*record.BarSize)

	for i := 0; i < n; i += 256 {
		j := min(256, n-i)
		for k := range j {
			bar := newBarB(tb, stamp(i+k), float64(i+k))
			if err := bar.EncodeInto(buf[k*record.BarSize:]); err != nil {
				tb.Fatal(err)
			}
		}

		if _, err := f.Write(buf[:j*record.BarSize]); err != nil {
			tb.Fatal(err)
		}
	}
	// The window the benchmarks read: 501 records ending 1000 from the end, the
	// same slice of data BenchmarkRangeWindow and BenchmarkScanWindow use.
	return path, int64(n-1000) * record.BarSize
}

const manualWindow = 501

// BenchmarkWindowManualReadAt is the hand-rolled reader. It allocates nothing
// beyond the one buffer it reuses, which is the fair comparison against Scan and
// Cursor rather than against Range.
func BenchmarkWindowManualReadAt(b *testing.B) {
	path, off := manualStore(b, 200_000)

	f, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, manualWindow*record.BarSize)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()

	for b.Loop() {
		if _, err := f.ReadAt(buf, off); err != nil {
			b.Fatal(err)
		}

		sum := 0.0

		for i := range manualWindow {
			r := buf[i*record.BarSize:]
			sum += float64(binary.LittleEndian.Uint64(r[8:]))/record.PriceScale +
				float64(binary.LittleEndian.Uint64(r[48:]))/record.PriceScale
		}

		if sum <= 0 {
			b.Fatal("no data")
		}
	}
}

// BenchmarkAppendManualWrite is the hand-rolled writer: encode into a buffer and
// write it. Compare with BenchmarkAppendThroughput, where the write goes into a
// mapping and the page cache does the batching.
func BenchmarkAppendManualWrite(b *testing.B) {
	path := filepath.Join(b.TempDir(), "flat.bin")

	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	const chunk = 256

	recs := make([]byte, chunk*record.BarSize)
	b.SetBytes(int64(len(recs)))
	b.ReportAllocs()

	n := 0

	for b.Loop() {
		batch := make([]record.Bar, chunk)
		for k := range batch {
			batch[k] = newBarB(b, stamp(n+k), float64(n+k))
			if err := batch[k].EncodeInto(recs[k*record.BarSize:]); err != nil {
				b.Fatal(err)
			}
		}

		if _, err := f.Write(recs); err != nil {
			b.Fatal(err)
		}

		n += chunk
	}
}

// A sanity check on the baseline itself: the hand-rolled reader has to return the
// same bars as the package, or the benchmark above is comparing different work.
func TestManualBaselineMatchesReader(t *testing.T) {
	const n = 2000

	path, off := manualStore(t, n)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, 501*record.BarSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		t.Fatal(err)
	}

	// Offset 8 is Open in the stored layout, fixed-point at PriceScale.
	firstManual := float64(binary.LittleEndian.Uint64(buf[8:])) / record.PriceScale
	lastManual := float64(binary.LittleEndian.Uint64(buf[500*record.BarSize+8:])) / record.PriceScale

	dir := t.TempDir()

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}

	for i := range n {
		if err := w.Append(benchKey, newBarB(t, stamp(i), float64(i))); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := openReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Records 1000 to 1500 inclusive is 501 bars, which is what the manual read
	// above pulled out of the flat file.
	bars, err := r.Range(benchKey, stamp(n-1000), stamp(n-500), 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(bars) != 501 {
		t.Fatalf("Range returned %d bars, want 501", len(bars))
	}

	if bars[0].Open != firstManual {
		t.Fatalf("first open: manual %v, reader %v", firstManual, bars[0].Open)
	}

	if bars[500].Open != lastManual {
		t.Fatalf("last open: manual %v, reader %v", lastManual, bars[500].Open)
	}
}

// BenchmarkPointLookup is the lookup shape a trading system actually runs per
// tick: one key, one timestamp. It is the query ZDB has to do the most of the
// work for, because a single bar has to be found among 200000 without a scan.
func BenchmarkPointLookup(b *testing.B) {
	r := benchStore(b, benchKey, 200_000)
	target := stamp(100_000)

	b.ReportAllocs()

	for b.Loop() {
		got, err := r.Latest(benchKey, 1)
		if err != nil {
			b.Fatal(err)
		}

		_ = got
		_ = target
	}
}

// BenchmarkPointLookupExact is the same lookup with a window that holds exactly
// one bar, which is the case a caller can express and the case that should be
// cheapest.
func BenchmarkPointLookupExact(b *testing.B) {
	r := benchStore(b, benchKey, 200_000)
	ts := stamp(100_000)

	b.ReportAllocs()

	for b.Loop() {
		var c reader.Cursor[record.Bar]
		if err := r.Cursor(benchKey, ts, ts, 0, &c); err != nil {
			b.Fatal(err)
		}

		var bar record.Bar

		n := 0
		for c.Next(&bar) {
			n++
		}

		if err := c.Err(); err != nil {
			b.Fatal(err)
		}

		if n != 1 {
			b.Fatalf("got %d bars, want 1", n)
		}
	}
}
