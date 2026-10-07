//go:build bench

package zdb_test

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aldok10/zdb/internal/format"
	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
)

const (
	concSegBytes  = 1 << 20 // 1MB segments to reduce rollovers
	concBars      = 100000  // bars per key before the benchmark starts
	concDuration  = 2 * time.Second
	concBatch     = 10
	sampleStride  = 16 // record latency for every Nth op
	sampleBufCap  = 1 << 16
	concWriteKeys = 32 // enough keys for the widest writer fan-out
)

// benchStoreConc writes one key's worth of bars and returns the dir.
func benchStoreConc(b *testing.B, n int) (dir string) {
	b.Helper()
	dir = b.TempDir()
	key := "bench/conc/BTCUSDT"

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: concSegBytes})
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		if err := w.Append(key, newBarB(b, stamp(i), float64(i))); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	return dir
}

// concKeys returns the n keys readers and writers rotate over. The first key
// already holds the seed data; the rest start empty and belong to writers.
func concKeys(n int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("bench/conc/key%d", i)
	}
	return keys
}

// percentile returns the p-th percentile of sorted samples, or 0 when empty.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

// BenchmarkReadConcurrency measures read throughput and latency percentiles
// under a fixed reader count and no writers.
func BenchmarkReadConcurrency(b *testing.B) {
	dir := benchStoreConc(b, concBars)
	key := "bench/conc/BTCUSDT"

	for _, readers := range []int{1, 8, 32, 128, 256, 512, 1024} {
		b.Run(fmt.Sprintf("%dR", readers), func(b *testing.B) {
			runConc(b, dir, readers, 0, key)
		})
	}
}

// BenchmarkReadWriteConcurrency fan-out: many readers, several writers. Writers
// own distinct keys so a shard mutex is never the serializer being measured.
func BenchmarkReadWriteConcurrency(b *testing.B) {
	for _, tc := range []struct{ r, w int }{
		{8, 1}, {128, 4}, {128, 8}, {512, 16}, {1024, 32},
	} {
		b.Run(fmt.Sprintf("%dR_%dW", tc.r, tc.w), func(b *testing.B) {
			dir := benchStoreConc(b, concBars)
			keys := append([]string{"bench/conc/BTCUSDT"}, concKeys(concWriteKeys)...)
			runConc(b, dir, tc.r, tc.w, keys...)
		})
	}
}

// runConc drives readers and writers for concDuration and reports throughput,
// latency percentiles, allocations and seqlock movement. It does not use b.N:
// a concurrency level is a configuration, not an iteration count, and what we
// need is ops/sec plus the tail, not a mean per b.N.
func runConc(b *testing.B, dir string, readers, writers int, keys ...string) {
	b.Helper()

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	var w *writer.DB[record.Bar]
	if writers > 0 {
		w, err = writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: concSegBytes})
		if err != nil {
			b.Fatal(err)
		}
		defer w.Close()
	}

	// Seed writer keys so readers never hit an empty chain.
	for i := 1; i < len(keys) && i <= writers; i++ {
		for j := 0; j < 100; j++ {
			if err := w.Append(keys[i], newBarB(b, stamp(j), float64(j))); err != nil {
				b.Fatal(err)
			}
		}
	}

	var readOps, writeRecs atomic.Uint64
	var stop atomic.Bool
	var wg sync.WaitGroup

	retries0 := format.SeqlockRetries.Load()
	busy0 := format.SeqlockBusy.Load()

	samples := make([][]time.Duration, readers)
	for i := range samples {
		samples[i] = make([]time.Duration, 0, sampleBufCap)
	}

	// Readers rotate over the keys that actually hold data: the seed key plus
	// the keys the writers own. Scanning keys nobody writes would only measure
	// ErrKeyNotFound, not contention.
	readKeys := keys[:1+writers]
	if writers == 0 {
		readKeys = keys
	}

	// Readers: rotate keys, scan the newest 1000 bars, sample latency.
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var c reader.Cursor[record.Bar]
			var out record.Bar
			j := 0
			for !stop.Load() {
				key := readKeys[(id+j)%len(readKeys)]
				j++

				start := time.Now()
				err := r.Cursor(key, 0, 0, 1000, &c)
				if err == nil {
					for c.Next(&out) {
					}
					_ = c.Err()
				}
				dt := time.Since(start)

				readOps.Add(1)
				if j%sampleStride == 0 && len(samples[id]) < sampleBufCap {
					samples[id] = append(samples[id], dt)
				}
			}
		}(i)
	}

	// Writers: one key each, append a batch per op, keep stamps strictly increasing.
	writeBase := make([]int64, writers)
	for i := range writeBase {
		writeBase[i] = 200 // after the 100 seed bars
	}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := keys[1+id%(len(keys)-1)]
			n := writeBase[id]
			for !stop.Load() {
				batch := make([]record.Bar, concBatch)
				for k := range batch {
					batch[k] = newBarB(b, stamp(int(n)+k), float64(n)+float64(k))
				}
				n += concBatch
				if err := w.AppendBatch(key, batch); err != nil {
					continue
				}
				writeRecs.Add(uint64(concBatch))
			}
		}(i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	time.Sleep(concDuration)
	stop.Store(true)
	wg.Wait()
	b.StopTimer()

	// Merge and sort latency samples.
	var all []time.Duration
	for _, s := range samples {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	secs := concDuration.Seconds()
	b.ReportMetric(float64(readOps.Load())/secs, "read_scans/s")
	b.ReportMetric(float64(writeRecs.Load())/secs, "write_recs/s")
	b.ReportMetric(float64(percentile(all, 0.50).Nanoseconds()), "p50_ns")
	b.ReportMetric(float64(percentile(all, 0.95).Nanoseconds()), "p95_ns")
	b.ReportMetric(float64(percentile(all, 0.99).Nanoseconds()), "p99_ns")
	b.ReportMetric(float64(percentile(all, 0.999).Nanoseconds()), "p99.9_ns")
	b.ReportMetric(float64(format.SeqlockRetries.Load()-retries0), "seqlock_retries")
	b.ReportMetric(float64(format.SeqlockBusy.Load()-busy0), "seqlock_busy")
}
