package zdb_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
)

// Prune must delete closed segments that match the predicate, rewrite the
// bucket index, and leave an open shard alone until it is closed.
func TestDBPruneRemovesSealedSegments(t *testing.T) {
	dir := t.TempDir()

	const key = "ETHUSDT"

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

	before, err := filepath.Glob(filepath.Join(dir, "*.zseg"))
	if err != nil || len(before) < 2 {
		t.Fatalf("segments before prune = %d %v, want at least 2", len(before), err)
	}

	err = w.Prune(func(_ string, _ uint32, _ uint64, _ uint64, sealed bool) bool {
		return sealed
	})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}

	after, err := filepath.Glob(filepath.Join(dir, "*.zseg"))
	if err != nil {
		t.Fatal(err)
	}

	if len(after) >= len(before) {
		t.Fatalf("segments after prune = %d, want fewer than %d", len(after), len(before))
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

	if len(got) == 0 || len(got) >= n {
		t.Fatalf("got %d bars after prune, want a suffix of %d", len(got), n)
	}

	if got[len(got)-1].Datetime != int64(stamp(n-1)) {
		t.Fatalf("last bar %d, want %d", got[len(got)-1].Datetime, stamp(n-1))
	}

	for _, p := range after {
		if !strings.HasSuffix(p, ".zseg") {
			t.Fatalf("unexpected file %s", p)
		}
	}
}
