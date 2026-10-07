package zdb_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aldok10/zdb/internal/format"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
)

func TestSealedSegmentChecksumRejectsCorruption(t *testing.T) {
	dir := t.TempDir()

	const key = "ADAUSDT"

	w, err := writer.Open[record.Bar](writer.Options{Dir: dir, SegmentBytes: segBytes})
	if err != nil {
		t.Fatalf("writer.Open: %v", err)
	}

	for i := range 20 {
		if err := w.Append(key, newBar(t, stamp(i), float64(i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	paths, err := filepath.Glob(filepath.Join(dir, "*.zseg"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no segment files: %v", err)
	}

	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}

		if len(data) < format.HeaderSize+record.BarSize {
			t.Fatalf("%s too small to corrupt", p)
		}

		data[format.HeaderSize+record.BarSize] ^= 0xff

		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}

		if _, err := format.OpenSegment[record.Bar](p, 0); !errors.Is(err, format.ErrCorrupt) {
			t.Fatalf("corrupt segment %s: err = %v, want ErrCorrupt", p, err)
		}
	}
}
