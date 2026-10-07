package zql_test

import (
	"testing"

	"github.com/aldok10/zdb/zql"
)

// TestCloseIndexPrunesWholeSegmentsRunsRuns verifies a selective close filter
// is answered exactly across many pre-allocated segments: the index may skip
// segments wholesale, but it must never fabricate a match or lose one. This
// is the contract all column indexes must satisfy.
func TestCloseIndexPrunesWholeSegmentsRuns(t *testing.T) {
	db := zql.Open(store(t, 20000), zql.Options{})

	got := collect(t, db.Run(`SELECT * FROM "binance/spot/BTCUSDT" WHERE close >= 19990.5 ORDER BY datetime ASC`))
	if len(got) != 10 {
		t.Fatalf("expected 10 bars, got %d", len(got))
	}

	for i, b := range got {
		if b.Datetime != stamp(19990+i) {
			t.Fatalf("row %d: got datetime %d, want %d", i, b.Datetime, stamp(19990+i))
		}
	}
}

// TestOpenIndexPrunesWholeSegmentsRunsRuns exercises the other non-time
// columns through the same pruning path.
func TestOpenIndexPrunesWholeSegmentsRunsRuns(t *testing.T) {
	db := zql.Open(store(t, 20000), zql.Options{})

	got := collect(t, db.Run(`SELECT * FROM "binance/spot/BTCUSDT" WHERE open >= 19991 ORDER BY datetime ASC`))
	if len(got) != 10 {
		t.Fatalf("expected 10 bars, got %d", len(got))
	}

	if got[0].Datetime != stamp(19990) {
		t.Fatalf("first bar: got %v, want %v", got[0].Datetime, stamp(19990))
	}
}
