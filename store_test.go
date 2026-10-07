package zdb_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aldok10/zdb"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
	"github.com/aldok10/zdb/zql"
)

// testKey is one key for every test: each test owns its directory, and a key
// that collides across directories would only matter if they shared one.
const testKey = "binance/spot/BTCUSDT"

// bars returns three bars, one every 10 ms, closing at 100.00, 101.00 and
// 102.00. The prices are whole numbers of cents so they round trip through the
// fixed-point encoding exactly, and the test compares record for record.
func bars() []record.Bar {
	out := make([]record.Bar, 0, 3)

	for i := range 3 {
		out = append(out, record.Bar{
			Datetime: 1704067200000 + int64(i)*10,
			Open:     100 + float64(i),
			High:     101 + float64(i),
			Low:      99 + float64(i),
			Close:    100 + float64(i),
			Volume:   1000 + uint64(i),
		})
	}

	return out
}

// descriptors counts the descriptors this process holds, through /dev/fd. It is
// the measurement AGENTS.md section 11.3 asks for: the Go heap cannot see a
// mapping or a descriptor, so no allocation number proves that a Close gave
// either back.
func descriptors(t *testing.T) int {
	t.Helper()

	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Skipf("cannot read /dev/fd: %v", err)
	}

	return len(entries)
}

func assertBarsEqual(t *testing.T, got, want []record.Bar) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d bars, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("bar %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestOpenRoundTrip covers scenarios S1 and S2. A directory that does not exist
// is created, and the read straight after the write needs no Refresh: the
// bucket index is opened on demand and the segment is mapped on first use.
// Reopening the same directory with a fresh pair of handles proves the seal and
// the index entries the writer writes on Close are enough to find the bars
// again.
func TestOpenRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "store")

	s, err := zdb.Open[record.Bar](zdb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	want := bars()
	if err := s.Writer.AppendBatch(testKey, want); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	got, err := s.Reader.Range(testKey, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range after append, without Refresh: %v", err)
	}

	assertBarsEqual(t, got, want)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	again, err := zdb.Open[record.Bar](zdb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	defer again.Close()

	got, err = again.Reader.Range(testKey, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range after reopen: %v", err)
	}

	assertBarsEqual(t, got, want)
}

// TestQueryHonoursTheRetentionCeiling is scenario S3: Options.Query reaches the
// runner Open binds, and MaxBars keeps the newest bars rather than the first
// ones, which is what zql.Options promises.
func TestQueryHonoursTheRetentionCeiling(t *testing.T) {
	s, err := zdb.Open[record.Bar](zdb.Options{
		Dir:   t.TempDir(),
		Query: zql.Options{MaxBars: 2},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	defer s.Close()

	if err := s.Writer.AppendBatch(testKey, bars()); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	q, err := s.Query()
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	var got []record.Bar

	for bar, err := range q.Run(`SELECT * FROM "binance/spot/BTCUSDT"`) {
		if err != nil {
			t.Fatalf("Run: %v", err)
		}

		got = append(got, bar)
	}

	if len(got) != 2 {
		t.Fatalf("got %d bars, want the 2 MaxBars allows", len(got))
	}

	want := bars()
	if got[0].Datetime != want[1].Datetime || got[1].Datetime != want[2].Datetime {
		t.Errorf("MaxBars kept datetimes %d, %d; want %d, %d",
			got[0].Datetime, got[1].Datetime, want[1].Datetime, want[2].Datetime)
	}
}

// TestQueryRefusesAStoreThatIsNotBars is scenario S4. ZQL's field enum names
// the OHLC columns, so a tick store gets an error naming its record type rather
// than nil, which would fail at the first Run instead of at the call that
// cannot be satisfied.
func TestQueryRefusesAStoreThatIsNotBars(t *testing.T) {
	s, err := zdb.Open[record.Tick](zdb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	defer s.Close()

	q, err := s.Query()
	if err == nil {
		t.Fatalf("Query on a record.Tick store returned %v, want an error", q)
	}

	if !strings.Contains(err.Error(), "record.Tick") {
		t.Errorf("error %q does not name the record type", err)
	}
}

// TestOpenRefusesAnEmptyDir is scenario S5.
func TestOpenRefusesAnEmptyDir(t *testing.T) {
	s, err := zdb.Open[record.Bar](zdb.Options{})
	if err == nil {
		s.Close()

		t.Fatal("Open with no Dir returned nil error")
	}

	if !strings.Contains(err.Error(), "directory") {
		t.Errorf("error %q does not say what is missing", err)
	}
}

// TestOpenRefusesADisagreeingWriterDir is scenario S6. Two sources of truth for
// where the files live is a disagreement Open refuses rather than resolves by
// silently picking one of them.
func TestOpenRefusesADisagreeingWriterDir(t *testing.T) {
	dir := t.TempDir()
	other := t.TempDir()

	s, err := zdb.Open[record.Bar](zdb.Options{
		Dir:    dir,
		Writer: writer.Options{Dir: other},
	})
	if err == nil {
		s.Close()

		t.Fatal("Open with Dir and Writer.Dir set to different paths returned nil error")
	}

	if !strings.Contains(err.Error(), other) {
		t.Errorf("error %q does not name the conflicting directory", err)
	}
}

// TestOpenReleasesTheWriterWhenTheReaderRefuses is scenarios S7 and R6: the
// writer opens first, the reader then refuses a bucket index it cannot map, and
// Open must close the writer before it returns. A handle nobody holds is a
// handle nobody can close.
func TestOpenReleasesTheWriterWhenTheReaderRefuses(t *testing.T) {
	dir := t.TempDir()

	// A bucket index shorter than the header, so IndexHeaderAt refuses it and
	// reader.Open fails after writer.Open has already succeeded.
	if err := os.WriteFile(filepath.Join(dir, "ab"+zdb.IndexSuffix), []byte("garbagex"), 0o644); err != nil {
		t.Fatalf("write unreadable index: %v", err)
	}

	before := descriptors(t)

	s, err := zdb.Open[record.Bar](zdb.Options{Dir: dir})
	if err == nil {
		s.Close()

		t.Fatal("Open over an unreadable bucket index returned nil error")
	}

	if !errors.Is(err, zdb.ErrCorrupt) {
		t.Errorf("error %v does not carry zdb.ErrCorrupt", err)
	}

	if after := descriptors(t); after != before {
		t.Errorf("a failed Open left %d descriptors open (%d -> %d)", after-before, before, after)
	}
}

// TestCloseIsIdempotentAndConcurrent is scenario S8. The first call does the
// work and every later call reads the same result, so a second Close is a read
// and not a second unmap of memory the reader already gave back. The goroutines
// are the point: this test is what -race has to stay green against.
func TestCloseIsIdempotentAndConcurrent(t *testing.T) {
	dir := t.TempDir()

	s, err := zdb.Open[record.Bar](zdb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.Writer.AppendBatch(testKey, bars()); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	var wg sync.WaitGroup

	errs := make([]error, 4)

	for i := range errs {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errs[i] = s.Close()
		}()
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Close from goroutine %d: %v", i, err)
		}
	}

	if err := s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	again, err := zdb.Open[record.Bar](zdb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after a concurrent Close: %v", err)
	}

	defer again.Close()

	got, err := again.Reader.Range(testKey, 0, 0, 0)
	if err != nil {
		t.Fatalf("Range after reopen: %v", err)
	}

	assertBarsEqual(t, got, bars())
}

// TestOpenAppendReadCloseDoesNotLeakDescriptors is scenario S9: N rounds of the
// whole lifecycle, and the descriptor count must not move. Descriptors and
// mappings are what the Go GC cannot see, so this is the only evidence that
// Close released everything Open acquired.
func TestOpenAppendReadCloseDoesNotLeakDescriptors(t *testing.T) {
	const rounds = 20

	root := t.TempDir()
	before := descriptors(t)

	for i := range rounds {
		s, err := zdb.Open[record.Bar](zdb.Options{Dir: filepath.Join(root, fmt.Sprintf("round-%d", i))})
		if err != nil {
			t.Fatalf("round %d: Open: %v", i, err)
		}

		if err := s.Writer.AppendBatch(testKey, bars()); err != nil {
			t.Fatalf("round %d: AppendBatch: %v", i, err)
		}

		if _, err := s.Reader.Range(testKey, 0, 0, 0); err != nil {
			t.Fatalf("round %d: Range: %v", i, err)
		}

		if err := s.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", i, err)
		}
	}

	if after := descriptors(t); after != before {
		t.Errorf("%d open/append/read/close rounds moved the descriptor count by %d (%d -> %d)",
			rounds, after-before, before, after)
	}
}
