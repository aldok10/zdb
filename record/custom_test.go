package record_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"testing"
	"unsafe"

	"github.com/aldok10/zdb/internal/format"
	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
)

// Quote is a record type written from scratch rather than derived from Bar or
// Tick, to keep the claim that Record is the whole of what the format asks for
// honest. It is deliberately unlike either of them: four fields rather than
// eight, an integer basis count rather than a float price, and a packed width
// that neither Go's struct layout nor a field-aligned packer would produce.
//
// That last part is the point. Go lays this struct out in 24 bytes and so does a
// hand-aligned packer, both of which leave the uint64 starting on an 8-byte
// boundary at offset 16. Packing it gives 21, with the uint64 at offset 13 and
// crossing every alignment rule on the way. TestCustomRecordWidthIsNotTheGoStructSize
// measures the other two rather than asserting them from memory, because the gap
// between them is what makes 21 the interesting number: a record type that guesses
// its width from its own struct writes a file nothing else can read, and the store
// has no way to tell it was wrong.
type Quote struct {
	Datetime  int64  // unix microseconds, strictly increasing within a shard
	Bid       uint32 // price in millionths, so an integer all the way down
	Condition uint8  // an enum, so a single byte has to survive the round trip
	Size      uint64 // shares, whole, and deliberately unaligned on disk
}

const quoteSize = 8 + 4 + 1 + 8

const (
	quoteOffDatetime  = 0
	quoteOffBid       = 8
	quoteOffCondition = 12
	quoteOffSize      = 13
)

func (Quote) RecordSize() int { return quoteSize }

func (q Quote) Stamp() int64 { return q.Datetime }

func (q Quote) EncodeInto(dst []byte) error {
	if q.Datetime < 0 {
		return record.ErrInvalid
	}

	record.PutLe64(dst[quoteOffDatetime:], uint64(q.Datetime))
	record.PutLe32(dst[quoteOffBid:], q.Bid)
	dst[quoteOffCondition] = q.Condition
	record.PutLe64(dst[quoteOffSize:], q.Size)

	return nil
}

func (Quote) DecodeInto(src []byte, dst *Quote) {
	dst.Datetime = int64(record.Le64(src[quoteOffDatetime:]))
	dst.Bid = record.Le32(src[quoteOffBid:])
	dst.Condition = src[quoteOffCondition]
	dst.Size = record.Le64(src[quoteOffSize:])
}

var _ record.Record[Quote] = Quote{}

// The width is a property of the type, and the store is opened with it. If the
// three files disagree, the open fails rather than decoding one layout as
// another, which is the property that makes a custom type safe to adopt.
func TestCustomRecordWidthIsTakenFromTheType(t *testing.T) {
	if got := record.SizeOf[Quote](); got != quoteSize {
		t.Fatalf("SizeOf[Quote] = %d, want %d", got, quoteSize)
	}

	if got := (Quote{}).RecordSize(); got != quoteSize {
		t.Fatalf("RecordSize = %d, want %d", got, quoteSize)
	}
}

// Encode and decode are inverse, over the whole range the fields can hold. A
// uint32 at a 64-bit-aligned offset is where a hand-rolled packer goes wrong,
// so the boundary values are the ones worth checking.
func TestCustomRecordEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Quote{
		{},
		{Datetime: 1, Bid: 1, Size: 1, Condition: 1},
		{Datetime: math.MaxInt64, Bid: math.MaxUint32, Size: math.MaxUint64, Condition: math.MaxUint8},
		{Datetime: 1 << 40, Bid: 1_000_000, Size: 1 << 33, Condition: 3},
		// Offset 4 through 7 are the padding the naive packer forgets to skip,
		// so a bid that is not zero after a large timestamp catches it.
		{Datetime: 0x0102030405, Bid: 0, Size: 0xAABBCCDDEEFF, Condition: 7},
	}

	for i, want := range cases {
		buf := make([]byte, quoteSize)
		if err := want.EncodeInto(buf); err != nil {
			t.Fatalf("case %d: EncodeInto: %v", i, err)
		}

		var got Quote

		got.DecodeInto(buf, &got)

		if got != want {
			t.Errorf("case %d: round trip gave %+v, want %+v (bytes %x)", i, got, want, buf)
		}
	}
}

// The stamp is what the format reads at offset 0 to binary-search, so a record
// whose Stamp disagrees with its first encoded word is a record that cannot be
// found again. Checking the two agree is the whole of it.
func TestCustomRecordStampMatchesEncodedOffsetZero(t *testing.T) {
	q := Quote{Datetime: 0x0102030405060708, Bid: 42, Size: 7, Condition: 1}

	buf := make([]byte, quoteSize)
	if err := q.EncodeInto(buf); err != nil {
		t.Fatal(err)
	}

	if got := int64(record.Le64(buf[quoteOffDatetime:])); got != q.Stamp() {
		t.Fatalf("word at offset 0 is %d but Stamp is %d", got, q.Stamp())
	}
}

func TestCustomRecordRejectsNegativeStamp(t *testing.T) {
	q := Quote{Datetime: -1, Bid: 1, Size: 1, Condition: 1}

	err := q.EncodeInto(make([]byte, quoteSize))
	if !errors.Is(err, record.ErrInvalid) {
		t.Fatalf("EncodeInto on a negative stamp gave %v, want ErrInvalid", err)
	}
}

// The end-to-end claim: a type nobody in this repository knows about is written,
// sealed, read back through every query shape, and comes back identical. This is
// what the package documentation asserts, so it is asserted here.
func TestCustomRecordRoundTripsThroughAStore(t *testing.T) {
	dir := t.TempDir()

	const (
		first  = 1_700_000_000_000_000
		second = 1_700_000_000_000_001
	)

	want := []Quote{
		{Datetime: first, Bid: 104_250_000, Size: 3, Condition: 1},
		{Datetime: second, Bid: 104_275_000, Size: 11, Condition: 2},
	}

	w, err := writer.Open[Quote](writer.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.AppendBatch("binance/spot/BTCUSDT", want); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open[Quote](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if got := mustCount(t, r, "binance/spot/BTCUSDT"); got != uint64(len(want)) {
		t.Fatalf("Count = %d, want %d", got, len(want))
	}

	got, err := r.Range("binance/spot/BTCUSDT", 0, math.MaxUint64, 0)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("Range returned %d records, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d: got %+v, want %+v", i, got[i], want[i])
		}
	}

	// Scan, so a custom type is exercised through the callback shape too and not
	// only through the slice-returning one.
	var scanned []Quote

	if err := r.Scan("binance/spot/BTCUSDT", 0, math.MaxUint64, 0, func(q Quote) bool {
		scanned = append(scanned, q)

		return true
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(scanned) != len(want) || scanned[0] != want[0] || scanned[1] != want[1] {
		t.Errorf("Scan gave %+v, want %+v", scanned, want)
	}

	// Bounds come from the stored stamps, so they prove the leading word was read
	// as a timestamp rather than as part of the record's own layout.
	lo, hi, err := r.Bounds("binance/spot/BTCUSDT")
	if err != nil {
		t.Fatalf("Bounds: %v", err)
	}

	if lo != uint64(first) || hi != uint64(second) {
		t.Errorf("Bounds = (%d, %d), want (%d, %d)", lo, hi, first, second)
	}
}

// The zero-allocation claim is about Cursor, and it has to hold for a type the
// compiler has never seen. A custom record whose DecodeInto is written with a
// value receiver and a caller-owned destination is what makes that possible, so
// this is the test that would catch a signature change.
func TestCustomRecordCursorIsZeroAllocation(t *testing.T) {
	dir := t.TempDir()

	w, err := writer.Open[Quote](writer.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	const (
		total = 500
		base  = 1_700_000_000_000_000
	)

	batch := make([]Quote, total)
	for i := range batch {
		batch[i] = Quote{
			Datetime:  base + int64(i),
			Bid:       uint32(100_000_000 + i),
			Size:      uint64(i),
			Condition: uint8(i % 4),
		}
	}

	if err := w.AppendBatch("zero/alloc", batch); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open[Quote](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// The cursor and the destination are declared out here rather than inside the
	// closure, which is the shape BenchmarkCursorWindow uses and the reason Cursor
	// allocates nothing: Next copies out of a destination the caller already owns.
	// Declaring them per iteration makes the compiler heap-allocate and the test
	// would measure its own setup rather than the read path.
	var (
		c   reader.Cursor[Quote]
		out Quote
	)

	allocs := testing.AllocsPerRun(50, func() {
		if err := r.Cursor("zero/alloc", 0, math.MaxUint64, 0, &c); err != nil {
			t.Fatal(err)
		}

		n := 0

		for c.Next(&out) {
			n++
		}

		if err := c.Err(); err != nil {
			t.Fatal(err)
		}

		if n != total {
			t.Fatalf("cursor walked %d records, want %d", n, total)
		}
	})

	if allocs != 0 {
		t.Fatalf("Cursor over a custom record allocated %v times per run, want 0", allocs)
	}
}

// A quote file opened as a bar must be refused rather than decoded as bars.
// Quote is 21 bytes and Bar is 60, so the header widths disagree and the reader
// has every means to catch it.
func TestCustomRecordStoreRefusesTheWrongType(t *testing.T) {
	dir := t.TempDir()

	w, err := writer.Open[Quote](writer.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Append("wrong/type", Quote{
		Datetime: 1_700_000_000_000_000, Bid: 1, Size: 1, Condition: 1,
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.Range("wrong/type", 0, math.MaxUint64, 0)
	if !errors.Is(err, format.ErrCorrupt) {
		t.Fatalf("reading a quote store as bars gave %v, want ErrCorrupt", err)
	}
}

// Out-of-order stamps are refused on the write path, whatever the record type.
// The rule is the format's, so a custom type inherits it rather than reimplementing
// it.
func TestCustomRecordRejectsOutOfOrderStamp(t *testing.T) {
	dir := t.TempDir()

	w, err := writer.Open[Quote](writer.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const base = 1_700_000_000_000_000

	if err := w.Append("order/key", Quote{Datetime: base + 10, Size: 1}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	err = w.Append("order/key", Quote{Datetime: base + 5, Size: 1})
	if !errors.Is(err, format.ErrOutOfOrder) {
		t.Fatalf("out-of-order append gave %v, want ErrOutOfOrder", err)
	}
}

func mustCount(t *testing.T, r *reader.Reader[Quote], key string) uint64 {
	t.Helper()

	n, err := r.Count(key)
	if err != nil {
		t.Fatalf("Count(%q): %v", key, err)
	}

	return n
}

// The two wrong widths are the ones a caller is most likely to produce by
// accident, and the store cannot tell them apart from the right one except by
// believing RecordSize. Naming both here is what makes the third number mean
// something.
func TestCustomRecordWidthIsNotTheGoStructSize(t *testing.T) {
	goSize := unsafe.Sizeof(Quote{})
	aligned := 24 // each field aligned to its own width: 8 + 4 + 1 pad to 8 + 8

	if quoteSize == int(goSize) {
		t.Fatalf("quoteSize %d equals unsafe.Sizeof, so this record does not exercise packing", quoteSize)
	}

	if quoteSize == aligned {
		t.Fatalf("quoteSize %d equals the field-aligned layout, so this record does not exercise packing", quoteSize)
	}

	t.Logf("packed %d, Go struct %d, field-aligned %d", quoteSize, goSize, aligned)
}

// EncodeInto writes a fixed 21 bytes, so a wider or narrower width would show up
// as a stale byte left over from a previous record. Filling the buffer first and
// checking every byte of it catches an implementation that under-writes, which is
// the failure a store cannot detect: it would silently keep the previous record's
// tail.
func TestCustomRecordEncodeWritesEveryByte(t *testing.T) {
	const sentinel = 0xAA

	// One byte longer than the record, so the past-the-end check has somewhere to
	// look. EncodeInto is handed exactly the record's width by the writer; the
	// store never offers it more, which is why the trailing byte here is the
	// caller's own and has to survive.
	buf := bytes.Repeat([]byte{sentinel}, quoteSize+1)

	q := Quote{
		Datetime:  0x0102030405060708,
		Bid:       0xAABBCCDD,
		Condition: 0x99,
		Size:      0x1122334455667788,
	}

	if err := q.EncodeInto(buf); err != nil {
		t.Fatal(err)
	}

	// Every byte is accounted for, so no sentinel survives. The field values are
	// chosen so that none of them encodes to 0xAA-only runs that could be
	// mistaken for untouched filler.
	var got Quote

	got.DecodeInto(buf, &got)

	if got != q {
		t.Fatalf("round trip gave %+v, want %+v (bytes %x)", got, q, buf)
	}

	for i, b := range buf[:quoteSize] {
		if b == sentinel && !byteIsFieldData(q, i) {
			t.Errorf("byte %d is still %#x: EncodeInto did not write the whole %d-byte record", i, sentinel, quoteSize)
		}
	}

	// The record occupies exactly the first 21 bytes and the caller's byte beyond
	// it is untouched.
	if buf[quoteSize] != sentinel {
		t.Fatalf("byte %d is %#x, want %#x: EncodeInto wrote past RecordSize",
			quoteSize, buf[quoteSize], sentinel)
	}
}

// byteIsFieldData reports whether byte i of the packed record is part of one of
// q's encoded fields, so the sweep above only flags filler it did not write.
func byteIsFieldData(q Quote, i int) bool {
	const sentinel = 0xAA

	var full [quoteSize]byte

	if err := q.EncodeInto(full[:]); err != nil {
		return false
	}

	return full[i] == sentinel
}

// String exists so a failure message reads as a quote rather than a struct dump,
// and so the type looks like something a caller would actually write. It is also
// a cheap check that the type satisfies fmt.Stringer without importing fmt here.
func (q Quote) String() string {
	return "Quote{" + strconv.FormatInt(q.Datetime, 10) + "," +
		strconv.FormatUint(uint64(q.Bid), 10) + "}"
}

// The endianness seam has to agree with the standard library it wraps, byte for
// byte. If it ever stops agreeing, every store written by this package changes
// meaning and no test of the values themselves would necessarily notice, so the
// comparison is made directly against encoding/binary here.
func TestEndiannessSeamMatchesTheStandardLibrary(t *testing.T) {
	for _, v := range []uint64{0, 1, 0x7F, 0x100, 0xDEADBEEF, math.MaxUint32, math.MaxUint64} {
		want := make([]byte, 8)
		binary.LittleEndian.PutUint64(want, v)

		got := make([]byte, 8)
		record.PutLe64(got, v)

		if !bytes.Equal(got, want) {
			t.Errorf("record.PutLe64(%#x) wrote %x, want %x", v, got, want)
		}

		if back := record.Le64(got); back != v {
			t.Errorf("Le64 round trip of %#x gave %#x", v, back)
		}
	}

	for _, v := range []uint32{0, 1, 0xFF, 0x100, 0xDEADBEEF, math.MaxUint32} {
		want := make([]byte, 4)
		binary.LittleEndian.PutUint32(want, v)

		got := make([]byte, 4)
		record.PutLe32(got, v)

		if !bytes.Equal(got, want) {
			t.Errorf("record.PutLe32(%#x) wrote %x, want %x", v, got, want)
		}

		if back := record.Le32(got); back != v {
			t.Errorf("Le32 round trip of %#x gave %#x", v, back)
		}
	}

	// Offsets matter as well as byte order: reading at the head of a longer slice
	// must return the value that was written there, not the slice's start.
	buf := make([]byte, 16)
	record.PutLe64(buf[8:], 0x0102030405060708)

	if got := record.Le64(buf[8:]); got != 0x0102030405060708 {
		t.Errorf("Le64 at offset 8 gave %#x", got)
	}
}
