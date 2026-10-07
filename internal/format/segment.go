package format

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/aldok10/zdb/internal/platform"
	"github.com/aldok10/zdb/record"
)

// Segment is one memory-mapped file holding the ascending records of a single
// key. Every method except Close is read-only and safe for concurrent use by
// any number of goroutines: there is no lock on the read path.
//
// T is the record type. The segment stores its width in the header, and
// OpenSegment refuses a file whose width is not T's, so a reader cannot decode
// one record layout as another and get plausible prices out of garbage.
// Everything past that is opaque here: the format requires only that every record
// is the same width and that its leading field is an ascending timestamp, which
// is what tsAt reads.
//
// The mapping is created once and never remapped, because a writer
// pre-allocates the file to its full segment capacity and only ever raises
// count. Records past count are never touched, so the writer's unallocated tail
// never faults.
type Segment[T record.Record[T]] struct {
	Path string
	Key  string
	Seq  uint32

	file *os.File
	mm   []byte
	hdr  *SegmentHeader
	off  int // byte offset of record 0

	// The trailer at the end of the file holds a per-column min/max index used
	// to reject segments a query cannot match, plus the 8-byte sealed-segment
	// checksum. Counting it out of Capacity is what guarantees records never
	// overwrite it.
	trailer   int
	cols      []record.Column
	statsOff  int // byte offset of the stats block within mm
	maxBlocks int // zone-map rows reserved in the trailer
}

// openVerifyMaxBytes caps the region CRC-verified at OpenSegment.
const openVerifyMaxBytes = 256 << 10

// OpenSegment maps path read-only and validates its header.
func OpenSegment[T record.Record[T]](path string, seq uint32) (*Segment[T], error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("zdb: open segment %s: %w", path, err)
	}

	st, err := f.Stat()
	if err != nil {
		f.Close()

		return nil, fmt.Errorf("zdb: stat segment %s: %w", path, err)
	}

	mm, err := platform.Map(f, st.Size(), platform.ReadOnly)
	if err != nil {
		f.Close()

		return nil, err
	}

	hdr, off, err := SegmentHeaderAt(mm)
	if err != nil {
		platform.UnmapAndClose(mm, f)

		return nil, fmt.Errorf("zdb: %s: %w", path, err)
	}

	// The header says how wide a record is; the caller's type says how wide it
	// should be. A mismatch means this segment holds a different kind of record
	// than the reader was opened for, and decoding one layout as the other would
	// produce plausible-looking prices out of garbage, so it is refused here
	// rather than discovered much later.
	//
	// Every value the message needs is read before the mapping is released.
	// hdr points into mm, so reading it after Munmap is a fault and not a stale
	// value, which is exactly what happened when this read sat inside the
	// Errorf argument list below the Munmap.
	got, want := hdr.RecordSize(), record.SizeOf[T]()
	if got != want {
		name := record.TypeName[T]()

		platform.UnmapAndClose(mm, f)

		return nil, fmt.Errorf("zdb: %s: records are %d bytes, %s needs %d: %w",
			path, got, name, want, ErrCorrupt)
	}

	sc, err := record.SchemaOf[T]()

	var cols []record.Column
	if err == nil {
		cols = sc.StatsColumns()
	}

	if hdr.IsSealed() && !hdr.HasChecksum() {
		platform.UnmapAndClose(mm, f)

		return nil, fmt.Errorf("zdb: %s: sealed segment lacks a checksum: %w", path, ErrCorrupt)
	}

	if hdr.IsSealed() && hdr.HasChecksum() {
		var count uint64

		ok := false

		for range 32 {
			var okNow bool

			count, _, _, okNow = hdr.Snapshot()
			ok = okNow

			if ok {
				break
			}
		}

		if !ok {
			platform.UnmapAndClose(mm, f)

			return nil, fmt.Errorf("zdb: %s: %w", path, ErrBusy)
		}

		end := off + int(count)*hdr.RecordSize()
		if end+8 > len(mm) {
			platform.UnmapAndClose(mm, f)

			return nil, fmt.Errorf("zdb: %s: %w", path, ErrCorrupt)
		}

		// Open pays for a full CRC only on small segments: CRC64 runs at
		// ~750MB/s, so a store of 16MB segments spends ~400ms hashing before
		// the first record is read. The seal-time checksum plus an explicit
		// Verify keep the guarantee for larger segments.
		if end <= openVerifyMaxBytes {
			want := SegmentChecksum(mm[:end])
			gotSum := binary.LittleEndian.Uint64(mm[len(mm)-8:])

			if gotSum != want {
				platform.UnmapAndClose(mm, f)

				return nil, fmt.Errorf("zdb: %s: checksum mismatch: %w", path, ErrCorrupt)
			}
		}
	}

	return &Segment[T]{
		Path:      path,
		Key:       hdr.Key(mm),
		Seq:       seq,
		file:      f,
		mm:        mm,
		hdr:       hdr,
		off:       off,
		trailer:   TrailerBytes[T](int64(len(mm))),
		cols:      cols,
		statsOff:  len(mm) - TrailerBytes[T](int64(len(mm))),
		maxBlocks: blocksFor[T](int64(len(mm)), hdr.RecordSize()),
	}, nil
}

// Close unmaps the segment. Callers must not use the segment afterwards.
func (s *Segment[T]) Close() error {
	if s.mm == nil {
		return nil
	}

	err := platform.Unmap(s.mm)

	s.mm = nil
	if cerr := s.file.Close(); err == nil {
		err = cerr
	}

	return err
}

// Verify re-hashes the whole published region against the stored tail
// checksum. Open only verifies segments up to openVerifyMaxBytes; Verify is
// the explicit audit for larger, long-lived segments.
func (s *Segment[T]) Verify() error {
	if !s.hdr.HasChecksum() {
		return fmt.Errorf("zdb: %s: unsealed or unchecksummed: %w", s.Path, ErrCorrupt)
	}

	count, _, _, err := s.Snapshot()
	if err != nil {
		return err
	}

	end := s.off + int(count)*s.hdr.RecordSize()
	if end+8 > len(s.mm) || SegmentChecksum(s.mm[:end]) != binary.LittleEndian.Uint64(s.mm[len(s.mm)-8:]) {
		return fmt.Errorf("zdb: %s: checksum mismatch: %w", s.Path, ErrCorrupt)
	}

	return nil
}

// BlockValue reads the stored min/max of column colIdx over records
// [block*BlockRecords, (block+1)*BlockRecords) from the zone-map rows. ok is
// false when the block row is out of the reserved trailer, and the caller must
// then keep the block rather than trust it to be empty.
func (s *Segment[T]) BlockValue(colIdx, block int) (lo, hi uint64, ok bool) {
	if colIdx < 0 || colIdx >= len(s.cols) || block < 0 || block >= s.maxBlocks {
		return 0, 0, false
	}

	body := s.mm[s.statsOff+len(s.cols)*16+block*len(s.cols)*16+colIdx*16:]

	return record.Le64(body), record.Le64(body[8:]), true
}

// Capacity is the number of records the pre-allocated file can hold, minus the
// reserved trailer (column stats plus the sealed-segment checksum).
func (s *Segment[T]) Capacity() int {
	return (len(s.mm) - s.off - s.trailer) / s.hdr.RecordSize()
}

// ColumnSpan reports the min/max this segment holds for column name, read from
// the segment's trailer block. ok is false when the column is not indexed.
//
// The values are in the same CompareValue space a condition uses, so a caller
// can reject this segment when its whole span lies outside the condition's
// range. Reading them costs one map lookup per call and allocates nothing.
func (s *Segment[T]) ColumnSpan(name string) (lo, hi uint64, ok bool) {
	for i, c := range s.cols {
		if strings.EqualFold(c.Name, name) {
			body := s.mm[s.statsOff+i*16:]

			return record.Le64(body), record.Le64(body[8:]), true
		}
	}

	return 0, 0, false
}

// Seqlock counters. They are process-wide, not per-segment: a chart reads many
// segments and what matters for tuning is whether the writers are forcing the
// readers to retry, not which file caused it. Writers inflate the same counters
// when they collide on index flips, so the numbers need no second path.
var (
	// SeqlockRetries counts every failed consistency check across all callers.
	SeqlockRetries atomic.Uint64
	// SeqlockBusy counts the times a caller exhausted its retries and gave up,
	// plus the times the index fell back to an unverified read. A nonzero busy
	// number means the writer is too slow to publish; see Phase 5 notes.
	SeqlockBusy atomic.Uint64
)

// Snapshot returns a consistent view of the header. It retries while a writer
// holds the seqlock, then gives up with ErrBusy rather than spinning forever.
func (s *Segment[T]) Snapshot() (count, first, last uint64, err error) {
	for range 32 {
		count, first, last, ok := s.hdr.Snapshot()
		if ok {
			if count > uint64(s.Capacity()) {
				return 0, 0, 0, fmt.Errorf("zdb: %s: %w", s.Path, ErrCorrupt)
			}

			return count, first, last, nil
		}

		SeqlockRetries.Add(1)
		runtime.Gosched()
	}

	SeqlockBusy.Add(1)

	return 0, 0, 0, fmt.Errorf("zdb: %s: %w", s.Path, ErrBusy)
}

// Sync flushes this segment's mapping and file to disk. A read-only segment
// has nothing of its own to flush; the writer's own handle owns that.
func (s *Segment[T]) Sync() error {
	if s.file == nil {
		return nil
	}

	return s.file.Sync()
}

// tsAt reads record i's leading timestamp. That field is the one the format
// fixes to offset 0 for every record type, because it is the field the binary
// search orders on.
func tsAt(d []byte, i, w int) uint64 {
	return record.Le64(d[i*w:])
}

// Record returns record i as a value, so the caller can keep it after the mapping
// goes away. It allocates once per call: a generic instantiation cannot prove a
// value it returns stays on the stack, which is why the read path uses DecodeAt
// instead. Reach for DecodeAt in a loop and Record outside one.
func (s *Segment[T]) Record(i int) T {
	var v T
	s.DecodeAt(i, &v)

	return v
}

// DecodeAt decodes record i straight from the mapping into dst. It is the
// zero-copy read primitive and it allocates nothing: src is the mapping, dst is
// the caller's own record, and there is no staging buffer between them. dst is
// fully overwritten.
func (s *Segment[T]) DecodeAt(i int, dst *T) {
	var zero T
	zero.DecodeInto(s.mm[s.off+i*s.hdr.RecordSize():], dst)
}

// Len is the number of records currently readable in this segment.
func (s *Segment[T]) Len() int {
	count, _, _, err := s.Snapshot()
	if err != nil {
		return 0
	}

	return int(count)
}

// MinTs returns the segment's minimum timestamp. It reads through Snapshot so
// the value is consistent with a concurrent writer.
func (s *Segment[T]) MinTs() (uint64, error) {
	_, first, _, err := s.Snapshot()

	return first, err
}

// MaxTs returns the segment's maximum timestamp. It reads through Snapshot so
// the value is consistent with a concurrent writer.
func (s *Segment[T]) MaxTs() (uint64, error) {
	_, _, last, err := s.Snapshot()

	return last, err
}

// IsSealed reports whether this segment has been closed/sealed.
func (s *Segment[T]) IsSealed() bool {
	if s.hdr == nil {
		return false
	}

	return s.hdr.IsSealed()
}

// Window returns the record range of this segment that covers [from, to],
// reading the header itself. It exists so a caller can drive its own loop
// without allocating: no slice, no closure, no callback.
func (s *Segment[T]) Window(from, to uint64) (lo, hi int, err error) {
	count, first, last, err := s.Snapshot()
	if err != nil {
		return 0, 0, err
	}

	lo, hi = s.Plan(count, first, last, from, to)

	return lo, hi, nil
}

// lowerBound returns the first record index whose timestamp is >= ts.
func (s *Segment[T]) lowerBound(d []byte, n int, ts uint64) int {
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if tsAt(d, mid, s.hdr.RecordSize()) < ts {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// upperBound returns the first record index whose timestamp is > ts.
func (s *Segment[T]) upperBound(d []byte, n int, ts uint64) int {
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if tsAt(d, mid, s.hdr.RecordSize()) <= ts {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// Plan returns the half-open record range [lo, hi) covering [from, to]. The
// header's first and last timestamps reject non-overlapping segments in O(1),
// so a range query touches only the segments that can hold the answer.
//
// A zero bound is unbounded on that side. Milliseconds since 1970 are never
// negative and no market timestamp is zero, so this costs one comparison and
// needs no sentinel of its own.
func (s *Segment[T]) Plan(count, first, last, from, to uint64) (lo, hi int) {
	n := int(count)
	d := s.mm[s.off:]

	if n == 0 || from > last {
		return 0, 0
	}

	if to != 0 && first > to {
		return 0, 0
	}

	lo = s.lowerBound(d, n, from)
	if to == 0 {
		return lo, n // unbounded above: everything past the lower bound counts
	}

	return lo, s.upperBound(d, n, to)
}

// RecordAt is the byte offset of record i, which is the only arithmetic the
// format performs on a record position.
func (s *Segment[T]) RecordAt(i int) int { return s.off + i*s.hdr.RecordSize() }

// Each calls fn for every record in [from, to] in ascending order, stopping
// early if fn returns false. from and to are in the record's own Datetime unit
// and are inclusive. fn receives a value copy, so retaining it costs nothing and
// the mapping can be released as soon as Each returns.
func (s *Segment[T]) Each(from, to uint64, fn func(T) bool) error {
	count, first, last, err := s.Snapshot()
	if err != nil {
		return err
	}

	lo, hi := s.Plan(count, first, last, from, to)
	// One destination reused across the whole scan: the callback takes T by
	// value, so what it receives is a copy and out never has to be a fresh
	// variable per record.
	var out T
	for i := lo; i < hi; i++ {
		s.DecodeAt(i, &out)

		if !fn(out) {
			return nil
		}
	}

	return nil
}

// EachBackward calls fn for the newest record first, stopping after limit
// records or when fn returns false. It is the cheap path for "give me the last
// N candles" because a chart only ever needs the recent end of a range.
func (s *Segment[T]) EachBackward(from, to uint64, limit int, fn func(T) bool) error {
	count, first, last, err := s.Snapshot()
	if err != nil {
		return err
	}

	lo, hi := s.Plan(count, first, last, from, to)

	var out T
	for i := hi - 1; i >= lo && limit > 0; i-- {
		s.DecodeAt(i, &out)

		if !fn(out) {
			return nil
		}

		limit--
	}

	return nil
}

// Range returns the records in [from, to] in ascending order, capped at limit
// records. A limit of zero or less means no cap. The newest limit records in
// the window are returned, because that is what a chart renders.
func (s *Segment[T]) Range(from, to uint64, limit int) ([]T, error) {
	if limit > 0 {
		rev := make([]T, 0, min(limit, 256))
		err := s.EachBackward(from, to, limit, func(c T) bool {
			rev = append(rev, c)

			return true
		})

		for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
			rev[i], rev[j] = rev[j], rev[i]
		}

		return rev, err
	}

	out := make([]T, 0, 64)
	err := s.Each(from, to, func(c T) bool {
		out = append(out, c)

		return true
	})

	return out, err
}
