// Package reader serves candles from a ZDB store written by package writer.
//
// Resolution runs through the bucket indexes, never through a directory scan.
// A key's bucket is the leading byte of sha256(key), so a lookup is: map the
// bucket index, binary-search it for the key's segment chain, then map those
// segments. Everything a query touches is a memory-mapped file.
//
// # Multi-file mapping
//
// A store holds many files: one index per bucket, plus one segment file per key
// per rollover. The Reader maps them lazily and keeps every mapping for the life
// of the Reader. Segment files are pre-allocated, so their size never changes
// and a mapping is never remapped; a new segment appears only when a writer
// rolls over or seals, and Refresh maps it.
//
// # Read path
//
// A query takes no lock and allocates nothing. A Cursor walks the key's
// segments; each segment rejects non-overlapping ranges in O(1) from its header
// bounds, and the survivors binary-search their mapped bytes. Concurrent readers
// share one Reader with no coordination.
package reader

import (
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/aldok10/zdb/internal/format"
	"github.com/aldok10/zdb/record"
)

// zoneBound is one column cutoff a cursor can use to skip whole blocks.
type zoneBound struct {
	col int // index into the segment's stats column list, or -1
	lo  uint64
	hi  uint64
}

// ErrKeyNotFound means no segment exists for the key.
var ErrKeyNotFound = errors.New("zdb/reader: key not found")

// keyCacheMax bounds the per-key resolution cache. It holds one pointer slice per
// distinct key queried, so it costs ~32 bytes per key and no descriptor; the
// cache is dropped wholesale when it fills. Entry eviction costs O(1) amortized
// and no bookkeeping per hit, which is why this is a reset and not an LRU.
//
// ponytail: an LRU would keep the hot keys resident under churn. Add one if a
// measured workload queries far more distinct keys than keyCacheMax and shows
// the re-resolve cost.
const keyCacheMax = 4096

// Reader is a read handle over a ZDB directory. It is safe for concurrent use.
type Reader[T record.Record[T]] struct {
	dir     string
	indexes *format.BucketSet

	mu   sync.RWMutex
	segs map[string]*format.Segment[T] // segment path -> mapping; the only map of
	// what is mapped, so nothing can be mapped twice
	keys map[string][]*format.Segment[T] // key -> its segments, ascending seq
}

// Open maps the store's bucket indexes and prepares the segment cache. T is the
// record type the store holds: Open[format.Bar] for candles, Open[format.Tick] for
// ticks. A segment whose header disagrees about the record width is refused, so
// the wrong T fails at the segment rather than returning plausible garbage.
func Open[T record.Record[T]](dir string) (*Reader[T], error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("zdb/reader: open %s: %w", dir, err)
	}

	if !st.IsDir() {
		return nil, fmt.Errorf("zdb/reader: %s is not a directory", dir)
	}

	indexes, err := format.OpenBucketSet(dir)
	if err != nil {
		return nil, err
	}

	return &Reader[T]{
		dir:     dir,
		indexes: indexes,
		segs:    make(map[string]*format.Segment[T]),
		keys:    make(map[string][]*format.Segment[T]),
	}, nil
}

// resolve returns key's segments in ascending sequence order. A cache hit costs
// one map lookup and allocates nothing. The returned slice is shared with the
// cache and must be treated as read-only.
func (r *Reader[T]) resolve(key string) ([]*format.Segment[T], error) {
	r.mu.RLock()
	segs, ok := r.keys[key]
	r.mu.RUnlock()

	if ok {
		return segs, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if segs, ok = r.keys[key]; ok {
		return segs, nil
	}

	segs, err := r.resolveLocked(key)
	if err != nil {
		return nil, err
	}

	if len(r.keys) >= keyCacheMax {
		clear(r.keys)
	}

	r.keys[key] = segs

	return segs, nil
}

// resolveLocked maps the segments holding key. It runs once per key: the index
// lookup reads the segment sequences in place and never copies them out of the
// mapping, so the only allocations here are the path strings and the slice.
func (r *Reader[T]) resolveLocked(key string) ([]*format.Segment[T], error) {
	seqs, found, err := r.indexes.Locate(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
		}

		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	id := format.KeyID(key)

	segs := make([]*format.Segment[T], 0, len(seqs)/4)
	for i := range len(seqs) / 4 {
		seg, err := r.segmentLocked(id, format.SeqAt(seqs, i))
		if err != nil {
			return nil, err
		}

		segs = append(segs, seg)
	}

	return segs, nil
}

// segmentLocked returns the mapping for id's segment seq, mapping it on first
// use. r.segs is the single record of what is mapped, so a path can never be
// opened twice and Refresh cannot duplicate a descriptor.
func (r *Reader[T]) segmentLocked(id string, seq uint32) (*format.Segment[T], error) {
	path := filepath.Join(r.dir, segName(id, seq))
	if seg := r.segs[path]; seg != nil {
		return seg, nil
	}

	seg, err := format.OpenSegment[T](path, seq)
	if err != nil {
		if errors.Is(err, format.ErrCorrupt) {
			return nil, fmt.Errorf("zdb/reader: %s: %w", path, err)
		}

		return nil, err
	}

	r.segs[path] = seg

	return seg, nil
}

// RetentionFloor returns the oldest timestamp a ceiling of keep still shows
// for key in [from, to]: everything older would have to be walked and then
// discarded by a retention view that keeps the newest, so a caller can start
// its ascending walk there instead of buffering from the other end. keep <= 0
// means unbounded, so from comes back unchanged.
//
// It costs one pass over the segment headers and one decoded record, never a
// scan of the records themselves.
func (r *Reader[T]) RetentionFloor(key string, from, to uint64, keep uint64) (uint64, error) {
	if keep == 0 {
		return from, nil
	}

	segs, err := r.resolve(key)
	if err != nil {
		return 0, err
	}

	var total int

	for _, s := range segs {
		lo, hi, err := s.Window(from, to)
		if err != nil {
			continue
		}

		total += hi - lo
	}

	skip := total - int(keep)
	if skip <= 0 {
		return from, nil
	}

	var seen int

	for _, s := range segs {
		lo, hi, err := s.Window(from, to)
		if err != nil {
			continue
		}

		n := hi - lo
		if seen+n > skip {
			var rec T

			s.DecodeAt(lo+(skip-seen), &rec)

			return uint64(rec.Stamp()), nil
		}

		seen += n
	}

	return from, nil
}

// Refresh maps segments created since the last scan. Existing mappings are left
// untouched: a segment's size never changes, so there is nothing to remap, and
// r.segs already holds every mapped path so nothing is opened twice. A bucket
// index that did not exist at Open is mapped on demand instead.
//
// Refresh drops the resolution cache because a writer may have rolled or sealed
// a segment, which changes a key's chain. Mappings survive, so the next query
// re-reads the index rather than re-opening files.
func (r *Reader[T]) Refresh() error {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return fmt.Errorf("zdb/reader: read dir %s: %w", r.dir, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, format.SegmentSuffix) {
			continue
		}

		path := filepath.Join(r.dir, name)
		if _, dup := r.segs[path]; dup {
			continue
		}

		seq, ok := parseSeq(name)
		if !ok {
			continue
		}

		seg, err := format.OpenSegment[T](path, seq)
		if err != nil {
			// A file the writer is still creating cannot be mapped yet; the next
			// Refresh picks it up. Anything else is a real problem.
			if errors.Is(err, format.ErrCorrupt) {
				continue
			}

			return err
		}

		r.segs[path] = seg
	}

	clear(r.keys)

	return nil
}

// parseSeq reads the segment sequence out of "<keyid>-<seq>.zseg".
func parseSeq(path string) (uint32, bool) {
	base := strings.TrimSuffix(filepath.Base(path), format.SegmentSuffix)

	i := strings.LastIndexByte(base, '-')
	if i < 0 {
		return 0, false
	}

	seq, err := strconv.ParseUint(base[i+1:], 10, 32)

	return uint32(seq), err == nil
}

func segName(id string, seq uint32) string {
	return id + "-" + strconv.FormatUint(uint64(seq), 10) + format.SegmentSuffix
}

// Cursor walks one query's result. The caller owns it and reuses it across
// redraws, so a chart does no allocation at all after the first pass.
//
// A Cursor borrows the Reader's mappings. Closing the Reader while a Cursor is
// live is a use-after-unmap, exactly as for any other borrowed mapping.
type Cursor[T record.Record[T]] struct {
	segs []*format.Segment[T]
	seg  int // segment being read
	i    int // next record index within it
	stop int // index to stop before, inclusive of the direction's end
	step int // +1 ascending, -1 descending
	left int // records left under the limit; <= 0 means unbounded
	rev  bool
	from uint64
	to   uint64
	err  error

	// bounds optionally rejects segments whose per-column span cannot hold a
	// qualifying record. Nil keeps every segment, so the common caller pays
	// nothing.
	bounds []record.ColumnBound

	// z resolves bounds to stats-column indexes once per positioning. The
	// cursor's Next checks the current block's min/max row before decoding it,
	// so a block whose whole range fails the predicate is skipped by an index
	// jump, not by 256 decodes.
	z []zoneBound

	// zSeg/zBlock/zOK cache the verdict for the last checked block of one
	// segment, so a block's zone row is read once instead of per record.
	zSeg   int
	zBlock int
	zOK    bool
}

// Cursor positions c over key's records in [from, to], ascending by default.
// from and to are in the record's own Datetime unit and inclusive; zero leaves
// that side unbounded, which is exact because no market timestamp is zero.
// limit <= 0 means no cap. When limit bites, the newest records in the window
// win, because that is the end a chart renders, so the cursor walks newest first
// and the caller reverses if it needs ascending order.
//
// c is overwritten, not reset, so it must be reused across queries.
func (r *Reader[T]) Cursor(key string, from, to uint64, limit int, c *Cursor[T]) error {
	return r.cursor(key, from, to, limit, c, nil, limit > 0)
}

// CursorFiltered positions c over key's records in [from, to], additionally
// skipping any segment whose per-column index lies entirely outside one of
// bounds. Zero records in a segment still cost one window lookup, so pruning a
// whole segment where it is certain.
func (r *Reader[T]) CursorFiltered(key string, from, to uint64, limit int, bounds []record.ColumnBound, c *Cursor[T]) error {
	return r.cursor(key, from, to, limit, c, bounds, limit > 0)
}

// CursorDesc positions c over key's records in [from, to], always newest
// first, regardless of limit. A limit caps the rows walked; pass limit <= 0
// to walk the whole window backwards. It exists for filtered queries, where a
// limit on matches cannot come from the reader: the filter runs per record,
// so walking newest-first until enough matches survive must not cap rows.
func (r *Reader[T]) CursorDesc(key string, from, to uint64, limit int, c *Cursor[T]) error {
	return r.cursor(key, from, to, limit, c, nil, true)
}

// CursorDescFiltered is CursorDesc with the segment-pruned bounds.
func (r *Reader[T]) CursorDescFiltered(key string, from, to uint64, limit int, bounds []record.ColumnBound, c *Cursor[T]) error {
	return r.cursor(key, from, to, limit, c, bounds, true)
}

func (r *Reader[T]) cursor(key string, from, to uint64, limit int, c *Cursor[T], bounds []record.ColumnBound, rev bool) error {
	segs, err := r.resolve(key)
	if err != nil {
		return err
	}

	c.segs = segs
	c.from, c.to = from, to
	c.err = nil
	c.rev = rev
	c.bounds = bounds

	c.zSeg, c.zBlock, c.zOK = -1, -1, true
	if len(bounds) == 0 {
		c.z = nil
	} else {
		sc, err := record.SchemaOf[T]()
		if err != nil {
			return err
		}

		cols := sc.StatsColumns()

		c.z = make([]zoneBound, len(bounds))
		for i, b := range bounds {
			c.z[i] = zoneBound{col: -1, lo: b.Lo, hi: b.Hi}

			for j, col := range cols {
				if col.Name == b.Name {
					c.z[i].col = j

					break
				}
			}
		}
	}

	// left is a countdown, so an unbounded query gets a count it can never
	// reach. Zero therefore means exactly "exhausted" and nothing else.
	if limit > 0 {
		c.left = limit
	} else {
		c.left = math.MaxInt
	}

	if c.rev {
		c.seg = len(segs)
	} else {
		c.seg = -1
	}

	if !c.advance() {
		return c.err
	}

	return nil
}

// Next writes the next record to out and reports whether one was available. It
// allocates nothing and takes no lock.
//
// out receives a copy, so a caller that keeps what it gets must copy it again;
// the alternative is one heap allocation per record. When out points at the
// cursor's own Next argument and the caller only reads it inside the loop, which
// is what a chart does, the copy is the whole cost.
func (c *Cursor[T]) Next(out *T) bool {
	for {
		if c.left == 0 {
			return false
		}

		if (c.step > 0 && c.i < c.stop) || (c.step < 0 && c.i > c.stop) {
			if len(c.z) != 0 {
				b := c.i >> format.BlockRecordsShift

				if b != c.zBlock || c.seg != c.zSeg {
					ok := true

					for _, zb := range c.z {
						if zb.col < 0 {
							continue
						}

						lo, hi, found := c.segs[c.seg].BlockValue(zb.col, b)
						if !found {
							continue
						}

						if hi < zb.lo || lo > zb.hi {
							ok = false

							break
						}
					}

					c.zOK = ok
					c.zBlock = b
					c.zSeg = c.seg
				}

				if !c.zOK {
					if c.step > 0 {
						c.i = (b + 1) << format.BlockRecordsShift
					} else {
						c.i = (b << format.BlockRecordsShift) - 1
					}

					continue
				}
			}

			c.segs[c.seg].DecodeAt(c.i, out)
			c.i += c.step
			c.left--

			return true
		}

		if !c.advance() {
			return false
		}
	}
}

// Err reports why iteration stopped early. Check it after Next returns false: a
// query that walked cleanly reports nil.
func (c *Cursor[T]) Err() error { return c.err }

// segmentOK reports whether the current segment might hold a matching record,
// given bounds. A bound whose column is not indexed keeps the segment, so an
// unindexed field never prunes; a span outside every bound rejects it.
func (c *Cursor[T]) segmentOK() bool {
	for _, b := range c.bounds {
		spanLo, spanHi, ok := c.segs[c.seg].ColumnSpan(b.Name)
		if !ok {
			continue
		}

		if spanHi < b.Lo || spanLo > b.Hi {
			return false
		}
	}

	return true
}

// advance moves to the next segment with data in the window.
func (c *Cursor[T]) advance() bool {
	if c.left == 0 {
		return false
	}

	for {
		if c.rev {
			c.seg--
		} else {
			c.seg++
		}

		if c.seg < 0 || c.seg >= len(c.segs) {
			return false
		}

		lo, hi, err := c.segs[c.seg].Window(c.from, c.to)
		if err != nil {
			c.err = err

			return false
		}

		if lo >= hi {
			continue // this segment holds nothing in the window
		}

		// Whole-segment pruning: a segment whose stored column range cannot
		// overlap the condition can never hold a matching record. The window
		// test below still runs per record, so pruning can only reject, never
		// invent, results.
		if !c.segmentOK() {
			continue
		}

		if c.rev {
			c.i, c.stop, c.step = hi-1, lo-1, -1
		} else {
			c.i, c.stop, c.step = lo, hi, 1
		}

		return true
	}
}

// All iterates the same bars Range returns, one at a time, without building the
// result slice. That is the difference between a 500k-bar query holding 30 MB of
// bars alive and holding one: Range's slice is a copy of what the mapping already
// holds, and All never makes it.
//
//	for bar, err := range r.All("BTCUSDT", from, to, 500) {
//	    if err != nil {
//	        return err
//	    }
//	    draw(bar)
//	    if enough() {
//	        break // stops the walk here; nothing after this point is touched
//	    }
//	}
//
// It costs one closure per call rather than nothing at all: the sequence has to
// capture the query. That is one small allocation per query instead of one slice
// per query, and a caller that cannot afford even that should drive Cursor
// directly.
func (r *Reader[T]) All(key string, from, to uint64, limit int) iter.Seq2[T, error] {
	return r.all(key, from, to, limit, nil)
}

// AllBounds streams like All but skips segments whose index cannot contain a
// qualifying record for bounds.
func (r *Reader[T]) AllBounds(key string, from, to uint64, limit int, bounds []record.ColumnBound) iter.Seq2[T, error] {
	return r.all(key, from, to, limit, bounds)
}

func (r *Reader[T]) all(key string, from, to uint64, limit int, bounds []record.ColumnBound) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var c Cursor[T]
		if err := r.cursor(key, from, to, limit, &c, bounds, limit > 0); err != nil {
			var zero T
			yield(zero, err)

			return
		}

		var out T
		if limit <= 0 {
			// Unbounded: the cursor walks ascending, so the mapping can be handed
			// over one bar at a time and never materialized.
			for c.Next(&out) {
				if !yield(out, nil) {
					return
				}
			}

			if err := c.Err(); err != nil {
				var zero T
				yield(zero, err)
			}

			return
		}

		// A limit makes the cursor walk newest first, which is the only direction a
		// streaming walk can go. Range reverses before returning, so buffering at
		// most limit bars keeps All and Range in the same order: a chart that
		// streamed the newest 500 would otherwise be drawn backwards. The bound is
		// the caller's own limit, so this cannot grow without the caller choosing it.
		buf := make([]T, 0, limit)
		for c.Next(&out) {
			buf = append(buf, out)
		}

		if err := c.Err(); err != nil {
			var zero T
			yield(zero, err)

			return
		}

		slices.Reverse(buf)

		for _, bar := range buf {
			if !yield(bar, nil) {
				return
			}
		}
	}
}

// Range returns up to limit candles in [from, to] for key, ascending. See
// Cursor for the window and limit semantics. Range allocates its result slice,
// so a caller that redraws should use Scan or Cursor instead; RangeInto with a
// caller-owned buffer avoids even that allocation.
func (r *Reader[T]) Range(key string, from, to uint64, limit int) ([]T, error) {
	return r.RangeInto(key, from, to, limit, nil)
}

// RangeInto is Range writing into dst, which it overwrites from index zero. Pass
// a reused slice and the query allocates nothing at all; the returned slice may
// be the same array as dst.
func (r *Reader[T]) RangeInto(key string, from, to uint64, limit int, dst []T) ([]T, error) {
	var c Cursor[T]
	if err := r.Cursor(key, from, to, limit, &c); err != nil {
		return nil, err
	}

	dst = dst[:0]

	// Size the slice up front: append over a nil slice grows by doubling, which
	// is ten allocations for a 501-bar window. Window math per segment is exact
	// for the Range path, so one pass buys one slice allocation.
	if limit <= 0 || cap(dst) < limit {
		total := 0

		for _, seg := range c.segs {
			lo, hi, err := seg.Window(from, to)
			if err != nil {
				return nil, err
			}

			total += hi - lo
		}

		if limit > 0 && total > limit {
			total = limit
		}

		if cap(dst) < total {
			dst = make([]T, 0, total)
		}
	}

	// Decode straight into the slice's own storage: there is no local record
	// variable to escape through the generic DecodeInto call, so the per-query
	// record allocation disappears.
	dst = dst[:cap(dst)]
	n := 0
	for n < cap(dst) && c.Next(&dst[n]) {
		n++
	}

	dst = dst[:n]
	if c.rev {
		slices.Reverse(dst)
	}

	return dst, c.Err()
}

// Latest returns the newest limit candles for key, ascending.
func (r *Reader[T]) Latest(key string, limit int) ([]T, error) {
	return r.Range(key, 0, 0, limit)
}

// Scan calls fn for up to limit candles in [from, to], ascending, stopping
// early if fn returns false. It allocates nothing, so it is the path for
// rendering straight from the mapping.
func (r *Reader[T]) Scan(key string, from, to uint64, limit int, fn func(T) bool) error {
	var c Cursor[T]
	if err := r.Cursor(key, from, to, limit, &c); err != nil {
		return err
	}

	var out T
	for c.Next(&out) {
		if !fn(out) {
			return nil
		}
	}

	return c.Err()
}

// Keys lists every shard key in the store, sorted. It reads the bucket indexes,
// so it costs one pass over the index tables and no segment mapping.
// Keys lists every shard key in the store, sorted. It walks the bucket indexes
// with AllEntries, so a store with many keys does not need every Entry alive at
// once; only the key slice itself is materialized, because a sorted result has to
// be materialized to be sorted.
//
// ponytail: there is no AllKeys, because a sorted sequence cannot be lazy without
// a k-way merge across 256 bucket streams. Add one if enumeration ever becomes a
// hot path; for now it would only look lazy.
func (r *Reader[T]) Keys() []string {
	var out []string

	for _, bucket := range r.indexes.Buckets() {
		ix, err := r.indexes.Bucket(bucket)
		if err != nil {
			continue
		}

		for e, err := range ix.AllEntries() {
			if err != nil {
				break
			}

			out = append(out, e.Key)
		}
	}

	slices.Sort(out)

	return out
}

// List lists shard keys under prefix, sorted. Keys are symbol paths, so
// List("binance/spot/BTC") narrows to one exchange's BTC pairs.
func (r *Reader[T]) List(prefix string) []string {
	all := r.Keys()
	if prefix == "" {
		return all
	}

	out := make([]string, 0, len(all))
	for _, k := range all {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}

	return out
}

// Bounds reports the first and last candle timestamps stored for key. It reads
// headers only, so it does not touch record bytes.
func (r *Reader[T]) Bounds(key string) (first, last uint64, err error) {
	segs, err := r.resolve(key)
	if err != nil {
		return 0, 0, err
	}

	if len(segs) == 0 {
		return 0, 0, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	_, f, _, err := segs[0].Snapshot()
	if err != nil {
		return 0, 0, fmt.Errorf("zdb/reader: %s: %w", key, err)
	}

	_, _, l, err := segs[len(segs)-1].Snapshot()
	if err != nil {
		return 0, 0, fmt.Errorf("zdb/reader: %s: %w", key, err)
	}

	return f, l, nil
}

// Count reports how many candles key holds, read from the segment headers. It
// is O(segments), not O(candles).
func (r *Reader[T]) Count(key string) (uint64, error) {
	segs, err := r.resolve(key)
	if err != nil {
		return 0, err
	}

	var total uint64

	for _, seg := range segs {
		count, _, _, err := seg.Snapshot()
		if err != nil {
			return 0, fmt.Errorf("zdb/reader: %s: %w", key, err)
		}

		total += count
	}

	return total, nil
}

// Segments reports how many files hold key. A key crosses into a second file
// only on rollover, so this is 1 for almost every key.
func (r *Reader[T]) Segments(key string) (int, error) {
	seqs, found, err := r.indexes.Locate(key)
	if err != nil {
		return 0, err
	}

	if !found {
		return 0, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	return len(seqs) / 4, nil
}

// Close unmaps every segment and bucket index. It is idempotent.
func (r *Reader[T]) Close() error {
	r.mu.Lock()
	segs := slices.Collect(maps.Values(r.segs))
	clear(r.segs)
	clear(r.keys)
	r.mu.Unlock()

	var errs []error
	for _, seg := range segs {
		errs = append(errs, seg.Close())
	}

	errs = append(errs, r.indexes.Close())

	return errors.Join(errs...)
}
