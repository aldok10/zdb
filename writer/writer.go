// Package writer appends candles to a ZDB store, sharded by key.
//
// One key is one shard: its own segment files, its own lock, its own index
// entry. No cross-symbol contention, and an independent write path per symbol.
//
// Keys are grouped into bucket index files, one per leading byte of
// sha256(key). A bucket's index maps every key it owns to that key's segment
// chain, so the directory stays small and a reader resolves a symbol to its
// files without scanning anything.
//
// A shard is a chain of pre-allocated segments. Appending only raises the
// header's count, never the file size, so a reader maps a segment once and
// never remaps it. The header update happens inside a seqlock window, so a
// reader either sees the record or does not; it never sees half of one.
package writer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/aldok10/zdb/internal/format"
	"github.com/aldok10/zdb/internal/platform"
	"github.com/aldok10/zdb/record"
)

// DefaultSegmentBytes is the pre-allocated size of one segment file.
const DefaultSegmentBytes int64 = 16 << 20

// Options configures a store. Zero fields take their defaults.
type Options struct {
	// Dir is the directory holding segment and index files. Created if missing.
	Dir string
	// SegmentBytes is the pre-allocated size of each segment file.
	// Defaults to DefaultSegmentBytes. Raising it costs sparse space, not
	// disk, until the records arrive.
	SegmentBytes int64
	// FileMode is the permission bits of new files.
	FileMode fs.FileMode
	// IndexRegionBytes is each bucket index region's size. Defaults to
	// format.DefaultIndexRegionBytes. A region must fit every key in one bucket
	// plus its segment lists, so raise it when a store holds very many symbols
	// per bucket or very deep rollovers.
	IndexRegionBytes int64
	// BucketDuration is how much wall-clock time one segment holds before the
	// writer rolls to the next one, which is what makes a segment a deletable
	// unit of time for a retention pass. Zero, the default, keeps rollover on
	// segment size alone.
	//
	// The width is measured in the record type's own stamp unit, so one store
	// can ask for an hour and get it whether its records count milliseconds or
	// microseconds. A bucket shorter than one stamp, or a record type whose
	// time unit is not known, is refused by Open rather than stored as a bucket
	// of width zero.
	BucketDuration time.Duration
	// Sync is an optional Policy that decides when SyncIfDue flushes. Nil leaves
	// the store unadapted and SyncIfDue never flushes, so nothing changes for a
	// caller that does not want it.
	Sync *Policy
}

func (o Options) withDefaults() Options {
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = DefaultSegmentBytes
	}

	if o.FileMode == 0 {
		o.FileMode = 0o644
	}

	if o.IndexRegionBytes <= 0 {
		o.IndexRegionBytes = format.DefaultIndexRegionBytes
	}

	return o
}

// DB is a writer handle. It is safe for concurrent use.
//
// T is the record type. The width comes from T and is written into every segment
// header, so a reader learns the layout from the file. A store is one record
// type for its whole life: resuming a segment whose recorded width is not T's is
// refused rather than decoded as the wrong layout.
type DB[T record.Record[T]] struct {
	dir       string
	size      int64
	recSize   int
	indexSize int64
	mode      fs.FileMode
	// bucketTicks is BucketDuration expressed in stamps, zero when no bucket
	// was asked for. It is resolved once at Open so the append path only
	// divides, never converts a duration.
	bucketTicks uint64
	mu          sync.RWMutex
	shard       map[string]*shard
	buckets     map[string]*bucket
	bucketsMu   sync.Mutex // guards bucket creation only
	pol         *Policy

	// statsIdx caches the indexable columns (non-time, non-bytes) and the
	// trailer offset, so the append and open paths never revisit the schema.
	statsCols []record.Column
	trailer   int
	statsOff  int
}

// Open prepares dir for writing.
func Open[T record.Record[T]](opts Options) (*DB[T], error) {
	opts = opts.withDefaults()
	if opts.Dir == "" {
		return nil, errors.New("zdb: writer requires a directory")
	}

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("zdb: create writer dir %s: %w", opts.Dir, err)
	}

	ticks, err := bucketWidth[T](opts.BucketDuration)
	if err != nil {
		return nil, err
	}

	return &DB[T]{
		dir:         opts.Dir,
		size:        opts.SegmentBytes,
		recSize:     record.SizeOf[T](),
		indexSize:   opts.IndexRegionBytes,
		mode:        opts.FileMode,
		bucketTicks: ticks,
		shard:       make(map[string]*shard),
		buckets:     make(map[string]*bucket),
		pol:         opts.Sync,
		statsCols:   statsCols[T](),
		trailer:     format.TrailerBytes[T](opts.SegmentBytes),
		statsOff:    int(opts.SegmentBytes) - format.TrailerBytes[T](opts.SegmentBytes),
	}, nil
}

// statsCols returns the columns a temporary index is kept for. It is a thin
// wrapper so DB.Open can cache the slice and the append path never recomputes
// it along the hot path.
func statsCols[T record.Record[T]]() []record.Column {
	sc, err := record.SchemaOf[T]()
	if err != nil {
		return nil
	}

	return sc.StatsColumns()
}

// bucketWidth converts a wall-clock bucket into stamps of T's own time unit.
// It is called once at Open, so a caller who sets no bucket never pays for a
// schema lookup either.
func bucketWidth[T record.Record[T]](d time.Duration) (uint64, error) {
	if d <= 0 {
		return 0, nil
	}

	sch, err := record.SchemaOf[T]()
	if err != nil {
		return 0, fmt.Errorf("zdb: bucket duration needs a time unit: %w", err)
	}

	n := sch.Unit.Ticks(d)
	if n < 0 {
		return 0, fmt.Errorf("zdb: record type's time unit is unknown, so %s cannot be measured in stamps: %w",
			d, format.ErrInvalid)
	}

	if n == 0 {
		return 0, fmt.Errorf("zdb: bucket of %s is shorter than the record's %s time unit: %w",
			d, sch.Unit, format.ErrInvalid)
	}

	return uint64(n), nil
}

// bucketOf returns the bucket stamp falls in. width must be non-zero, which
// bucketWidth guarantees for every store that has one.
func bucketOf(stamp, width uint64) uint64 { return stamp / width }

// bucket is one bucket index file plus the entries the writer knows for it.
// The writer holds every entry in memory and rewrites the region when the set
// changes, which keeps the on-disk format free of an incremental update path.
type bucket struct {
	mu      sync.Mutex
	index   *format.Index
	entries map[uint64]format.Entry
}

// bucketFor returns the writable bucket index for key, creating it if needed.
func (db *DB[T]) bucketFor(key string) (*bucket, error) {
	name := format.Bucket(key)

	db.bucketsMu.Lock()
	b := db.buckets[name]
	db.bucketsMu.Unlock()

	if b != nil {
		return b, nil
	}

	db.bucketsMu.Lock()
	defer db.bucketsMu.Unlock()

	if b = db.buckets[name]; b != nil {
		return b, nil
	}

	ix, err := format.CreateIndex(format.IndexPath(db.dir, key), name, db.indexSize)
	if err != nil {
		return nil, err
	}

	entries := make(map[uint64]format.Entry)

	if existing, err := ix.Entries(); err == nil {
		for _, e := range existing {
			entries[e.Hash] = e
		}
	} else if !errors.Is(err, format.ErrCorrupt) {
		ix.Close()

		return nil, err
	}

	b = &bucket{index: ix, entries: entries}
	db.buckets[name] = b

	return b, nil
}

// publishLocked writes the bucket's entry set into its idle index region. The
// caller holds b.mu.
func (b *bucket) publishLocked() error {
	list := make([]format.Entry, 0, len(b.entries))
	for _, e := range b.entries {
		list = append(list, e)
	}

	return b.index.Put(list)
}

// shardFor returns the writable shard for key, opening or creating it.
func (db *DB[T]) shardFor(key string) (*shard, error) {
	db.mu.RLock()
	sh := db.shard[key]
	db.mu.RUnlock()

	if sh != nil {
		return sh, nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if sh = db.shard[key]; sh != nil {
		return sh, nil
	}

	sh, err := openShard[T](db.dir, key, db.size, db.mode, db.recSize)
	if err != nil {
		return nil, err
	}

	db.shard[key] = sh

	return sh, db.registerLocked(sh)
}

// registerLocked points the key's bucket index at the shard's segments. The
// caller holds db.mu.
func (db *DB[T]) registerLocked(sh *shard) error {
	b, err := db.bucketFor(sh.key)
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.entries[sh.hash]
	if e.Key == "" {
		e = format.Entry{Hash: sh.hash, Key: sh.key}
	}
	// The open segment is indexed even though it is still growing: its header
	// count is what readers check, and rolling a segment never hides the one
	// before it.
	if !slices.Contains(e.Segs, sh.seq) {
		e.Segs = append(e.Segs, sh.seq)
		slices.Sort(e.Segs)
	}

	b.entries[sh.hash] = e

	return b.publishLocked()
}

// Append writes one record. It fails with ErrOutOfOrder when the record's stamp
// is not strictly greater than the shard's last.
func (db *DB[T]) Append(key string, c T) error {
	return db.AppendBatch(key, []T{c})
}

// AppendBatch writes records in one lock acquisition, split across as many
// segments as the batch needs. A batch that does not fit the current segment's
// remainder starts a fresh one; a batch larger than one segment fills segment
// after segment. This is the unit a market-data feed should use (one write
// barrier per symbol per tick) and the unit a bulk load should use (one call
// for any number of records).
//
// Every chunk commits in its own seqlock window, so a reader concurrent with a
// spanning batch sees a consistent prefix: each published header is complete
// and the stamps keep rising across a segment boundary. An unencodable record
// fails the whole batch with nothing committed (see validateEncodable). An I/O
// failure mid-span is different: chunks already committed stay committed, and
// the error names how many records made it.
func (db *DB[T]) AppendBatch(key string, cs []T) error {
	if len(cs) == 0 {
		return nil
	}

	sh, err := db.shardFor(key)
	if err != nil {
		return err
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	prev := sh.last

	for i, c := range cs {
		// The header stores timestamps unsigned and the binary search compares them
		// as unsigned, so a negative stamp would wrap into a value that sorts
		// after every real record. Reject it rather than encode it.
		if c.Stamp() < 0 {
			return fmt.Errorf("zdb: %s: record %d has negative stamp %d: %w",
				key, i, c.Stamp(), format.ErrInvalid)
		}

		if i > 0 && uint64(c.Stamp()) <= prev {
			return fmt.Errorf("zdb: %s: record %d at %d is not newer than record %d: %w",
				key, i, c.Stamp(), i-1, format.ErrOutOfOrder)
		}

		prev = uint64(c.Stamp())
	}

	if sh.count > 0 && uint64(cs[0].Stamp()) <= sh.last {
		return fmt.Errorf("zdb: %s: record at %d is not newer than the last stored record: %w",
			key, cs[0].Stamp(), format.ErrOutOfOrder)
	}

	if err := db.ensureOpenLocked(sh); err != nil {
		return err
	}

	// A batch larger than one segment commits its first chunk before the rest is
	// encoded, so the tail is proven encodable first: an unencodable record there
	// then fails with nothing committed, which is the guarantee the single-chunk
	// path gets from encoding before it commits.
	if len(cs) > sh.capacity() {
		bad, verr := validateEncodable(cs[sh.capacity():], db.recSize)
		if verr != nil {
			return fmt.Errorf("zdb: %s: record %d at %d: %w",
				key, sh.capacity()+bad, cs[sh.capacity()+bad].Stamp(), verr)
		}
	}

	for i := 0; i < len(cs); {
		// Roll only from a segment that holds records: rolling an empty one
		// produces another empty one, and would leave an orphaned file behind.
		if room := sh.capacity() - int(sh.count); sh.count > 0 && len(cs)-i > room {
			if err := db.rolloverLocked(sh); err != nil {
				return fmt.Errorf("zdb: %s: %d of %d records committed: %w",
					key, i, len(cs), err)
			}
		}

		// A time-bounded segment holds one bucket only. Roll before encoding
		// when the next record would strand records from two buckets in one
		// file, which would make "delete everything older than T" delete data
		// younger than T too.
		if db.bucketTicks > 0 && sh.count > 0 &&
			bucketOf(uint64(cs[i].Stamp()), db.bucketTicks) != bucketOf(sh.first, db.bucketTicks) {
			if err := db.rolloverLocked(sh); err != nil {
				return fmt.Errorf("zdb: %s: %d of %d records committed: %w",
					key, i, len(cs), err)
			}
		}

		n := min(len(cs)-i, sh.capacity()-int(sh.count))

		if db.bucketTicks > 0 {
			b0 := uint64(0)
			if sh.count > 0 {
				b0 = bucketOf(sh.first, db.bucketTicks)
			} else {
				b0 = bucketOf(uint64(cs[i].Stamp()), db.bucketTicks)
			}

			for k := 0; k < n; k++ {
				if bucketOf(uint64(cs[i+k].Stamp()), db.bucketTicks) != b0 {
					n = k

					break
				}
			}

			if n == 0 {
				return fmt.Errorf("zdb: %s: bucket of record at %d does not match segment: %w",
					key, cs[i].Stamp(), format.ErrInvalid)
			}
		}

		// Encode before opening the write window. The destination sits past
		// sh.count, which no reader looks at until the count is published, so
		// writing it early is invisible. In exchange an encoding error never
		// commits a torn window, and the chunk pays for exactly one encode.
		dst := sh.mm[sh.off+int(sh.count)*db.recSize:]

		for j := range n {
			if err := cs[i+j].EncodeInto(dst[j*db.recSize:]); err != nil {
				// Unreachable for a spanning batch, which validateEncodable
				// proved encodable; unreachable for a single chunk, whose encode
				// runs before any commit.
				return fmt.Errorf("zdb: %s: record %d at %d (%d of %d records committed): %w",
					key, i+j, cs[i+j].Stamp(), i, len(cs), err)
			}
		}

		segFirst := sh.first
		if sh.count == 0 {
			segFirst = uint64(cs[i].Stamp())
		}

		count := sh.count + uint64(n)
		last := uint64(cs[i+n-1].Stamp())

		// Update the running per-column ranges before the commit window opens, so
		// the trailer block rendered in the window covers them.
		for j := range n {
			if bi := int(sh.count+uint64(j)) >> format.BlockRecordsShift; bi != sh.curBlock {
				sh.flushBlock()
				sh.curBlock = bi
			}

			p := unsafe.Pointer(&cs[i+j])
			for k := range sh.cols {
				v := sh.cols[k].CompareValue(p)
				if v < sh.colMin[k] {
					sh.colMin[k] = v
				}

				if v > sh.colMax[k] {
					sh.colMax[k] = v
				}

				if v < sh.blkMin[k] {
					sh.blkMin[k] = v
				}

				if v > sh.blkMax[k] {
					sh.blkMax[k] = v
				}
			}
		}

		sh.hdr.Commit(count, segFirst, last, sh.renderStats)
		sh.count, sh.first, sh.last = count, segFirst, last

		// Counted after the commit is published, so bytes that a reader can see are
		// bytes the policy counts, and never the reverse. Two atomic adds, no
		// allocation, and on a store with no policy this is one nil comparison.
		if db.pol != nil {
			db.pol.Append(n * db.recSize)
		}

		i += n
	}

	return nil
}

// validateEncodable proves every record in cs encodes, and returns the index of
// the first that does not. It exists for batches larger than one segment: their
// first chunk commits before the later chunks are encoded, so without this a
// bad record deep in a bulk load would be found after earlier chunks were
// already visible to readers.
//
// The scratch is one record's width, paid once per spanning batch by a caller
// that is writing at least a segment's worth of records in that call. It cannot
// be a stack array: the slice is passed to a method reached through a type
// parameter, which the compiler cannot prove does not escape, so a stack array
// would move to the heap at MaxRecordSize instead of at recSize.
func validateEncodable[T record.Record[T]](cs []T, recSize int) (int, error) {
	buf := make([]byte, recSize)

	for i, c := range cs {
		if err := c.EncodeInto(buf); err != nil {
			return i, err
		}
	}

	return 0, nil
}

// Seal closes key's current segment so the next Append starts a new one. Call it
// at a bar or day boundary when the open segment would otherwise straddle it.
func (db *DB[T]) Seal(key string) error {
	sh, err := db.shardFor(key)
	if err != nil {
		return err
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.file == nil {
		return nil
	}

	sealed := sh.seq
	if err := sh.closeLocked(); err != nil {
		return err
	}

	sh.seq++

	return db.indexSegmentLocked(sh, sealed)
}

// indexSegmentLocked records that seq is one of key's segments. The seq is a
// parameter rather than derived from the shard: Seal and Close both close a
// segment, and deriving it from a counter that only one of them bumps is how
// the index ends up pointing at the wrong file.
func (db *DB[T]) indexSegmentLocked(sh *shard, seq uint32) error {
	b, err := db.bucketFor(sh.key)
	if err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.entries[sh.hash]
	if e.Key == "" {
		e = format.Entry{Hash: sh.hash, Key: sh.key}
	}

	if !slices.Contains(e.Segs, seq) {
		e.Segs = append(e.Segs, seq)
		slices.Sort(e.Segs)
	}

	b.entries[sh.hash] = e

	return b.publishLocked()
}

// SyncIfDue flushes only when the store's Policy says it is due, and reports
// whether it did.
//
// With no Policy installed it never flushes and reports false, so a caller can
// write the same loop either way. With one, it is how a store adapts to the
// machine it landed on: call it wherever Sync would have been called and the
// flush happens as often as the budget requires rather than as often as a timer
// fired.
//
// It does not flush on an interval of its own and it runs nothing in the
// background, so a caller that never calls it never flushes. That is deliberate:
// see the note on Policy about why a flush is not hidden inside Append.
func (db *DB[T]) SyncIfDue() (bool, error) {
	if db.pol == nil || !db.pol.Due() {
		return false, nil
	}

	if err := db.Sync(); err != nil {
		return false, err
	}

	db.pol.Flushed()

	return true, nil
}

// Policy returns the installed Policy, or nil if the store was opened without
// one.
func (db *DB[T]) Policy() *Policy { return db.pol }

// Sync fsyncs every open segment and index. Appended data survives a process
// crash without this call, because mmap stores land in the page cache; Sync is
// what makes it survive a machine crash.
func (db *DB[T]) Sync() error {
	db.mu.RLock()
	keys := slices.Collect(maps.Keys(db.shard))

	shards := make([]*shard, 0, len(keys))
	for _, k := range keys {
		shards = append(shards, db.shard[k])
	}

	db.mu.RUnlock()

	var errs []error

	for _, sh := range shards {
		sh.mu.Lock()
		if sh.file != nil {
			if err := sh.syncLocked(); err != nil {
				errs = append(errs, err)
			}
		}
		sh.mu.Unlock()
	}

	db.bucketsMu.Lock()
	names := slices.Collect(maps.Keys(db.buckets))

	buckets := make([]*bucket, 0, len(names))
	for _, n := range names {
		buckets = append(buckets, db.buckets[n])
	}
	db.bucketsMu.Unlock()

	for _, b := range buckets {
		b.mu.Lock()
		if err := b.index.Sync(); err != nil {
			errs = append(errs, err)
		}
		b.mu.Unlock()
	}

	return errors.Join(errs...)
}

// Close seals every shard and releases its mappings.
func (db *DB[T]) Close() error {
	db.mu.Lock()
	keys := slices.Collect(maps.Keys(db.shard))

	shards := make([]*shard, 0, len(keys))
	for _, k := range keys {
		shards = append(shards, db.shard[k])
	}

	db.shard = make(map[string]*shard)
	db.mu.Unlock()

	var errs []error

	for _, sh := range shards {
		sh.mu.Lock()

		sealed := sh.seq
		if err := sh.closeLocked(); err != nil {
			errs = append(errs, err)
		}
		sh.mu.Unlock()

		if err := db.indexSegmentLocked(sh, sealed); err != nil {
			errs = append(errs, err)
		}
	}

	db.bucketsMu.Lock()
	names := slices.Collect(maps.Keys(db.buckets))

	buckets := make([]*bucket, 0, len(names))
	for _, n := range names {
		buckets = append(buckets, db.buckets[n])
	}

	db.buckets = make(map[string]*bucket)
	db.bucketsMu.Unlock()

	for _, b := range buckets {
		b.mu.Lock()
		errs = append(errs, b.index.Close())
		b.mu.Unlock()
	}

	return errors.Join(errs...)
}

// Keys lists the shards this handle has touched, sorted.
func (db *DB[T]) Keys() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return slices.Sorted(maps.Keys(db.shard))
}

// PrunePredicate returns true if the segment identified by key, seq and its
// time bounds should be deleted. It is called off the write path with
// read-only access to segment headers. sealed reports whether the segment was
// closed at the time it was inspected.
type PrunePredicate[T record.Record[T]] func(key string, seq uint32, minTs, maxTs uint64, sealed bool) bool

// Prune removes whole segments for which pred returns true. It never touches
// a shard that is still open for appending: an active segment is never removed,
// and an unreadable segment is kept rather than deleted on uncertainty.
//
// It rewrites affected bucket indexes and unlinks matching segment files. A
// Reader that already holds a mapping to a pruned segment keeps it mapping
// until Close or Refresh, so Prune is safe to run off the query path.
func (db *DB[T]) Prune(pred PrunePredicate[T]) error {
	activeOpen := make(map[string]uint32)

	db.mu.RLock()

	for k, sh := range db.shard {
		activeOpen[k] = sh.seq
	}

	db.mu.RUnlock()

	db.bucketsMu.Lock()

	buckets := make([]*bucket, 0, len(db.buckets))
	for _, b := range db.buckets {
		buckets = append(buckets, b)
	}

	db.bucketsMu.Unlock()

	var errs []error

	for _, b := range buckets {
		changed := false

		b.mu.Lock()
		for _, e := range b.entries {
			keep := make([]uint32, 0, len(e.Segs))
			for _, seq := range e.Segs {
				if a, ok := activeOpen[e.Key]; ok && seq == a {
					keep = append(keep, seq)

					continue
				}

				id := format.KeyID(e.Key)
				path := filepath.Join(db.dir, segName(id, seq))

				seg, err := format.OpenSegment[T](path, seq)
				if err != nil {
					keep = append(keep, seq)

					continue
				}

				_, minTs, maxTs, serr := seg.Snapshot()
				sealed := seg.IsSealed()
				_ = seg.Close()

				if serr != nil {
					keep = append(keep, seq)

					continue
				}

				if pred(e.Key, seq, minTs, maxTs, sealed) {
					_ = os.Remove(path)
					changed = true

					continue
				}

				keep = append(keep, seq)
			}

			if len(keep) == 0 {
				delete(b.entries, e.Hash)

				changed = true

				continue
			}

			e.Segs = keep
			b.entries[e.Hash] = e
		}

		if changed {
			if err := b.publishLocked(); err != nil {
				errs = append(errs, err)
			}
		}
		b.mu.Unlock()
	}

	return errors.Join(errs...)
}

// shard is one key's open segment.
type shard struct {
	mu      sync.Mutex
	dir     string
	key     string
	id      string
	hash    uint64
	size    int64
	recSize int
	mode    fs.FileMode

	seq   uint32
	path  string
	file  *os.File
	mm    []byte
	hdr   *format.SegmentHeader
	off   int
	count uint64
	first uint64
	last  uint64

	// Column index: the running min/max over every record appended to this
	// segment so far. Recomputed ranges are rendered into the trailer block on
	// every commit, so a reader can prune the whole segment from its bytes.
	trailer    int
	statsBytes int
	statsOff   int
	cols       []record.Column
	colMin     []uint64
	colMax     []uint64
	// Zone map: one min/max row per column per completed BlockRecords block.
	// blkMin/blkMax accumulate the block being written; the row is flushed to
	// the trailer the moment the block fills, and renderStats publishes the
	// partial row alongside the segment summary.
	curBlock int
	blkMin   []uint64
	blkMax   []uint64
}

func openShard[T record.Record[T]](dir, key string, size int64, mode fs.FileMode, recSize int) (*shard, error) {
	if key == "" {
		return nil, errors.New("zdb: empty key")
	}

	if len(key) > format.MaxKeyLen {
		return nil, fmt.Errorf("zdb: key is %d bytes, cap is %d", len(key), format.MaxKeyLen)
	}

	trailer := format.TrailerBytes[T](size)
	if format.RecordOffset(len(key))+trailer+recSize > int(size) {
		return nil, fmt.Errorf("zdb: key %d bytes exceeds segment size %d", len(key), size)
	}

	sc, err := record.SchemaOf[T]()
	cols := []record.Column(nil)

	if err == nil {
		cols = sc.StatsColumns()
	}

	sh := &shard{
		dir:        dir,
		key:        key,
		id:         format.KeyID(key),
		hash:       format.KeyHash(key),
		size:       size,
		recSize:    recSize,
		mode:       mode,
		trailer:    trailer,
		statsBytes: trailer - 8,
		statsOff:   int(size) - trailer,
		cols:       cols,
		colMin:     make([]uint64, len(cols)),
		colMax:     make([]uint64, len(cols)),
		blkMin:     make([]uint64, len(cols)),
		blkMax:     make([]uint64, len(cols)),
	}

	seq, err := latestSeq(dir, sh.id)
	if err != nil {
		return nil, err
	}

	if err := sh.openLocked(seq); err != nil {
		return nil, err
	}
	// A segment left full by a previous run rolls over on the next append.
	if sh.count >= uint64(sh.capacity()) {
		sh.seq++
		if err := sh.createLocked(); err != nil {
			return nil, err
		}
	}

	return sh, nil
}

// openLocked attaches to segment seq, resuming it when it already exists.
func (sh *shard) openLocked(seq uint32) error {
	path := filepath.Join(sh.dir, segName(sh.id, seq))

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, sh.mode)
	if err != nil {
		return fmt.Errorf("zdb: open segment %s: %w", path, err)
	}

	st, err := f.Stat()
	if err != nil {
		f.Close()

		return fmt.Errorf("zdb: stat segment %s: %w", path, err)
	}

	if st.Size() == 0 {
		// Fresh file: pre-allocate so its size never changes again, which is
		// what lets a reader keep one mapping for the segment's whole life.
		if err := f.Truncate(sh.size); err != nil {
			f.Close()

			return fmt.Errorf("zdb: allocate segment %s: %w", path, err)
		}

		mm, err := platform.Map(f, sh.size, platform.ReadWrite)
		if err != nil {
			f.Close()

			return err
		}

		off := format.InitSegmentHeader(mm, sh.key, sh.recSize)
		if err := f.Sync(); err != nil {
			platform.UnmapAndClose(mm, f)

			return fmt.Errorf("zdb: fsync segment %s: %w", path, err)
		}

		sh.seq, sh.path, sh.file, sh.mm = seq, path, f, mm
		sh.hdr, sh.off = format.SegmentHeaderAtUnchecked(mm), off
		sh.count, sh.first, sh.last = 0, 0, 0
		sh.resetStats()

		return nil
	}

	if st.Size() != sh.size {
		// Honour the on-disk capacity so a size change between runs cannot
		// silently truncate the segment or overrun it.
		f.Close()

		return fmt.Errorf("zdb: segment %s is %d bytes, want %d", path, st.Size(), sh.size)
	}

	mm, err := platform.Map(f, sh.size, platform.ReadWrite)
	if err != nil {
		f.Close()

		return err
	}

	hdr, off, err := format.SegmentHeaderAt(mm)
	if err != nil {
		platform.UnmapAndClose(mm, f)

		return fmt.Errorf("zdb: %s: %w", path, err)
	}
	// The key is read before the mapping is released, because Key reads out of mm
	// and reading it after Munmap is a fault rather than a stale value.
	if key := hdr.Key(mm); key != sh.key {
		platform.UnmapAndClose(mm, f)

		return fmt.Errorf("zdb: %s holds key %q, want %q", path, key, sh.key)
	}
	// A store is one record type for its whole life. A segment whose recorded
	// width is not this writer's is a different store being opened by mistake,
	// and appending into it would pack records at the wrong stride, so it is
	// refused here rather than corrupting the file one write later.
	if got := hdr.RecordSize(); got != sh.recSize {
		platform.UnmapAndClose(mm, f)

		return fmt.Errorf("zdb: %s holds %d-byte records, this store writes %d-byte records: %w",
			path, got, sh.recSize, format.ErrCorrupt)
	}

	count, first, last, err := snapshotOf(hdr, sh.capacity(), path)
	if err != nil {
		platform.UnmapAndClose(mm, f)

		return err
	}

	sh.seq, sh.path, sh.file, sh.mm = seq, path, f, mm
	sh.hdr, sh.off = hdr, off
	sh.count, sh.first, sh.last = count, first, last

	if count == 0 {
		sh.resetStats()
	} else {
		sh.loadStats()
	}

	return nil
}

// resetStats starts the per-column index empty and zeroes the trailer block, so
// a reader never reads garbage before the first commit. Called whenever a
// segment begins fresh so the accumulator never carries a previous segment's
// ranges.
func (sh *shard) resetStats() {
	for i := range sh.colMin {
		sh.colMin[i] = math.MaxUint64
		sh.colMax[i] = 0
		sh.blkMin[i] = math.MaxUint64
		sh.blkMax[i] = 0
	}

	sh.curBlock = 0

	for i := range sh.mm[sh.statsOff : sh.statsOff+sh.statsBytes] {
		sh.mm[sh.statsOff+i] = 0
	}
}

// loadStats reconstructs the accumulator from the segment's trailer block, so
// resuming an interrupted append keeps the index consistent rather than
// dropping earlier records' ranges.
func (sh *shard) loadStats() {
	for i := range sh.colMin {
		sh.colMin[i] = record.Le64(sh.mm[sh.statsOff+i*16:])
		sh.colMax[i] = record.Le64(sh.mm[sh.statsOff+i*16+8:])
		sh.blkMin[i] = math.MaxUint64
		sh.blkMax[i] = 0
	}

	// Rebuild the partial block's accumulator from the records it already
	// holds, so resuming an interrupted append extends the trailer row a
	// reader can see rather than resetting it.
	sh.curBlock = int(sh.count >> format.BlockRecordsShift)

	for i := uint64(sh.curBlock << format.BlockRecordsShift); i < sh.count; i++ {
		p := unsafe.Pointer(&sh.mm[sh.off+int(i)*sh.recSize])
		for k := range sh.cols {
			v := sh.cols[k].CompareValue(p)
			if v < sh.blkMin[k] {
				sh.blkMin[k] = v
			}

			if v > sh.blkMax[k] {
				sh.blkMax[k] = v
			}
		}
	}
}

// renderStats writes the accumulator into the trailer block. Called inside the
// commit window, so a reader that observes the new count also observes ranges
// that cover at least those records.
func (sh *shard) renderStats() {
	for i := range sh.colMin {
		record.PutLe64(sh.mm[sh.statsOff+i*16:], sh.colMin[i])
		record.PutLe64(sh.mm[sh.statsOff+i*16+8:], sh.colMax[i])
	}

	// The partial block's row rides the same publish as the summary: a reader
	// that sees the new count also sees its own block bounds up to date.
	row := sh.statsOff + len(sh.cols)*16 + sh.curBlock*len(sh.cols)*16
	for i := range sh.blkMin {
		record.PutLe64(sh.mm[row+i*16:], sh.blkMin[i])
		record.PutLe64(sh.mm[row+i*16+8:], sh.blkMax[i])
	}
}

// flushBlock writes the finished block's row and resets the block accumulator.
func (sh *shard) flushBlock() {
	row := sh.statsOff + len(sh.cols)*16 + sh.curBlock*len(sh.cols)*16
	for i := range sh.blkMin {
		record.PutLe64(sh.mm[row+i*16:], sh.blkMin[i])
		record.PutLe64(sh.mm[row+i*16+8:], sh.blkMax[i])
	}

	for i := range sh.blkMin {
		sh.blkMin[i] = math.MaxUint64
		sh.blkMax[i] = 0
	}
}

func snapshotOf(h *format.SegmentHeader, capacity int, path string) (uint64, uint64, uint64, error) {
	for range 32 {
		count, first, last, ok := h.Snapshot()
		if ok {
			if count > uint64(capacity) {
				return 0, 0, 0, fmt.Errorf("zdb: %s: %w", path, format.ErrCorrupt)
			}

			return count, first, last, nil
		}

		format.SeqlockRetries.Add(1)
	}

	format.SeqlockBusy.Add(1)

	return 0, 0, 0, fmt.Errorf("zdb: %s: %w", path, format.ErrBusy)
}

func (sh *shard) capacity() int {
	return int((sh.size - int64(sh.off) - int64(sh.trailer)) / int64(sh.recSize))
}

// createLocked starts segment sh.seq. The file must not already exist.
func (sh *shard) createLocked() error {
	path := filepath.Join(sh.dir, segName(sh.id, sh.seq))

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, sh.mode)
	if err != nil {
		return fmt.Errorf("zdb: create segment %s: %w", path, err)
	}

	if err := f.Truncate(sh.size); err != nil {
		f.Close()

		return fmt.Errorf("zdb: allocate segment %s: %w", path, err)
	}

	mm, err := platform.Map(f, sh.size, platform.ReadWrite)
	if err != nil {
		f.Close()

		return err
	}

	off := format.InitSegmentHeader(mm, sh.key, sh.recSize)
	if err := f.Sync(); err != nil {
		platform.UnmapAndClose(mm, f)

		return fmt.Errorf("zdb: fsync segment %s: %w", path, err)
	}

	sh.path, sh.file, sh.mm = path, f, mm
	sh.hdr, sh.off = format.SegmentHeaderAtUnchecked(mm), off
	sh.count, sh.first, sh.last = 0, 0, 0
	sh.resetStats()

	return nil
}

// rolloverLocked seals the current segment, opens the next one, and indexes it.
// Indexing the new segment is not optional: the index is how a reader finds the
// file, so an unindexed segment is invisible no matter what it contains.
func (db *DB[T]) rolloverLocked(sh *shard) error {
	if sh.file != nil {
		sealed := sh.seq
		if err := sh.closeLocked(); err != nil {
			return err
		}

		if err := db.indexSegmentLocked(sh, sealed); err != nil {
			return err
		}
	}

	sh.seq++
	if err := sh.createLocked(); err != nil {
		return err
	}

	return db.indexSegmentLocked(sh, sh.seq)
}

// ensureOpenLocked reopens a segment after an explicit Seal, and indexes it. A
// segment the index does not name is invisible to every reader, so creating one
// and leaving the index alone silently loses the candles written to it.
func (db *DB[T]) ensureOpenLocked(sh *shard) error {
	if sh.file != nil {
		return nil
	}

	if err := sh.createLocked(); err != nil {
		return err
	}

	return db.indexSegmentLocked(sh, sh.seq)
}

func (sh *shard) syncLocked() error {
	return platform.Sync(sh.mm, sh.file)
}

func (sh *shard) closeLocked() error {
	var errs []error

	if sh.hdr != nil {
		sh.hdr.MarkSealed()

		var count uint64

		ok := false

		for range 32 {
			var okNow bool

			count, _, _, okNow = sh.hdr.Snapshot()
			ok = okNow

			if ok {
				break
			}
		}

		if ok {
			end := sh.off + int(count)*sh.recSize
			if end >= sh.off && end+8 <= len(sh.mm) {
				sh.hdr.MarkChecksummed()
				binary.LittleEndian.PutUint64(sh.mm[len(sh.mm)-8:], format.SegmentChecksum(sh.mm[:end]))
			}
		}
	}

	if sh.file != nil {
		if err := sh.syncLocked(); err != nil {
			errs = append(errs, err)
		}
	}

	if sh.mm != nil {
		if err := platform.Unmap(sh.mm); err != nil {
			errs = append(errs, err)
		}

		sh.mm = nil
	}

	if sh.file != nil {
		if err := sh.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("zdb: close %s: %w", sh.path, err))
		}

		sh.file = nil
	}

	return errors.Join(errs...)
}

func segName(id string, seq uint32) string {
	return id + "-" + strconv.FormatUint(uint64(seq), 10) + format.SegmentSuffix
}

// latestSeq returns the highest segment sequence for id, or 0 when the shard
// does not exist yet.
func latestSeq(dir, id string) (uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("zdb: read writer dir %s: %w", dir, err)
	}

	newest := uint32(0)
	prefix := id + "-"

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, format.SegmentSuffix) {
			continue
		}

		seq, err := strconv.ParseUint(name[len(prefix):len(name)-len(format.SegmentSuffix)], 10, 32)
		if err != nil {
			continue
		}

		if uint32(seq) > newest {
			newest = uint32(seq)
		}
	}

	return newest, nil
}
