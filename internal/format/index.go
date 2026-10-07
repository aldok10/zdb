package format

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/aldok10/zdb/internal/platform"
	"github.com/aldok10/zdb/record"
)

// Index file geometry. Both regions are pre-allocated, so the file's size is
// fixed at creation and a reader maps it once for the life of the bucket.
const (
	// IndexHeaderSize is the fixed portion of a bucket index header.
	IndexHeaderSize = 64

	// DefaultIndexRegionBytes is each region's size. It holds the offsets
	// array plus every entry's key and segment list.
	DefaultIndexRegionBytes = 1 << 20

	// indexMaxSeq caps a key's segment chain, since the count is a uint16.
	indexMaxSeq = 1 << 16
)

// Index entry layout. Encode and decode both read these offsets, so the two
// sides cannot drift:
//
//	0  8  hash      uint64   sort key, the leading bits of sha256(key)
//	8  2  keyLen    uint16
//	10 2  segCount  uint16
//	12 .. key bytes
//	..  .. 4 bytes per segment sequence
const (
	entryHashOff     = 0
	entryKeyLenOff   = 8
	entrySegCountOff = 10
	entryBodyOff     = 12
)

// entrySize is how many bytes one entry needs on disk.
func entrySize(keyLen, segCount int) int {
	return entryBodyOff + keyLen + 4*segCount
}

// IndexHeader is the 64-byte bucket index header. It overlays the start of the
// mapping and follows the same seqlock contract as a segment header: a reader
// that sees an even, unchanged commit sees a whole, consistent table.
type IndexHeader struct {
	magic   [8]byte
	version uint32
	flags   uint32
	count   atomic.Uint64 // entries in the active region
	active  atomic.Uint64 // 0 or 1: which region is live
	region  atomic.Uint64 // region size in bytes
	commit  atomic.Uint64
	key     atomic.Uint64 // keys indexed in this bucket, for diagnostics
}

// Index is one bucket's key-to-segments mapping, memory-mapped.
//
// Readers are lock-free: Find reads the header, indexes the offsets array, and
// returns the entry. Writers rebuild the idle region and flip active, which is
// the only write the format ever does.
type Index struct {
	Path   string
	Bucket string

	file *os.File
	mm   []byte
	hdr  *IndexHeader
}

// IndexPath returns the index file path of a key's bucket.
func IndexPath(dir, key string) string {
	return filepath.Join(dir, Bucket(key)+IndexSuffix)
}

// OpenIndex maps a bucket index read-only.
func OpenIndex(path string) (*Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("zdb: open index %s: %w", path, err)
	}

	st, err := f.Stat()
	if err != nil {
		f.Close()

		return nil, fmt.Errorf("zdb: stat index %s: %w", path, err)
	}

	mm, err := platform.Map(f, st.Size(), platform.ReadOnly)
	if err != nil {
		f.Close()

		return nil, err
	}

	if _, _, err := IndexHeaderAt(mm); err != nil {
		platform.UnmapAndClose(mm, f)

		return nil, fmt.Errorf("zdb: %s: %w", path, err)
	}

	return &Index{
		Path:   path,
		Bucket: strings.TrimSuffix(filepath.Base(path), IndexSuffix),
		file:   f,
		mm:     mm,
		hdr:    IndexHeaderAtUnchecked(mm),
	}, nil
}

// CreateIndex allocates a bucket index with both regions pre-allocated, or
// reopens it when it already exists. regionBytes of zero takes the default.
func CreateIndex(path, bucket string, regionBytes int64) (*Index, error) {
	if regionBytes <= 0 {
		regionBytes = DefaultIndexRegionBytes
	}

	size := int64(IndexHeaderSize) + 2*regionBytes

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("zdb: create index %s: %w", path, err)
	}

	st, err := f.Stat()
	if err != nil {
		f.Close()

		return nil, fmt.Errorf("zdb: stat index %s: %w", path, err)
	}

	if st.Size() == 0 {
		if err := f.Truncate(size); err != nil {
			f.Close()

			return nil, fmt.Errorf("zdb: allocate index %s: %w", path, err)
		}

		mm, err := platform.Map(f, size, platform.ReadWrite)
		if err != nil {
			f.Close()

			return nil, err
		}

		InitIndexHeader(mm, uint64(regionBytes))

		if err := f.Sync(); err != nil {
			platform.UnmapAndClose(mm, f)

			return nil, fmt.Errorf("zdb: fsync index %s: %w", path, err)
		}

		ix := &Index{
			Path:   path,
			Bucket: bucket,
			file:   f,
			mm:     mm,
			hdr:    IndexHeaderAtUnchecked(mm),
		}

		return ix, nil
	}

	if st.Size() != size {
		f.Close()

		return nil, fmt.Errorf("zdb: index %s is %d bytes, want %d", path, st.Size(), size)
	}

	mm, err := platform.Map(f, size, platform.ReadWrite)
	if err != nil {
		f.Close()

		return nil, err
	}

	if _, _, err := IndexHeaderAt(mm); err != nil {
		platform.UnmapAndClose(mm, f)

		return nil, fmt.Errorf("zdb: %s: %w", path, err)
	}

	return &Index{
		Path:   path,
		Bucket: bucket,
		file:   f,
		mm:     mm,
		hdr:    IndexHeaderAtUnchecked(mm),
	}, nil
}

// IndexHeaderAtUnchecked overlays the header on a mapping the caller just wrote.
func IndexHeaderAtUnchecked(mm []byte) *IndexHeader {
	return (*IndexHeader)(unsafe.Pointer(&mm[0]))
}

// IndexHeaderAt validates a mapping and returns its header and region size.
func IndexHeaderAt(mm []byte) (*IndexHeader, uint64, error) {
	if len(mm) < IndexHeaderSize {
		return nil, 0, ErrCorrupt
	}

	h := IndexHeaderAtUnchecked(mm)
	if string(h.magic[:]) != string(IndexMagic[:]) || h.version != Version {
		return nil, 0, ErrCorrupt
	}

	return h, h.region.Load(), nil
}

// InitIndexHeader stamps a fresh index header.
func InitIndexHeader(mm []byte, regionBytes uint64) {
	h := IndexHeaderAtUnchecked(mm)
	copy(h.magic[:], IndexMagic[:])
	h.version = Version
	h.flags = 0
	h.count.Store(0)
	h.active.Store(0)
	h.region.Store(regionBytes)
	h.key.Store(0)
	h.commit.Store(1)
	h.commit.Store(0)
}

// Sync flushes the index's dirty pages and its file to disk, so a rebuild
// survives a machine crash. Put holds a rebuilding region, not a caller, so it
// makes no durability promise of its own.
func (ix *Index) Sync() error {
	return platform.Sync(ix.mm, ix.file)
}

// Close unmaps the index. Callers must not use it afterwards.
func (ix *Index) Close() error {
	if ix.mm == nil {
		return nil
	}

	err := platform.Unmap(ix.mm)

	ix.mm = nil
	if cerr := ix.file.Close(); err == nil {
		err = cerr
	}

	return err
}

// Entry is one key's index record: which segments hold its candles, in write
// order. It carries no counts: a count in the index goes stale on every append
// and double-counts on reopen, while each segment header already holds its own.
type Entry struct {
	Hash uint64
	Key  string
	Segs []uint32
}

// regionBase is the byte offset of region r.
func (ix *Index) regionBase(r uint64) int {
	return IndexHeaderSize + int(r)*int(ix.hdr.region.Load())
}

func (ix *Index) region() (active, count, regionBytes uint64) {
	var ok bool

	for range 32 {
		before := ix.hdr.commit.Load()
		if before%2 == 1 {
			SeqlockRetries.Add(1)
			runtime.Gosched()

			continue
		}

		active = ix.hdr.active.Load()
		count = ix.hdr.count.Load()

		regionBytes = ix.hdr.region.Load()
		if ix.hdr.commit.Load() == before {
			ok = true

			break
		}

		SeqlockRetries.Add(1)
		runtime.Gosched()
	}

	if !ok {
		// Fall back to a consistent-enough read: the seqlock protects against
		// seeing a half-written rebuild, and a stale active region still holds
		// the previous consistent table.
		SeqlockBusy.Add(1)

		active = ix.hdr.active.Load()
		count = ix.hdr.count.Load()
		regionBytes = ix.hdr.region.Load()
	}

	return active, count, regionBytes
}

// entryBytes returns the raw bytes of entry i of the live region. The result
// points into the mapping, so nothing is copied and nothing is allocated; the
// caller must not retain it past Close.
func (ix *Index) entryBytes(base, regionLen, i int) ([]byte, error) {
	off := int(readU32(ix.mm[base+i*4:]))
	if off < 0 || off+entryBodyOff > regionLen {
		return nil, ErrCorrupt
	}

	buf := ix.mm[base+off : base+regionLen]
	keyLen := int(uint16(buf[entryKeyLenOff]) | uint16(buf[entryKeyLenOff+1])<<8)

	segCount := int(uint16(buf[entrySegCountOff]) | uint16(buf[entrySegCountOff+1])<<8)
	if entrySize(keyLen, segCount) > len(buf) {
		return nil, ErrCorrupt
	}

	return buf, nil
}

// keyEquals compares key against an entry's inline key in place. It never
// materializes the stored key as a Go string, so a lookup allocates nothing.
func keyEquals(entry []byte, key string) bool {
	keyLen := int(uint16(entry[entryKeyLenOff]) | uint16(entry[entryKeyLenOff+1])<<8)

	stored := entry[entryBodyOff : entryBodyOff+keyLen]
	if len(stored) != len(key) {
		return false
	}

	return string(stored) == key
}

// seqBytes returns an entry's segment sequences in place.
func seqBytes(entry []byte) []byte {
	keyLen := int(uint16(entry[entryKeyLenOff]) | uint16(entry[entryKeyLenOff+1])<<8)
	segCount := int(uint16(entry[entrySegCountOff]) | uint16(entry[entrySegCountOff+1])<<8)

	return entry[entryBodyOff+keyLen : entryBodyOff+keyLen+4*segCount]
}

func entryKeyLen(buf []byte) int {
	return int(uint16(buf[entryKeyLenOff]) | uint16(buf[entryKeyLenOff+1])<<8)
}

// SeqAt reads segment sequence i from a slice returned by Locate.
func SeqAt(seqs []byte, i int) uint32 { return readU32(seqs[i*4:]) }

// Locate resolves key and returns its segment sequences as a slice pointing
// straight into the index mapping. Neither the lookup nor the result allocates:
// the key is compared in place and the sequence list is never copied out.
//
// The returned slice is only valid while the index stays mapped. Callers must
// read what they need from it and not retain it past Reader.Close.
func (ix *Index) Locate(key string) (seqs []byte, found bool, err error) {
	buf, found, err := ix.search(key)
	if err != nil || !found {
		return nil, false, err
	}

	return seqBytes(buf), true, nil
}

// search binary-searches the live region for key and returns its raw entry
// bytes. The comparison is on the stored key bytes, so no string is built.
func (ix *Index) search(key string) (entry []byte, found bool, err error) {
	hash := KeyHash(key)
	active, count, regionBytes := ix.region()
	base := ix.regionBase(active)
	regionLen := int(regionBytes)
	n := int(count)

	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)

		eh, err := ix.entryHash(base, regionLen, mid)
		if err != nil {
			return nil, false, fmt.Errorf("zdb: %s: %w", ix.Path, err)
		}

		if eh < hash {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// Walk the equal-hash run and compare keys. A 64-bit hash prefix makes a run
	// one entry almost always, but a collision must never return a wrong key.
	for i := lo; i < n; i++ {
		buf, err := ix.entryBytes(base, regionLen, i)
		if err != nil {
			return nil, false, fmt.Errorf("zdb: %s: %w", ix.Path, err)
		}

		if record.Le64(buf[entryHashOff:entryHashOff+8]) != hash {
			break
		}

		if keyEquals(buf, key) {
			return buf, true, nil
		}
	}

	return nil, false, nil
}

// Find returns the index entry for key, if the bucket holds it. The lookup is a
// binary search over the offsets array: O(log entries) mmap reads and no lock.
// It allocates a key string and a segment slice, so it is for the writer and for
// enumeration. A query should use Locate, which allocates nothing.
func (ix *Index) Find(key string) (Entry, bool, error) {
	buf, found, err := ix.search(key)
	if err != nil || !found {
		return Entry{}, found, err
	}

	return ix.decodeEntry(buf), true, nil
}

// decodeEntry materializes an entry from its raw bytes. search has already
// bounds-checked the entry and matched the key against it, so decoding cannot
// fail and the signature does not pretend otherwise.
func (ix *Index) decodeEntry(buf []byte) Entry {
	keyLen := entryKeyLen(buf)
	seqs := seqBytes(buf)

	e := Entry{
		Hash: record.Le64(buf[entryHashOff : entryHashOff+8]),
		Key:  string(buf[entryBodyOff : entryBodyOff+keyLen]),
		Segs: make([]uint32, len(seqs)/4),
	}
	for j := range e.Segs {
		e.Segs[j] = readU32(seqs[j*4:])
	}

	return e
}

func (ix *Index) entryHash(base, regionLen, i int) (uint64, error) {
	off := int(readU32(ix.mm[base+i*4:]))
	if off < 0 || off+8 > regionLen {
		return 0, ErrCorrupt
	}

	return record.Le64(ix.mm[base+off : base+off+8]), nil
}

// Len returns the number of keys indexed in this bucket.
func (ix *Index) Len() int {
	_, count, _ := ix.region()

	return int(count)
}

// Entries returns every entry in the bucket, sorted by key. It is for
// enumeration (Keys, List), not for lookup.
func (ix *Index) Entries() ([]Entry, error) {
	var out []Entry

	for e, err := range ix.AllEntries() {
		if err != nil {
			return nil, err
		}

		out = append(out, e)
	}

	return out, nil
}

// keyAt returns entry i's inline key bytes, pointing into the mapping.
func (ix *Index) keyAt(base, regionLen, i int) ([]byte, error) {
	buf, err := ix.entryBytes(base, regionLen, i)
	if err != nil {
		return nil, err
	}

	return buf[entryBodyOff : entryBodyOff+entryKeyLen(buf)], nil
}

// order returns the live region's entry indices sorted by key. It is []int32
// rather than []Entry on purpose: sorting 500k keys then costs 2 MB of ordering
// instead of 500k key strings plus 500k segment slices. The keys themselves stay
// in the mapping and are only read to compare.
//
// ponytail: the ordering still has to exist because the region is sorted by hash,
// not by key. A key-sorted on-disk region would drop this entirely and make
// enumeration a pure walk, at the cost of a different index layout.
func (ix *Index) order() ([]int32, error) {
	active, count, regionBytes := ix.region()
	base := ix.regionBase(active)
	regionLen := int(regionBytes)

	order := make([]int32, count)
	for i := range int(count) {
		order[i] = int32(i)
	}

	var bad error

	slices.SortFunc(order, func(a, b int32) int {
		ka, err := ix.keyAt(base, regionLen, int(a))
		if err != nil {
			bad = err

			return 0
		}

		kb, err := ix.keyAt(base, regionLen, int(b))
		if err != nil {
			bad = err

			return 0
		}

		return bytes.Compare(ka, kb)
	})

	if bad != nil {
		return nil, fmt.Errorf("zdb: %s: %w", ix.Path, bad)
	}

	return order, nil
}

// AllEntries iterates every key in the index, sorted by key, one entry at a
// time. Entries builds the whole slice up front, so enumerating a large store
// needs a key string and a segment slice alive per key at once; this holds only
// the current one. Stopping the loop early stops the work.
func (ix *Index) AllEntries() iter.Seq2[Entry, error] {
	order, err := ix.order()
	if err != nil {
		return func(yield func(Entry, error) bool) { yield(Entry{}, err) }
	}

	return func(yield func(Entry, error) bool) {
		active, _, regionBytes := ix.region()
		base := ix.regionBase(active)

		regionLen := int(regionBytes)
		for _, i := range order {
			buf, err := ix.entryBytes(base, regionLen, int(i))
			if err != nil {
				yield(Entry{}, fmt.Errorf("zdb: %s: %w", ix.Path, err))

				return
			}

			if !yield(ix.decodeEntry(buf), nil) {
				return
			}
		}
	}
}

// Put writes entries into the idle region and flips it live.
//
// The whole table is rewritten on every change. That is the right trade here:
// the table holds one entry per symbol, a rebuild is a few hundred KB of
// sequential mmap writes, and in exchange the format has no incremental update
// path to get wrong and a reader never takes a lock.
func (ix *Index) Put(entries []Entry) error {
	slices.SortFunc(entries, func(a, b Entry) int { return cmp(a.Hash, b.Hash) })

	active, _, regionBytes := ix.region()
	idle := active ^ 1
	base := ix.regionBase(idle)
	region := ix.mm[base : base+int(regionBytes)]

	// The offsets array is a prefix; the blob follows it. Sizing it from the
	// region and the worst-case entry keeps entry i at a fixed stride.
	offsetsBytes := 4 * (len(entries) + 1)
	if offsetsBytes > len(region) {
		return fmt.Errorf("zdb: %s holds more than %d keys: %w",
			ix.Path, (len(region)/4)-1, ErrIndexFull)
	}

	blob := offsetsBytes

	for i, e := range entries {
		if len(e.Key) > MaxKeyLen {
			return fmt.Errorf("zdb: key %d bytes exceeds %d: %w", len(e.Key), MaxKeyLen, ErrCorrupt)
		}

		if len(e.Segs) > indexMaxSeq {
			return fmt.Errorf("zdb: key %q has %d segments, cap is %d: %w",
				e.Key, len(e.Segs), indexMaxSeq, ErrIndexFull)
		}

		need := entrySize(len(e.Key), len(e.Segs))
		if blob+need > len(region) {
			return fmt.Errorf("zdb: %s: key %q does not fit: %w", ix.Path, e.Key, ErrIndexFull)
		}

		putLE32(region[i*4:], uint32(blob))

		b := region[blob : blob+need]
		putLE64(b[entryHashOff:entryHashOff+8], e.Hash)
		b[entryKeyLenOff] = byte(len(e.Key))
		b[entryKeyLenOff+1] = byte(len(e.Key) >> 8)
		b[entrySegCountOff] = byte(len(e.Segs))
		b[entrySegCountOff+1] = byte(len(e.Segs) >> 8)
		copy(b[entryBodyOff:], e.Key)

		p := entryBodyOff + len(e.Key)
		for _, seq := range e.Segs {
			putLE32(b[p:], seq)
			p += 4
		}

		blob += need
	}
	// Terminate the offsets array so a reader can bound-check.
	putLE32(region[offsetsBytes-4:], uint32(blob))

	// Seqlock: the idle region is already fully written, so the flip is three
	// stores. A reader that catches commit odd retries and then sees the whole
	// new table or the whole old one.
	ix.hdr.commit.Add(1) // odd: rebuilding
	ix.hdr.active.Store(idle)
	ix.hdr.count.Store(uint64(len(entries)))
	ix.hdr.key.Store(uint64(len(entries)))
	ix.hdr.commit.Add(1) // even: consistent

	return nil
}

// readU32 decodes a little-endian uint32 from the head of b.
func readU32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// cmp orders two uint64s for slices.SortFunc.
func cmp(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func putLE32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func putLE64(b []byte, v uint64) {
	for i := range 8 {
		b[i] = byte(v >> (8 * i))
	}
}

// BucketSet is the collection of a directory's bucket indexes, mapping
// key-to-segments for every key in the store. It is the "main database" a
// reader consults to discover which file holds which symbol.
type BucketSet struct {
	Dir string

	mu      sync.RWMutex
	buckets map[string]*Index
}

// OpenBucketSet maps the bucket index of every key in dir. It maps them all at
// open, so a lookup touches nothing but memory afterwards.
func OpenBucketSet(dir string) (*BucketSet, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("zdb: read dir %s: %w", dir, err)
	}

	bs := &BucketSet{Dir: dir, buckets: make(map[string]*Index)}

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, IndexSuffix) {
			continue
		}

		bucket := strings.TrimSuffix(name, IndexSuffix)

		ix, err := OpenIndex(filepath.Join(dir, name))
		if err != nil {
			bs.Close()

			return nil, err
		}

		bs.buckets[bucket] = ix
	}

	return bs, nil
}

// Bucket returns a bucket's index, opening it on first use. Callers that go
// through Find do not need this: Find opens the bucket itself.
func (bs *BucketSet) Bucket(bucket string) (*Index, error) {
	bs.mu.RLock()
	ix := bs.buckets[bucket]
	bs.mu.RUnlock()

	if ix != nil {
		return ix, nil
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	if ix = bs.buckets[bucket]; ix != nil {
		return ix, nil
	}

	path := filepath.Join(bs.Dir, bucket+IndexSuffix)

	ix, err := OpenIndex(path)
	if err != nil {
		return nil, err
	}

	bs.buckets[bucket] = ix

	return ix, nil
}

// Locate resolves key to its raw segment sequences, opening the bucket on
// demand. It allocates nothing on a cache hit and copies nothing out of the
// mapping, so it is the resolution path a query should use.
func (bs *BucketSet) Locate(key string) ([]byte, bool, error) {
	ix, err := bs.Bucket(Bucket(key))
	if err != nil {
		return nil, false, err
	}

	return ix.Locate(key)
}

// Find resolves key to its index entry, opening the bucket on demand.
func (bs *BucketSet) Find(key string) (Entry, bool, error) {
	ix, err := bs.Bucket(Bucket(key))
	if err != nil {
		return Entry{}, false, err
	}

	return ix.Find(key)
}

// Buckets returns every loaded bucket id, sorted.
func (bs *BucketSet) Buckets() []string {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	out := make([]string, 0, len(bs.buckets))
	for b := range bs.buckets {
		out = append(out, b)
	}

	slices.Sort(out)

	return out
}

// Close unmaps every bucket index.
func (bs *BucketSet) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	var errs []error
	for _, ix := range bs.buckets {
		errs = append(errs, ix.Close())
	}

	clear(bs.buckets)

	return errors.Join(errs...)
}
