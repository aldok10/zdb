// Package format defines the ZDB on-disk format: a memory-mapped, append-only
// candle store sharded by key (symbol path or symbol name).
package format

import (
	"fmt"
	"hash/crc64"
	"sync/atomic"
	"unsafe"
)

// SegmentHeader is the 56-byte segment header. It overlays the start of the
// mapping and is shared between a writer's read-write mapping and a reader's
// read-only mapping, so every mutable field is atomic and every update happens
// inside a seqlock window.
type SegmentHeader struct {
	magic   [8]byte
	version uint32
	flags   atomic.Uint32
	count   atomic.Uint64
	firstTs atomic.Uint64
	lastTs  atomic.Uint64
	commit  atomic.Uint64
	keyLen  uint32
	recSize uint32
}

// InitSegmentHeader stamps a fresh header for key over mm and returns the
// record offset. recSize is the encoded width of one record and is written into
// the header, so a reader learns the layout from the file rather than from the
// record type it was handed. The caller must have mapped at least
// RecordOffset(len(key)) bytes.
func InitSegmentHeader(mm []byte, key string, recSize int) int {
	h := SegmentHeaderAtUnchecked(mm)
	copy(h.magic[:], Magic[:])
	h.version = Version
	h.flags.Store(0)
	h.count.Store(0)
	h.firstTs.Store(0)
	h.lastTs.Store(0)
	h.commit.Store(0)
	h.keyLen = uint32(len(key))
	h.recSize = uint32(recSize)

	copy(mm[HeaderSize:HeaderSize+len(key)], key)
	h.commit.Store(1) // odd: uncommitted, so no reader trusts an empty segment
	h.commit.Store(0)

	return RecordOffset(len(key))
}

// SegmentHeaderAtUnchecked overlays the header on a mapping the caller just
// wrote, so there is nothing to validate.
func SegmentHeaderAtUnchecked(mm []byte) *SegmentHeader {
	return (*SegmentHeader)(unsafe.Pointer(&mm[0]))
}

// SegmentHeaderAt validates a mapping and returns its header and record offset.
func SegmentHeaderAt(mm []byte) (*SegmentHeader, int, error) {
	if len(mm) < HeaderSize {
		return nil, 0, ErrCorrupt
	}

	h := SegmentHeaderAtUnchecked(mm)
	if string(h.magic[:]) != string(Magic[:]) || h.version != Version {
		return nil, 0, ErrCorrupt
	}

	if HeaderSize+int(h.keyLen) > len(mm) {
		return nil, 0, ErrCorrupt
	}

	// The width came from a file, which is a trust boundary like any other. A
	// zero or absurd width would make every record offset meaningless, and a
	// reader handed the wrong record type would decode one layout as another
	// without anything failing loudly, so both are refused here rather than
	// discovered as garbage prices later.
	if h.recSize == 0 || h.recSize > MaxRecordSize {
		return nil, 0, fmt.Errorf("zdb: record size %d is out of range: %w",
			h.recSize, ErrCorrupt)
	}

	return h, RecordOffset(int(h.keyLen)), nil
}

// RecordSize is the encoded width of one record in this segment, read from the
// file rather than from the record type the caller holds.
func (h *SegmentHeader) RecordSize() int { return int(h.recSize) }

// IsSealed reports whether the segment has been sealed (closed/immutable).
func (h *SegmentHeader) IsSealed() bool { return h.flags.Load()&FlagSealed != 0 }

// MarkSealed sets the sealed flag. It is safe for a writer to call while
// holding the segment shard lock; readers load it atomically.
func (h *SegmentHeader) MarkSealed() { h.flags.Store(h.flags.Load() | FlagSealed) }

// HasChecksum reports whether the segment file carries its tail checksum.
func (h *SegmentHeader) HasChecksum() bool { return h.flags.Load()&FlagChecksummed != 0 }

// MarkChecksummed marks the segment as carrying a tail checksum. A writer
// sets this once it has written the checksum, and readers use the flag to
// know there is something to verify.
func (h *SegmentHeader) MarkChecksummed() { h.flags.Store(h.flags.Load() | FlagChecksummed) }

// SegmentChecksum hashes the published part of a segment file: header, key
// and records [0,count). It allocates once per seal/open, never per record.
func SegmentChecksum(data []byte) uint64 {
	h := crc64.New(crc64.MakeTable(crc64.ECMA))
	h.Write(data)

	return h.Sum64()
}

// Key returns the segment's shard key.
func (h *SegmentHeader) Key(mm []byte) string {
	return string(mm[HeaderSize : HeaderSize+int(h.keyLen)])
}

// RecordOffset is the byte offset of record 0 for a key of n bytes.
func RecordOffset(n int) int { return (n + HeaderSize + 7) &^ 7 }

// Commit runs fn inside the seqlock write window, then publishes count and the
// timestamp bounds. A reader that sees an even, unchanged commit therefore sees
// records [0, count) fully written. If fn panics, count keeps its previous
// value, so a reader under-reads instead of seeing a torn record.
//
// Commit publishes count, first and last, running fn inside the write window if
// one is given. fn is nil when the records were already written: a writer that
// encodes a batch up front can then surface an encoding error before the window
// opens instead of after it, and pay nothing for a closure it does not need.
func (h *SegmentHeader) Commit(count uint64, first, last uint64, fn func()) {
	h.commit.Add(1) // odd: write in progress
	defer h.commit.Add(1)

	if fn != nil {
		fn()
	}

	h.count.Store(count)
	h.firstTs.Store(first)
	h.lastTs.Store(last)
}

// Snapshot reads the header fields as one consistent set. ok is false when a
// writer was mid-commit and the caller must retry.
func (h *SegmentHeader) Snapshot() (count, first, last uint64, ok bool) {
	before := h.commit.Load()
	if before%2 == 1 {
		return 0, 0, 0, false
	}

	count = h.count.Load()
	first = h.firstTs.Load()
	last = h.lastTs.Load()

	return count, first, last, h.commit.Load() == before
}
