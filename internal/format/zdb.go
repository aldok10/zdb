// Package format defines the ZDB on-disk format: a memory-mapped, append-only
// candle store sharded by key (symbol path or symbol name).
//
// # Layout
//
// A directory holds two kinds of file. Buckets shard the key space; segments
// hold the candles.
//
//	<dir>/<bb>.idx            bucket index: the mapping from key to segments
//	<dir>/<keyid>-<seq>.zseg  one file per key, one per rollover
//
// Bucket bb is the first byte of sha256(key), so a key resolves to its bucket
// by arithmetic: no directory scan, no fanout table.
//
// # Index file
//
//	+----------------+ 0
//	| header (64 B)  |  magic, version, seqlock, count, active region
//	+----------------+ 64
//	| region A       |  offsets array + entry blob
//	| region B       |  offsets array + entry blob
//	+----------------+ EOF
//
// A region is an offsets array of (capacity+1) uint32 followed by the entries
// themselves, so entry i is reached in one indexed read with no walking. The
// two regions alternate: a rebuild writes the idle one and then flips active
// under a seqlock, so a reader never sees a half-written table. Both regions
// are pre-allocated, so the file never grows and a reader maps it once.
//
// # Segment file
//
//	+----------------+ 0
//	| header (56 B)  |  magic, version, seqlock, count, key length
//	+----------------+ 56
//	| key bytes      |  symbol path or name
//	+----------------+ align8(56+len(key))
//	| record 0 (48 B)|  ts, open, high, low, close, volume, all uint64
//	| ...            |  timestamps ascending
//	+----------------+ EOF (pre-allocated to segment capacity)
//
// Every record is fixed width, so a range query binary-searches the mapped
// bytes directly: no parsing, no allocation beyond the result. Segments are
// pre-allocated too, so appending raises only the header's count and a reader
// keeps one mapping for the life of the segment.
//
// # Records
//
// This package is the format and the read path. The record types it stores live
// in the record package, which implements the Record interface this package
// constrains Segment to. A store holds one record type for its whole life.
package format

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"

	"github.com/aldok10/zdb/internal/platform"
	"github.com/aldok10/zdb/record"
)

const (
	// HeaderSize is the fixed portion of a segment header in bytes.
	HeaderSize = 56

	// Version is the on-disk format version.
	Version = 1

	// SegmentSuffix is the file extension of a segment file.
	SegmentSuffix = ".zseg"

	// IndexSuffix is the file extension of a bucket index file.
	IndexSuffix = ".idx"

	// FlagSealed marks a segment as immutable (closed/sealed). It is written
	// into the header when the segment is closed or rolled over and remains
	// read-only thereafter.
	FlagSealed uint32 = 1 << 0

	// FlagChecksummed means the segment tail carries the checksum of its
	// header, key and published records. It is required on a sealed segment.
	FlagChecksummed uint32 = 1 << 1
)

const (
	// keyIDBytes is how much of sha256(key) names a key's files. Twelve bytes
	// is 96 bits: collisions are not a concern, and the same prefix drives both
	// the bucket and the filename.
	keyIDBytes = 12

	// MaxKeyLen caps a shard key so it fits the index's inline key field.
	MaxKeyLen = 255

	// MaxRecordSize is the widest record a segment header may claim. It is a
	// sanity bound on a value read from disk, not a design limit: the widest
	// record this package ships is well under it. A segment file smaller than
	// one record is rejected on its own terms, so the bound only has to be large
	// enough to let that rejection happen first.
	MaxRecordSize = 1 << 20
)

// Magic identifies a ZDB segment file.
var Magic = [8]byte{'Z', 'D', 'B', 'S', 'G', 0, 1, 0}

// IndexMagic identifies a ZDB bucket index file.
var IndexMagic = [8]byte{'Z', 'D', 'B', 'I', 'D', 'X', 0, 1}

var (
	// ErrCorrupt means a file failed magic, version or bounds validation.
	ErrCorrupt = errors.New("zdb: corrupt file")
	// ErrUnsupported means this operating system has no primitive for what the
	// caller asked for, which in practice means no file mapping. It is the same
	// value internal/platform reports, so a caller can test for it here without
	// importing an internal package, and errors.Is works across both spellings.
	ErrUnsupported = platform.ErrUnsupported
	// ErrInvalid means a record could not be stored as given: a negative
	// timestamp, or a price that is non-finite or out of range. It is an alias
	// for record.ErrInvalid so a caller matching on it does not have to know
	// which package the record type came from.
	ErrInvalid = record.ErrInvalid

	// ErrOutOfOrder means a write would break the ascending timestamp invariant.
	ErrOutOfOrder = errors.New("zdb: timestamp not strictly increasing")
	// ErrIndexFull means a key would not fit in its bucket's spare region.
	ErrIndexFull = errors.New("zdb: bucket index region full")
	// ErrBusy means a writer held a seqlock across every retry.
	ErrBusy = errors.New("zdb: busy")
)

// KeyID is the hex name shared by a key's index lookup and its segment files.
func KeyID(key string) string {
	sum := sha256.Sum256([]byte(key))

	return hex.EncodeToString(sum[:keyIDBytes])
}

// KeyHash is the sort key of an index entry. It is the leading 64 bits of the
// same digest KeyID uses, so a lookup never needs the key bytes unless two
// hashes collide.
func KeyHash(key string) uint64 {
	sum := sha256.Sum256([]byte(key))

	return binary.LittleEndian.Uint64(sum[:8])
}

// Bucket is the index file a key belongs to: the first byte of its key ID.
// Sharding falls out of the digest, so it needs no directory listing to resolve.
func Bucket(key string) string { return KeyID(key)[:2] }

var (
	_ record.Record[record.Bar]  = record.Bar{}
	_ record.Record[record.Tick] = record.Tick{}
)

// BlockRecords is the zone-map granularity: one min/max row per column per
// BlockRecords records. A cursor checks the bounds of the current block before
// decoding it, so a whole block of obviously-failing records is skipped with an
// index jump instead of a decode per record. Power of two on purpose.
const BlockRecords = 256

// BlockRecordsShift shifts a record index into its block index.
const BlockRecordsShift = 8

// BlocksPerSegment bounds how many zone-map rows a segment file can hold for a
// column set. The bound assumes a key as short as possible, so the true row
// count per segment is the same or smaller, never larger, for any key. The
// worst-case row count is what the trailer reserves.
func blocksFor[T record.Record[T]](fileSize int64, recSize int) int {
	sc, err := record.SchemaOf[T]()
	if err != nil {
		return 0
	}

	ncols := len(sc.StatsColumns())
	if ncols == 0 {
		return 0
	}

	off := int64(HeaderSize) + 1

	num := fileSize - off - int64(ncols*16) - 8

	den := int64(BlockRecords*recSize + ncols*16)
	if num <= 0 || den <= 0 {
		return 0
	}

	return int(num/den) + 1
}

// TrailerBytes is the portion of a segment file reserved for the column stats
// index (per-segment ranges plus one zone-map row per block) and the
// sealed-segment checksum. It is fixed for a record type and a segment size, so
// both the writer's open-capacity check and the reader's capacity subtract the
// same number.
func TrailerBytes[T record.Record[T]](fileSize int64) int {
	sc, err := record.SchemaOf[T]()
	if err != nil {
		return 8
	}

	ncols := len(sc.StatsColumns())
	perRow := ncols * 16

	return perRow*(blocksFor[T](fileSize, record.SizeOf[T]())+1) + 8
}
