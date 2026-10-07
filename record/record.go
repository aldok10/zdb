// Package record defines the records a ZDB store holds: the fixed-width
// encodings, and the interface any custom record type implements to become
// storable.
//
// A store is one record type for its whole life, and Bar and Tick are the two
// this package ships. Anything else is a struct the caller defines, which is
// what Record is for.
//
// # What the format asks of a record
//
// The on-disk format requires exactly two things of a record, and Record is
// those two things written as Go methods:
//
//   - a fixed width, so record i sits at byte offset i*RecordSize() and the
//     binary search can address it without reading anything;
//   - a leading timestamp, stored unsigned at offset 0 for every record type,
//     strictly increasing within a shard.
//
// The segment header carries the width, so a file describes its own layout. A
// reader opened as the wrong type is refused with ErrCorrupt rather than
// decoding one record layout as another and returning plausible-looking values
// out of garbage.
//
// # Writing your own record
//
// Implement Record on your struct and every layer above it works unchanged. The
// smallest complete thing that could work:
//
//	type Quote struct {
//		Datetime int64  // unix microseconds, strictly increasing
//		Bid      uint32 // price in millionths
//	}
//
//	const quoteSize = 12
//
//	func (Quote) RecordSize() int { return quoteSize }
//	func (q Quote) Stamp() int64  { return q.Datetime }
//
//	func (q Quote) EncodeInto(dst []byte) error {
//		if q.Datetime < 0 {
//			return ErrInvalid
//		}
//		PutLe64(dst[0:], uint64(q.Datetime))
//		PutLe32(dst[8:], q.Bid)
//		return nil
//	}
//
//	func (Quote) DecodeInto(src []byte, dst *Quote) {
//		dst.Datetime = int64(Le64(src[0:]))
//		dst.Bid = Le32(src[8:])
//	}
//
//	db, err := writer.Open[Quote](writer.Options{Dir: "quotes"})
//	r, err := reader.Open[Quote]("quotes")
//
// PutLe64, Le64, PutLe32 and Le32 are one-line wrappers over
// binary.LittleEndian, and calling the standard library directly is just as
// correct. They are exported so that the format's endianness is decided in one
// file rather than restated by every record type that ever gets it wrong, which is
// the failure a store cannot detect: a big-endian write produces a file that only
// its own reader parses, and nothing complains until the values come back wrong.
//
// That example is small enough to read in one pass, which means it dodges the
// part that actually goes wrong. A record whose fields do not all start on their
// own alignment boundary needs its offsets written out by hand, and the width it
// reports is the packed one rather than what the Go struct occupies. So the
// worked example in custom_test.go uses a uint64 starting at offset 13: 21 bytes
// packed, against 24 for both unsafe.Sizeof and a field-aligned packer. Nothing
// in the store can detect that discrepancy, which is why RecordSize is the only
// thing standing between a record type and a file nothing else can read.
//
// Three rules are worth stating because the format cannot check them:
//
//   - RecordSize must be the packed width, not the Go struct's size. Padding
//     between fields is read on every scan, and Go's natural stride for a
//     uint32 field is what a naive sizeof reports.
//   - Datetime must be the first field, because the format fixes the timestamp
//     at offset 0. Stamp must return that field's value.
//   - The record type must not contain a pointer. It is copied in and out of
//     the mapping by value, and a pointer field would outlive both.
//
// # Why DecodeInto takes its destination
//
// DecodeInto has a value receiver and is handed the destination as an argument,
// rather than being a pointer-receiver method on the record. That is not a style
// choice, it is the only shape that keeps the read path at zero allocations, and
// the alternatives were measured rather than argued about:
//
//	A pointer-receiver DecodeFrom, with the cursor generic over *Bar: Range, All
//	and Latest would hand back one pointer reused across the whole scan, so
//	every element of a collected slice would alias the last record.
//
//	The same, with the cursor owning a *Bar and copying out: 0 allocs, and about
//	0.96x the concrete cursor, so that one works but costs a copy per record.
//
//	A decode that returns the record by value: `moved to heap: v` for every
//	record, because the generic conversion defeats escape analysis. 4096 allocs
//	for 4096 records. Unusable.
//
//	Splitting the decode out as a second type parameter, or passing it as a func
//	value: 16x slower than the concrete decode. The compiler inlines the direct
//	method call and neither of those.
//
// So the reader decodes into a destination it owns and copies out, and a caller's
// own record never escapes. The trade is one 60-byte copy per Bar against the
// alternative of one heap allocation per record.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	// PriceScale is the fixed-point scale for prices. uint64 holds ~1.8e11 at
	// this scale, far beyond any traded price.
	PriceScale = 1e8
)

var (
	// ErrInvalid means a record could not be stored as given: a negative
	// timestamp, or a price that is non-finite, negative for an unsigned
	// record type, or out of range at PriceScale.
	ErrInvalid = errors.New("zdb: invalid record")

	// ErrPriceRange is returned when a price is finite but does not survive the
	// fixed-point round trip. It is distinct from a non-finite price so a caller
	// can tell a bad instrument from a bad feed.
	ErrPriceRange = errors.New("zdb: price out of range")
)

// Record is a fixed-width record ZDB can store, and it is the whole of what the
// format asks for: a width, a way to write one, and a way to read one. The width
// comes from RecordSize rather than a package constant, which is what lets a bar
// store and a tick store share every file-level mechanism. Stamp is the value a
// shard orders on; it must be the record's Datetime, because that is what a
// segment stores unsigned at offset 0 for every record type and therefore what
// the binary search reads.
//
// See the package documentation for what implementing this requires beyond the
// method set.
type Record[T any] interface {
	RecordSize() int
	Stamp() int64
	EncodeInto(dst []byte) error
	DecodeInto(src []byte, dst *T)
}

// SizeOf is the encoded width of the record type T.
//
// It is a function because a type parameter cannot be composite-literal'd, so
// T{}.RecordSize() does not compile, and it deliberately does not spell that as
// (*new(T)).RecordSize() either: that dereferences a pointer to a type
// parameter's zero value, and on arm64 it faults reading the receiver when the
// method has a value receiver. A local value is the only form that works, and it
// is the one place a caller should be reaching for when it has a type parameter
// and needs the width.
func SizeOf[T Record[T]]() int {
	var zero T

	return zero.RecordSize()
}

// TypeName is the record type's name for an error message. It takes an already
// boxed zero value rather than using reflect, because %T on a value is free and
// reflect on a type parameter would drag the package onto the read path's
// dependency list for a string that only ever appears when a file is wrong.
func TypeName[T any]() string { return fmt.Sprintf("%T", *new(T)) }

// Le64 reads one little-endian uint64 at the head of b, and PutLe64 writes one.
// Le32 and PutLe32 are the same for 32 bits.
//
// These exist for a caller writing its own record type, because endianness is the
// one property of the format a record gets wrong silently: a big-endian write
// produces a file that only its own reader can parse, and nothing complains until
// the values come back wrong. Calling the stdlib directly is exactly as correct and
// exactly as cheap, so this is a seam rather than a capability, and it is here so
// that "little-endian" is named in one file instead of in every record type.
//
// Each is a one-line wrapper that inlines to the same PutUint64 or Uint64, so
// using them costs nothing on the append or scan path.
func Le64(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }

// PutLe64 writes v little-endian at the head of b.
func PutLe64(b []byte, v uint64) { binary.LittleEndian.PutUint64(b, v) }

// Le32 reads one little-endian uint32 at the head of b. It satisfies Le64's
// counterpart for a 32-bit field; see Le64.
func Le32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }

// PutLe32 writes v little-endian at the head of b.
func PutLe32(b []byte, v uint32) { binary.LittleEndian.PutUint32(b, v) }

// Le16 reads one little-endian uint16 at the head of b. It satisfies Le64's
// counterpart for a 16-bit field; see Le64.
func Le16(b []byte) uint16 { return binary.LittleEndian.Uint16(b) }

// PutLe16 writes v little-endian at the head of b.
func PutLe16(b []byte, v uint16) { binary.LittleEndian.PutUint16(b, v) }

// price is fixedPoint's inverse for an unsigned record.
func price(v uint64) float64 { return float64(v) / PriceScale }

// scalePrice converts a price to unsigned fixed-point at PriceScale.
func scalePrice(v float64) (uint64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, errors.New("zdb: non-finite price")
	}

	if v < 0 {
		return 0, errors.New("zdb: negative price")
	}

	if v*PriceScale > math.MaxUint64 {
		return 0, ErrPriceRange
	}

	return uint64(math.Round(v * PriceScale)), nil
}

// signed is scaleSigned's inverse. The division is exact for every value
// scaleSigned produced, because Round made it an integer multiple of
// 1/PriceScale.
func signed(v int64) float64 { return float64(v) / PriceScale }

// scaleSigned converts a price to signed fixed-point at PriceScale. Unlike
// scalePrice it accepts negatives, because a tick's prices are signed and a
// market that printed negative is a market that has to be storable. The ceiling
// is math.MaxInt64 at PriceScale, so about 92 billion, which is past any price
// that has traded.
func scaleSigned(v float64) (int64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, errors.New("zdb: non-finite price")
	}

	scaled := v * PriceScale
	if scaled >= math.MaxInt64 || scaled <= math.MinInt64 {
		return 0, ErrPriceRange
	}

	return int64(math.Round(scaled)), nil
}
