package record

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// TagKey is the struct tag Schema reads.
//
//	type Bar struct {
//		Datetime   int64   `zdb:"datetime,time,primary"`
//		Open       float64 `zdb:"open,price,index"`
//		TickVolume uint64  `zdb:"tick_volume,index"`
//		Venue      [8]byte `zdb:"venue,index"`
//	}
//
// A schema is what makes ZQL work for a record type nobody anticipated, so the
// column names, their types and their roles all come from here rather than from
// a switch in the query language. A field with no tag is not a column, which is
// what lets a caller keep bookkeeping next to the record without the query
// language seeing it.
//
// The options are:
//
//	time       this is the ordering column; exactly one record has one
//	unit=      ms, us or ns; the ordering column's wall-clock unit
//	price      compare at PriceScale rather than as a raw float
//	primary    part of the primary key; the time column is always the last one
//	index      keep a min/max for this column so a segment can be rejected
//	alias=a|b  extra spellings for the column name
//
// Those four option words are the whole set, and there are deliberately no
// synonyms for them: an earlier version accepted ts, pk and indexed as well,
// which meant a column legitimately named ts could not be spelled. A tag's
// first part is read as the column name unless it is one of the four option
// words, so the cost of that choice is that a column named exactly time, price,
// primary or index takes its name from the Go field instead, and a caller who
// needs the literal spelling uses an alias.
const TagKey = "zdb"

// Kind is a column's stored type.
//
// The set is what a fixed-width record can be compared against without
// conversion, and every kind here compares as an integer, a float or a byte
// string, so a filter never has to know what the record was written as.
//
// The widths exist because record.Bar stores a spread in four bytes and a
// timestamp in eight, and a schema that could only describe eight-byte fields
// would not be able to describe either of the record types this package ships.
type Kind uint8

// The column kinds. The order is the order a schema error message walks, so a
// bad tag names the kind it wanted rather than a number.
const (
	KindInt64 Kind = iota
	KindUint64
	KindInt32
	KindUint32
	KindInt16
	KindUint16
	KindInt8
	KindUint8
	KindFloat64
	KindFloat32
	KindBool
	KindBytes
)

// String returns the Go type a kind maps to, for an error message and for a
// schema dump. A byte column renders with its width, because [4]byte and
// [8]byte are different columns and an error naming "bytes" for both would not
// say which one is wrong.
func (k Kind) String() string {
	switch k {
	case KindInt64:
		return "int64"
	case KindUint64:
		return "uint64"
	case KindInt32:
		return "int32"
	case KindUint32:
		return "uint32"
	case KindInt16:
		return "int16"
	case KindUint16:
		return "uint16"
	case KindInt8:
		return "int8"
	case KindUint8:
		return "uint8"
	case KindFloat64:
		return "float64"
	case KindFloat32:
		return "float32"
	case KindBool:
		return "bool"
	case KindBytes:
		return "bytes"
	}

	return "?"
}

// integer reports whether k compares as an integer, signed or unsigned.
//
// The Time and Primary rules depend on it, and so does the choice of compare
// path, which is why it is one predicate rather than three switch statements
// that could disagree about which kinds are numeric.
func (k Kind) integer() bool {
	switch k {
	case KindInt64, KindUint64, KindInt32, KindUint32,
		KindInt16, KindUint16, KindInt8, KindUint8, KindBool:
		return true
	}

	return false
}

// ErrSchema is returned when a struct cannot be a record's schema. It is one
// error value with a wrapped message rather than a family, because every cause
// is the same fact from the caller's side: this struct is not storable as given,
// and the message names which rule it broke.
var ErrSchema = errors.New("zdb: invalid schema")

// Column is one field's role in a record.
//
// The whole of what the query language knows about a record lives here, and the
// reason is §2.3 of the project rules: reflection must not reach the read path.
// A Schema is built once from tags and every column is reduced to an offset, a
// kind and a width, so testing a condition is an unsafe.Add and a switch rather
// than a FieldByName and an Interface call per record.
type Column struct {
	// Name is the ZQL spelling, taken from the tag or from the Go field name.
	Name string
	// Aliases are additional spellings. They exist because the same column is
	// called close, c and px in different feeds, and a query language that
	// refuses all but one of them gets abandoned rather than rewritten.
	Aliases []string
	// Offset is the field's byte offset inside the struct, as reflect reports
	// it. The compiler guarantees a field is aligned for its own type, so a
	// typed load through this offset is aligned and needs no padding check.
	Offset uintptr
	// Kind is the field's stored type.
	Kind Kind
	// Width is a byte column's length, and zero for every other kind. It is
	// carried rather than derived because comparing two byte columns means
	// comparing this many bytes, and a compare that re-read the reflect.Type to
	// learn its own length would put reflection back on the scan path.
	Width int
	// Time marks the ordering column. Exactly one column carries it, and a
	// condition on it folds into the segment's binary search rather than being
	// tested per record.
	Time bool
	// Price marks a float column compared at PriceScale. See the note on
	// ScalePrice for why the conversion is not optional.
	Price bool
	// Primary marks a component of the primary key. The Time column is always
	// the last one; this marks the ones before it.
	Primary bool
	// Indexed marks a column a segment keeps a min/max for, so a condition can
	// reject a whole segment without reading a record from it.
	Indexed bool
}

// TimeUnit is the wall-clock unit a Time column carries. It is a schema property
// rather than a column one because exactly one column is the Time column, and the
// query language needs the unit to tell a caller whether its bounds are
// milliseconds or microseconds.
type TimeUnit uint8

// The supported timestamp units. They are the two a market feed actually
// produces and they are distinguished because the difference decides whether a
// store is usable: a tick feed that stamps milliseconds loses most of its
// records to ErrOutOfOrder, because a liquid symbol prints several ticks per
// millisecond and a shard requires strictly increasing stamps.
const (
	UnitUnknown TimeUnit = iota
	// UnitMilli is unix milliseconds, what a candle carries.
	UnitMilli
	// UnitMicro is unix microseconds, what a print carries.
	UnitMicro
	// UnitNano is unix nanoseconds.
	UnitNano
)

// String returns the unit's name for an error message and for a schema dump.
func (u TimeUnit) String() string {
	switch u {
	case UnitMilli:
		return "milliseconds"
	case UnitMicro:
		return "microseconds"
	case UnitNano:
		return "nanoseconds"
	}

	return "unknown"
}

// Ticks reports how many ticks of unit u fit in d, truncating toward zero, so a
// caller can measure a wall-clock duration in the stamps a record type actually
// carries. A candle is milliseconds and a print is microseconds, so one hour is
// a different stamp width for each and neither can guess at the other's.
//
// It returns -1 for UnitUnknown, which has no conversion: an unrecognized
// unit= tag leaves the schema valid and the width unknowable, and the caller
// decides whether that is fatal.
func (u TimeUnit) Ticks(d time.Duration) int64 {
	switch u {
	case UnitMilli:
		return d.Milliseconds()
	case UnitMicro:
		return d.Microseconds()
	case UnitNano:
		return d.Nanoseconds()
	}

	return -1
}

// Schema is a record type's columns, built once and then read-only.
//
// It is generic because a column's Offset is only meaningful against the struct
// it was measured from, and a []Column of offsets computed for one type would be
// silently wrong for another. The generic parameter is what stops that.
type Schema[T any] struct {
	// Name is the record type's name, for an error message.
	Name string
	// Columns are the queryable fields in declaration order. Declaration order
	// rather than a sort, because a schema's String is what a caller reads to
	// find out what it may query, and alphabetical order would put the
	// timestamp somewhere unhelpful.
	Columns []Column
	// Unit is the Time column's wall-clock unit.
	Unit TimeUnit

	time int // index of the Time column, or -1
	// stats holds the indexable columns: every column except the ordering one,
	// and excluding byte columns. It is built once at schema construction.
	stats []Column
}

// schemaCache holds one Schema per record type, successful or not. Schema
// construction is reflection over every field, which is the expensive part, and
// it would be paid again by every Open of every store of that type if it were
// not cached. Keyed by reflect.Type because that is the one thing that identifies
// T without a T.
//
// Failures are cached too, and that is the half that is easy to leave out: a
// store whose record type has a bad tag fails at Open, and a service retrying
// Open would otherwise re-walk every field of the struct on every attempt. The
// cached value is a schemaEntry so that a miss and a hit are told apart without
// a second lookup.
var schemaCache sync.Map // reflect.Type -> schemaEntry

// schemaEntry is what the cache holds: either a schema or the reason there is
// none. One value rather than a *Schema[T] that is nil on failure, because a nil
// schema in a sync.Map is indistinguishable from a stored nil unless every reader
// re-checks for nil, and the error is what the caller needs anyway.
type schemaEntry struct {
	// schema is a *Schema[T] boxed in an any, because schemaEntry cannot be
	// generic without the cache being generic, and a generic sync.Map is a
	// separate cache per instantiation rather than one shared map.
	schema any
	err    error
}

// SchemaProvider is how a generated schema reaches SchemaOf.
//
// A type whose schema was produced by cmd/gen-schema-record declares a ZDBSchema
// method, and SchemaOf finds it by interface assertion before it reaches for the
// cache or for reflection. That assertion is the whole of the fast path.
//
// It is an interface rather than a registry because a registry needs a key, and the
// only key available without a T is a reflect.Type — which is precisely the thing
// the fast path exists to avoid computing. An interface is resolved by the
// compiler, at compile time, into a check on the method set.
//
// The method takes a value receiver and cmd/gen-schema-record emits one. A pointer
// receiver would remove an allocation from SchemaOf, and it is refused here because
// SchemaOf asserts on the zero value: a value-receiver method is also in *T's
// method set, so a hand-written pointer-receiver version would work by accident and
// a hand-written value-receiver version would work today and break the moment
// SchemaOf changed. One spelling, held by the generator, is worth one allocation
// on a function called once per query plan.
type SchemaProvider[T any] interface {
	// ZDBSchema returns T's schema. Generated code returns a package-level
	// value, so this never builds anything.
	ZDBSchema() *Schema[T]
}

// SchemaOf returns T's schema.
//
// Three sources, tried in order of how much they cost:
//
//  1. **Generated.** If cmd/gen-schema-record produced a ZDBSchema method for T,
//     one interface assertion returns a package-level schema. No reflection, no
//     map, no allocation.
//  2. **Cached.** Otherwise the schema is reflected over once per type and kept
//     in schemaCache, so the second and later calls for the same type pay a map
//     load. A cached failure is returned as-is rather than re-derived.
//  3. **Reflected.** The last resort, and the only one that walks fields.
//
// The order matters because the first two are cheap enough to call from a query
// planner without thinking about it, which is what lets a caller resolve the
// schema at the same place whether or not anyone ran a generator.
//
// It returns an error rather than a partial schema. A record type that cannot be
// queried is not a record type that can be written either, and discovering that
// at Open is the difference between a clear message and a panic on the first
// query.
func SchemaOf[T any]() (*Schema[T], error) {
	// One zero value serves both sources below. It moves to the heap when it is
	// converted to an interface, which is one small allocation per call and is
	// documented on SchemaProvider along with why it is not avoided.
	var zero T

	// A pointer to the zero value, not the zero value itself: boxing a struct
	// copies it, while a *T is what an interface's data word already holds. A
	// value-receiver method is in *T's method set, so the assertion works either
	// way.
	if p, ok := any(&zero).(SchemaProvider[T]); ok {
		return p.ZDBSchema(), nil
	}

	// reflect.TypeOf(&zero).Elem() rather than reflect.TypeOf(T{}), because T's
	// zero value may itself be nil for a nilable type and TypeOf(nil) is nil.
	rt := reflect.TypeOf(&zero).Elem()

	if v, ok := schemaCache.Load(rt); ok {
		e, _ := v.(schemaEntry)

		if e.err != nil {
			return nil, e.err
		}

		// Safe because the key is the type: two different T cannot produce the
		// same reflect.Type, so the stored *Schema[T] is the one for this T.
		s, _ := e.schema.(*Schema[T])

		return s, nil
	}

	s, err := buildSchema[T](rt)

	// LoadOrStore rather than Store, so two goroutines racing to open two stores
	// of the same type converge on one entry. Losing the race is not a failure:
	// both schemas are equal by construction, and the stored one is returned so
	// that two racing Opens hand back the same pointer rather than two equal
	// schemas a caller might compare by identity.
	//
	// It is also the reason schemaBuilds can exceed one for a type. Two goroutines
	// that miss simultaneously both reflect, and only one result survives. The
	// count is therefore a measure of how often reflection ran, not a promise
	// about how many schemas exist.
	actual, _ := schemaCache.LoadOrStore(rt, schemaEntry{schema: s, err: err})

	e, _ := actual.(schemaEntry)

	if e.err != nil {
		return nil, e.err
	}

	stored, _ := e.schema.(*Schema[T])

	return stored, nil
}

// MustSchemaOf is SchemaOf for a type whose schema is known good at compile
// time. It panics on a bad schema, which is the right outcome for a record type
// written by hand with tags: the mistake is in the program's own source and
// there is nothing a caller could do about it at runtime.
//
// Package-level usage in an init or a var initializer is the intended caller.
// Do not reach for it from a query path, where an error is what a caller can
// actually act on.
func MustSchemaOf[T any]() *Schema[T] {
	s, err := SchemaOf[T]()
	if err != nil {
		panic(err)
	}

	return s
}

// NewSchema builds a schema from columns a caller already has, which is what
// generated code calls.
//
// It exists because Schema's ordering index is derived, not stated: a caller
// handing over four columns cannot know which index the time column lands at
// without counting, and a generated literal that got that count wrong would fail
// at the first query rather than at construction. Every rule buildSchema enforces
// is enforced here too, so a generated schema is refused under exactly the
// conditions a reflected one is.
//
// The error names the column rather than the Go field, because generated code
// has no field to name and the column is what a caller queries by.
func NewSchema[T any](name string, unit TimeUnit, cols []Column) (*Schema[T], error) {
	s := &Schema[T]{Name: name, Unit: unit, Columns: cols, time: -1}

	seen := make(map[string]string, len(cols))

	for i := range cols {
		c := &cols[i]

		if err := checkColumn(c, seen, c.Name); err != nil {
			return nil, fmt.Errorf("%w: %s.%s: %w", ErrSchema, name, c.Name, err)
		}

		if !c.Time {
			continue
		}

		if s.time >= 0 {
			return nil, fmt.Errorf("%w: %s has two time columns, %s and %s",
				ErrSchema, name, s.Columns[s.time].Name, c.Name)
		}

		s.time = i
	}

	if s.time < 0 {
		return nil, fmt.Errorf("%w: %s has no time column: tag exactly one field %q",
			ErrSchema, name, "time")
	}

	if !cols[s.time].Kind.integer() {
		return nil, fmt.Errorf("%w: %s.%s is %s, a time column is an integer",
			ErrSchema, name, cols[s.time].Name, cols[s.time].Kind)
	}

	applyStats(s)

	return s, nil
}

// applyStats populates the indexable column list. Built once so the read path
// never filters on every query.
func applyStats[T any](s *Schema[T]) {
	out := make([]Column, 0, len(s.Columns))
	for _, c := range s.Columns {
		if c.Time || c.Kind == KindBytes {
			continue
		}

		out = append(out, c)
	}

	s.stats = out
}

// StatsColumns returns the columns a segment keeps a min/max for: every column
// except the ordering (time) column and byte columns. Exported so the format
// package can lay the stats block out from the same list the writer fills.
func (s *Schema[T]) StatsColumns() []Column { return s.stats }

// StatsBytes is the byte size of the per-segment stats block: two little-endian
// uint64 (min, max) per indexable column.
func (s *Schema[T]) StatsBytes() int { return len(s.stats) * 16 }

// MustSchema is NewSchema for a package-level variable, which is what generated
// code initialises.
//
// A two-value return cannot be assigned to a var, so the generated file either
// needs an init function that panics or a helper that does it here. The helper is
// chosen because a panic from a package initialiser names the offending variable
// and the error, where a discarded error would leave a nil schema behind for the
// first query to fault on.
//
// It cannot fire for code this package's own generator produced: the generator
// runs the same rules and refuses to write the file. It fires for a hand-edited
// generated file, which is exactly when a loud failure is wanted.
func MustSchema[T any](name string, unit TimeUnit, cols []Column) *Schema[T] {
	s, err := NewSchema[T](name, unit, cols)
	if err != nil {
		panic("zdb/record: generated schema for " + name + " is invalid: " + err.Error())
	}

	return s
}

// TimeIndex returns the Time column's index into Columns, or -1 when the schema
// has none. Every schema this package builds has one, so -1 only happens for a
// schema a caller assembled itself.
func (s *Schema[T]) TimeIndex() int { return s.time }

// Time returns the ordering column.
func (s *Schema[T]) Time() Column { return s.Columns[s.time] }

// Len is the number of columns.
func (s *Schema[T]) Len() int { return len(s.Columns) }

// Field resolves a ZQL column name or one of its aliases, case-insensitively.
//
// The linear scan is deliberate. A store has a handful of columns, the scan is
// branch-predictable, and a map here would put a hash lookup and an allocation
// on the path that decides which column a query filters on. This runs once per
// condition at plan time, not once per record, so even a map would be
// defensible; the scan is simply less to get wrong.
func (s *Schema[T]) Field(name string) (Column, int, bool) {
	want := strings.ToLower(name)

	for i := range s.Columns {
		c := &s.Columns[i]
		if strings.EqualFold(c.Name, want) {
			return *c, i, true
		}

		for _, a := range c.Aliases {
			if strings.EqualFold(a, want) {
				return *c, i, true
			}
		}
	}

	return Column{}, 0, false
}

// Fields resolves every name, and reports the first one that is not a column.
// All-or-nothing because a half-resolved projection is a projection the caller
// did not ask for.
func (s *Schema[T]) Fields(names []string) ([]Column, error) {
	out := make([]Column, 0, len(names))

	for _, n := range names {
		c, _, ok := s.Field(n)
		if !ok {
			return nil, fmt.Errorf("%w: %s has no column %q, columns are %s",
				ErrSchema, s.Name, n, s)
		}

		out = append(out, c)
	}

	return out, nil
}

// String lists the columns the way a caller may write them, which is what makes
// an unknown-column error actionable instead of a guess.
func (s *Schema[T]) String() string {
	var sb strings.Builder

	sb.WriteString(s.Name)
	sb.WriteByte('(')

	for i := range s.Columns {
		c := &s.Columns[i]

		if i > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(c.Name)
		sb.WriteByte(':')
		sb.WriteString(c.Kind.String())

		if c.Kind == KindBytes {
			sb.WriteByte('[')
			sb.WriteString(strconv.Itoa(c.Width))
			sb.WriteByte(']')
		}

		if c.Price {
			sb.WriteString("/price")
		}
	}

	sb.WriteByte(')')

	return sb.String()
}

// Value is a numeric column's value widened for comparison.
//
// The three fields rather than one interface{} is the §2.3 rule applied to a
// value that came out of reflection: an interface here would box on every
// record, and a struct of three scalars stays in registers. IsFloat says which
// pair is live, and it is the Kind the caller already switched on to get here,
// so it is not a second judgement.
//
// There is no string field, and that is deliberate rather than an oversight. A
// Go string is a pointer and a length, so putting one here would make Value
// escape and would make every scan allocate for a column that holds eight bytes.
// A byte column compares through Column.CompareBytes, which needs no value at
// all.
type Value struct {
	Int     int64
	Uint    uint64
	Float   float64
	IsFloat bool
}

// ColumnBound is a closed (inclusive) range of values a column is allowed to
// intersect with for a segment to be worth scanning. Lo and Hi are in the same
// CompareValue space the conditions are. A segment whose whole column lies
// outside [Lo, Hi] can never hold a matching record, so a reader may skip it.
type ColumnBound struct {
	// Name is the ZQL column spelling, matched case-insensitively.
	Name string
	// Lo and Hi are [min, max] of the values that survive the query's
	// conditions on this column. Lo of 0 through Hi of math.MaxUint64 keeps
	// every segment; a range a column never reaches can be pruned.
	Lo, Hi uint64
}

// Load reads a numeric column out of the record at p.
//
// The unsafe.Add is sound because Offset came from reflect on this exact struct
// type, and the compiler laid that field out aligned for its own type, so a
// typed load through it is an aligned load. That is the entire invariant, and it
// is why Offset is never computed here: a hand-written offset would have no such
// guarantee, which is why nothing in this package takes one from a caller.
//
// p must point at a T. Getting that wrong is a fault, and it is the same
// lifetime contract the SegmentHeader overlay carries.
//
// A byte column returns the zero Value: its length is not a number and reading
// it as one would produce a plausible wrong answer. CompareBytes is the only
// correct way to load one, and this returns zero so that a caller who forgets
// gets a wrong answer rather than a fault, which is the worse of the two.
func (c Column) Load(p unsafe.Pointer) Value {
	switch c.Kind {
	case KindInt64:
		return Value{Int: *(*int64)(unsafe.Add(p, c.Offset))}
	case KindUint64:
		return Value{Uint: *(*uint64)(unsafe.Add(p, c.Offset))}
	case KindInt32:
		return Value{Int: int64(*(*int32)(unsafe.Add(p, c.Offset)))}
	case KindUint32:
		return Value{Uint: uint64(*(*uint32)(unsafe.Add(p, c.Offset)))}
	case KindInt16:
		return Value{Int: int64(*(*int16)(unsafe.Add(p, c.Offset)))}
	case KindUint16:
		return Value{Uint: uint64(*(*uint16)(unsafe.Add(p, c.Offset)))}
	case KindInt8:
		return Value{Int: int64(*(*int8)(unsafe.Add(p, c.Offset)))}
	case KindUint8:
		return Value{Uint: uint64(*(*uint8)(unsafe.Add(p, c.Offset)))}
	case KindBool:
		// A bool compares as 0 or 1 rather than through its own Value field,
		// because a filter on it is a range test and a range test needs a number.
		// The Go spec fixes a bool's representation so the load is aligned and
		// the read is sound.
		if *(*bool)(unsafe.Add(p, c.Offset)) {
			return Value{Uint: 1}
		}

		return Value{}
	default:
		return Value{Float: c.LoadFloat(p), IsFloat: true}
	}
}

// LoadFloat reads a float column at full precision, which is what the Price
// conversion needs: rounding a float32 to a PriceScale integer and rounding the
// same number as a float64 disagree, and the caller has to be told which one it
// got.
func (c Column) LoadFloat(p unsafe.Pointer) float64 {
	if c.Kind == KindFloat32 {
		return float64(*(*float32)(unsafe.Add(p, c.Offset)))
	}

	return *(*float64)(unsafe.Add(p, c.Offset))
}

// Compare orders two records' values for column c, and is the whole of what a
// filter, a min/max and a sort need.
//
// It takes two pointers rather than two Values because a byte column has no
// Value to carry: it is Width bytes at each address, and materialising either
// one as a Go string or slice would put a pointer to a stack frame into the
// result and hand the caller something it cannot keep. Two addresses in, an
// int out, nothing allocated.
//
// Both pointers must point at a T. See Load for why that offset arithmetic is
// sound.
func (c Column) Compare(p, q unsafe.Pointer) int {
	switch c.Kind {
	case KindBytes:
		return c.CompareBytes(p, q)
	case KindFloat64, KindFloat32:
		return cmpFloat(c.LoadFloat(p), c.LoadFloat(q))
	case KindUint64, KindUint32, KindUint16, KindUint8, KindBool:
		return cmpUint(c.Load(p).Uint, c.Load(q).Uint)
	default:
		return cmpInt(c.Load(p).Int, c.Load(q).Int)
	}
}

// CompareBytes orders two records' byte columns.
//
// The slices point into the two records and are handed to bytes.Compare, which
// does not retain them. They are not returned, and that is what keeps this
// zero-allocation: a returned []byte into a caller's record would carry a
// pointer to that record, which is a heap escape per comparison.
//
// Lexicographic byte order is the right order for the codes this is meant for,
// and a shorter prefix sorts first, which is the ordering a caller comparing
// padded venue codes expects.
func (c Column) CompareBytes(p, q unsafe.Pointer) int {
	if c.Width == 0 {
		return 0
	}

	a := unsafe.Slice((*byte)(unsafe.Add(p, c.Offset)), c.Width)
	b := unsafe.Slice((*byte)(unsafe.Add(q, c.Offset)), c.Width)

	return bytes.Compare(a, b)
}

// CompareValue is the unsigned numeric form a condition on this column compares
// against. The query layer (zql.stored) and the segment index both reduce a
// price/volume/spread to this single space, so a min/max index built from
// CompareValue rejects exactly the segments a condition would skip.
//
// Prices are compared at PriceScale — the same fixed-point integer zql tests a
// record with — rather than as a float, so an integer window can never decide a
// boundary a float would round differently. A byte column has no scalar window
// and reports 0; it is excluded from the index.
func (c Column) CompareValue(p unsafe.Pointer) uint64 {
	v := c.Load(p)

	if c.Price {
		return uint64(v.Float * PriceScale)
	}

	switch c.Kind {
	case KindUint64:
		return v.Uint
	case KindUint32:
		return uint64(uint32(v.Uint))
	case KindUint16:
		return uint64(uint16(v.Uint))
	case KindUint8:
		return uint64(uint8(v.Uint))
	case KindInt64:
		return uint64(v.Int)
	case KindInt32:
		return uint64(uint32(int32(v.Int)))
	case KindInt16:
		return uint64(uint16(int16(v.Int)))
	case KindInt8:
		return uint64(uint8(int8(v.Int)))
	case KindBool:
		return v.Uint
	case KindFloat64:
		return uint64(v.Float)
	case KindFloat32:
		return uint64(v.Float)
	}

	return 0
}

// Equal reports whether two records agree on column c, which is a Compare that
// stopped early. It exists because equality is the only comparison a filter
// usually wants and the three-way form costs a second branch.
func (c Column) Equal(p, q unsafe.Pointer) bool {
	if c.Kind == KindBytes {
		if c.Width == 0 {
			return true
		}

		a := unsafe.Slice((*byte)(unsafe.Add(p, c.Offset)), c.Width)
		b := unsafe.Slice((*byte)(unsafe.Add(q, c.Offset)), c.Width)

		return bytes.Equal(a, b)
	}

	return c.Compare(p, q) == 0
}

// cmpFloat orders two floats, and it is here rather than left to the caller
// because a filter that writes `a > b` instead of `a.Compare(b) > 0` gets NaN
// wrong, and NaN is exactly the value a bad price carries.
func cmpFloat(a, b float64) int {
	// NaN is ordered last, and it has to be tested before the comparisons
	// rather than after, because every comparison against NaN is false. The
	// fall-through below would then order NaN greater than everything in both
	// directions, which is not an order at all: a sort using it would not
	// converge.
	//
	// Last rather than as an error, because the alternative is a filter that
	// silently matches nothing on a column holding one bad print, which reads
	// as "this column never matches" rather than as "this value is not a
	// number".
	if math.IsNaN(a) {
		if math.IsNaN(b) {
			return 0
		}

		return 1
	}

	if math.IsNaN(b) {
		return -1
	}

	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

// ScalePrice is scalePrice for a caller outside this package, which is the query
// layer: a float condition has to become an integer to be compared against a
// fixed-point column without letting a rounded price decide the result.
//
// It is exported rather than left private because the alternative is the query
// package reimplementing the rounding rule, and a rounding rule written twice is
// a rounding rule that will differ.
func ScalePrice(v float64) (uint64, error) { return scalePrice(v) }

// ScaleSignedPrice is scaleSigned for a caller outside this package. It accepts
// negatives, which a market that printed negative produces and a price column
// tagged Price on a signed field must therefore handle.
func ScaleSignedPrice(v float64) (int64, error) { return scaleSigned(v) }

// schemaBuilds counts how many times buildSchema has run, across every type.
//
// It exists because "reflection happens once per type" is a claim about runtime
// behaviour and every other way of checking it is indirect: a pointer comparison
// shows two calls returned the same schema but not that the second one skipped the
// reflection, and a benchmark shows the cost of the cache without showing the cache
// works. Counting the calls is the only assertion that tests the claim itself.
//
// It is an atomic increment on the cold path. Schema construction runs at most a
// couple of times per record type in the life of a process, so the increment costs
// nothing that anyone can measure, and the cached path never touches it.
var schemaBuilds atomic.Int64

// SchemaBuilds is the number of schemas built by reflection since the process
// started. It is exported for tests and for a caller profiling startup; nothing in
// this package reads it.
func SchemaBuilds() int64 { return schemaBuilds.Load() }

// buildSchema reflects over rt and validates it into a schema. Every rule it
// enforces is one the format relies on and cannot itself check, so each refusal
// below is a refusal to store something that would later read back as garbage.
func buildSchema[T any](rt reflect.Type) (*Schema[T], error) {
	schemaBuilds.Add(1)

	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: %s is a %s, a record is a struct",
			ErrSchema, rt, rt.Kind())
	}

	s := &Schema[T]{Name: rt.Name(), time: -1}

	seen := make(map[string]string, rt.NumField())

	for i := range rt.NumField() {
		f := rt.Field(i)

		// Unexported fields cannot be read through a pointer the caller holds, and
		// a schema that named one would build a column Load cannot honestly read.
		// Skipping rather than erroring is right because they are not columns:
		// the tag is what makes a field one, and an unexported field has none.
		if !f.IsExported() {
			continue
		}

		tag, tagged := f.Tag.Lookup(TagKey)

		// An untagged field is not a column, so its type is not the format's
		// business. The kind check has to come after this test rather than before
		// it, or a record carrying a uint8 flag or a string field beside its
		// columns would be rejected for a field the query language never sees.
		if !tagged {
			continue
		}

		c, err := parseTag(f, tag)
		if err != nil {
			return nil, fmt.Errorf("%w: %s.%s: %w", ErrSchema, rt.Name(), f.Name, err)
		}

		if err := checkColumn(&c, seen, f.Name); err != nil {
			return nil, fmt.Errorf("%w: %s.%s: %w", ErrSchema, rt.Name(), f.Name, err)
		}

		if c.Time {
			if s.time >= 0 {
				return nil, fmt.Errorf("%w: %s has two time columns, %s and %s",
					ErrSchema, rt.Name(), s.Columns[s.time].Name, c.Name)
			}

			s.Unit = unitOf(tag)
			s.time = len(s.Columns)
		}

		s.Columns = append(s.Columns, c)
	}

	if s.time < 0 {
		return nil, fmt.Errorf("%w: %s has no time column: tag exactly one field %q",
			ErrSchema, rt.Name(), "time")
	}

	// The format reads the ordering field as an unsigned integer at byte offset
	// 0. A float stamp would have to be rounded into that order, and two stamps a
	// fraction apart would collide into one position, so it is refused here
	// rather than producing a store whose order is not its order.
	if !s.Columns[s.time].Kind.integer() {
		return nil, fmt.Errorf("%w: %s.%s is %s, a time column is an integer",
			ErrSchema, rt.Name(), s.Columns[s.time].Name, s.Columns[s.time].Kind)
	}

	applyStats(s)

	return s, nil
}

// checkColumn enforces the rules a single field has to satisfy on its own, and
// records the names it claimed so a second claim is caught.
func checkColumn(c *Column, seen map[string]string, field string) error {
	if c.Name == "" {
		return errors.New("a tagged field needs a column name")
	}

	if err := validateName(c.Name); err != nil {
		return err
	}

	if prev, dup := seen[c.Name]; dup {
		return fmt.Errorf("column %q is already claimed by %s", c.Name, prev)
	}

	seen[c.Name] = field

	for _, a := range c.Aliases {
		if err := validateName(a); err != nil {
			return err
		}

		if prev, dup := seen[a]; dup {
			return fmt.Errorf("alias %q is already claimed by %s", a, prev)
		}

		seen[a] = field
	}

	// Price is a fixed-point conversion, and a fixed-point conversion of an
	// integer is just a worse integer. Catching it here means the tag can be
	// written on any field without the query language having to wonder whether
	// it meant anything.
	if c.Price && c.Kind != KindFloat64 {
		return fmt.Errorf("column %q is %s, price applies to a float64 column", c.Name, c.Kind)
	}

	// A min/max over a time column is what the header already stores, so tagging
	// it would ask for a second copy of a value that already exists and can never
	// disagree with it.
	if c.Indexed && c.Time {
		return fmt.Errorf("column %q is the time column, which is indexed by definition", c.Name)
	}

	return nil
}

// validateName refuses a name the ZQL lexer could not read back. A schema whose
// column name cannot be typed is a schema whose column cannot be selected.
func validateName(n string) error {
	if n == "" {
		return errors.New("empty column name")
	}

	for i := range len(n) {
		if !isNameByte(n[i]) {
			return fmt.Errorf("column name %q may not contain %q", n, string(n[i]))
		}
	}

	return nil
}

// isNameByte reports whether c may appear in a column name. It is the lexer's
// word rule: letters, digits, underscore and dot, which is what lets a dotted
// name survive Parse(String(q)) unchanged.
func isNameByte(c byte) bool {
	return c == '_' || c == '.' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// parseTag reads one field's tag into a Column. The caller has already
// established that the field has a tag, so there is no "absent tag" case here.
func parseTag(f reflect.StructField, tag string) (Column, error) {
	c := Column{Offset: f.Offset}

	k, width, err := KindOf(f.Type)
	if err != nil {
		return c, fmt.Errorf("%s: %w", f.Name, err)
	}

	c.Kind, c.Width = k, width

	parts := strings.Split(tag, ",")

	// The first part is the name only when it is not an option. A tag of
	// `zdb:"price"` names no column, and silently calling it "price" while also
	// setting the price flag is the kind of ambiguity that costs an afternoon.
	if parts[0] != "" && !isOption(parts[0]) {
		c.Name = parts[0]
		parts = parts[1:]
	}

	for _, o := range parts {
		switch {
		case o == "":
			return c, errors.New("empty option in tag")
		case o == "time":
			c.Time = true
		case o == "price":
			c.Price = true
		case o == "primary":
			c.Primary = true
		case o == "index":
			c.Indexed = true
		case strings.HasPrefix(o, "alias="):
			al, err := splitAliases(o[len("alias="):])
			if err != nil {
				return c, err
			}

			c.Aliases = append(c.Aliases, al...)
		case strings.HasPrefix(o, "unit="):
			// Parsed by unitOf. A bad value is caught there, against the same
			// rules, rather than in a second place that could disagree.
		default:
			return c, fmt.Errorf("unknown tag option %q", o)
		}
	}

	// A tagged field with no name gets the Go field name, because a tag that
	// carries only options is the common way to mark the timestamp, and forcing
	// every author to restate the field name to say `time` is friction with no
	// benefit.
	if c.Name == "" {
		c.Name = f.Name
	}

	return c, nil
}

// isOption reports whether a comma-separated tag part is an option rather than a
// column name.
func isOption(s string) bool {
	switch s {
	case "time", "price", "primary", "index":
		return true
	}

	return strings.HasPrefix(s, "alias=") || strings.HasPrefix(s, "unit=")
}

// splitAliases splits an alias= list, and refuses an empty one. `alias=` with
// nothing after it claims a name of "", and an empty column name is unspellable
// in a query, so it is an error rather than a silently ignored option.
func splitAliases(s string) ([]string, error) {
	parts := strings.Split(s, "|")

	out := make([]string, 0, len(parts))

	for _, p := range parts {
		if p == "" {
			return nil, errors.New("empty column name in alias=")
		}

		out = append(out, p)
	}

	return out, nil
}

// unitOf reads the unit= option, defaulting to milliseconds. Milliseconds is the
// default because that is what a candle carries and a candle is the common case;
// a tick feed is expected to say so, and getting it wrong is visible immediately
// as ErrOutOfOrder rather than as subtly wrong data.
func unitOf(tag string) TimeUnit {
	_, rest, found := strings.Cut(tag, "unit=")
	if !found {
		return UnitMilli
	}

	if end := strings.IndexByte(rest, ','); end >= 0 {
		rest = rest[:end]
	}

	u, ok := ParseUnit(rest)
	if !ok {
		return UnitUnknown
	}

	return u
}

// KindOf maps a Go type to a column kind and, for a byte column, its width.
//
// It is exported because cmd/gen-schema-record has to decide the same thing from
// a parsed source file, where no reflect.Type exists yet, and two programs that
// each decide which Go types are columns is two programs that will disagree.
// The generator's test asserts its table against this function for every type it
// handles, which turns that disagreement into a failing test rather than a
// schema that queries the wrong bytes.
//
// The refusals are the interesting part, and each one names the way to store
// what it refused rather than only saying no:
//
//   - A variable-width string or slice cannot be a column at all, because the
//     format addresses record i at byte offset i*width and a string's width is
//     not known until runtime. The two real answers are a fixed [N]byte field,
//     which is what kindOf accepts for byte columns, and putting the dimension
//     in the shard key, which is where a symbol or a venue belongs and costs
//     nothing per record.
//   - A pointer, slice, map, channel, func or interface is refused because a
//     record is copied in and out of the mapping by value, and any of them would
//     outlive both.
//   - time.Time is refused because it is 24 bytes holding a *time.Location, and
//     a record that stores a pointer stores a pointer into a timezone database
//     that may not be loaded when it is read back. An int64 tagged time with the
//     unit= option is the same information and is storable.
//
// A nilable type is not a column for the same reason, and without this check
// KindOf would fall through to its final return and report a *T as "a column is
// an integer" rather than as a pointer.
func KindOf(t reflect.Type) (Kind, int, error) {
	if t == nil {
		return 0, 0, errors.New("has no type")
	}

	switch t.Kind() {
	case reflect.Int64:
		return KindInt64, 0, nil
	case reflect.Uint64:
		return KindUint64, 0, nil
	case reflect.Int32:
		return KindInt32, 0, nil
	case reflect.Uint32:
		return KindUint32, 0, nil
	case reflect.Int16:
		return KindInt16, 0, nil
	case reflect.Uint16:
		return KindUint16, 0, nil
	case reflect.Int8:
		return KindInt8, 0, nil
	case reflect.Uint8:
		return KindUint8, 0, nil
	case reflect.Float64:
		return KindFloat64, 0, nil
	case reflect.Float32:
		return KindFloat32, 0, nil
	case reflect.Bool:
		return KindBool, 0, nil
	case reflect.Array:
		// A byte array is the one fixed-width way to hold text, and it is how a
		// venue code or an instrument symbol belongs in a record. Its length is
		// the width, and the schema keeps it so a compare does not have to ask
		// reflect again.
		if t.Elem().Kind() == reflect.Uint8 {
			return KindBytes, t.Len(), nil
		}

		return 0, 0, fmt.Errorf("is %s, a column array is [N]byte", t)
	case reflect.String:
		return 0, 0, errors.New("is a string, whose width is not known until runtime: " +
			"use [N]byte, or put the value in the shard key")
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return 0, 0, errors.New("is a []byte, whose width is not known until runtime: " +
				"use [N]byte, or put the value in the shard key")
		}

		return 0, 0, fmt.Errorf("is a %s, a column is a scalar or [N]byte", t)
	case reflect.Pointer:
		return 0, 0, fmt.Errorf("is a %s, a record is copied by value and cannot hold a pointer", t)
	case reflect.Map:
		return 0, 0, fmt.Errorf("is a %s, which has no fixed width", t)
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer:
		return 0, 0, fmt.Errorf("is a %s, which a record cannot hold", t)
	case reflect.Struct:
		if t.Name() == "Time" && t.PkgPath() == "time" {
			return 0, 0, errors.New("is a time.Time, which holds a *time.Location: " +
				"use an int64 tagged time with unit=")
		}

		return 0, 0, fmt.Errorf("is a %s struct, a column is a scalar or [N]byte", t)
	}

	return 0, 0, fmt.Errorf("is a %s, a column is an integer, a float, a bool or [N]byte", t)
}

// ParseUnit resolves a unit name, so a caller configuring a store from a config
// file does not have to spell the constant.
func ParseUnit(s string) (TimeUnit, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "ms", "milli", "millisecond", "milliseconds":
		return UnitMilli, true
	case "us", "micro", "microsecond", "microseconds":
		return UnitMicro, true
	case "ns", "nano", "nanosecond", "nanoseconds":
		return UnitNano, true
	}

	return UnitUnknown, false
}

// StringUint renders an unsigned value, which exists so a caller formatting a
// query or a log line does not reach for strconv with a cast it gets wrong.
func StringUint(v uint64) string { return strconv.FormatUint(v, 10) }
