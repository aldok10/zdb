// Package zql queries a ZDB store: a typed builder for composing a query in Go,
// a parser for composing the same query as text, and a runner that streams the
// result out of a mapping without building it first.
//
// The layering is one-way: zql imports reader, reader imports zdb, and nothing
// imports zql. The storage format knows nothing about queries, so a format change
// never has to keep this in step with it.
package zql

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/aldok10/zdb/record"
)

// A query is a range on one key plus filters. There is no
// secondary index, so a condition on anything but Datetime is a sequential test
// over the window. That is a property of the format, not of this file, and it is
// why a Datetime condition is worth as much as the window itself: it is folded
// into the binary search, so `datetime >= x` costs O(log n) and everything else
// costs O(matches examined).

// Field names a bar field. The order matches the record's byte order.
type Field uint8

// FieldDatetime is the timestamp. The rest follow it in record order.
const (
	FieldDatetime Field = iota
	FieldOpen
	FieldHigh
	FieldLow
	FieldClose
	FieldTickVolume
	FieldSpread
	FieldVolume
)

// String returns the lowercase ZQL spelling of f.
func (f Field) String() string {
	switch f {
	case FieldDatetime:
		return "datetime"
	case FieldOpen:
		return "open"
	case FieldHigh:
		return "high"
	case FieldLow:
		return "low"
	case FieldClose:
		return "close"
	case FieldTickVolume:
		return "tick_volume"
	case FieldSpread:
		return "spread"
	case FieldVolume:
		return "volume"
	}

	return "?"
}

// FieldOf resolves a ZQL field name. It is the one place names are spelled, so
// the parser and the builder cannot drift apart.
func FieldOf(name string) (Field, bool) {
	switch strings.ToLower(name) {
	case "datetime", "time", "ts":
		return FieldDatetime, true
	case "open":
		return FieldOpen, true
	case "high":
		return FieldHigh, true
	case "low":
		return FieldLow, true
	case "close":
		return FieldClose, true
	case "tick_volume", "tickvolume":
		return FieldTickVolume, true
	case "spread":
		return FieldSpread, true
	case "volume":
		return FieldVolume, true
	}

	return 0, false
}

// FieldMask is a set of bar fields.
type FieldMask uint16

// FDatetime selects the timestamp. The rest mirror the Field order.
const (
	FDatetime FieldMask = 1 << iota
	FOpen
	FHigh
	FLow
	FClose
	FTickVolume
	FSpread
	FVolume
)

// FAll is every field, and the default when a query names no projection.
const FAll = FDatetime | FOpen | FHigh | FLow | FClose | FTickVolume | FSpread | FVolume

// mask clears the fields a query did not select. The record is still read whole
// because the mapping is the record; a projection only stops the caller from
// depending on a field it never asked for.
func mask(b record.Bar, m FieldMask) record.Bar {
	if m == FAll {
		return b
	}

	if m&FDatetime == 0 {
		b.Datetime = 0
	}

	if m&FOpen == 0 {
		b.Open = 0
	}

	if m&FHigh == 0 {
		b.High = 0
	}

	if m&FLow == 0 {
		b.Low = 0
	}

	if m&FClose == 0 {
		b.Close = 0
	}

	if m&FTickVolume == 0 {
		b.TickVolume = 0
	}

	if m&FSpread == 0 {
		b.Spread = 0
	}

	if m&FVolume == 0 {
		b.Volume = 0
	}

	return b
}

// Op is a comparison.
type Op uint8

// Eq is the equality comparison. The rest are the usual ordering operators.
const (
	Eq Op = iota
	Ne
	Lt
	Lte
	Gt
	Gte
)

// String returns the ZQL spelling of op.
func (o Op) String() string {
	switch o {
	case Eq:
		return "=="
	case Ne:
		return "!="
	case Lt:
		return "<"
	case Lte:
		return "<="
	case Gt:
		return ">"
	case Gte:
		return ">="
	}

	return "?"
}

// OpOf resolves a ZQL comparison spelling. `=` is accepted as a synonym for `==`
// because a query written by hand almost always types it.
func OpOf(s string) (Op, bool) {
	switch s {
	case "==", "=":
		return Eq, true
	case "!=", "<>":
		return Ne, true
	case "<":
		return Lt, true
	case "<=":
		return Lte, true
	case ">":
		return Gt, true
	case ">=":
		return Gte, true
	}

	return 0, false
}

// Cond is one comparison against a stored field.
//
// Val is in the stored form: prices already carry PriceScale, so every
// comparison is an integer compare and a filter never puts a float on the scan
// path. The zero Cond is false, which makes an empty condition list a no-op
// rather than a special case at every call site.
type Cond struct {
	Field Field
	Op    Op
	Val   uint64
}

// maxExact is the largest integer a float64 holds without losing a unit. A
// condition beyond it would silently compare a rounded value, so it is refused
// rather than approximated.
const maxExact = 1 << 53

// NewCond builds a condition. v is interpreted by the field: a price is scaled by
// PriceScale, so the caller passes 100.5 and the condition stores 10050000000.
// Datetime is unix milliseconds, which is exact at 1.7e12, well under maxExact.
func NewCond(f Field, op Op, v float64) (Cond, error) {
	if f > FieldVolume {
		return Cond{}, fmt.Errorf("zql: unknown field %d", f)
	}

	if op > Gte {
		return Cond{}, fmt.Errorf("zql: unknown operator %d", op)
	}

	switch f {
	case FieldOpen, FieldHigh, FieldLow, FieldClose:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return Cond{}, errors.New("zql: non-finite price in condition")
		}

		if v < 0 {
			return Cond{}, errors.New("zql: negative price in condition")
		}

		return Cond{Field: f, Op: op, Val: uint64(math.Round(v * record.PriceScale))}, nil
	}

	if v < 0 {
		return Cond{}, errors.New("zql: negative value in condition")
	}

	if v >= maxExact {
		return Cond{}, fmt.Errorf("zql: condition value %v exceeds the exactly comparable range", v)
	}

	return Cond{Field: f, Op: op, Val: uint64(v)}, nil
}

// test evaluates c against a stored value.
func (c Cond) test(v uint64) bool {
	switch c.Op {
	case Eq:
		return v == c.Val
	case Ne:
		return v != c.Val
	case Lt:
		return v < c.Val
	case Lte:
		return v <= c.Val
	case Gt:
		return v > c.Val
	default:
		return v >= c.Val
	}
}

// stored is a bar's field f in the form a condition compares against.
func stored(b record.Bar, f Field) uint64 {
	switch f {
	case FieldDatetime:
		return uint64(b.Datetime)
	case FieldOpen:
		return uint64(b.Open * record.PriceScale)
	case FieldHigh:
		return uint64(b.High * record.PriceScale)
	case FieldLow:
		return uint64(b.Low * record.PriceScale)
	case FieldClose:
		return uint64(b.Close * record.PriceScale)
	case FieldTickVolume:
		return b.TickVolume
	case FieldSpread:
		// A negative spread is stored as its two's-complement bits, so widening
		// rather than converting is what makes a filter on one compare correctly.
		return uint64(uint32(b.Spread))
	case FieldVolume:
		return b.Volume
	}

	return 0
}

// Query is a range on one key, with filters, an order, a cap and a projection.
// It is a plain value with no dependency on a Reader, so it can be built, parsed
// from ZQL, cached and compared without opening a file.
type Query struct {
	// Key is the symbol path, the same string Append takes.
	Key string
	// From and To bound the window and are unix milliseconds. Zero leaves that
	// side unbounded, which is exact because no market timestamp is zero.
	From, To uint64
	// Limit caps the result. Zero or less means no cap. It counts the rows that
	// survive Conds and takes from the end the order selects: the oldest matches
	// by default, the newest under Desc. Retention, not Limit, is what takes the
	// newest bars of the window.
	Limit int
	// Desc walks newest first instead of ascending.
	Desc bool
	// Conds are applied after the window, in order. All must hold.
	Conds []Cond
	// Fields is the projection. Zero means every field.
	Fields FieldMask
}

// NewQuery starts a query for key. It sets Fields to FAll rather than leaving it
// zero, because FAll is the one value that survives a text round trip unchanged.
func NewQuery(key string) Query {
	return Query{Key: key, Fields: FAll}
}

// Between bounds the window, in unix milliseconds. Zero leaves that side
// unbounded. A Datetime condition narrows this further when the query runs.
//
// The builder methods are named apart from the fields on purpose: Go does not
// allow a method and a field to share a name, and the fields stay exported so a
// Query can also be written as a literal or compared in a test.
func (q Query) Between(from, to uint64) Query {
	q.From, q.To = from, to

	return q
}

// NewestFirst walks newest first, and Take then takes the newest bars.
func (q Query) NewestFirst() Query {
	q.Desc = true

	return q
}

// OldestFirst walks oldest first, which is the default.
func (q Query) OldestFirst() Query {
	q.Desc = false

	return q
}

// Take caps the result at n bars. Zero or less means no cap.
func (q Query) Take(n int) Query {
	q.Limit = n

	return q
}

// Where appends a condition. It is a builder method so the error cannot be
// ignored at a call site that has one already; use NewCond and set Conds
// directly when it does not.
func (q Query) Where(f Field, op Op, v float64) (Query, error) {
	c, err := NewCond(f, op, v)
	if err != nil {
		return q, err
	}

	q.Conds = append(q.Conds, c)

	return q, nil
}

// Select narrows the projection. It does not reduce bytes read; see Bar.mask.
func (q Query) Select(m FieldMask) Query {
	q.Fields = m

	return q
}

// String renders the query back to ZQL. A query that cannot round-trip is a bug,
// so Parse(q.String()) must equal q for every query this package can build, with
// one normal form: a bare `*` parses to FAll rather than to 0, because both mean
// every field and only one of them is worth carrying.
func (q Query) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")

	if q.Fields == 0 || q.Fields == FAll {
		sb.WriteString("*")
	} else {
		for f := FieldDatetime; f <= FieldVolume; f++ {
			if q.Fields&(1<<f) != 0 {
				if sb.Len() > len("SELECT ") {
					sb.WriteString(", ")
				}

				sb.WriteString(f.String())
			}
		}
	}

	sb.WriteString(" FROM ")
	sb.WriteString(quote(q.Key))
	// The window is rendered explicitly. Folding it into WHERE datetime clauses
	// would read back as conditions rather than as a window, and a query that
	// does not survive its own text form is a query whose meaning depends on
	// how it was built.
	if q.From != 0 || q.To != 0 {
		fmt.Fprintf(&sb, " RANGE %d TO %d", q.From, q.To)
	}

	if len(q.Conds) > 0 {
		sb.WriteString(" WHERE ")

		for i, c := range q.Conds {
			if i > 0 {
				sb.WriteString(" AND ")
			}

			sb.WriteString(c.Field.String())
			sb.WriteByte(' ')
			sb.WriteString(c.Op.String())
			sb.WriteByte(' ')

			if c.Field >= FieldOpen && c.Field <= FieldClose {
				sb.WriteString(formatPrice(c.Val))
			} else {
				fmt.Fprintf(&sb, "%d", c.Val)
			}
		}
	}

	if q.Desc {
		sb.WriteString(" ORDER BY datetime DESC")
	}

	fmt.Fprintf(&sb, " LIMIT %d", q.Limit)

	return sb.String()
}

func quote(s string) string {
	if s == "" {
		return `""`
	}

	if !strings.ContainsAny(s, ` "'\`) {
		return s
	}

	return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"`
}

// formatPrice renders a stored price back at human scale, without the trailing
// zeros a %f would add. It exists so a query round-trips exactly.
func formatPrice(v uint64) string {
	whole := v / record.PriceScale

	frac := v % record.PriceScale
	if frac == 0 {
		return fmt.Sprintf("%d", whole)
	}

	digits := fmt.Sprintf("%08d", frac)
	digits = strings.TrimRight(digits, "0")

	return fmt.Sprintf("%d.%s", whole, digits)
}

// window returns the window to binary-search, with any Datetime condition folded
// in. This is the whole reason a datetime filter is cheap: it narrows the search
// instead of testing every record in the window.
//
// Eq and Ne cannot narrow a range, so they stay as per-record tests.
func (q Query) window() (from, to uint64) {
	from, to = q.From, q.To
	for _, c := range q.Conds {
		if c.Field != FieldDatetime {
			continue
		}

		switch c.Op {
		case Gt:
			// A strict lower bound of v is the inclusive lower bound v+1.
			if c.Val+1 > from {
				from = c.Val + 1
			}
		case Gte:
			if c.Val > from {
				from = c.Val
			}
		case Lt:
			if c.Val > 0 && c.Val-1 < toOrMax(to) {
				to = c.Val - 1
			}
		case Lte:
			if c.Val < toOrMax(to) {
				to = c.Val
			}
		}
	}

	return from, to
}

// toOrMax resolves the unbounded-above sentinel: a To of zero means no upper
// bound, so any condition has to beat math.MaxUint64, not zero.
func toOrMax(to uint64) uint64 {
	if to == 0 {
		return math.MaxUint64
	}

	return to
}

// scanConds returns the conditions that are left after the window absorbed the
// Datetime ones. A datetime condition that narrowed is dropped, because applying
// it again would be redundant work on every record.
func (q Query) scanConds() []Cond {
	// If nothing folded into the window, the caller's slice is already the right
	// answer. Copying it would cost an allocation on every query for nothing,
	// which is the one thing a query path must not do.
	folded := false

	for _, c := range q.Conds {
		if c.Field == FieldDatetime && c.Op >= Gt && c.Op <= Lte {
			folded = true

			break
		}
	}

	if !folded {
		return q.Conds
	}

	out := make([]Cond, 0, len(q.Conds))
	for _, c := range q.Conds {
		if c.Field == FieldDatetime && c.Op >= Gt && c.Op <= Lte {
			continue
		}

		out = append(out, c)
	}

	return out
}

// condBounds folds the non-datetime conditions into one per-column
// [low, high] range each in the same stored-value space the index records, so
// the reader can skip a whole segment whose span cannot intersect. Datetime is
// handled by the time window and is skipped; Ne contributes no range, because
// excluding a point cannot shrink the span that contains it.
func condBounds(conds []Cond) []record.ColumnBound {
	sc, err := record.SchemaOf[record.Bar]()
	if err != nil {
		return nil
	}

	var slot [FieldVolume + 1]struct {
		lo, hi uint64
		have   bool
	}
	for i := range slot {
		slot[i].hi = math.MaxUint64
	}

	for _, c := range conds {
		if c.Field == FieldDatetime {
			continue
		}

		i := int(c.Field)
		slot[i].have = true

		switch c.Op {
		case Eq:
			slot[i].lo = max(slot[i].lo, c.Val)
			slot[i].hi = min(slot[i].hi, c.Val)
		case Lt:
			if c.Val == 0 {
				slot[i].lo, slot[i].hi = 1, 0
			} else {
				slot[i].hi = min(slot[i].hi, c.Val-1)
			}
		case Lte:
			slot[i].hi = min(slot[i].hi, c.Val)
		case Gt:
			if c.Val == math.MaxUint64 {
				slot[i].lo, slot[i].hi = math.MaxUint64, 0
			} else {
				slot[i].lo = max(slot[i].lo, c.Val+1)
			}
		case Gte:
			slot[i].lo = max(slot[i].lo, c.Val)
		}
	}

	var out []record.ColumnBound

	for i := 1; i < len(slot); i++ {
		if !slot[i].have {
			continue
		}

		if slot[i].hi < slot[i].lo {
			return []record.ColumnBound{{Name: sc.Columns[i].Name, Lo: 1, Hi: 0}}
		}

		if slot[i].lo == 0 && slot[i].hi == math.MaxUint64 {
			continue
		}

		out = append(out, record.ColumnBound{Name: sc.Columns[i].Name, Lo: slot[i].lo, Hi: slot[i].hi})
	}

	return out
}

// matches reports whether b passes every scan condition. It is a method on the
// condition list so the reader can hold it once per query.
func matches(conds []Cond, b record.Bar) bool {
	for _, c := range conds {
		if !c.test(stored(b, c.Field)) {
			return false
		}
	}

	return true
}
