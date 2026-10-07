package record_test

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/aldok10/zdb/record"
)

// innerType is a struct a column cannot be, used by the refusal table.
type innerType struct {
	A int64
}

// schemaErr asks SchemaOf about a type the caller cannot name generically,
// which is the situation a table-driven rejection test is in: the interesting
// types are anonymous structs declared inline, and a []any cannot be made
// generic without reflect at the call site.
func schemaErr[T any]() error {
	_, err := record.SchemaOf[T]()

	return err
}

// The record types here are deliberately unlike record.Bar and record.Tick. A
// schema test that ran against a shipped record would pass for the wrong reason:
// those two already have every field a query might want, so it would never
// exercise a field being skipped, a name being claimed twice, or a kind that the
// format cannot compare.

type taggedQuote struct {
	Datetime  int64   `zdb:"datetime,time,primary,unit=us"`
	Bid       float64 `zdb:"bid,price,index"`
	Ask       float64 `zdb:"ask,price,index"`
	Condition uint8   // no tag: not a column, so not queryable
	Feed      string  // no tag, and a type a column cannot be
	Size      uint64  `zdb:"size,index,alias=qty|volume"`
}

func TestSchemaOfBuildsFromTags(t *testing.T) {
	s, err := record.SchemaOf[taggedQuote]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	if got, want := s.Len(), 4; got != want {
		t.Errorf("columns = %d, want %d (%s)", got, want, s)
	}

	// Declaration order, not sorted order. A schema's String is what a caller
	// reads to learn what it may query, and a timestamp that has moved to the
	// bottom of that list is a cost paid every time somebody reads an error.
	want := []string{"datetime", "bid", "ask", "size"}
	for i, w := range want {
		if got := s.Columns[i].Name; got != w {
			t.Errorf("column %d = %q, want %q", i, got, w)
		}
	}
}

func TestSchemaOfResolvesTimeColumnAndUnit(t *testing.T) {
	s := record.MustSchemaOf[taggedQuote]()

	i := s.TimeIndex()
	if i != 0 {
		t.Fatalf("TimeIndex = %d, want 0", i)
	}

	if got, want := s.Time().Name, "datetime"; got != want {
		t.Errorf("Time = %q, want %q", got, want)
	}

	// The unit is a schema property because a caller passing bounds to a query
	// has to know whether it is writing milliseconds or microseconds, and getting
	// that wrong looks like an empty result rather than an error.
	if got, want := s.Unit, record.UnitMicro; got != want {
		t.Errorf("Unit = %v, want %v", got, want)
	}
}

func TestSchemaOfDefaultsToMilliseconds(t *testing.T) {
	s := record.MustSchemaOf[milliBarLike]()

	if got, want := s.Unit, record.UnitMilli; got != want {
		t.Errorf("Unit = %v, want %v (%s)", got, want, s)
	}
}

func TestSchemaOfHonoursAliases(t *testing.T) {
	s := record.MustSchemaOf[taggedQuote]()

	for _, spelling := range []string{"size", "SIZE", "qty", "volume"} {
		c, i, ok := s.Field(spelling)
		if !ok {
			t.Fatalf("Field(%q) not found", spelling)
		}

		if c.Name != "size" || i != 3 {
			t.Errorf("Field(%q) = %s at %d, want size at 3", spelling, c.Name, i)
		}
	}

	if _, _, ok := s.Field("Condition"); ok {
		t.Error("an untagged field resolved as a column")
	}

	if _, _, ok := s.Field("Feed"); ok {
		t.Error("an untagged string field resolved as a column")
	}
}

func TestSchemaFieldRejectsUnknown(t *testing.T) {
	s := record.MustSchemaOf[taggedQuote]()

	if _, _, ok := s.Field("nope"); ok {
		t.Error("Field(nope) resolved")
	}

	if _, err := s.Fields([]string{"bid", "nope"}); !errors.Is(err, record.ErrSchema) {
		t.Errorf("Fields error = %v, want ErrSchema", err)
	} else if !strings.Contains(err.Error(), "bid") {
		// The error has to name what the caller may write, or it is a guess.
		t.Errorf("error does not list the columns: %v", err)
	}
}

func TestColumnLoadReadsEveryKind(t *testing.T) {
	type mixed struct {
		At  int64   `zdb:"at,time"`
		U64 uint64  `zdb:"u64"`
		I32 int32   `zdb:"i32"`
		U32 uint32  `zdb:"u32"`
		F64 float64 `zdb:"f64,price"`
	}

	s := record.MustSchemaOf[mixed]()

	q := mixed{At: -7, U64: 1 << 40, I32: -3, U32: 9, F64: 2.5}
	p := unsafe.Pointer(&q)

	for _, tc := range []struct {
		col  string
		want record.Value
	}{
		{"at", record.Value{Int: -7}},
		{"u64", record.Value{Uint: 1 << 40}},
		{"i32", record.Value{Int: -3}},
		{"u32", record.Value{Uint: 9}},
		{"f64", record.Value{Float: 2.5, IsFloat: true}},
	} {
		c, _, ok := s.Field(tc.col)
		if !ok {
			t.Fatalf("no column %q", tc.col)
		}

		if got := c.Load(p); got != tc.want {
			t.Errorf("Load(%s) = %+v, want %+v", tc.col, got, tc.want)
		}
	}
}

// TestColumnLoadMatchesReflection is the one that holds Load honest. Load is an
// unsafe.Add and a typed dereference, so a wrong Offset produces plausible wrong
// numbers rather than a fault. Comparing every column against reflect on a real
// value is what turns "plausible wrong" into a failing test.
//
// It walks reflect's fields and resolves each tagged one through the schema,
// rather than walking the schema and looking up a field name, because the
// mapping between a column name and a Go field is exactly what the schema
// guessed and what this test has to check.
func TestColumnLoadMatchesReflection(t *testing.T) {
	s := record.MustSchemaOf[taggedQuote]()

	q := taggedQuote{
		Datetime:  1_700_000_000_000_000,
		Bid:       101.25,
		Ask:       101.5,
		Condition: 7,
		Feed:      "binance",
		Size:      4096,
	}

	rv := reflect.ValueOf(q)
	rt := rv.Type()

	checked := 0

	for i := range rt.NumField() {
		tag, tagged := rt.Field(i).Tag.Lookup(record.TagKey)
		if !tagged {
			continue
		}

		col, _, ok := s.Field(strings.Split(tag, ",")[0])
		if !ok {
			t.Fatalf("tagged field %s has no column", rt.Field(i).Name)
		}

		checked++

		got := col.Load(unsafe.Pointer(&q))
		rf := rv.Field(i)

		switch col.Kind {
		case record.KindFloat64:
			if got.Float != rf.Float() || !got.IsFloat {
				t.Errorf("%s: Load = %v (IsFloat=%v), reflect = %v",
					col.Name, got.Float, got.IsFloat, rf.Float())
			}
		case record.KindUint64, record.KindUint32:
			if got.Uint != rf.Uint() || got.IsFloat {
				t.Errorf("%s: Load = %+v, reflect = %d", col.Name, got, rf.Uint())
			}
		default:
			if got.Int != rf.Int() || got.IsFloat {
				t.Errorf("%s: Load = %+v, reflect = %d", col.Name, got, rf.Int())
			}
		}
	}

	if checked != s.Len() {
		t.Errorf("checked %d columns, schema has %d", checked, s.Len())
	}
}

// TestSchemaAcceptsEveryFixedWidthKind is the table that keeps KindOf honest as
// kinds are added. It names each Go type the format can compare and asserts both
// directions: the right Kind, and a Load that returns the value the record was
// built with. A new kind added to the enum without a case here fails, which is
// the point of the table rather than four hand-written assertions.
func TestSchemaAcceptsEveryFixedWidthKind(t *testing.T) {
	type wide struct {
		I8   int8    `zdb:"i8"`
		I16  int16   `zdb:"i16"`
		I32  int32   `zdb:"i32"`
		I64  int64   `zdb:"at,time"`
		U8   uint8   `zdb:"u8"`
		U16  uint16  `zdb:"u16"`
		U32  uint32  `zdb:"u32"`
		U64  uint64  `zdb:"u64"`
		F32  float32 `zdb:"f32"`
		F64  float64 `zdb:"f64,price"`
		Flag bool    `zdb:"flag"`
		Code [6]byte `zdb:"code"`
	}

	s := record.MustSchemaOf[wide]()

	w := wide{
		I8:   -8,
		I16:  -1600,
		I32:  -320000,
		I64:  -64000000,
		U8:   8,
		U16:  1600,
		U32:  320000,
		U64:  64000000,
		F32:  1.5,
		F64:  2.25,
		Flag: true,
		Code: [6]byte{'b', 'i', 'n', 'a', 'n', 'c'},
	}

	p := unsafe.Pointer(&w)

	for _, tc := range []struct {
		col  string
		kind record.Kind
		want record.Value
	}{
		{"i8", record.KindInt8, record.Value{Int: -8}},
		{"i16", record.KindInt16, record.Value{Int: -1600}},
		{"i32", record.KindInt32, record.Value{Int: -320000}},
		{"at", record.KindInt64, record.Value{Int: -64000000}},
		{"u8", record.KindUint8, record.Value{Uint: 8}},
		{"u16", record.KindUint16, record.Value{Uint: 1600}},
		{"u32", record.KindUint32, record.Value{Uint: 320000}},
		{"u64", record.KindUint64, record.Value{Uint: 64000000}},
		{"f32", record.KindFloat32, record.Value{Float: 1.5, IsFloat: true}},
		{"f64", record.KindFloat64, record.Value{Float: 2.25, IsFloat: true}},
		{"flag", record.KindBool, record.Value{Uint: 1}},
	} {
		c, _, ok := s.Field(tc.col)
		if !ok {
			t.Fatalf("no column %q", tc.col)
		}

		if c.Kind != tc.kind {
			t.Errorf("%s: Kind = %v, want %v", tc.col, c.Kind, tc.kind)
		}

		if got := c.Load(p); got != tc.want {
			t.Errorf("Load(%s) = %+v, want %+v", tc.col, got, tc.want)
		}
	}
}

// TestSchemaByteColumnCompares checks the width is carried and the compare is
// lexicographic, which is the order a caller comparing padded codes expects: a
// prefix sorts first.
func TestSchemaByteColumnCompares(t *testing.T) {
	type code struct {
		At    int64   `zdb:"at,time"`
		Venue [8]byte `zdb:"venue"`
	}

	s := record.MustSchemaOf[code]()

	c, _, ok := s.Field("venue")
	if !ok {
		t.Fatal("no venue column")
	}

	if c.Kind != record.KindBytes {
		t.Fatalf("Kind = %v, want KindBytes", c.Kind)
	}

	if c.Width != 8 {
		t.Errorf("Width = %d, want 8", c.Width)
	}

	// Compare reads Width bytes out of the struct, so a width that disagreed with
	// the type would read the next field. The comparison itself has to be against
	// two live values, since Compare takes two pointers.
	pad := func(s string) [8]byte {
		var b [8]byte

		copy(b[:], s)

		return b
	}

	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"binance", "binance", 0},
		{"binance", "bybit", -1},
		{"bybit", "binance", 1},
		{"bin", "binance", -1}, // a shorter prefix sorts first
		{"", "binance", -1},
	} {
		x := code{At: 1, Venue: pad(tc.a)}
		y := code{At: 1, Venue: pad(tc.b)}

		if got := c.Compare(unsafe.Pointer(&x), unsafe.Pointer(&y)); got != tc.want {
			t.Errorf("Compare(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// TestCompareOrdersEveryNumericKind is the compare path a filter and a min/max
// share, so it is checked directly rather than only through Load. The values are
// chosen to straddle zero, because a signedness bug in a narrow kind shows up
// only when the value is negative.
func TestCompareOrdersEveryNumericKind(t *testing.T) {
	type nums struct {
		I8  int8    `zdb:"i8"`
		U8  uint8   `zdb:"u8"`
		I16 int16   `zdb:"i16"`
		U16 uint16  `zdb:"u16"`
		I32 int32   `zdb:"i32"`
		U32 uint32  `zdb:"u32"`
		I64 int64   `zdb:"at,time"`
		U64 uint64  `zdb:"u64"`
		F32 float32 `zdb:"f32"`
		F64 float64 `zdb:"f64,price"`
		No  float64 `zdb:"no,price"`
		Fl  bool    `zdb:"fl"`
	}

	s := record.MustSchemaOf[nums]()

	low := nums{
		I8: -1, U8: 1, I16: -1, U16: 1, I32: -1, U32: 1, I64: -1, U64: 1,
		F32: -1, F64: -1, No: -1, Fl: false,
	}
	high := nums{
		I8: 1, U8: 2, I16: 1, U16: 2, I32: 1, U32: 2, I64: 1, U64: 2,
		F32: 1, F64: 1, No: 1, Fl: true,
	}

	p := func(n *nums) unsafe.Pointer { return unsafe.Pointer(n) }

	for _, name := range []string{"i8", "u8", "i16", "u16", "i32", "u32", "at", "u64", "f32", "f64", "no", "fl"} {
		c, _, ok := s.Field(name)
		if !ok {
			t.Fatalf("no column %q", name)
		}

		if got := c.Compare(p(&low), p(&high)); got != -1 {
			t.Errorf("%s: Compare(low, high) = %d, want -1", name, got)
		}

		if got := c.Compare(p(&high), p(&low)); got != 1 {
			t.Errorf("%s: Compare(high, low) = %d, want 1", name, got)
		}

		if got := c.Compare(p(&low), p(&low)); got != 0 {
			t.Errorf("%s: Compare(low, low) = %d, want 0", name, got)
		}

		if !c.Equal(p(&low), p(&low)) {
			t.Errorf("%s: Equal(low, low) = false", name)
		}
	}
}

// TestCompareOrdersNaNLast pins the NaN rule. A filter that wrote `a > b` instead
// of Compare would return false for both orderings against a NaN, which reads as
// "this column never matches" rather than as "this value is not a number".
func TestCompareOrdersNaNLast(t *testing.T) {
	type f struct {
		At  int64   `zdb:"at,time"`
		Val float64 `zdb:"val,price"`
	}

	s := record.MustSchemaOf[f]()
	c := s.Columns[1]

	num := f{At: 1, Val: 5}
	nan := f{At: 1, Val: math.NaN()}

	p := func(v *f) unsafe.Pointer { return unsafe.Pointer(v) }

	if got := c.Compare(p(&nan), p(&num)); got != 1 {
		t.Errorf("Compare(NaN, 5) = %d, want 1 (NaN last)", got)
	}

	if got := c.Compare(p(&num), p(&nan)); got != -1 {
		t.Errorf("Compare(5, NaN) = %d, want -1", got)
	}
}

func TestSchemaRejectsVariableWidthAndPointerTypes(t *testing.T) {
	// The refusals that name a way forward, because each of these is a thing a
	// caller reaches for first and a bare "unsupported type" sends them looking.
	for _, tc := range []struct {
		name string
		give func() error
		want []string
	}{
		{
			name: "string",
			give: schemaErr[struct {
				At     int64  `zdb:"at,time"`
				Symbol string `zdb:"symbol"`
			}],
			want: []string{"[N]byte", "shard key"},
		},
		{
			name: "byte slice",
			give: schemaErr[struct {
				At     int64  `zdb:"at,time"`
				Symbol []byte `zdb:"symbol"`
			}],
			want: []string{"[N]byte", "shard key"},
		},
		{
			name: "time.Time",
			give: schemaErr[struct {
				At time.Time `zdb:"at,time"`
			}],
			want: []string{"time.Time", "unit="},
		},
		{
			name: "map",
			give: schemaErr[struct {
				At int64          `zdb:"at,time"`
				M  map[string]int `zdb:"m"`
			}],
			want: []string{"no fixed width"},
		},
		{
			name: "int array",
			give: schemaErr[struct {
				At int64    `zdb:"at,time"`
				A  [4]int64 `zdb:"a"`
			}],
			want: []string{"[N]byte"},
		},
		{
			name: "nested struct",
			give: schemaErr[struct {
				At int64     `zdb:"at,time"`
				S  innerType `zdb:"s"`
			}],
			want: []string{"scalar or [N]byte"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.give()
			if err == nil {
				t.Fatal("SchemaOf accepted the type")
			}

			if !errors.Is(err, record.ErrSchema) {
				t.Errorf("error = %v, want ErrSchema", err)
			}

			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %q, want it to mention %q", err, want)
				}
			}
		})
	}
}

func TestSchemaOfIsCached(t *testing.T) {
	a, err := record.SchemaOf[taggedQuote]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	b, err := record.SchemaOf[taggedQuote]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	// Same pointer, so Open and every query plan can resolve the schema without
	// each of them paying for the reflection.
	if a != b {
		t.Error("SchemaOf built two schemas for one type")
	}
}

func TestSchemaRejectsBadStructs(t *testing.T) {
	for _, tc := range []struct {
		name string
		give func() error
		want string
	}{
		{
			name: "no time column",
			give: schemaErr[struct {
				Close float64 `zdb:"close,price"`
			}],
			want: "no time column",
		},
		{
			name: "two time columns",
			give: schemaErr[struct {
				A int64 `zdb:"a,time"`
				B int64 `zdb:"b,time"`
			}],
			want: "two time columns",
		},
		{
			name: "float time column",
			give: schemaErr[struct {
				A float64 `zdb:"a,time"`
			}],
			want: "a time column is an integer",
		},
		{
			name: "duplicate column name",
			give: schemaErr[struct {
				A int64   `zdb:"ts,time"`
				B float64 `zdb:"ts,price"`
			}],
			want: "already claimed",
		},
		{
			// ts was an option word in an earlier version, so a column tagged
			// `ts,time` was silently renamed to the Go field name and this
			// collision did not happen. The tag's first part and its option
			// words share one namespace, and a synonym widens it quietly, so the
			// case stays: it is what proves ts is now a name.
			name: "alias collides with a column named ts",
			give: schemaErr[struct {
				A int64   `zdb:"ts,time"`
				B float64 `zdb:"px,price,alias=ts"`
			}],
			want: "already claimed",
		},
		{
			name: "alias collides with a column",
			give: schemaErr[struct {
				A int64   `zdb:"at,time"`
				B float64 `zdb:"px,price,alias=at"`
			}],
			want: "already claimed",
		},
		{
			name: "price on an integer",
			give: schemaErr[struct {
				A int64  `zdb:"ts,time"`
				B uint64 `zdb:"n,price"`
			}],
			want: "price applies to a float64 column",
		},
		{
			// A float32 carries about seven significant digits, so scaling it to
			// a 1e8 fixed-point integer throws away more precision than the
			// value ever had. Storing the float and comparing it as a float is
			// the honest version of what price would have meant here.
			name: "price on a float32",
			give: schemaErr[struct {
				A int64   `zdb:"ts,time"`
				B float32 `zdb:"px,price"`
			}],
			want: "price applies to a float64 column",
		},
		{
			name: "index on the time column",
			give: schemaErr[struct {
				A int64 `zdb:"ts,time,index"`
			}],
			want: "indexed by definition",
		},
		{
			name: "unknown option",
			give: schemaErr[struct {
				A int64 `zdb:"ts,time,bogus"`
			}],
			want: "unknown tag option",
		},
		{
			name: "unspellable name",
			give: schemaErr[struct {
				A int64   `zdb:"ts,time"`
				B float64 `zdb:"with space,price"`
			}],
			want: "may not contain",
		},
		{
			name: "empty alias list",
			give: schemaErr[struct {
				A int64   `zdb:"ts,time"`
				B float64 `zdb:"px,price,alias="`
			}],
			want: "empty column name",
		},
		{
			name: "not a struct",
			give: schemaErr[int64],
			want: "a record is a struct",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.give()
			if err == nil {
				t.Fatal("SchemaOf accepted the type")
			}

			if !errors.Is(err, record.ErrSchema) {
				t.Errorf("error = %v, want ErrSchema", err)
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestSchemaOfUnexportedFieldsAreSkipped(t *testing.T) {
	type withPrivate struct {
		At int64 `zdb:"at,time"`
		// secret carries no tag, which is the only thing that makes it not a
		// column. Written unexported so a future change that ignored
		// IsExported would build a column Load cannot read.
		secret uint64 //nolint:unused // the point is that it is never a column
	}

	s, err := record.SchemaOf[withPrivate]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	// An unexported field has no tag by construction, so it cannot be a column.
	// Naming it would build a column Load cannot read through a caller's pointer.
	if got := s.Len(); got != 1 {
		t.Errorf("columns = %d, want 1 (%s)", got, s)
	}
}

func TestSchemaStringListsColumnsAndPrices(t *testing.T) {
	s := record.MustSchemaOf[taggedQuote]()

	got := s.String()
	for _, want := range []string{"taggedQuote", "datetime", "bid:float64/price", "size:uint64"} {
		if !strings.Contains(got, want) {
			t.Errorf("String() = %q, want it to contain %q", got, want)
		}
	}

	if strings.Contains(got, "Condition") {
		t.Error("String() listed an untagged field")
	}
}

func TestParseUnit(t *testing.T) {
	for _, tc := range []struct {
		give string
		want record.TimeUnit
		ok   bool
	}{
		{"ms", record.UnitMilli, true},
		{"microseconds", record.UnitMicro, true},
		{"NS", record.UnitNano, true},
		{"  us  ", record.UnitMicro, true},
		{"fortnight", record.UnitUnknown, false},
		{"", record.UnitUnknown, false},
	} {
		got, ok := record.ParseUnit(tc.give)
		if ok != tc.ok || got != tc.want {
			t.Errorf("ParseUnit(%q) = %v, %v, want %v, %v",
				tc.give, got, ok, tc.want, tc.ok)
		}
	}
}

func TestScalePriceMatchesThePrivateRule(t *testing.T) {
	// The exported seam exists so the query package does not reimplement the
	// rounding rule. This holds it to the rule it wraps.
	got, err := record.ScalePrice(100.5)
	if err != nil {
		t.Fatalf("ScalePrice: %v", err)
	}

	if got != 10_050_000_000 {
		t.Errorf("ScalePrice(100.5) = %d, want 10050000000", got)
	}

	if _, err := record.ScalePrice(-1); err == nil {
		t.Error("ScalePrice(-1) accepted a negative price")
	}
}

// milliBarLike has no unit=, so it exercises the default rather than the option.
type milliBarLike struct {
	Datetime int64   `zdb:"datetime,time"`
	Close    float64 `zdb:"close,price"`
}

// benchRow is the shape the benchmark filters over. It carries one column of
// every kind the compare switch has a case for, so the benchmark measures the
// widest dispatch rather than the cheapest kind: a benchmark that loaded one
// float64 column would report a number that says nothing about a query over a
// record with a mix.
type benchRow struct {
	Datetime int64   `zdb:"datetime,time"`
	Open     float64 `zdb:"open,price"`
	Size     uint32  `zdb:"size"`
	Delta    int16   `zdb:"delta"`
	Flag     bool    `zdb:"flag"`
	Venue    [8]byte `zdb:"venue"`
}

// benchRows is the working set, built once so the benchmark measures the filter
// rather than the construction. 1024 rows is about 64 KiB, so it exceeds L1 and
// the benchmark is reading memory rather than reusing a hot line.
func benchRows() []benchRow {
	rows := make([]benchRow, 1024)

	for i := range rows {
		rows[i] = benchRow{
			Datetime: int64(i) * 1000,
			Open:     float64(i%512) + 0.25,
			Size:     uint32(i % 97),
			Delta:    int16(i%7 - 3),
			Flag:     i%2 == 0,
			Venue:    [8]byte{'b', 'i', 'n', byte('a' + i%4)},
		}
	}

	return rows
}

// benchSchema is resolved once. A benchmark that called SchemaOf per iteration
// would be measuring the cache lookup, which is the point of §6.1: the claim
// under test is that the per-record cost is zero, and SchemaOf's cost belongs to
// Open, not to a scan.
var (
	benchRowsData   = benchRows()
	benchSchemaData = record.MustSchemaOf[benchRow]()
)

func benchColumn(name string) record.Column {
	c, _, ok := benchSchemaData.Field(name)
	if !ok {
		panic("no column " + name)
	}

	return c
}

// TestSchemaComparePathIsZeroAllocation is the assertion the benchmark measures,
// because a benchmark's allocs/op column is only checked by hand and a regression
// there would otherwise be noticed a release later.
//
// The loop has to be one the compiler cannot elide, so the sink is a package
// variable rather than a local: a local whose value is never read is dead, and a
// dead loop runs zero times and reports zero allocations for the wrong reason.
var benchSink int

func TestSchemaComparePathIsZeroAllocation(t *testing.T) {
	rows := benchRowsData
	c := benchColumn("open")

	got := testing.AllocsPerRun(200, func() {
		n := 0

		for i := range rows {
			// Two distinct rows, not one against itself: comparing a record with
			// itself short-circuits on the first byte for a byte column and the
			// first branch for a numeric one, so a self-comparison would report the
			// cost of the dispatch and not the cost of the compare.
			if c.Compare(unsafe.Pointer(&rows[i]), unsafe.Pointer(&rows[(i+1)%len(rows)])) > 0 {
				n++
			}
		}

		benchSink = n
	})

	if got != 0 {
		t.Errorf("Compare over %d rows allocated %v times per run, want 0",
			len(rows), got)
	}
}

// BenchmarkColumnCompare is the measurement backing the claim that a schema-built
// compare costs what a hand-written field read costs.
//
// The two sub-benchmarks differ in exactly one thing: one loads through a Column,
// the other reads the struct field directly. The gap between them is the whole
// price of the reflection, paid once at Open and then not again per record.
func BenchmarkColumnCompare(b *testing.B) {
	rows := benchRowsData

	names := []string{"open", "size", "delta", "flag", "venue"}

	b.ReportAllocs()
	b.ResetTimer()

	for _, name := range names {
		b.Run(name, func(b *testing.B) {
			c := benchColumn(name)

			p := unsafe.Pointer(&rows[0])
			q := unsafe.Pointer(&rows[1])

			var n int

			b.ReportAllocs()

			for b.Loop() {
				if c.Compare(p, q) != 0 {
					n++
				}
			}

			benchSink = n
		})
	}
}

// BenchmarkColumnCompareDirect is the other half of the comparison: the same
// work with the field address taken by the compiler rather than by reflect.
func BenchmarkColumnCompareDirect(b *testing.B) {
	rows := benchRowsData

	b.ReportAllocs()
	b.ResetTimer()

	b.Run("open", func(b *testing.B) {
		var n int

		b.ReportAllocs()

		for b.Loop() {
			if cmpF64(rows[0].Open, rows[1].Open) != 0 {
				n++
			}
		}

		benchSink = n
	})

	b.Run("venue", func(b *testing.B) {
		var n int

		b.ReportAllocs()

		// rows[0] venue sorts before rows[1]'s, so this walks the whole
		// comparison rather than stopping on the first byte. It is the same call
		// Column.CompareBytes makes, which is the whole point of the pair.
		for b.Loop() {
			if bytes.Compare(rows[0].Venue[:], rows[1].Venue[:]) < 0 {
				n++
			}
		}

		benchSink = n
	})
}

func cmpF64(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}
