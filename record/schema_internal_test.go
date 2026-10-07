package record

import (
	"reflect"
	"strings"
	"testing"
)

// benchRecord is a record type wide enough to be representative: eight fields, a
// time column, a price column, and a byte array.
//
// The internal test package is used here rather than record_test because
// buildSchema is unexported and the walk it performs is exactly what the
// generator removes. Benchmarking the cached SchemaOf would report the lookup, not
// the reflection.
type benchRecord struct {
	Datetime int64   `zdb:"datetime,time"`
	Open     float64 `zdb:"open,price"`
	High     float64 `zdb:"high,price,index"`
	Low      float64 `zdb:"low,price,index"`
	Close    float64 `zdb:"close,price"`
	Volume   uint64  `zdb:"volume"`
	Spread   int32   `zdb:"spread"`
	Venue    [8]byte `zdb:"venue"`
}

var (
	benchSchemaResult *Schema[benchRecord]
	benchSchemaErr    error
)

// BenchmarkBuildSchemaReflective measures the reflective walk a type pays exactly
// once, which is the cost cmd/gen-schema-record removes.
//
// It calls buildSchema rather than SchemaOf on purpose. SchemaOf caches, so the
// second call through it would report a map load, and a benchmark that measures a
// map load under a heading about reflection is a benchmark that measures the wrong
// thing and reports it as the right number. Loopable precisely because
// buildSchema has no cache of its own, so every iteration does the full walk.
func BenchmarkBuildSchemaReflective(b *testing.B) {
	b.ReportAllocs()

	rt := reflect.TypeOf(benchRecord{})

	b.ResetTimer()

	for b.Loop() {
		benchSchemaResult, benchSchemaErr = buildSchema[benchRecord](rt)
	}

	if benchSchemaErr != nil {
		b.Fatal(benchSchemaErr)
	}
}

// TestSchemaBuildCountIsOnePerTypeType is the internal companion to the external
// reflection count test, and it exists because the external one cannot see
// buildSchema at all. It pins the exact number: one walk per type, on the first
// call, and never again.
func TestSchemaBuildCountIsOnePerTypeType(t *testing.T) {
	rt := reflect.TypeOf(benchRecord{})

	before := schemaBuilds.Load()

	if _, err := buildSchema[benchRecord](rt); err != nil {
		t.Fatalf("buildSchema: %v", err)
	}

	if got := schemaBuilds.Load() - before; got != 1 {
		t.Fatalf("buildSchema counted %d walks for one call", got)
	}

	// Calling buildSchema directly bypasses the cache — it is the only way to
	// reach the walk without SchemaOf, and SchemaOf is the only supported caller
	// of it. So the first SchemaOf below still walks, and that walk is the one it
	// caches. What is asserted here is that SchemaOf itself walks once and then
	// stops, not that a direct call primed anything.
	if _, err := SchemaOf[benchRecord](); err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	afterSchemaOf := schemaBuilds.Load()

	first, err := SchemaOf[benchRecord]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	for range 100 {
		again, err := SchemaOf[benchRecord]()
		if err != nil {
			t.Fatalf("SchemaOf: %v", err)
		}

		if again != first {
			t.Fatal("SchemaOf returned a different schema on a cached call")
		}
	}

	if got := schemaBuilds.Load() - afterSchemaOf; got != 0 {
		t.Errorf("100 cached calls ran %d walks, want 0", got)
	}

	if got := schemaBuilds.Load() - before; got != 2 {
		t.Errorf("%d walks total, want 2: one direct buildSchema and one SchemaOf miss",
			got)
	}
}

// TestNewSchemaAgreesWithBuildSchema is the internal check that the constructor
// the generator calls and the reflection path a hand-written record type takes
// enforce the same rules.
//
// It compares them by running both over the same columns, because the risk is not
// that one is wrong but that they differ: a generated schema that accepted
// something a reflected one refused would move a mistake from `go generate` to
// Open, which is the surprise the generator exists to remove.
func TestNewSchemaAgreesWithBuildSchema(t *testing.T) {
	reflected, err := SchemaOf[benchRecord]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	built, err := NewSchema[benchRecord]("benchRecord", reflected.Unit, reflected.Columns)
	if err != nil {
		t.Fatalf("NewSchema rejected a schema SchemaOf accepted: %v", err)
	}

	if built.Len() != reflected.Len() {
		t.Fatalf("NewSchema has %d columns, SchemaOf has %d", built.Len(), reflected.Len())
	}

	for i := range built.Columns {
		b, r := &built.Columns[i], &reflected.Columns[i]

		if b.Name != r.Name || b.Offset != r.Offset || b.Kind != r.Kind ||
			b.Width != r.Width || b.Time != r.Time || b.Price != r.Price ||
			b.Primary != r.Primary || b.Indexed != r.Indexed {
			t.Errorf("column %d differs:\n  NewSchema  %+v\n  SchemaOf   %+v", i, *b, *r)
		}
	}

	if built.TimeIndex() != reflected.TimeIndex() {
		t.Errorf("time index: NewSchema %d, SchemaOf %d",
			built.TimeIndex(), reflected.TimeIndex())
	}
}

// TestMustSchemaPanicsOnABadSchema covers the one behaviour a generated file
// depends on and cannot be told about: that a hand-edited schema fails loudly at
// package init rather than leaving a nil behind for the first query to fault on.
func TestMustSchemaPanicsOnABadSchema(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustSchema accepted a struct with no time column")
		}

		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value is %T, want a string: %v", r, r)
		}

		if !strings.Contains(msg, "no time column") {
			t.Errorf("panic %q does not explain the failure", msg)
		}
	}()

	MustSchema[benchRecord]("benchRecord", UnitMilli, []Column{
		{Name: "volume", Kind: KindUint64},
	})
}

// schemaView is a Schema reduced to the parts that do not depend on T, so a
// Schema[Bar] and a Schema[Tick] can be compared without a common type parameter
// the two would have no reason to share.
type schemaView struct {
	name string
	unit TimeUnit
	time int
	cols []Column
}

// viewOf reduces a schema for comparison. It is only called from this file, which
// is why the reduction is a function rather than fields on Schema: a projection
// that exists only for a test belongs in the test.
func viewOf[T any](s *Schema[T]) schemaView {
	return schemaView{
		name: s.Name,
		unit: s.Unit,
		time: s.TimeIndex(),
		cols: s.Columns,
	}
}

// TestGeneratedSchemasMatchReflection is the answer to "did `go generate` agree
// with the struct tags" for the two record types this package actually ships.
//
// Both sides are independent, which is the whole point:
//
//   - generated comes from bar_schema_gen.go and tick_schema_gen.go, produced by
//     cmd/gen-schema-record parsing the AST with its own tag parser.
//   - reflected comes from buildSchema walking reflect.Type at runtime, with this
//     package's tag parser.
//
// They agree only if two programs written against one specification read that
// specification the same way. That is not a tautology, and it is the property a
// caller needs before trusting a generated file: if the two ever disagree, one of
// them is describing a schema the store does not have, and the schema a caller
// queries with is whichever path it happened to take.
//
// It also runs against the checked-in files rather than a freshly generated pair,
// so an edited struct tag with a stale generated file fails here. That is the
// drift `go generate` is supposed to prevent and cannot prevent by itself.
func TestGeneratedSchemasMatchReflection(t *testing.T) {
	for _, tc := range []struct {
		typeName string
		generate func() schemaView
		build    func(t *testing.T) schemaView
	}{
		{
			typeName: "Bar",
			generate: func() schemaView { return viewOf(barSchema) },
			build: func(t *testing.T) schemaView {
				s, err := buildSchema[Bar](reflect.TypeOf(Bar{}))
				if err != nil {
					t.Fatalf("reflecting over Bar: %v", err)
				}

				return viewOf(s)
			},
		},
		{
			typeName: "Tick",
			generate: func() schemaView { return viewOf(tickSchema) },
			build: func(t *testing.T) schemaView {
				s, err := buildSchema[Tick](reflect.TypeOf(Tick{}))
				if err != nil {
					t.Fatalf("reflecting over Tick: %v", err)
				}

				return viewOf(s)
			},
		},
	} {
		t.Run(tc.typeName, func(t *testing.T) {
			got := tc.generate()
			want := tc.build(t)

			if got.name != want.name {
				t.Errorf("name: generated %q, reflected %q", got.name, want.name)
			}

			if got.unit != want.unit {
				t.Errorf("unit: generated %v, reflected %v", got.unit, want.unit)
			}

			if got.time != want.time {
				t.Errorf("time column index: generated %d, reflected %d",
					got.time, want.time)
			}

			// The column count first, because a length mismatch would otherwise
			// report as a confusing run of missing columns rather than as what it
			// is: one side read a tag the other did not.
			if len(got.cols) != len(want.cols) {
				t.Fatalf("generated %d columns, reflected %d — a tag was added or\n"+
					"removed without regenerating:\n  generated: %s\n  reflected: %s",
					len(got.cols), len(want.cols),
					names(got.cols), names(want.cols))
			}

			for i := range want.cols {
				g, r := &got.cols[i], &want.cols[i]

				// Field by field rather than with ==, because Column carries an
				// Aliases slice and a struct holding a slice is not comparable.
				// Naming the field is also what makes a divergence usable: two
				// structs dumped at each other say less than the one column name
				// that moved.
				switch {
				case g.Name != r.Name:
					t.Errorf("column %d: name %q vs %q", i, g.Name, r.Name)
				case g.Offset != r.Offset:
					t.Errorf("column %s: offset %d vs %d", r.Name, g.Offset, r.Offset)
				case g.Kind != r.Kind:
					t.Errorf("column %s: kind %v vs %v", r.Name, g.Kind, r.Kind)
				case g.Width != r.Width:
					t.Errorf("column %s: width %d vs %d", r.Name, g.Width, r.Width)
				case g.Time != r.Time:
					t.Errorf("column %s: time %v vs %v", r.Name, g.Time, r.Time)
				case g.Price != r.Price:
					t.Errorf("column %s: price %v vs %v", r.Name, g.Price, r.Price)
				case g.Primary != r.Primary:
					t.Errorf("column %s: primary %v vs %v", r.Name, g.Primary, r.Primary)
				case g.Indexed != r.Indexed:
					t.Errorf("column %s: indexed %v vs %v", r.Name, g.Indexed, r.Indexed)
				case !equalStrings(g.Aliases, r.Aliases):
					t.Errorf("column %s: aliases %v vs %v",
						r.Name, g.Aliases, r.Aliases)
				}
			}
		})
	}
}

// TestBuiltinTypesTakeTheGeneratedPath asserts that Bar and Tick, having been
// generated, no longer touch the cache or reflect at all.
//
// The pointer check matters as much as the count: a SchemaOf that skipped
// reflection but rebuilt the schema anyway would report zero walks and hand a
// caller a schema equal to, but distinct from, the one the guards in the generated
// file were checked against.
func TestBuiltinTypesTakeTheGeneratedPath(t *testing.T) {
	before := SchemaBuilds()

	bar, err := SchemaOf[Bar]()
	if err != nil {
		t.Fatalf("SchemaOf[Bar]: %v", err)
	}

	tick, err := SchemaOf[Tick]()
	if err != nil {
		t.Fatalf("SchemaOf[Tick]: %v", err)
	}

	if got := SchemaBuilds() - before; got != 0 {
		t.Errorf("Bar and Tick ran %d reflections, want 0 — they are generated", got)
	}

	if bar != barSchema {
		t.Error("SchemaOf[Bar] did not return the generated schema")
	}

	if tick != tickSchema {
		t.Error("SchemaOf[Tick] did not return the generated schema")
	}
}

// names is a comma-separated column list for an error message that has to show
// what each side read.
func names(cols []Column) string {
	out := make([]string, 0, len(cols))

	for i := range cols {
		out = append(out, cols[i].Name)
	}

	return strings.Join(out, ", ")
}

// equalStrings reports whether two string slices hold the same elements in the
// same order. Aliases are compared as a list because two spellings in a different
// order are the same schema to a reader and a different struct to ==.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
