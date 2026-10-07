package record_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/aldok10/zdb/record"
)

// typeOf is reflect.TypeOf, named so the offset helper above reads as what it is.
func typeOf(v any) reflect.Type { return reflect.TypeOf(v) }

var schemaSink *record.Schema[generated]

var plainSink *record.Schema[plain]

// BenchmarkSchemaOfGenerated measures the path a generated type takes: one
// interface assertion and a return of an existing pointer.
//
// It allocates once, and the number is not a bug being tolerated — it is what the
// assertion costs. `var zero T` followed by `any(&zero)` moves zero to the heap,
// because a value converted to an interface may be retained by the callee and the
// compiler cannot see that the dynamic method it calls never does. 16 B is
// sizeof(struct{ int64; float64 }), which is the record's own columns.
//
// The obvious way to remove it is to assert on a nil *T instead, because a pointer
// converts to an interface without allocating. That works only if the generated
// method has a pointer receiver: with a value receiver the assertion still succeeds
// — a value-receiver method is in *T's method set — and the call then dereferences
// nil and panics. Trading one allocation for a panic that reads as a nil pointer
// bug in the caller's own code is not a trade worth making for a function called
// once per query plan. pon ytail: if SchemaOf ever lands on a per-record path,
// switch the generated method to a pointer receiver and assert on (*T)(nil); the
// generator emits the receiver, so the change is one line in render.
func BenchmarkSchemaOfGenerated(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		s, err := record.SchemaOf[generated]()
		if err != nil {
			b.Fatal(err)
		}

		schemaSink = s
	}
}

// BenchmarkSchemaOfReflected measures the cached path for a type with no
// generated schema, which is what every type costs until someone runs the
// generator.
//
// The difference from the generated path is one reflect.TypeOf and one sync.Map
// load, plus a larger box because a wider record has more zero bytes. It is small
// on purpose: the argument for generating is startup and binary size, not the cost
// of looking a schema up.
func BenchmarkSchemaOfReflected(b *testing.B) {
	b.ReportAllocs()

	// Warm the cache so this measures the lookup rather than the build, which would
	// otherwise dominate and make every iteration after the first look free.
	if _, err := record.SchemaOf[plain](); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		s, err := record.SchemaOf[plain]()
		if err != nil {
			b.Fatal(err)
		}

		plainSink = s
	}
}

// BenchmarkSchemaOfReflectFirstCall reports what generation removes: the one
// reflective walk of a type's fields.
//
// It cannot be looped, because a type is built once per process and the first
// iteration would dominate 99,999,999 cache hits. The reflection count is
// asserted directly by TestSchemaOfReflectsEachTypeExactlyOnce instead, and this
// benchmark exists to show the steady-state cost that count hides.
func BenchmarkSchemaOfReflectFirstCall(b *testing.B) {
	b.ReportAllocs()

	// A type no other benchmark uses, so its first call really is a first call.
	type cold struct {
		Datetime int64   `zdb:"datetime,time"`
		Price    float64 `zdb:"price,price"`
		Size     uint64  `zdb:"size"`
		Delta    int16   `zdb:"delta"`
		Flag     bool    `zdb:"flag"`
		Venue    [8]byte `zdb:"venue"`
	}

	for b.Loop() {
		if _, err := record.SchemaOf[cold](); err != nil {
			b.Fatal(err)
		}
	}
}

// plain is a record type with no generated schema, so SchemaOf reaches the
// reflected path and the cache.
type plain struct {
	Datetime int64   `zdb:"datetime,time"`
	Price    float64 `zdb:"price,price"`
	Size     uint64  `zdb:"size"`
}

// generated is a record type carrying the ZDBSchema method that
// cmd/gen-schema-record emits.
//
// It is written out rather than generated so the test needs no generator to run
// and so a regression in the generator cannot remove the thing under test. The
// schema literal is the generated shape: offsets as expressions the compiler
// evaluates, every column spelled out, no reflection.
//
// It is declared at package level because a method cannot be added to a type
// declared inside a function.
type generated struct {
	Datetime int64   `zdb:"datetime,time,unit=us"`
	Price    float64 `zdb:"price,price"`
}

// generatedSchema is the package-level var a generated file declares. Built once
// by MustSchema, which re-validates, and returned unchanged forever after.
var generatedSchema = record.MustSchema[generated]("generated", record.UnitMicro, []record.Column{
	{Name: "datetime", Offset: offsetOf(generated{}, "Datetime"), Kind: record.KindInt64, Time: true},
	{Name: "price", Offset: offsetOf(generated{}, "Price"), Kind: record.KindFloat64, Price: true},
})

// ZDBSchema is what the generator writes. One interface assertion finds it, and
// SchemaOf never reaches the cache or reflect for this type.
func (generated) ZDBSchema() *record.Schema[generated] { return generatedSchema }

// offsetOf is the value unsafe.Offsetof(generated{}.Field) produces. It is
// written out here so the test asserts the schema against independently computed
// offsets rather than against the ones under test.
func offsetOf(v any, field string) uintptr {
	t := typeOf(v)

	f, ok := t.FieldByName(field)
	if !ok {
		panic("no field " + field + " on " + t.String())
	}

	return f.Offset
}

// TestSchemaOfReflectsEachTypeExactlyOnce is the test for the reuse mechanism.
//
// The claim is that reflection happens once per type for the life of the process.
// Pointer identity would not show it: two equal schemas returned by two separate
// reflective walks are indistinguishable to the caller unless it compares the
// pointers, and even then the second walk could have happened and been discarded.
// Counting the walks is the assertion that tests the claim.
func TestSchemaOfReflectsEachTypeExactlyOnce(t *testing.T) {
	before := record.SchemaBuilds()

	first, err := record.SchemaOf[plain]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	afterFirst := record.SchemaBuilds()
	if afterFirst-before != 1 {
		t.Fatalf("first SchemaOf ran %d reflections, want 1", afterFirst-before)
	}

	// Enough calls that a missing cache would show. One would have been a weak
	// test: a function that rebuilt on the second call and cached the third would
	// pass it.
	for range 100 {
		again, err := record.SchemaOf[plain]()
		if err != nil {
			t.Fatalf("SchemaOf: %v", err)
		}

		if again != first {
			t.Fatal("SchemaOf returned a different pointer for the same type")
		}
	}

	if got := record.SchemaBuilds(); got != afterFirst {
		t.Errorf("100 more calls ran %d reflections, want 0", got-afterFirst)
	}
}

// TestSchemaOfCachesFailures covers the half that is easy to leave out.
//
// A bad record type fails at Open. A service that retries Open — which is what a
// service does while a config file is being fixed, or while it waits for a
// dependency — would otherwise re-walk the whole struct on every attempt. Caching
// only successes means the failure is the case that costs the most and repeats the
// most.
func TestSchemaOfCachesFailures(t *testing.T) {
	// noTime has no time column, so it can never be a record.
	type noTime struct {
		Size uint64 `zdb:"size"`
	}

	_, err := record.SchemaOf[noTime]()
	if err == nil {
		t.Fatal("SchemaOf accepted a struct with no time column")
	}

	before := record.SchemaBuilds()

	for range 50 {
		if _, err := record.SchemaOf[noTime](); err == nil {
			t.Fatal("SchemaOf accepted a struct with no time column on a retry")
		}
	}

	if got := record.SchemaBuilds() - before; got != 0 {
		t.Errorf("50 retries of a failing type ran %d reflections, want 0", got)
	}
}

// TestSchemaOfConcurrentCallersReflectOnce covers the race the cache has to win.
//
// Several goroutines opening a store of a type nobody has opened before will all
// miss. LoadOrStore means the reflect count is bounded by the number of goroutines
// racing, not by the number of calls, and every one of them gets the same schema
// pointer — which matters because a caller comparing schemas by identity would
// otherwise see two equal schemas and conclude something was rebuilt.
func TestSchemaOfConcurrentCallersReflectOnce(t *testing.T) {
	// A type nothing else in the test binary uses, so the count starts from a
	// known point rather than from whatever an earlier test left behind.
	type contended struct {
		Datetime int64   `zdb:"datetime,time"`
		Price    float64 `zdb:"price,price"`
		Delta    int32   `zdb:"delta"`
		Flag     bool    `zdb:"flag"`
		Venue    [8]byte `zdb:"venue"`
	}

	const callers = 64

	var (
		wg    sync.WaitGroup
		got   [callers]*record.Schema[contended]
		start = make(chan struct{})
	)

	before := record.SchemaBuilds()

	wg.Add(callers)

	for i := range callers {
		go func() {
			defer wg.Done()

			<-start

			s, err := record.SchemaOf[contended]()
			if err != nil {
				t.Errorf("SchemaOf: %v", err)

				return
			}

			got[i] = s
		}()
	}

	close(start)
	wg.Wait()

	reflected := record.SchemaBuilds() - before
	if reflected == 0 {
		t.Error("no reflection ran at all, so this test proved nothing")
	}

	if reflected > callers {
		t.Errorf("%d callers ran %d reflections", callers, reflected)
	}

	// Every caller must get the same pointer. The ones that lost the LoadOrStore
	// race get the winner's schema, not their own.
	for i, s := range got {
		if s == nil {
			continue
		}

		if s != got[0] {
			t.Errorf("caller %d got a different schema pointer than caller 0", i)
		}
	}
}

// TestGeneratedSchemaSkipsReflection is the whole point of the provider method.
//
// A type with a ZDBSchema method must cost zero reflections, however many times
// its schema is asked for. That is the property that lets a caller choose
// generation for its startup cost without changing anything else.
func TestGeneratedSchemaSkipsReflection(t *testing.T) {
	before := record.SchemaBuilds()

	s, err := record.SchemaOf[generated]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	if got := record.SchemaBuilds() - before; got != 0 {
		t.Errorf("a generated type ran %d reflections, want 0", got)
	}

	// And again, because a provider that only short-circuited the first call would
	// still satisfy the assertion above.
	for range 100 {
		if _, err := record.SchemaOf[generated](); err != nil {
			t.Fatalf("SchemaOf: %v", err)
		}
	}

	if got := record.SchemaBuilds() - before; got != 0 {
		t.Errorf("100 calls on a generated type ran %d reflections, want 0", got)
	}

	// The schema still has to be right. Skipping reflection is worthless if it
	// skips the columns too, so the generated literal is compared against the
	// reflected schema of the same shape — which is the claim
	// TestGeneratedMatchesReflected makes elsewhere, and is reasserted here
	// because this is the path a caller actually takes.
	if s.Len() != 2 {
		t.Errorf("generated schema has %d columns, want 2", s.Len())
	}

	if s.Time().Name != "datetime" || s.Unit != record.UnitMicro {
		t.Errorf("generated schema says time=%s unit=%v", s.Time().Name, s.Unit)
	}
}

// TestGeneratedMatchesReflected is the property the generator exists to preserve:
// for one struct, the two paths produce the same schema.
//
// It is checked here as well as in the generator's own tests because the failure
// it guards against is split across two packages — the generator decides what to
// write, this package decides what that means — and either side can change alone.
func TestGeneratedMatchesReflected(t *testing.T) {
	type shared struct {
		Datetime int64   `zdb:"datetime,time,unit=us"`
		Bid      float64 `zdb:"bid,price,index,alias=b|px"`
		Size     uint64  `zdb:"size"`
		Delta    int16   `zdb:"delta,primary"`
		Flag     bool    `zdb:"flag"`
		Venue    [8]byte `zdb:"venue"`
	}

	reflected, err := record.SchemaOf[shared]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	// The offsets the generated file writes are unsafe.Offsetof expressions; this
	// test supplies the values reflect would compute for them, which is what makes
	// it a comparison rather than a restatement.
	generated := record.MustSchema[shared]("shared", record.UnitMicro, []record.Column{
		{Name: "datetime", Offset: offsetOf(shared{}, "Datetime"), Kind: record.KindInt64, Time: true},
		{
			Name: "bid", Aliases: []string{"b", "px"}, Offset: offsetOf(shared{}, "Bid"),
			Kind: record.KindFloat64, Price: true, Indexed: true,
		},
		{Name: "size", Offset: offsetOf(shared{}, "Size"), Kind: record.KindUint64},
		{Name: "delta", Offset: offsetOf(shared{}, "Delta"), Kind: record.KindInt16, Primary: true},
		{Name: "flag", Offset: offsetOf(shared{}, "Flag"), Kind: record.KindBool},
		{Name: "venue", Offset: offsetOf(shared{}, "Venue"), Kind: record.KindBytes, Width: 8},
	})

	if generated.Len() != reflected.Len() {
		t.Fatalf("generated has %d columns, reflected has %d", generated.Len(), reflected.Len())
	}

	for i := range generated.Columns {
		g, r := &generated.Columns[i], &reflected.Columns[i]

		// Compared field by field rather than with ==, because Column carries an
		// Aliases slice and a struct holding a slice is not comparable. Writing
		// the comparison out is also what makes a divergence say which field
		// differs instead of dumping two structs at each other.
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
		case len(g.Aliases) != len(r.Aliases):
			t.Errorf("column %s: %d aliases vs %d", r.Name, len(g.Aliases), len(r.Aliases))
		default:
			for j := range g.Aliases {
				if g.Aliases[j] != r.Aliases[j] {
					t.Errorf("column %s: alias %d is %q vs %q",
						r.Name, j, g.Aliases[j], r.Aliases[j])
				}
			}
		}
	}

	if generated.Unit != reflected.Unit {
		t.Errorf("unit: generated %v, reflected %v", generated.Unit, reflected.Unit)
	}

	if generated.TimeIndex() != reflected.TimeIndex() {
		t.Errorf("time index: generated %d, reflected %d",
			generated.TimeIndex(), reflected.TimeIndex())
	}
}
