package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aldok10/zdb/record"
)

// TestGeneratorKindTableMatchesRecordKindOf is the test that makes the
// duplication between kindOfExpr and record.KindOf safe.
//
// The generator decides a column's kind from a parsed source file, where no
// reflect.Type exists, so it cannot call record.KindOf. Two tables that both
// answer "which types are columns" will drift unless something compares them, so
// this compares them: every type the generator accepts must produce the same Kind
// and width record.KindOf does, and every type record.KindOf accepts must be one
// the generator can also name.
//
// A divergence here is a schema that reads the wrong bytes at the right offset,
// which is exactly the failure a fixed-width format cannot detect at read time.
func TestGeneratorKindTableMatchesRecordKindOf(t *testing.T) {
	// gen is the record.Kind constant the generator must name. It is spelled out
	// rather than derived, because deriving it from record.Kind would make the
	// test agree with a generator that names a constant which does not exist.
	for _, tc := range []struct {
		src   string
		rt    reflect.Type
		gen   string
		valid bool
	}{
		{src: "int64", rt: reflect.TypeOf(int64(0)), gen: "KindInt64", valid: true},
		{src: "uint64", rt: reflect.TypeOf(uint64(0)), gen: "KindUint64", valid: true},
		{src: "int32", rt: reflect.TypeOf(int32(0)), gen: "KindInt32", valid: true},
		{src: "uint32", rt: reflect.TypeOf(uint32(0)), gen: "KindUint32", valid: true},
		{src: "int16", rt: reflect.TypeOf(int16(0)), gen: "KindInt16", valid: true},
		{src: "uint16", rt: reflect.TypeOf(uint16(0)), gen: "KindUint16", valid: true},
		{src: "int8", rt: reflect.TypeOf(int8(0)), gen: "KindInt8", valid: true},
		{src: "uint8", rt: reflect.TypeOf(uint8(0)), gen: "KindUint8", valid: true},
		// byte and uint8 are one kind written two ways. A generator that mapped
		// them differently would produce a schema whose Kind depends on which
		// spelling the author happened to use.
		{src: "byte", rt: reflect.TypeOf(byte(0)), gen: "KindUint8", valid: true},
		{src: "float64", rt: reflect.TypeOf(float64(0)), gen: "KindFloat64", valid: true},
		{src: "float32", rt: reflect.TypeOf(float32(0)), gen: "KindFloat32", valid: true},
		{src: "bool", rt: reflect.TypeOf(true), gen: "KindBool", valid: true},
		{src: "[8]byte", rt: reflect.TypeOf([8]byte{}), gen: "KindBytes", valid: true},
		{src: "[16]uint8", rt: reflect.TypeOf([16]uint8{}), gen: "KindBytes", valid: true},

		{src: "string", rt: reflect.TypeOf(""), valid: false},
		{src: "[]byte", rt: reflect.TypeOf([]byte(nil)), valid: false},
		{src: "[]int64", rt: reflect.TypeOf([]int64(nil)), valid: false},
		{src: "*float64", rt: reflect.TypeOf(new(float64)), valid: false},
		{src: "[4]int64", rt: reflect.TypeOf([4]int64{}), valid: false},
		{src: "map[string]int", rt: reflect.TypeOf(map[string]int(nil)), valid: false},
		{src: "interface{}", rt: reflect.TypeOf((*interface{})(nil)).Elem(), valid: false},
		{src: "chan int", rt: reflect.TypeOf((chan int)(nil)), valid: false},
		{src: "func()", rt: reflect.TypeOf((func())(nil)), valid: false},
	} {
		t.Run(tc.src, func(t *testing.T) {
			gotKind, gotWidth, gotErr := kindOfExpr(fieldTypeOf(t, tc.src))

			wantKind, wantWidth, wantErr := record.KindOf(tc.rt)

			if tc.valid {
				if gotErr != nil {
					t.Fatalf("generator refused %s: %v", tc.src, gotErr)
				}

				if wantErr != nil {
					t.Fatalf("record.KindOf refused %s: %v", tc.src, wantErr)
				}

				if gotKind != tc.gen {
					t.Errorf("generator names the kind %s, want %s", gotKind, tc.gen)
				}

				// The constant name and the enum value are compared separately on
				// purpose. A generator that spelled KindUint32 correctly while
				// mapping the wrong Go type would pass the name check and fail
				// here, and that is the failure that reads the wrong bytes.
				if kindValue(t, gotKind) != wantKind {
					t.Errorf("generator's %s is %v, record.KindOf says %v",
						gotKind, kindValue(t, gotKind), wantKind)
				}

				if gotWidth != wantWidth {
					t.Errorf("width: generator says %d, record.KindOf says %d", gotWidth, wantWidth)
				}

				return
			}

			// Both refusing is the desired outcome for this half of the table, and
			// it is the half that matters: a type the generator accepts and
			// reflection refuses produces a schema the runtime cannot use, and a
			// type reflection accepts and the generator refuses is a column nobody
			// can generate.
			if gotErr == nil {
				t.Errorf("generator accepted %s as %s, record.KindOf refused it (%v)",
					tc.src, gotKind, wantErr)
			}

			if wantErr == nil {
				t.Errorf("record.KindOf accepted %s as %v, the generator refused it", tc.src, wantKind)
			}
		})
	}
}

// recordKinds is every record.Kind, by the name the generator must use to write
// it.
//
// It is written out rather than derived because deriving it is the thing that
// would make the test agree with a wrong generator: record.Kind.String() returns
// the Go type ("int64"), not the constant's name ("KindInt64"), so the two have to
// be connected by hand. That is twelve lines of duplication buying a test that
// fails when record adds a Kind and nobody teaches the generator about it, which is
// otherwise a build error discovered only after someone writes the tag.
var recordKinds = map[string]record.Kind{
	"KindInt64":   record.KindInt64,
	"KindUint64":  record.KindUint64,
	"KindInt32":   record.KindInt32,
	"KindUint32":  record.KindUint32,
	"KindInt16":   record.KindInt16,
	"KindUint16":  record.KindUint16,
	"KindInt8":    record.KindInt8,
	"KindUint8":   record.KindUint8,
	"KindFloat64": record.KindFloat64,
	"KindFloat32": record.KindFloat32,
	"KindBool":    record.KindBool,
	"KindBytes":   record.KindBytes,
}

// kindValue resolves a Kind constant name to its value, so the table test can
// compare the generator's decision against record's rather than only comparing
// names.
func kindValue(t *testing.T, name string) record.Kind {
	t.Helper()

	k, ok := recordKinds[name]
	if !ok {
		t.Fatalf("%s is not a record.Kind constant this test knows", name)
	}

	return k
}

// TestGeneratorNamesEveryKindRecordDefines asserts every Kind in record's enum is
// one the generator can produce.
//
// The table above proves the two agree on the types it lists; this proves the list
// is complete. A Kind added to record and left out of the generator is a column
// nobody can generate, and the failure would otherwise be a reference to a constant
// that does not exist — caught by the build, but only after someone writes the tag
// and only for the type they happened to try first.
func TestGeneratorNamesEveryKindRecordDefines(t *testing.T) {
	// Walk the enum by value, not by name. record's Kind is a contiguous iota
	// block from KindInt64 to KindBytes, so a new constant added to the middle
	// lands inside this range and shows up here as a value recordKinds does not
	// map. Walking by value is what makes the check able to fail; walking the
	// names would only ever re-assert the table against itself.
	for want := record.KindInt64; int(want) <= int(record.KindBytes); want++ {
		if names := namesFor(want); len(names) != 1 {
			t.Errorf("kind %d is named %d times in the generator's table, want exactly 1: %v",
				want, len(names), names)
		}
	}

	// And the reverse: every name in the table must correspond to a real Kind, or
	// it names a constant the generated code would reference and not find.
	for name, k := range recordKinds {
		if int(k) < int(record.KindInt64) || int(k) > int(record.KindBytes) {
			t.Errorf("%s maps to kind %d, which is outside record's Kind enum", name, k)
		}
	}
}

// namesFor is every name the test's table gives a Kind, which is one for a correct
// table and two for a table that lists a kind twice under different spellings — the
// mistake this test exists to catch.
func namesFor(k record.Kind) []string {
	var out []string

	for name, v := range recordKinds {
		if v == k {
			out = append(out, name)
		}
	}

	sort.Strings(out)

	return out
}

// TestGeneratorReachesEveryKindRecordDefines checks each constant name in
// recordKinds is one kindOfExpr can actually produce for some type, so the
// generator's spelling and its parser agree with each other.
func TestGeneratorReachesEveryKindRecordDefines(t *testing.T) {
	// The Go type each Kind constant is reached by. Derived from record.Kind's own
	// String, which is the Go type name, so a Kind whose String stops matching its
	// Go type fails here as a duplicate or a miss.
	for name, k := range recordKinds {
		t.Run(name, func(t *testing.T) {
			goType := k.String()

			// KindBytes is reached by an array, not a bare identifier.
			if k == record.KindBytes {
				for _, width := range []string{"1", "8", "32"} {
					_, gotWidth, err := kindOfExpr(fieldTypeOf(t, "["+width+"]byte"))
					if err != nil {
						t.Fatalf("[%s]byte: %v", width, err)
					}

					if gotWidth != atoi(t, width) {
						t.Errorf("[%s]byte width = %d", width, gotWidth)
					}
				}

				return
			}

			got, _, err := identKind(goType)
			if err != nil {
				t.Fatalf("identKind(%s) for %s: %v", goType, name, err)
			}

			if got != name {
				t.Errorf("identKind(%s) = %s, want %s", goType, got, name)
			}
		})
	}
}

func atoi(t *testing.T, s string) int {
	t.Helper()

	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}

	return n
}

// TestGenerateWritesCompilableSource runs the generator over a temporary package,
// checks the output says what a reviewer needs to see, and then compiles it.
//
// Compiling is the only way to know the output is Go rather than something that
// merely resembles Go. Running it is the way to know the schema it built is the
// schema reflection would have built, which is a separate claim and has its own
// test below.
func TestGenerateWritesCompilableSource(t *testing.T) {
	const fixture = `package fixture

// Quote carries one column of every kind that generates, plus an untagged field
// that must not become a column and a byte column.
type Quote struct {
	Datetime int64   ` + "`zdb:\"datetime,time,unit=us\"`" + `
	Bid      float64 ` + "`zdb:\"bid,price,index,alias=b|px\"`" + `
	Size     uint64  ` + "`zdb:\"size\"`" + `
	Delta    int16   ` + "`zdb:\"delta,primary\"`" + `
	Flag     bool    ` + "`zdb:\"flag\"`" + `
	Venue    [8]byte ` + "`zdb:\"venue\"`" + `
	Internal float64
}
`

	dir := t.TempDir()
	writeFileIn(t, dir, "quote.go", fixture)

	if err := gen(t, dir); err != nil {
		t.Fatalf("generate: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "zdb_schema_gen.go"))
	if err != nil {
		t.Fatalf("reading generated file: %v", err)
	}

	for _, want := range []string{
		"// Code generated by gen-schema-record. DO NOT EDIT.",
		"func (Quote) ZDBSchema() *record.Schema[Quote]",
		`Name: "datetime"`,
		// The offset is an expression, not a number. That is the property that
		// makes a generated schema unable to drift from its struct, so it is
		// asserted rather than assumed.
		"Offset: unsafe.Offsetof(Quote{}.Datetime)",
		"Kind: record.KindInt64",
		"Time: true",
		"record.UnitMicro",
		`Aliases: []string{"b", "px"}`,
		"Kind: record.KindBytes",
		"Width: 8",
		"Primary: true",
		"Indexed: true",
		// The generated var goes through MustSchema rather than assigning a
		// two-value return, which is the only shape a package-level var accepts.
		"var quoteSchema = record.MustSchema[Quote]",
	} {
		if !strings.Contains(string(got), want) {
			t.Errorf("generated file is missing %q:\n%s", want, got)
		}
	}

	// An untagged field is not a column, so it must not appear anywhere in the
	// output. This is checked as an absence because that is the failure: a
	// generated column for a field the query language was never told about.
	if strings.Contains(string(got), "Internal") {
		t.Errorf("generated file mentions the untagged field Internal:\n%s", got)
	}

	if err := buildAndRun(t, fixture, string(got)); err != nil {
		t.Fatalf("generated source did not build: %v\n%s", err, got)
	}
}

// TestGeneratedSchemaEqualsReflectedSchema is the property that matters most: for
// one struct, generation and reflection must produce the same schema.
//
// It is a separate test from the compile check because it is a different claim.
// One is that the output is valid Go; the other is that it is the right schema.
func TestGeneratedSchemaEqualsReflectedSchema(t *testing.T) {
	type quote struct {
		Datetime int64   `zdb:"datetime,time,unit=us"`
		Bid      float64 `zdb:"bid,price,index,alias=b|px"`
		Size     uint64  `zdb:"size"`
		Delta    int16   `zdb:"delta,primary"`
		Flag     bool    `zdb:"flag"`
		Venue    [8]byte `zdb:"venue"`
	}

	reflected, err := record.SchemaOf[quote]()
	if err != nil {
		t.Fatalf("SchemaOf: %v", err)
	}

	// The same columns, built the way the generated file builds them. The offsets
	// come from reflect rather than being written by hand, so the expected values
	// are derived from the struct rather than restated from the generator.
	rt := reflect.TypeOf(quote{})

	off := func(field string) uintptr {
		f, ok := rt.FieldByName(field)
		if !ok {
			t.Fatalf("no field %s", field)
		}

		return f.Offset
	}

	generated, err := record.NewSchema[quote]("quote", record.UnitMicro, []record.Column{
		{Name: "datetime", Offset: off("Datetime"), Kind: record.KindInt64, Time: true},
		{
			Name: "bid", Aliases: []string{"b", "px"}, Offset: off("Bid"),
			Kind: record.KindFloat64, Price: true, Indexed: true,
		},
		{Name: "size", Offset: off("Size"), Kind: record.KindUint64},
		{Name: "delta", Offset: off("Delta"), Kind: record.KindInt16, Primary: true},
		{Name: "flag", Offset: off("Flag"), Kind: record.KindBool},
		{Name: "venue", Offset: off("Venue"), Kind: record.KindBytes, Width: 8},
	})
	if err != nil {
		t.Fatalf("NewSchema: %v", err)
	}

	if generated.Len() != reflected.Len() {
		t.Fatalf("generated has %d columns, reflected has %d: %s vs %s",
			generated.Len(), reflected.Len(), generated, reflected)
	}

	for i := range generated.Columns {
		g, r := &generated.Columns[i], &reflected.Columns[i]

		if g.Name != r.Name {
			t.Errorf("column %d: generated name %q, reflected %q", i, g.Name, r.Name)
		}

		if g.Kind != r.Kind {
			t.Errorf("column %s: generated kind %v, reflected %v", r.Name, g.Kind, r.Kind)
		}

		if g.Offset != r.Offset {
			t.Errorf("column %s: generated offset %d, reflected %d", r.Name, g.Offset, r.Offset)
		}

		if g.Width != r.Width {
			t.Errorf("column %s: generated width %d, reflected %d", r.Name, g.Width, r.Width)
		}

		if g.Time != r.Time || g.Price != r.Price || g.Primary != r.Primary || g.Indexed != r.Indexed {
			t.Errorf("column %s: generated roles t=%v p=%v pk=%v ix=%v, reflected t=%v p=%v pk=%v ix=%v",
				r.Name, g.Time, g.Price, g.Primary, g.Indexed, r.Time, r.Price, r.Primary, r.Indexed)
		}

		if strings.Join(g.Aliases, ",") != strings.Join(r.Aliases, ",") {
			t.Errorf("column %s: generated aliases %v, reflected %v", r.Name, g.Aliases, r.Aliases)
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

// TestGenerateRefusesTheSameMistakesSchemaOfRefuses asserts the generator reports
// what reflection refuses, with the same wording where the wording exists.
//
// A generator that accepted a struct the runtime would reject would move the error
// from `go generate` to Open, which is exactly the runtime surprise the generator
// exists to remove.
func TestGenerateRefusesTheSameMistakesSchemaOfRefuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "no time column",
			src: `package fixture

type Bad struct {
	Size uint64 ` + "`zdb:\"size\"`" + `
}
`,
			want: "has no time column",
		},
		{
			name: "two time columns",
			src: `package fixture

type Bad struct {
	At  int64 ` + "`zdb:\"at,time\"`" + `
	End int64 ` + "`zdb:\"end,time\"`" + `
}
`,
			want: "has two time columns",
		},
		{
			name: "float time column",
			src: `package fixture

type Bad struct {
	At float64 ` + "`zdb:\"at,time\"`" + `
}
`,
			want: "a time column is an integer",
		},
		{
			name: "price on an integer",
			src: `package fixture

type Bad struct {
	At    int64  ` + "`zdb:\"at,time\"`" + `
	Count uint64 ` + "`zdb:\"count,price\"`" + `
}
`,
			want: "price applies to a float64 column",
		},
		{
			name: "duplicate column",
			src: `package fixture

type Bad struct {
	At  int64 ` + "`zdb:\"at,time\"`" + `
	End int64 ` + "`zdb:\"at\"`" + `
}
`,
			want: "already claimed",
		},
		{
			name: "string column",
			src: `package fixture

type Bad struct {
	At     int64  ` + "`zdb:\"at,time\"`" + `
	Symbol string ` + "`zdb:\"symbol\"`" + `
}
`,
			want: "use [N]byte, or put the value in the shard key",
		},
		{
			name: "unknown option",
			src: `package fixture

type Bad struct {
	At int64 ` + "`zdb:\"at,time,bogus\"`" + `
}
`,
			want: "unknown tag option",
		},
		{
			name: "alias collides with a column",
			src: `package fixture

type Bad struct {
	At  int64   ` + "`zdb:\"at,time\"`" + `
	Px  float64 ` + "`zdb:\"px,price,alias=at\"`" + `
}
`,
			want: "already claimed",
		},
		{
			name: "unspellable name",
			src: `package fixture

type Bad struct {
	At  int64   ` + "`zdb:\"at,time\"`" + `
	Px  float64 ` + "`zdb:\"with space,price\"`" + `
}
`,
			want: "may not contain",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFileIn(t, dir, "bad.go", tc.src)

			err := gen(t, dir)
			if err == nil {
				t.Fatal("generate accepted a struct SchemaOf would refuse")
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}

			// Nothing may be written when generation fails, or a broken schema
			// would sit in the tree being reported by the next unrelated error.
			if _, statErr := os.Stat(filepath.Join(dir, "zdb_schema_gen.go")); statErr == nil {
				t.Error("a file was written despite the error")
			}
		})
	}
}

// TestWriteFileRefusesToClobber guards the one destructive thing the generator
// does. A file it generated may be overwritten; anything else may not.
func TestWriteFileRefusesToClobber(t *testing.T) {
	dir := t.TempDir()

	handWritten := filepath.Join(dir, "trade_schema_gen.go")

	if err := os.WriteFile(handWritten, []byte("package p\n\n// written by hand\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := writeFile(handWritten, []byte(genHeader+"\npackage p\n"))
	if err == nil {
		t.Fatal("writeFile overwrote a file it did not generate")
	}

	if !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Errorf("error = %q, want it to explain the refusal", err)
	}

	got, err := os.ReadFile(handWritten)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(got, []byte("// written by hand")) {
		t.Error("the hand-written file was modified")
	}

	// And a file it did generate is overwritable, which is the whole point.
	generated := filepath.Join(dir, "other_schema_gen.go")

	if err := os.WriteFile(generated, []byte(genHeader+"\npackage p\n// old\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := writeFile(generated, []byte(genHeader+"\npackage p\n// new\n")); err != nil {
		t.Fatalf("writeFile refused its own output: %v", err)
	}
}

// TestGenerateIsIdempotent runs the generator twice over the same package and
// requires the second run to produce byte-identical output.
//
// A generator whose output depends on map iteration order produces a file that
// changes every time anyone runs it, and a file that changes when nothing changed
// is a file nobody reads the diff of.
func TestGenerateIsIdempotent(t *testing.T) {
	dir := t.TempDir()

	writeFileIn(t, dir, "multi.go", `package fixture

type A struct {
	At  int64   `+"`zdb:\"at,time\"`"+`
	Px  float64 `+"`zdb:\"px,price\"`"+`
}

type B struct {
	At int64 `+"`zdb:\"at,time\"`"+`
	Ok bool  `+"`zdb:\"ok\"`"+`
}
`)

	if err := gen(t, dir); err != nil {
		t.Fatalf("first generate: %v", err)
	}

	first, err := os.ReadFile(filepath.Join(dir, "zdb_schema_gen.go"))
	if err != nil {
		t.Fatal(err)
	}

	if err := gen(t, dir); err != nil {
		t.Fatalf("second generate: %v", err)
	}

	second, err := os.ReadFile(filepath.Join(dir, "zdb_schema_gen.go"))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(first, second) {
		t.Errorf("regenerating changed the output:\nfirst:\n%s\nsecond:\n%s", first, second)
	}

	// Two structs in one package means two schemas in one file, which is the case
	// that catches a generator that only ever handles one.
	if n := strings.Count(string(first), "ZDBSchema()"); n != 2 {
		t.Errorf("generated %d ZDBSchema methods, want 2:\n%s", n, first)
	}
}

// TestGenerateSelectsOneType covers -type, which is the flag a go:generate line
// uses. A generator that ignored it would rewrite every schema in the package
// from a directive that asked for one.
func TestGenerateSelectsOneType(t *testing.T) {
	dir := t.TempDir()

	writeFileIn(t, dir, "multi.go", `package fixture

type A struct {
	At int64 `+"`zdb:\"at,time\"`"+`
}

type B struct {
	At int64 `+"`zdb:\"at,time\"`"+`
}
`)

	if err := gen(t, dir, "-type", "A"); err != nil {
		t.Fatalf("generate: %v", err)
	}

	// The default output name for -type is derived from the type.
	got, err := os.ReadFile(filepath.Join(dir, "a_schema_gen.go"))
	if err != nil {
		t.Fatalf("reading a_schema_gen.go: %v", err)
	}

	if strings.Contains(string(got), "func (B) ZDBSchema") {
		t.Errorf("-type A generated B as well:\n%s", got)
	}

	if !strings.Contains(string(got), "func (A) ZDBSchema") {
		t.Errorf("-type A did not generate A:\n%s", got)
	}
}

// fieldTypeOf parses src as a type expression and returns the ast.Expr the
// generator would see for a field of that type.
func fieldTypeOf(t *testing.T, src string) ast.Expr {
	t.Helper()

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "x.go", "package p\n\nvar v "+src+"\n", parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing %q: %v", src, err)
	}

	gd, ok := f.Decls[0].(*ast.GenDecl)
	if !ok {
		t.Fatalf("%q did not parse as a declaration", src)
	}

	spec, ok := gd.Specs[0].(*ast.ValueSpec)
	if !ok {
		t.Fatalf("%q did not parse as a value spec", src)
	}

	if spec.Type == nil {
		t.Fatalf("%q has no type expression", src)
	}

	return spec.Type
}

func writeFileIn(t *testing.T, dir, name, body string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// gen runs the generator the way the binary does, with a directory of sources and
// whatever extra flags a case needs.
//
// It goes through run rather than calling generate directly because the flag
// defaults are set there. A test that hand-built a config would have to restate
// every default, and the one it forgot would be the qualifier — which produces
// unqualified identifiers and a generated file that does not compile. Using the
// real entry point means a new default is exercised by every test at once.
func gen(t *testing.T, dir string, args ...string) error {
	t.Helper()

	return run(append(args, dir), &config{}, io.Discard)
}

// buildAndRun compiles the generated source and runs a check against it.
//
// The fixture is placed in a module of its own under the temp directory, with a
// replace pointing at this repository, so `go run` can resolve the record import
// without the generated file having to live in the tree. The check asserts the
// two things only a compiled program can: the generated method satisfies
// record.SchemaProvider, and SchemaOf returns the generated schema rather than
// reflecting one.
func buildAndRun(t *testing.T, fixtureSrc, generated string) error {
	t.Helper()

	mod := t.TempDir()

	pkgDir := filepath.Join(mod, "fixture")

	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(pkgDir, "quote.go"), []byte(fixtureSrc), 0o644); err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(pkgDir, "zdb_schema_gen.go"), []byte(generated), 0o644); err != nil {
		return err
	}

	goMod := "module fixturecheck\n\ngo 1.24\n\nrequire github.com/aldok10/zdb v0.0.0\n\n" +
		"replace github.com/aldok10/zdb => " + moduleRoot(t) + "\n"

	if err := os.WriteFile(filepath.Join(mod, "go.mod"), []byte(goMod), 0o644); err != nil {
		return err
	}

	// The check reports what it found rather than failing silently, so a mismatch
	// names the value instead of just the expectation.
	check := `package main

import (
	"fmt"

	"fixturecheck/fixture"

	"github.com/aldok10/zdb/record"
)

func main() {
	provider, ok := any(fixture.Quote{}).(record.SchemaProvider[fixture.Quote])
	if !ok {
		fmt.Println("FAIL: Quote does not implement record.SchemaProvider")
		return
	}

	s, err := record.SchemaOf[fixture.Quote]()
	if err != nil {
		fmt.Println("FAIL SchemaOf:", err)
		return
	}

	if s != provider.ZDBSchema() {
		fmt.Println("FAIL: SchemaOf did not return the generated schema")
		return
	}

	fmt.Printf("OK columns=%d unit=%v time=%s offset=%d\n",
		s.Len(), s.Unit, s.Time().Name, s.Time().Offset)
}
`

	if err := os.WriteFile(filepath.Join(mod, "main.go"), []byte(check), 0o644); err != nil {
		return err
	}

	cmd := exec.Command("go", "run", ".")
	cmd.Dir = mod

	// A test with no network is the normal case, and GOFLAGS=-mod=mod plus a tidy
	// step would be needed otherwise. The replace means nothing is fetched, so the
	// module graph is just this repository and its standard library.
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w\n%s", err, out)
	}

	if !strings.HasPrefix(string(out), "OK ") {
		return fmt.Errorf("check did not pass: %s", out)
	}

	// Six columns, microsecond stamps, and the datetime offset. The offsets are
	// what make this more than a smoke test: a generated schema that compiled but
	// carried the wrong offset would still say OK here without them.
	if !strings.Contains(string(out), "columns=6") ||
		!strings.Contains(string(out), "unit=microseconds") ||
		!strings.Contains(string(out), "time=datetime") {
		return fmt.Errorf("check reported the wrong schema: %s", out)
	}

	return nil
}

// compileGenerated builds a temp module containing fixtureSrc and generated,
// without running it. It is the half of buildAndRun that a drift test needs:
// the assertion is that the build fails, so the run step would only be reached
// when it should not have been.
func compileGenerated(t *testing.T, fixtureSrc, generated string) error {
	t.Helper()

	mod := t.TempDir()
	pkgDir := filepath.Join(mod, "fixture")

	// The directory is created before the files, not in the same loop. Map
	// iteration order is random, so creating it in the loop means a run that
	// writes quote.go first fails with "no such file or directory", and a test
	// whose result depends on map order is a coin flip rather than a test.
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		return err
	}

	for name, body := range map[string]string{
		"quote.go":          fixtureSrc,
		"zdb_schema_gen.go": generated,
	} {
		if err := os.WriteFile(filepath.Join(pkgDir, name), []byte(body), 0o644); err != nil {
			return err
		}
	}

	goMod := "module fixturecheck\n\ngo 1.24\n\nrequire github.com/aldok10/zdb v0.0.0\n\n" +
		"replace github.com/aldok10/zdb => " + moduleRoot(t) + "\n"

	if err := os.WriteFile(filepath.Join(mod, "go.mod"), []byte(goMod), 0o644); err != nil {
		return err
	}

	cmd := exec.Command("go", "build", "./...")
	cmd.Dir = mod

	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w\n%s", err, out)
	}

	return nil
}

// TestGeneratedFileCatchesARetypedField is the test for the drift that a
// generated schema is supposed to make impossible, and the one it used to let
// through.
//
// unsafe.Offsetof is a constant over a field's position, so it catches a rename
// and a removal. It says nothing about the field's type: `Price float64` becomes
// `Price int64` at the same offset, the generated file still compiles, MustSchema
// still validates a float64 tag against a float64 Kind, and the first query reads
// eight bytes of integer as a float64. Every check passes and the data is wrong.
//
// So the retype case is asserted here as a build failure rather than described in
// a comment, because a comment asserting it is exactly the kind that stops being
// true when render is edited.
func TestGeneratedFileCatchesARetypedField(t *testing.T) {
	const before = `package fixture

type Quote struct {
	Datetime int64   ` + "`" + `zdb:"datetime,time,unit=us"` + "`" + `
	Bid      float64 ` + "`" + `zdb:"bid,price"` + "`" + `
	Venue    [8]byte ` + "`" + `zdb:"venue"` + "`" + `
}
`

	// Same fields, same order, same offsets. Only Bid's type changed, which is
	// the edit a real change makes when a price becomes an integer tick count.
	const after = `package fixture

type Quote struct {
	Datetime int64   ` + "`" + `zdb:"datetime,time,unit=us"` + "`" + `
	Bid      int64   ` + "`" + `zdb:"bid,price"` + "`" + `
	Venue    [8]byte ` + "`" + `zdb:"venue"` + "`" + `
}
`

	generated := generateFor(t, before)
	if err := compileGenerated(t, before, generated); err != nil {
		t.Fatalf("the unmodified fixture does not build: %v", err)
	}

	err := compileGenerated(t, after, generated)
	if err == nil {
		t.Fatal("a retyped field still compiles, which is the silent corruption this guard exists to prevent")
	}

	// The error has to name the field, or a reader cannot tell which column went
	// stale without opening the file and counting columns.
	if !strings.Contains(err.Error(), "Bid") {
		t.Errorf("the compile error does not name the field: %v", err)
	}
}

// TestGeneratedFileCatchesAReshapedByteColumn covers the array length, which is
// the one case where a type guard and an offset guard disagree.
//
// Venue stays a [N]byte and the offset stays put, but the width in the schema is
// now wrong: CompareBytes would read 8 bytes where the generator recorded 12, on
// every record, and the first record whose flags happen to differ orders wrong
// against the next.
func TestGeneratedFileCatchesAReshapedByteColumn(t *testing.T) {
	const before = `package fixture

type Quote struct {
	Datetime int64   ` + "`" + `zdb:"datetime,time,unit=us"` + "`" + `
	Venue    [12]byte ` + "`" + `zdb:"venue"` + "`" + `
}
`

	const after = `package fixture

type Quote struct {
	Datetime int64   ` + "`" + `zdb:"datetime,time,unit=us"` + "`" + `
	Venue    [8]byte ` + "`" + `zdb:"venue"` + "`" + `
}
`

	generated := generateFor(t, before)
	if err := compileGenerated(t, after, generated); err == nil {
		t.Fatal("a narrowed [N]byte column still compiles")
	}

	if got := guardsOf(generated)["Venue"]; got != "[12]byte" {
		t.Errorf("the guard says %q, want [12]byte — an offset cannot carry the length", got)
	}
}

// TestGeneratedFileEmitsOneGuardPerColumn is the cheap half of the two tests
// above, and it is the one that would still pass if a guard were emitted for every
// column but the one that mattered.
func TestGeneratedFileEmitsOneGuardPerColumn(t *testing.T) {
	generated := generateFor(t, `package fixture

type Quote struct {
	Datetime int64   `+"`"+`zdb:"datetime,time,unit=us"`+"`"+`
	Bid      float64 `+"`"+`zdb:"bid,price"`+"`"+`
	Size     uint64  `+"`"+`zdb:"size"`+"`"+`
	Aggressor bool   `+"`"+`zdb:"aggressor"`+"`"+`
	Venue    [8]byte `+"`"+`zdb:"venue"`+"`"+`
}
`)

	got := guardsOf(generated)

	for field, want := range map[string]string{
		"Datetime":  "int64",
		"Bid":       "float64",
		"Size":      "uint64",
		"Aggressor": "bool",
		"Venue":     "[8]byte",
	} {
		if got[field] != want {
			t.Errorf("guard for %s says %q, want %q\n%s", field, got[field], want, generated)
		}
	}

	if len(got) != 5 {
		t.Errorf("%d guards emitted for 5 columns:\n%s", len(got), generated)
	}
}

// guardRe matches one emitted guard: an underscore, a Go type, and the field it
// guards. It is a regexp rather than a string comparison because gofmt aligns the
// type column to the widest one, so the spacing depends on the other fields and a
// test asserting padding breaks the moment an unrelated column is added.
var guardRe = regexp.MustCompile(`_\s+(\S+)\s+=\s+\w+\{\}\.(\w+)`)

// guardsOf parses the generated file's type guards into field -> Go type.
//
// Parsing rather than substring matching is what makes the assertion survive
// reformatting, and what lets a test say *which* column lost its guard.
func guardsOf(generated string) map[string]string {
	out := map[string]string{}

	for _, m := range guardRe.FindAllStringSubmatch(generated, -1) {
		out[m[2]] = m[1]
	}

	return out
}

// generateFor runs the generator over a single-file fixture and returns the
// output. A fixture with no zdb tags on any field yields no sources and no
// output, so every caller here has at least one tagged field.
func generateFor(t *testing.T, fixtureSrc string) string {
	t.Helper()

	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "quote.go"), []byte(fixtureSrc), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := gen(t, dir); err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(filepath.Join(dir, "zdb_schema_gen.go"))
	if err != nil {
		t.Fatalf("no generated file: %v", err)
	}

	return string(out)
}

// moduleRoot finds the repository root by walking up from the test's working
// directory until go.mod appears.
func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the working directory")
		}

		dir = parent
	}
}
