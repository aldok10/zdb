package platform

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newFile allocates n bytes in a fresh file in a temp dir and returns it open
// for read and write.
//
// The file is truncated to n rather than written, for two reasons. A mapping
// needs a read-write descriptor on every platform, so a read-only handle would
// test the wrong thing on at least one of them. And the length has to be n
// before the mapping is made: a mapping that runs past EOF is accepted by both
// platforms and then faults, which is exactly what TestMapRefusesOverlongSize
// pins down.
func newFile(t *testing.T, n int) (*os.File, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "store.bin")

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}

	if err := f.Truncate(int64(n)); err != nil {
		f.Close()

		t.Fatalf("truncate %s: %v", path, err)
	}

	return f, path
}

// TestMapRefusesUnusableSize is the size guard the format depends on. A mapping
// of a truncated length would hand a reader a view shorter than the header it is
// about to trust, so the refusal has to happen before the syscall rather than
// being discovered as a fault on first touch.
func TestMapRefusesUnusableSize(t *testing.T) {
	for _, size := range []int64{0, -1, -4096} {
		f, path := newFile(t, 4096)

		b, err := Map(f, size, ReadOnly)
		if err == nil {
			Unmap(b)
			f.Close()

			t.Fatalf("Map(%s, %d) succeeded; a non-positive size is not a mapping",
				path, size)
		}

		if b != nil {
			t.Errorf("Map(%s, %d) returned %d bytes alongside its error", path, size, len(b))
		}

		f.Close()
	}
}

// TestMapRefusesOverlongSize is the check that makes the package behave the same
// on every OS rather than merely compile there.
//
// A mapping longer than the file is accepted by both platforms, and the two then
// diverge in the worst possible way: unix raises SIGBUS on the first read past
// EOF, in whatever goroutine got there, while Windows hands back zeros. For a
// store those zeros are not a crash, they are a segment whose header validates
// and whose records decode to plausible prices. Refusing here means the file is
// named in an error instead.
func TestMapRefusesOverlongSize(t *testing.T) {
	f, path := newFile(t, 4096)

	b, err := Map(f, 4097, ReadOnly)
	if err == nil {
		Unmap(b)
		f.Close()

		t.Fatalf("Map(%s, 4097) of a 4096 byte file succeeded", path)
	}

	if b != nil {
		t.Errorf("Map returned %d bytes alongside its error", len(b))
	}

	// The refusal names the file, which is the whole point: the alternative is a
	// fault with no idea which of a store's segments produced it.
	if !strings.Contains(err.Error(), filepath.Base(path)) {
		t.Errorf("Map error %q does not name the file it refused to map", err)
	}

	f.Close()
}

// TestMapRefusesUnknownAccess guards the switch that turns Access into the
// platform's own protection. An unmapped value there is not a no-op: on unix it
// would request protection zero and on Windows it would ask CreateFileMappingW
// for a protection the kernel accepts but no read can satisfy.
func TestMapRefusesUnknownAccess(t *testing.T) {
	f, path := newFile(t, 4096)

	b, err := Map(f, 4096, Access(9))
	if errors.Is(err, ErrUnsupported) {
		f.Close()
		t.Skipf("%s has no mapping at all; nothing to check", path)
	}

	if err == nil {
		Unmap(b)
		f.Close()

		t.Fatal("Map with an undefined Access succeeded; it would map with no protection")
	}

	if b != nil {
		t.Errorf("Map returned %d bytes alongside its error", len(b))
	}

	f.Close()
}

// TestUnmapEmptyIsNoOp is what lets a teardown path call Unmap unconditionally
// after a mapping that may never have been created, instead of every call site
// testing first and one of them forgetting.
func TestUnmapEmptyIsNoOp(t *testing.T) {
	if err := Unmap(nil); err != nil {
		t.Errorf("Unmap(nil) = %v; want nil", err)
	}

	if err := Unmap([]byte{}); err != nil {
		t.Errorf("Unmap(empty) = %v; want nil", err)
	}
}

// TestWriteIsVisibleThroughASecondMapping is the invariant the entire store
// rests on. A writer's append has to be readable through a reader's mapping the
// instant it happens, which is what makes a chart correct against a live store
// and is the one thing MAP_PRIVATE would quietly break.
func TestWriteIsVisibleThroughASecondMapping(t *testing.T) {
	w, path := newFile(t, 4096)

	mm, err := Map(w, 4096, ReadWrite)
	if err != nil {
		w.Close()
		t.Skipf("no read-write mapping here: %v", err)
	}

	// A second, read-only descriptor and mapping of the same file: a reader's
	// view, not a second writer's.
	r, err := os.Open(path)
	if err != nil {
		Unmap(mm)
		w.Close()

		t.Fatalf("reopen %s: %v", path, err)
	}

	ro, err := Map(r, 4096, ReadOnly)
	if err != nil {
		Unmap(mm)
		w.Close()
		r.Close()

		t.Fatalf("map %s read-only: %v", path, err)
	}

	mm[0], mm[1], mm[4095] = 'Z', 'D', 'B'

	if string(ro[:2]) != "ZD" || ro[4095] != 'B' {
		t.Errorf("read-only mapping read %q / %q, want \"ZD\" / \"B\"",
			ro[:2], ro[4095])
	}

	// The other direction, which is the one MAP_PRIVATE would break: a second
	// writable mapping of the same file, through its own descriptor, must see the
	// store too. That is not the writer package's pattern (one writer per shard),
	// but it is the property "shared" actually claims, and it is cheapest to see
	// from two mappings rather than from a comment.
	w2, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Skipf("cannot reopen %s for writing: %v", path, err)
	}

	mm2, err := Map(w2, 4096, ReadWrite)
	if err != nil {
		w2.Close()

		t.Fatalf("map %s a second time for writing: %v", path, err)
	}

	if string(mm2[:2]) != "ZD" {
		t.Errorf("a second writable mapping read %q, want \"ZD\"; MAP_PRIVATE would give zeros here",
			mm2[:2])
	}

	mm2[3] = 'G'

	if mm[3] != 'G' {
		t.Errorf("the first writable mapping read %q at offset 3, want \"G\"", mm[3])
	}

	Unmap(mm2)
	w2.Close()
	Unmap(ro)
	r.Close()
	Unmap(mm)
	w.Close()
}

// TestSyncReachesTheFile checks the durability contract through the file itself
// rather than through a return code. Sync is what makes a write survive a
// machine crash, so reading the bytes back with an ordinary read is the only
// check that says the flush actually happened.
func TestSyncReachesTheFile(t *testing.T) {
	f, path := newFile(t, 4096)

	mm, err := Map(f, 4096, ReadWrite)
	if err != nil {
		f.Close()
		t.Skipf("no read-write mapping here: %v", err)
	}

	copy(mm, "zdb")

	if err := Sync(mm, f); err != nil {
		Unmap(mm)
		f.Close()

		t.Fatalf("Sync: %v", err)
	}

	// Read through a fresh descriptor with no mapping involved at all, so what
	// arrives came from the file rather than from the page cache the writer
	// already had in hand.
	data, err := os.ReadFile(path)
	if err != nil {
		Unmap(mm)
		f.Close()

		t.Fatalf("read back %s: %v", path, err)
	}

	if string(data[:3]) != "zdb" {
		t.Errorf("file holds %q, want \"zdb\"", data[:3])
	}

	Unmap(mm)
	f.Close()
}

// TestSyncIsIdempotentOnAnUnmappedView keeps the two error paths apart. A closed
// mapping is not a reason to refuse a flush of the file, and a platform with no
// page flush is not a reason to refuse it either.
func TestSyncIsIdempotentOnAnUnmappedView(t *testing.T) {
	f, _ := newFile(t, 4096)

	if err := Sync(nil, f); err != nil {
		t.Errorf("Sync(nil) = %v; an empty view is nothing to flush", err)
	}

	if err := Sync(nil, f); err != nil {
		t.Errorf("second Sync(nil) = %v; flushing is not a one-shot", err)
	}

	f.Close()
}

// TestTotalRAMIsEitherAPlausibleValueOrUnknown is the shape of the answer the
// flush policy depends on. There is no middle ground: a wrong number becomes a
// wrong flush budget, so an unknown platform has to say false rather than
// approximate.
func TestTotalRAMIsEitherAPlausibleValueOrUnknown(t *testing.T) {
	ram, ok := TotalRAM()
	if !ok {
		t.Skip("total memory is not readable on this platform")
	}

	if ram < 1<<30 {
		t.Errorf("TotalRAM() = %d bytes; a machine has at least 1 GiB, so this is a misread", ram)
	}

	if ram>>40 != 0 {
		t.Errorf("TotalRAM() = %d bytes; above 1 TiB this is a misread of a wider value", ram)
	}
}
