//go:build unix

package platform

import (
	"os"
	"path/filepath"
	"testing"
)

// openFDs counts the descriptors this process holds, through /dev/fd.
//
// This is the measurement AGENTS.md section 11.3 asks for, and it is the only one
// that proves the point: the Go heap cannot see a mapping or a descriptor, so a
// leak here is invisible to every allocation number in this repository.
func openFDs(t *testing.T) int {
	t.Helper()

	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Skipf("cannot read /dev/fd: %v", err)
	}

	// ReadDir itself holds one descriptor while iterating, so subtract the
	// difference consistently rather than trying to be exact about it.
	return len(entries)
}

// TestUnmapAndCloseReleasesThePair is the release rule in section 11.1: a mapped
// region and its descriptor are acquired together and released together. The
// two were previously two copies of a four-line function in two packages, which
// is exactly the shape where one copy grows an extra step and the other does not.
func TestUnmapAndCloseReleasesThePair(t *testing.T) {
	const rounds = 50

	path := filepath.Join(t.TempDir(), "store.bin")
	before := openFDs(t)

	for i := range rounds {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			t.Fatalf("create %s: %v", path, err)
		}

		// Pre-allocate before mapping, exactly as the writer does: a mapping that
		// runs past EOF faults on first touch rather than failing here.
		if err := f.Truncate(4096); err != nil {
			f.Close()

			t.Fatalf("truncate %s: %v", path, err)
		}

		mm, err := Map(f, 4096, ReadWrite)
		if err != nil {
			f.Close()

			t.Fatalf("Map round %d: %v", i, err)
		}

		mm[0] = byte(i)

		UnmapAndClose(mm, f)
	}

	after := openFDs(t)
	if delta := after - before; delta != 0 {
		t.Errorf("after %d map/unmap rounds the descriptor count moved by %d (%d -> %d)",
			rounds, delta, before, after)
	}
}

// TestMapDoesNotLeakOnAFailedCall is the error path, which is where the pair used
// to drift: a mapping refused after the descriptor was opened must still release
// the descriptor, or a store that fails to open once has leaked a handle for the
// life of the process.
func TestMapDoesNotLeakOnAFailedCall(t *testing.T) {
	const rounds = 50

	dir := t.TempDir()
	before := openFDs(t)

	for i := range rounds {
		path := filepath.Join(dir, "store.bin")

		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			t.Fatalf("create %s: %v", path, err)
		}

		if err := f.Truncate(4096); err != nil {
			f.Close()

			t.Fatalf("truncate %s: %v", path, err)
		}

		// An overlong request is refused by the shared guard after the descriptor
		// is open, which is the path a real failure takes: acquired, then refused.
		if mm, merr := Map(f, 8192, ReadWrite); merr == nil {
			UnmapAndClose(mm, f)

			t.Fatalf("Map round %d of 8192 bytes over a 4096 byte file succeeded", i)
		}

		// The caller's own error branch, which is what has to release.
		UnmapAndClose(nil, f)
	}

	after := openFDs(t)
	if delta := after - before; delta != 0 {
		t.Errorf("after %d refused map/unmap rounds the descriptor count moved by %d (%d -> %d)",
			rounds, delta, before, after)
	}
}
