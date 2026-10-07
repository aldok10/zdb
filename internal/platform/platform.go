// Package platform holds every operating-system dependency zdb has, and nothing
// else.
//
// # Why the boundary exists
//
// Two rules of this project cannot both hold if these calls stay spread through
// the package. The first is that every mapped region and every descriptor has
// exactly one release site (AGENTS.md section 11.1). The second is that the
// store refuses to open on an operating system it cannot map rather than
// silently degrading to buffered reads, because buffered reads change the crash
// and concurrency guarantees (section 4.2).
//
// Keeping them here makes both checkable. Every OS decision is one table row,
// every other file in the package compiles identically on every OS, and gaining
// a platform is a new file in this directory instead of a new build tag in a
// file whose name does not describe the problem.
//
// # The names are not the syscall names
//
// mmap, munmap and msync are POSIX spellings. Map, Unmap and Sync say what the
// caller wants on every operating system, including the ones whose API spells it
// CreateFileMappingW and FlushViewOfFile. Access carries the read-only and
// read-write choice as one value instead of two functions, so no platform can
// implement one mode and quietly get the other wrong.
//
// Nothing here is re-exported from zdb. A file mapping is an implementation
// detail of the format, and the previous exported surface was not even the same
// on every OS: ProtRead and ProtWrite existed on unix and not on windows, so a
// caller who compiled against one could not compile against the other. One API,
// identical everywhere, is the safety property worth having.
//
// # The per-OS matrix
//
// Every cell is one build-tagged file in this directory. A platform either maps,
// or Map refuses with ErrUnsupported. There is no third outcome and there is no
// buffered fallback.
//
//	Platform                     | mapping            | page flush       | RAM probe
//	-----------------------------|--------------------|------------------|------------
//	linux, darwin, freebsd,      | syscall.Mmap       | msync(MS_SYNC)   | native
//	openbsd, dragonfly           |                    |                  |
//	netbsd, solaris, illumos,    | syscall.Mmap       | ErrUnsupported   | none yet
//	aix                          |                    |                  |
//	windows                      | kernel32 mapping   | FlushViewOfFile  | kernel32
//	js/wasm and any other GOOS   | ErrUnsupported     | ErrUnsupported   | none yet
//
// # When a primitive does not exist
//
// Every primitive used here genuinely exists on the target it is compiled for.
// Where it does not:
//
//  1. An equivalent native API is used instead. Windows has no syscall.Mmap in
//     the standard library, so mapping_windows.go calls CreateFileMappingW and
//     MapViewOfFile through syscall.NewLazyDLL, adding no dependency. That
//     LazyDLL is also what reads total RAM there, so the package holds one
//     handle rather than two.
//  2. Where there is no equivalent, the function reports ErrUnsupported
//     honestly instead of pretending to have succeeded. Go's syscall package has
//     no SYS_MSYNC for netbsd, solaris, illumos or aix, and hardcoding a
//     syscall number per architecture is a worse trade than losing an
//     optimisation. ponytail: add golang.org/x/sys/unix and say in the commit
//     why the number cannot be hardcoded.
//  3. There is no buffered-read fallback anywhere in this package, on any
//     platform, ever. Failing at Map is better than running on a promise the
//     kernel is not keeping.
package platform

import (
	"errors"
	"fmt"
	"os"
)

// ErrUnsupported means this operating system has no primitive for the operation.
// Callers must read it as "this platform cannot do this", not as a failure.
// Sync is the one place that acts on that reading, and the reason is written
// where it is acted on.
var ErrUnsupported = errors.New("zdb: mmap unsupported on this platform")

// Access is what a caller asks a mapping to permit.
//
// It is one type with two values rather than two functions because every
// platform has to implement both, and two functions means four call sites that
// can drift. A switch on this value is total and the compiler enforces it.
type Access uint8

const (
	// ReadOnly maps for reading. Every read path uses it, and no write through
	// such a mapping can fault the process, which is what lets a reader share
	// a file with a writer that is mid-append.
	ReadOnly Access = iota

	// ReadWrite maps for reading and writing, shared with every other mapping of
	// the same file. Only a writer's own descriptor ever asks for it. Shared is
	// the point, not an optimisation: MAP_PRIVATE on unix would make a writer's
	// stores invisible to a reader's mapping, which is the exact opposite of
	// what this store is for.
	ReadWrite
)

// Map maps the first size bytes of f with the given access, shared with every
// other mapping of the same file. The mapping is created once and never
// remapped, which is why the caller pre-allocates the file to its full capacity:
// the file size must never change under a live mapping.
//
// size must not exceed the file's current length. Requesting more maps fine on
// both platforms and then behaves differently on each one, so the check is here
// rather than left to each implementation: unix raises SIGBUS on the first touch
// past EOF, while Windows hands back zeros. A caller that mapped too much gets
// an error on every platform instead of a crash on one of them.
//
// The returned bytes alias the file, so a caller that keeps them past Unmap
// faults. Unmap is the only correct place to release them.
func Map(f *os.File, size int64, access Access) ([]byte, error) {
	if err := checkSize(f, size); err != nil {
		return nil, err
	}

	if err := checkLength(f, size); err != nil {
		return nil, err
	}

	return mapFile(f, size, access)
}

// Unmap releases a mapping returned by Map. An empty slice is already unmapped,
// which is what makes the teardown path callable from a failure branch without
// every site having to test first.
//
// After it returns, the bytes are gone: the GC cannot see a mapping, so this is
// the only thing that reclaims it.
func Unmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	return unmap(b)
}

// UnmapAndClose releases a mapping and its descriptor on a path that has already
// failed. Both errors are dropped rather than returned, because the caller is
// already returning the failure that brought it here and because a failed
// unmap is not recoverable: the mapping stays resident for the life of the
// process, and the only correct response to a corrupt file is to abort anyway.
//
// It lives here so there is one release site for the pair rather than one copy
// per package, which is how the two drifted apart in the first place.
func UnmapAndClose(b []byte, f *os.File) {
	_ = Unmap(b)
	_ = f.Close()
}

// Sync makes a mapping's dirty pages and then its file durable, which is the
// portable form of "this data is on disk now": msync(MS_SYNC) plus fsync, or
// FlushViewOfFile plus FlushFileBuffers on Windows.
//
// ErrUnsupported from the page flush is not a failure. A platform with no msync
// still reaches the page cache on a write, and the file sync is what survives a
// machine crash, so bailing out on the missing syscall would leave those
// platforms strictly less durable than the ones that have it. This is the one
// place that knows the matrix, and it is here rather than at each call site so
// that reading is a decision made once.
func Sync(b []byte, f *os.File) error {
	if err := flushMapping(b); err != nil && !errors.Is(err, ErrUnsupported) {
		return fmt.Errorf("zdb: msync %s: %w", f.Name(), err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("zdb: fsync %s: %w", f.Name(), err)
	}

	return nil
}

// TotalRAM returns the machine's physical memory and whether it could be read.
//
// The boolean is load-bearing. A wrong answer here becomes a wrong flush budget,
// and a wrong flush budget is a data-loss window the caller cannot see, so a
// platform with no source reports false rather than guessing. A caller that
// cannot budget from RAM is still correct on the time-based trigger alone.
func TotalRAM() (uint64, bool) { return totalRAM() }

// checkSize refuses a mapping request that cannot be honoured, in the one place
// every platform shares, so the same file cannot map on one OS and fail on
// another for a reason that belongs to zdb rather than to the kernel.
//
// The int round-trip is the 32-bit limit: a size above max int is a truncation,
// and mapping a truncated length would give a reader a view shorter than the
// header it is about to trust.
func checkSize(f *os.File, size int64) error {
	if size <= 0 {
		return fmt.Errorf("zdb: cannot map a %d byte file %s", size, f.Name())
	}

	if size != int64(int(size)) {
		return fmt.Errorf("zdb: file %s too large to map on this platform", f.Name())
	}

	return nil
}

// checkLength refuses a mapping longer than the file. Every caller in this
// repository maps a file it has just pre-allocated to exactly that length, so
// the check never fires for them; it exists because the failure it prevents is
// invisible from Go.
//
// A mapping that runs past EOF is not an error on either platform. unix faults
// on first access to the missing page, which surfaces as SIGBUS in whichever
// goroutine happened to read a header there. Windows returns zero-filled pages,
// which is worse for a store: the header check reads plausible values and the
// segment decodes to prices that came from nothing. One check here turns both
// into an error at the boundary that can name the file.
func checkLength(f *os.File, size int64) error {
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("zdb: stat %s: %w", f.Name(), err)
	}

	if size > st.Size() {
		return fmt.Errorf("zdb: cannot map %d bytes of %s, which is %d bytes long",
			size, f.Name(), st.Size())
	}

	return nil
}
