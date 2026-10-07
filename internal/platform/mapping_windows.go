//go:build windows

package platform

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// Windows mapping constants from winnt.h. They are not the unix PROT_ values and
// they are not interchangeable, which is the reason this file exists at all:
// there is no syscall.Mmap to call.
const (
	protectRead      = 0x01 // PAGE_READONLY
	protectWrite     = 0x04 // PAGE_READWRITE
	fileMapAllAccess = 0x000F001F
	flushSync        = 0x01 // FlushViewOfFile dwFlags
)

var (
	kernel32     = syscall.NewLazyDLL("kernel32.dll")
	pCreateMap   = kernel32.NewProc("CreateFileMappingW")
	pMapView     = kernel32.NewProc("MapViewOfFile")
	pUnmapView   = kernel32.NewProc("UnmapViewOfFile")
	pFlushView   = kernel32.NewProc("FlushViewOfFile")
	pCloseHandle = kernel32.NewProc("CloseHandle")
)

// mappedView is one live mapping plus the section handle backing it.
type mappedView struct {
	bytes  []byte
	handle syscall.Handle
}

// views keeps section handles alive until their view is unmapped.
//
// The key is the address of the mapping's first byte, because the caller holds
// only that slice and Unmap is given nothing else. Entries are removed in
// unmap, so the registry is correct as long as every caller unmaps exactly
// once. It cannot detect a caller that never does, which is the same limit every
// other hand-rolled release site in a project without a GC-visible handle has.
var views sync.Map // uintptr -> mappedView

// mapFile is the Windows mapping: a section object plus a view onto it.
//
// There is no read-only equivalent of MAP_SHARED here. Both protections ask
// for FILE_MAP_ALL_ACCESS on the view and the section is shared, so a writer's
// stores are visible through a reader's view exactly as on unix. The protection
// is still honoured at the section, so a reader's pages are not writable.
func mapFile(f *os.File, size int64, access Access) ([]byte, error) {
	prot, err := protectFor(access)
	if err != nil {
		return nil, fmt.Errorf("zdb: mmap %s: %w", f.Name(), err)
	}

	// amd64 and arm64 both cap user mappings far below this, and CreateFileMappingW
	// takes the size as two 32-bit halves, so the guard is what stops a wrapped
	// low word from silently requesting a mapping of the wrong size.
	if size > 1<<47 {
		return nil, fmt.Errorf("zdb: file %s too large to map on this platform", f.Name())
	}

	hFile := syscall.Handle(f.Fd())
	hMap, _, cerr := pCreateMap.Call(
		uintptr(hFile),
		0, // lpFileMappingAttributes
		prot,
		uintptr(uint64(size)>>32),
		uintptr(uint64(size)&0xFFFFFFFF),
		0, // lpName
	)
	if hMap == 0 {
		return nil, fmt.Errorf("zdb: CreateFileMappingW %s: %w", f.Name(), cerr)
	}

	addr, _, verr := pMapView.Call(
		hMap,
		fileMapAllAccess,
		0, 0,
		uintptr(uint64(size)),
	)
	if addr == 0 {
		pCloseHandle.Call(hMap)

		return nil, fmt.Errorf("zdb: MapViewOfFile %s: %w", f.Name(), verr)
	}

	// A view is not GC memory, which is why it needs the registry below rather than
	// the collector. The address arrives as a uintptr because that is the only
	// shape syscall.Proc.Call has, and it is converted to a pointer in the very
	// next statement: a uintptr that outlived one more line would already be a
	// reference the collector cannot see.
	//
	// # go vet under GOOS=windows
	//
	// This conversion is reported as "possible misuse of unsafe.Pointer" by the
	// unsafeptr check, and it was reported identically before this package
	// existed. That check accepts a uintptr-to-Pointer conversion only from
	// reflect.Value.Pointer or a reflect header's Data field, so every Windows
	// syscall wrapper is flagged, including x/sys/windows. The conversion here is
	// the documented pattern for a call result that is not heap memory, and the
	// registry below is what makes its lifetime explicit.
	//
	// The reason it is documented rather than silenced: `go vet ./...` on the host
	// never reads this file, because it is windows-only, so the finding cannot
	// reach a CI run that has not asked for GOOS=windows. ponytail: if a
	// cross-platform vet gate is ever added, exclude the unsafeptr check for this
	// file with a nolint comment carrying this reasoning, and check that the
	// exclusion is not reported as unused.
	b := unsafe.Slice((*byte)(unsafe.Pointer(addr)), size)

	// The section handle must outlive the view: Windows ties the view's lifetime
	// to the handle, so letting it be collected would fault an unrelated goroutine
	// later. Hence the registry.
	views.Store(uintptr(unsafe.Pointer(&b[0])), mappedView{
		bytes:  b,
		handle: syscall.Handle(hMap),
	})

	return b, nil
}

// protectFor turns the portable access into a PAGE_ protection. An unrecognised
// value is refused rather than passed through, because CreateFileMappingW takes
// it as a raw uintptr and protection zero is a real, mappable request.
func protectFor(access Access) (uintptr, error) {
	switch access {
	case ReadOnly:
		return protectRead, nil
	case ReadWrite:
		return protectRead | protectWrite, nil
	default:
		return 0, fmt.Errorf("unknown mapping access %d", uint8(access))
	}
}

// unmap releases the view and then the section handle that backs it. The order
// is not interchangeable: closing the handle first would leave the view pointing
// at nothing, and the process could fault between the two calls.
func unmap(b []byte) error {
	v, ok := views.LoadAndDelete(uintptr(unsafe.Pointer(&b[0])))
	if !ok {
		return fmt.Errorf("zdb: munmap: not a mapped view")
	}

	m := v.(mappedView)
	pUnmapView.Call(uintptr(unsafe.Pointer(&m.bytes[0])))
	pCloseHandle.Call(uintptr(m.handle))

	return nil
}

// flushMapping is FlushViewOfFile, which is Windows' msync. A shared write
// reaches the page cache on its own, so this only matters for surviving a
// machine crash rather than a process crash, alongside the file sync.
func flushMapping(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	if r, _, err := pFlushView.Call(
		uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), flushSync,
	); r == 0 {
		return fmt.Errorf("zdb: FlushViewOfFile: %w", err)
	}

	return nil
}
