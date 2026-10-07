//go:build windows

package platform

import (
	"unsafe"
)

// totalRAM returns total physical memory through GlobalMemoryStatusEx.
//
// The standard library's syscall package on Windows has no memory-status call at
// all, so this goes through the same kernel32 handle mapping_windows.go already
// opens for CreateFileMappingW: one LazyDLL for the package rather than one per
// concern, and no dependency.
//
// GetSystemInfo would be shorter but its TotalPhys member is documented to stop
// at 4 GiB, which is the same truncation bug the darwin probe above exists to
// avoid.
func totalRAM() (uint64, bool) {
	var st struct {
		length               uint32
		load                 uint32
		totalPhys            uint64
		availPhys            uint64
		totalPageFile        uint64
		availPageFile        uint64
		totalVirtual         uint64
		availVirtual         uint64
		availExtendedVirtual uint64
	}
	st.length = uint32(unsafe.Sizeof(st))

	// LazyProc.Call returns a GetLastError that is meaningless on success, so the
	// error is discarded and the boolean comes from the return value.
	r, _, _ := kernel32.NewProc("GlobalMemoryStatusEx").Call(uintptr(unsafe.Pointer(&st)))
	if r == 0 {
		return 0, false
	}

	return st.totalPhys, true
}
