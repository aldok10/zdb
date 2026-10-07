//go:build unix && (linux || darwin || freebsd || openbsd || dragonfly)

package platform

import (
	"fmt"
	"syscall"
	"unsafe"
)

// flushMapping is msync(MS_SYNC) on the unixes whose syscall package exposes
// SYS_MSYNC.
//
// A MAP_SHARED store reaches the page cache on its own, so this is not what makes
// a write visible to a reader. It is what makes a write survive a machine crash
// rather than only a process crash, which is why Sync runs it and then syncs the
// file as well.
func flushMapping(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	if _, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(syscall.MS_SYNC)); errno != 0 {
		return fmt.Errorf("zdb: msync: %w", errno)
	}

	return nil
}
