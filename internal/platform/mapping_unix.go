//go:build unix

package platform

import (
	"fmt"
	"os"
	"syscall"
)

// mapFile is the POSIX mapping, and it is the one the rest of this package is
// written against: mmap exists on every unix Go supports, so this file has no
// fallbacks and no refusal path.
//
// MAP_SHARED is not a performance choice and is not negotiable. A writer's
// stores have to be visible through a reader's mapping immediately, and
// MAP_PRIVATE would copy the pages instead, so a chart could serve stale bars
// from a store that was being written correctly.
func mapFile(f *os.File, size int64, access Access) ([]byte, error) {
	prot, err := protFor(access)
	if err != nil {
		return nil, fmt.Errorf("zdb: mmap %s: %w", f.Name(), err)
	}

	b, merr := syscall.Mmap(int(f.Fd()), 0, int(size), prot, syscall.MAP_SHARED)
	if merr != nil {
		return nil, fmt.Errorf("zdb: mmap %s: %w", f.Name(), merr)
	}

	return b, nil
}

// protFor turns the portable access into POSIX protections. A value outside the
// two defined ones is refused rather than mapped with protection zero, which
// would succeed and then fault on first touch.
func protFor(access Access) (int, error) {
	switch access {
	case ReadOnly:
		return syscall.PROT_READ, nil
	case ReadWrite:
		return syscall.PROT_READ | syscall.PROT_WRITE, nil
	default:
		return 0, fmt.Errorf("unknown mapping access %d", uint8(access))
	}
}

func unmap(b []byte) error {
	if err := syscall.Munmap(b); err != nil {
		return fmt.Errorf("zdb: munmap: %w", err)
	}

	return nil
}
