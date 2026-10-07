//go:build !unix && !windows

package platform

import (
	"fmt"
	"os"
	"runtime"
)

// This platform has no portable mapping in the standard library, and zdb refuses
// to run without one. A buffered-read fallback would look like it worked and
// would quietly change the crash and concurrency guarantees the format is built
// on: a MAP_SHARED write is visible to another process the instant it happens,
// and a read into a caller-owned buffer is not.
//
// Every function here therefore reports ErrUnsupported, and Map's refusal is the
// one that matters: it happens when a store is opened, so nothing is created
// and no half-working path is reachable afterwards.

func mapFile(f *os.File, size int64, access Access) ([]byte, error) {
	return nil, fmt.Errorf("%w: %s needs an internal/platform/mapping_%s.go implementation",
		ErrUnsupported, f.Name(), runtime.GOOS)
}

func unmap(b []byte) error {
	return fmt.Errorf("%w: munmap on %s", ErrUnsupported, runtime.GOOS)
}

func flushMapping(b []byte) error {
	return fmt.Errorf("%w: page flush on %s", ErrUnsupported, runtime.GOOS)
}
