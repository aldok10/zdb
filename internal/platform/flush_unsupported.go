//go:build unix && !(linux || darwin || freebsd || openbsd || dragonfly)

package platform

import (
	"fmt"
	"runtime"
)

// flushMapping reports that this unix has no msync, rather than pretending the
// pages were flushed. Go's syscall package exports no SYS_MSYNC for netbsd,
// solaris, illumos or aix, and hardcoding a syscall number per architecture is a
// worse trade than losing an optimisation.
//
// The consequence is bounded and stated. A write to a shared mapping reaches the
// page cache on its own, so nothing is lost if the process dies; only a machine
// crash needs disk writeback, and on these platforms that comes from the file
// sync in Sync rather than from here. Sync treats ErrUnsupported as information
// and runs that file sync anyway, which is what keeps these platforms from being
// less durable than the ones that do have the syscall.
//
// ponytail: add golang.org/x/sys/unix and say in the commit why the syscall
// number cannot be hardcoded. One call replaces this whole file.
func flushMapping(b []byte) error {
	return fmt.Errorf("%w: msync on %s", ErrUnsupported, runtime.GOOS)
}
