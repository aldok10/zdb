//go:build !linux && !darwin && !windows

package platform

// totalRAM has no source on this platform.
//
// This reports unknown rather than approximating, because a wrong answer here
// becomes a wrong flush budget, and a wrong flush budget is a data loss window
// the caller cannot see. A flush policy built without it still works: the
// caller's MaxInterval alone decides when to flush, and RAMKnown reports false
// so the caller knows which of the two inputs is missing.
//
// ponytail: every remaining target has the answer and each needs one file, which
// is why this is a stub and not a guess. freebsd and dragonfly read
// vm.stats.vm.v_page_count and multiply by the page size; openbsd reads
// hw.physmem and netbsd reads hw.physmem64, both as a string so nothing above
// 4 GiB truncates the way SysctlUint32 would. solaris and illumos need kstat
// rather than sysctl, which is a different shape entirely. js/wasm should use
// js.Value for performance.memory, since a wasm host's RAM is another
// process's memory and budgeting against it is misleading anyway. Add them when
// someone runs a store on those targets.
func totalRAM() (uint64, bool) { return 0, false }
