//go:build linux

package platform

import (
	"bytes"
	"os"
	"strconv"
)

// totalRAM returns total system memory from /proc/meminfo.
//
// Linux is the only platform in this package that also exposes the kernel's own
// dirty page budget, in /proc/sys/vm/dirty_bytes and dirty_ratio. This reads the
// memory that budget is a fraction of. Reading the budget itself is deliberately
// not done: the flush policy derives its own budget from RAM, and the kernel's
// thresholds are a machine-wide setting that belongs to the machine, not to one
// store inside it.
func totalRAM() (uint64, bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, false
	}

	i := bytes.Index(data, []byte("MemTotal:"))
	if i < 0 {
		return 0, false
	}

	rest := data[i+len("MemTotal:"):]
	if j := bytes.IndexByte(rest, '\n'); j >= 0 {
		rest = rest[:j]
	}

	fields := bytes.Fields(rest)
	if len(fields) == 0 {
		return 0, false
	}

	kb, err := strconv.ParseUint(string(fields[0]), 10, 64)
	if err != nil {
		return 0, false
	}

	return kb * 1024, true
}
