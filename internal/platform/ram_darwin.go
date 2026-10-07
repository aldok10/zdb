//go:build darwin

package platform

import (
	"encoding/binary"
	"syscall"
)

// totalRAM returns total physical memory from the hw.memsize sysctl.
//
// # The trap
//
// This function is longer than the Linux one because every obvious route on
// darwin returns something wrong, and all three were measured before this
// implementation was written:
//
//	syscall.Sysctl("hw.memsize")           -> "\x00\x00\x00\x00\x04\x00\x00"  (7 bytes, on a 16 GiB machine)
//	syscall.SysctlUint32("hw.memsize")     -> error "result too large"
//	syscall.SysctlRaw, syscall.Sysinfo     -> undefined on darwin
//
// The first one is the dangerous one. hw.memsize is declared uint64_t in XNU's
// sys/sysctl.h, so its raw bytes are a little-endian integer, but Go's darwin
// Sysctl returns those raw bytes as a string and then discards a terminating
// NUL. On a machine whose RAM is a multiple of 256 MiB the top byte is zero, so
// the byte removed is the most significant one and the value comes back both
// truncated and silently plausible to a reader who does not check the length. A
// strconv.ParseUint on that string fails, which is fortunate: it fails loudly
// rather than reporting a quarter of the real memory.
//
// The raw sysctl(2) would return the value intact, but SYS_SYSCTL is not exported
// for darwin in the standard library, so reaching it means hardcoding a syscall
// number, which AGENTS.md section 4.2 rules out for exactly this situation.
//
// # The invariant
//
// hw.memsize is uint64_t and every supported darwin port is little-endian, so the
// raw bytes are a little-endian uint64. Go removed at most one trailing zero byte.
// Therefore:
//
//	8 bytes -> nothing was removed, the top byte is non-zero, use as is
//	7 bytes -> one zero byte was removed, restoring it gives the exact value
//	4 bytes -> a 4 byte MIB whose top byte is non-zero, use as is
//	other   -> not a shape this reasoning covers, report unknown
//
// The 7 byte case is only reached when the true value is below 2^56, which is
// 72 petabytes of RAM, so the "restore the top byte" step is not an assumption
// about the hardware. It is an assumption about Go's NUL strip, which is one line
// of the standard library and can be re-checked by anyone who doubts it.
func totalRAM() (uint64, bool) {
	s, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0, false
	}

	b := []byte(s)

	switch len(b) {
	case 8:
		return binary.LittleEndian.Uint64(b), true
	case 7:
		var full [8]byte

		copy(full[:], b)

		return binary.LittleEndian.Uint64(full[:]), true
	case 4:
		return uint64(binary.LittleEndian.Uint32(b)), true
	default:
		return 0, false
	}
}
