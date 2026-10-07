package zdb

import (
	"github.com/aldok10/zdb/internal/format"
)

const (
	// IndexSuffix is the file extension of a bucket index file.
	IndexSuffix = format.IndexSuffix
	// SegmentSuffix is the file extension of a segment file.
	SegmentSuffix = format.SegmentSuffix
)

var (
	// ErrCorrupt means a file failed magic, version or bounds validation.
	ErrCorrupt = format.ErrCorrupt
	// ErrUnsupported means this operating system has no primitive for what the caller asked for.
	ErrUnsupported = format.ErrUnsupported
	// ErrInvalid means a record could not be stored as given.
	ErrInvalid = format.ErrInvalid
	// ErrOutOfOrder means a write would break the ascending timestamp invariant.
	ErrOutOfOrder = format.ErrOutOfOrder
	// ErrIndexFull means a key would not fit in its bucket's spare region.
	ErrIndexFull = format.ErrIndexFull
	// ErrBusy means a writer held a seqlock across every retry.
	ErrBusy = format.ErrBusy
)
