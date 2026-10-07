// Package zdb opens a ZDB directory in one call: the writer, the reader and
// the query runner that package writer, package reader and package zql would
// each otherwise be opened and closed by hand.
//
// A store is one record type for its whole life, exactly as it is below this
// package:
//
//	s, err := zdb.Open[record.Bar](zdb.Options{
//		Dir:   "data",
//		Query: zql.Options{MaxBars: 3000},
//	})
//	if err != nil {
//		return err
//	}
//
//	defer s.Close()
//
//	q, err := s.Query()
//	if err != nil {
//		return err
//	}
//
//	for bar, err := range q.Run(`SELECT close FROM "binance/spot/BTCUSDT" LIMIT 100`) {
//		if err != nil {
//			return err
//		}
//
//		fmt.Println(bar.Close)
//	}
//
// What this package owns is the lifecycle: one Open that orders the two handles
// correctly, one Close that releases both, and the runner only a bar store can
// have. The operations stay on the handles, reached as s.Writer.Append and
// s.Reader.Range. Nothing is forwarded, because a forwarding layer would be a
// dozen functions kept in step with reader for no gain, and a read would then
// run through a layer with nothing to add to it.
//
// # Reading what this process just wrote
//
// The first read after the first append needs no refresh: the bucket index is
// opened on demand and the segment is mapped on first use. A later roll or seal
// is different. The reader caches each key's segment chain, so a segment that
// appears after that key was queried stays invisible until s.Reader.Refresh
// maps it and drops the cache.
package zdb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
	"github.com/aldok10/zdb/writer"
	"github.com/aldok10/zdb/zql"
)

// Options configures a store. Dir is the only required field.
type Options struct {
	// Dir is the directory holding segment and index files. It is created if
	// missing, and it is the store's directory: the writer's and the reader's.
	Dir string

	// Writer carries the write path's tuning: SegmentBytes, FileMode,
	// IndexRegionBytes and Sync. Its Dir field must be empty or equal to Dir
	// above, because two sources of truth for where the files live is a
	// disagreement Open refuses rather than resolves by picking one.
	Writer writer.Options

	// Query is the retention ceiling bound to Store.Query: MaxBars caps what a
	// query can return and MaxAge clamps its window against the newest stored
	// bar. It is honoured only by a store holding record.Bar, because ZQL is
	// bar-shaped; a store of any other record type never gets a runner to apply
	// it to, and Store.Query says so instead of returning nil.
	Query zql.Options
}

// Store is an open directory: a writer, a reader, and the ZQL runner a bar
// store can query through. The two handles are safe for concurrent use, and so
// is Store, including Close.
type Store[T record.Record[T]] struct {
	// Writer appends records to the store's directory.
	Writer *writer.DB[T]

	// Reader serves them back, memory-mapped. Call its Refresh after this
	// process has rolled or sealed a segment that this key has already been
	// read from; see the package doc.
	Reader *reader.Reader[T]

	q    *zql.DB
	once sync.Once
	err  error
}

// Open opens opts.Dir for writing and reading, once, and binds the query
// runner.
//
// The writer opens first because it creates the directory and the reader
// refuses one that does not exist. If the reader then fails, the writer is
// closed before Open returns: a partially constructed handle is never handed
// back, because a handle nobody holds is a handle nobody can close.
func Open[T record.Record[T]](opts Options) (*Store[T], error) {
	if opts.Dir == "" {
		return nil, errors.New("zdb: store requires a directory")
	}

	if wd := opts.Writer.Dir; wd != "" && wd != opts.Dir {
		return nil, fmt.Errorf("zdb: Writer.Dir %q disagrees with Dir %q: set Dir and leave Writer.Dir empty", wd, opts.Dir)
	}

	opts.Writer.Dir = opts.Dir

	w, err := writer.Open[T](opts.Writer)
	if err != nil {
		return nil, err
	}

	r, err := reader.Open[T](opts.Dir)
	if err != nil {
		return nil, errors.Join(err, w.Close())
	}

	s := &Store[T]{Writer: w, Reader: r}

	// ZQL is bar-shaped, so only a store holding record.Bar gets a runner. The
	// conversion is a type assertion rather than a type parameter because T is
	// not known here: for T = record.Bar it holds, and for every other record
	// type s.q stays nil and Store.Query refuses with the record's name.
	if br, ok := any(r).(*reader.Reader[record.Bar]); ok {
		s.q = zql.Open(br, opts.Query)
	}

	return s, nil
}

// Query returns the ZQL runner bound to this store, carrying Options.Query as
// its retention ceiling.
//
// It exists for a store holding record.Bar only. ZQL's field enum names the
// OHLC columns, so a runner over any other record type would answer about
// columns the record does not have; such a store gets an error here, and reads
// through Store.Reader instead.
func (s *Store[T]) Query() (*zql.DB, error) {
	if s.q == nil {
		return nil, fmt.Errorf("zdb: ZQL is bar-shaped and this store holds %s: read it through Store.Reader", record.TypeName[T]())
	}

	return s.q, nil
}

// Close seals every shard the writer holds, then releases both handles. Both
// are closed even when the first one fails, and errors.Join reports what each
// of them said.
//
// It is idempotent and safe from any number of goroutines: the first call does
// the work and every call gets the same error back, so a double Close is a
// read rather than a second unmap of memory the reader already gave back.
func (s *Store[T]) Close() error {
	s.once.Do(func() {
		s.err = errors.Join(s.Writer.Close(), s.Reader.Close())
	})

	return s.err
}
