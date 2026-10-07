package zql

import (
	"errors"
	"iter"
	"slices"
	"time"

	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
)

// ErrNoMatch is what First returns when a query is valid and matched nothing. It
// is distinct from an error, because "no such bar" is an answer and not a fault.
var ErrNoMatch = errors.New("zql: no matching bar")

// Options is the retention ceiling every query on a DB is held to.
//
// Retention here is a read-side view, not storage reclamation: it decides what a
// query can see, and the bytes stay on disk. That is the cheap half and it costs
// nothing on the write path. Reclaiming them needs segment deletion plus an
// index rewrite, which is a format change, and until that exists this is the
// whole of what "30 days" means.
//
// MaxAge is measured against the newest bar in the store, not the wall clock. A
// store that stopped receiving data six months ago still has bars a chart wants
// to draw; ageing them out by wall clock would leave the caller with nothing.
// ponytail: a wall-clock variant needs an explicit `Before time.Time`; add it if a
// caller ever needs retention tied to when data arrived rather than how fresh the
// store is.
type Options struct {
	// MaxBars caps how many bars any query can return, taking the newest. Zero
	// or less means no cap. A query's own LIMIT wins when it is the smaller of
	// the two, so retention can only ever narrow a result.
	MaxBars int
	// MaxAge drops anything older than this, relative to the newest stored bar.
	// Zero means no age limit.
	MaxAge time.Duration
}

// DB runs queries against a store. It is a thin view over a Reader: the Reader
// stays responsible for mapping and reading, and nothing here opens a file.
type DB struct {
	r    *reader.Reader[record.Bar]
	opts Options
}

// Open wraps an open Reader. The Reader's lifetime still governs the DB; Close
// the Reader, not the DB.
// Open binds a query runner to a candle reader. ZQL is bar-shaped on purpose: its
// Field enum names OHLC fields, so it cannot express a tick condition. A tick
// store is read through reader directly. ponytail: a tick-aware ZQL needs its
// own field registry, and nothing here is reusable for it, so it should be a
// separate language rather than this one with a second enum.
func Open(r *reader.Reader[record.Bar], opts Options) *DB {
	return &DB{r: r, opts: opts}
}

// Run compiles ZQL text and streams the result.
//
// A syntax error arrives through the sequence rather than as a second return
// value, because Go cannot range over a two-value call and assigning to a variable
// first is the wrong shape for the most common thing a caller does. The error is
// still reported before any bar is produced, so nothing is half read. A caller
// that wants to reject bad text at build time calls Parse directly and holds the
// Query.
func (db *DB) Run(src string) iter.Seq2[record.Bar, error] {
	q, err := Parse(src)
	if err != nil {
		return errSeq(err)
	}

	return db.Query(q)
}

// Query streams the bars matching q, oldest first, filtered and projected. Stop
// the loop early and the walk stops with it.
//
// Desc is not honoured as an order here: the mapping is read forward, and
// reversing a window means holding it. Collect applies Desc; a caller that
// streams wants ascending order anyway, which is the order a chart draws in.
// Desc still decides which end Limit takes — the newest matches — so a limited
// descending query holds at most its own limit bars before streaming them.
//
// Limit counts the rows that survive Conds, never the rows the scan walked
// through, so a filter can never shrink a result below the rows it matched.
func (db *DB) Query(q Query) iter.Seq2[record.Bar, error] {
	conds := q.scanConds()

	from, to, err := db.plan(q)
	if err != nil {
		return errSeq(err)
	}

	fields := q.Fields
	if fields == 0 {
		fields = FAll
	}

	limit := q.Limit

	bounds := condBounds(conds)

	return func(yield func(record.Bar, error) bool) {
		// A descending limit cannot be answered from the front: the newest
		// matches sit at the end of an ascending walk. Walking the cursor
		// backwards stops at the limit-th match, so a filtered DESC LIMIT 10
		// decodes tens of records rather than all of them. Query still
		// streams ascending (the contract TestDescIsCollectOnly pins), so the
		// bounded match slice is replayed in reverse.
		if q.Desc && limit > 0 {
			buf := make([]record.Bar, 0, limit)

			var c reader.Cursor[record.Bar]

			if err := db.r.CursorDescFiltered(q.Key, from, to, 0, bounds, &c); err != nil {
				yield(record.Bar{}, err)

				return
			}

			var bar record.Bar
			for c.Next(&bar) {
				if len(conds) != 0 && !matches(conds, bar) {
					continue
				}

				buf = append(buf, bar)

				if len(buf) == limit {
					break
				}
			}

			if err := c.Err(); err != nil {
				yield(record.Bar{}, err)

				return
			}

			for i := len(buf) - 1; i >= 0; i-- {
				if !yield(mask(buf[i], fields), nil) {
					return
				}
			}

			return
		}

		seen := 0

		for bar, err := range db.r.AllBounds(q.Key, from, to, 0, bounds) {
			if err != nil {
				yield(record.Bar{}, err)

				return
			}

			if len(conds) != 0 && !matches(conds, bar) {
				continue
			}

			if !yield(mask(bar, fields), nil) {
				return
			}

			seen++

			if limit > 0 && !q.Desc && seen == limit {
				return
			}
		}
	}
}

// First returns the first matching bar, oldest first, and stops there. A lookup
// wants one row, and an iterator that can stop is how one row costs one row.
func (db *DB) First(q Query) (record.Bar, error) {
	for bar, err := range db.Query(q) {
		if err != nil {
			return record.Bar{}, err
		}

		return bar, nil
	}

	return record.Bar{}, ErrNoMatch
}

// Collect runs q and holds every match. This is the shape that allocates, and it
// exists for the caller who genuinely wants the slice; it also honours Desc.
func (db *DB) Collect(q Query) ([]record.Bar, error) {
	var out []record.Bar

	for bar, err := range db.Query(q) {
		if err != nil {
			return nil, err
		}

		out = append(out, bar)
	}

	if q.Desc {
		slices.Reverse(out)
	}

	return out, nil
}

// plan resolves a query against the retention ceiling and returns the window to
// binary-search. Folding the Datetime conditions here is what keeps a filter on
// time O(log n) instead of a test per record.
//
// MaxBars and MaxAge both narrow from: MaxAge to an age cutoff, MaxBars to the
// floor of its newest bars (RetentionFloor), so the reader walks exactly the
// visible window and never buffers a tail it will only discard.
func (db *DB) plan(q Query) (from, to uint64, err error) {
	if q.Key == "" {
		return 0, 0, errors.New("zql: query has no key")
	}

	scan := db.opts.MaxBars
	if scan < 0 {
		scan = 0
	}

	from, to = q.window()

	if db.opts.MaxAge > 0 {
		_, last, berr := db.r.Bounds(q.Key)
		if berr != nil {
			return 0, 0, berr
		}

		if ms := db.opts.MaxAge.Milliseconds(); ms > 0 && last > uint64(ms) {
			if cut := last - uint64(ms); cut > from {
				from = cut
			}
		}
	}

	if scan > 0 {
		// The newest-MaxBars view starts at the floor; without the clamp the
		// ascending walk would have to buffer the same tail the descent
		// already walks backwards through.
		floor, err := db.r.RetentionFloor(q.Key, from, to, uint64(scan))
		if err != nil {
			return 0, 0, err
		}

		if floor > from {
			from = floor
		}
	}

	return from, to, nil
}

func errSeq(err error) iter.Seq2[record.Bar, error] {
	return func(yield func(record.Bar, error) bool) {
		yield(record.Bar{}, err)
	}
}
