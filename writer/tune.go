package writer

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/aldok10/zdb/internal/platform"
)

// ErrRAMUnknown is returned by NewPolicy when a RAM-derived budget was asked for
// on a platform whose total memory cannot be read. The probes live in
// internal/platform, one file per platform; a target with no probe yet reports
// unknown rather than guessing, because a wrong budget is a silent data-loss
// window.
var ErrRAMUnknown = errors.New("zdb: total system memory is not readable on this platform")

const (
	// DefaultRAMFraction is the share of system memory a store will hold as
	// un-flushed data before it wants a flush.
	//
	// The kernel's own dirty page ceiling is the thing being kept clear. Linux
	// defaults dirty_background_ratio to 10 and dirty_ratio to 20, both as
	// percentages of RAM, and a writer that crosses dirty_ratio is throttled hard
	// rather than failed. One percent leaves a factor of ten of headroom against
	// the background writeback threshold on every default kernel, and it is a
	// budget about loss exposure, not about capacity: the data is already in the
	// page cache either way, this is only how much of it is unprotected.
	DefaultRAMFraction = 0.01

	// MinSyncInterval is the floor on a computed interval, and it exists because
	// of the measurement in AGENTS.md section 4.4 rather than out of caution.
	// Sync costs about 10 ms whether it flushes 60 KB or 3 MB, because a flush is
	// a filesystem commit and the barrier is the cost. Flushing more often than
	// this spends barrier time protecting bytes the page cache was going to write
	// back on its own, and per-tick durability measured 119x the cost of batched
	// for no reduction in what a crash could lose.
	MinSyncInterval = time.Second
)

// TuneOptions configures a Policy. The zero value is valid and means: one percent
// of RAM, no time ceiling beyond that, and the real clock.
type TuneOptions struct {
	// RAMFraction is the share of system memory this store may hold as un-flushed
	// data. Zero uses DefaultRAMFraction. Negative disables the RAM budget, which
	// is the only way to run on a platform whose memory this package cannot read
	// without also choosing MaxInterval.
	RAMFraction float64

	// MaxInterval is the longest the caller will tolerate before a flush,
	// independent of how much data has accumulated. Zero means the RAM budget
	// alone decides. This is the caller's recovery point objective in time form:
	// a store configured with a minute loses at most a minute of appends to a
	// power cut, and never more than the RAM budget regardless.
	MaxInterval time.Duration

	// Now is the clock. Nil uses time.Now. It exists so a test can advance time
	// without sleeping, and because a rate cannot be measured without a clock.
	Now func() time.Time
}

// Policy decides when a store should flush, from the machine it runs on and the
// rate it is actually being written at.
//
// It is a calculation, not a scheduler. It runs no goroutine, opens nothing, and
// allocates nothing after construction, so wiring it in cannot introduce a leak
// or make Append pay for it. The caller polls it from the loop it already has:
//
//	for bar := range ticks {
//	    if err := db.Append(key, bar); err != nil {
//	        return err
//	    }
//	    if err := db.SyncIfDue(); err != nil {
//	        return err
//	    }
//	}
//
// # What is derived from what
//
// Total RAM and the append rate are the two inputs. Total RAM is read once, from
// a native source per platform, because a float budget is as good as the number
// under it. The append rate is observed, not declared: it is the bytes this store
// has appended divided by the time since it started appending. A rate cannot come
// from a specification, because the specification describes the device and the
// rate describes the caller.
//
//	budget   = RAMFraction x total_ram
//	rate     = bytes_appended / seconds_elapsed
//	derived  = max(MinSyncInterval, budget / rate)      a rate of zero gives zero
//	interval = min(derived, MaxInterval)                 MaxInterval 0 means no ceiling
//	due      = at_risk >= budget  OR  seconds_since_flush >= interval
//
// Two independent triggers, because they answer different questions. The budget
// branch is the hardware-derived one: it fires when the dirty set has grown to a
// fixed fraction of RAM, which is the condition that would otherwise let the
// kernel throttle a writer. The interval branch is the caller's one: it fires on
// time, and it applies even before any rate has been observed, which is the case
// of a slow writer that would take an hour to reach the budget.
//
// The interval is a lifetime average rather than a windowed one on purpose. A
// one-second window tracks a burst correctly and then decays to nonsense the
// moment the burst stops, and sizing a flush interval from a decayed rate means
// flushing far more often than the exposure warrants. Call Rebase if the write
// rate changes by a large factor and the average is no longer representative.
//
// # What this does not compute
//
// Device throughput, which is the missing third term, and the reason this is not
// called an optimum. How many bytes per second this disk can absorb is not
// knowable from RAM, CPU count or page size, and the honest ways to get it are to
// measure by writing, which perturbs the thing being measured, or to read the
// device's advertised figures, which is what ZDB rejects elsewhere. So the budget
// bounds the dirty set relative to RAM, and the kernel's own writeback stays
// responsible for the rest. That is enough to avoid a throttle and it is not
// enough to promise an IOPS figure.
//
// ponytail: throughput becomes knowable without a destructive probe once a
// caller volunteers a measurement instead of having the store take one. Options
// gets a ThroughputBytesPerSecond, and Interval divides the budget by the smaller
// of the observed rate and that figure, so a store that is outrunning its device
// stops trusting the kernel to absorb the difference. Add it when a caller has a
// real number and finds the RAM budget is not holding the dirty set.
//
// # Why not flush automatically
//
// Two answers were available and both are worse. Flushing from inside Append makes
// a 176 ns operation occasionally cost 10 ms, which is a latency cliff on the hot
// path and would make every throughput number in AGENTS.md untrue depending on
// sample. A background timer would be genuinely automatic, and it needs a
// shutdown path that owns a mapping flush, which DB.Close has no reason to know
// about. Polling from the caller's loop costs one atomic load per append and
// keeps the flush exactly as rare as the calculation says it should be.
type Policy struct {
	budget      uint64
	maxInterval time.Duration
	now         func() time.Time

	// Atomic because appends may come from several goroutines while Sync is
	// called from one. All four are plain counters; none of them boxes a value
	// into an interface, and none allocates.
	start      atomic.Int64 // unix nanos of the first Append, 0 until then
	totalBytes atomic.Uint64
	atRisk     atomic.Uint64
	lastFlush  atomic.Int64

	totalRAM uint64
	ramKnown bool
}

// NewPolicy returns a Policy sized for the running machine.
//
// It reports ErrRAMUnknown only when a RAM-derived budget was actually asked for
// and the memory cannot be read. A policy with no RAM budget still works and is
// governed by MaxInterval alone; pass RAMFraction: -1 and a MaxInterval to ask
// for that deliberately rather than have it happen as a surprise on a platform
// this package cannot probe.
func NewPolicy(o TuneOptions) (*Policy, error) {
	ram, known := platform.TotalRAM()

	frac := o.RAMFraction
	switch {
	case frac < 0: // explicit opt out
		frac = 0
	case frac == 0:
		frac = DefaultRAMFraction
	}

	if frac > 0 && !known {
		return nil, ErrRAMUnknown
	}

	p := &Policy{
		maxInterval: o.MaxInterval,
		now:         o.Now,
		totalRAM:    ram,
		ramKnown:    known,
	}
	if p.now == nil {
		p.now = time.Now
	}

	if frac > 0 {
		p.budget = uint64(float64(ram) * frac)
	}

	return p, nil
}

// RAMKnown reports whether the policy has a RAM-derived budget. False means
// MaxInterval is the only input in play.
func (p *Policy) RAMKnown() bool { return p.ramKnown }

// BudgetBytes is the un-flushed volume that makes a flush due, or zero when there
// is no RAM budget.
func (p *Policy) BudgetBytes() uint64 { return p.budget }

// RAMBytes is the total system memory the budget was derived from.
func (p *Policy) RAMBytes() uint64 { return p.totalRAM }

// AtRiskBytes is the volume appended since the last Flushed.
func (p *Policy) AtRiskBytes() uint64 { return p.atRisk.Load() }

// Append records n bytes written through the mapping. It is the only call on the
// write path, it is two atomic adds, and it allocates nothing.
func (p *Policy) Append(n int) {
	if n <= 0 {
		return
	}

	p.atRisk.Add(uint64(n))
	p.totalBytes.Add(uint64(n))

	if p.start.Load() == 0 {
		p.start.CompareAndSwap(0, p.now().UnixNano())
	}
}

// Flushed resets the exposure after a successful Sync. Call it after Sync returns
// nil and not before, because a failed flush has not moved anything.
func (p *Policy) Flushed() {
	p.atRisk.Store(0)
	p.lastFlush.Store(p.now().UnixNano())
}

// Rebase restarts rate measurement, keeping the current exposure. Call it when
// the write rate changes by a large factor, since the interval is sized from an
// average since the first append.
func (p *Policy) Rebase() {
	p.start.Store(p.now().UnixNano())
	p.totalBytes.Store(0)
}

// Interval returns the flush interval the observed rate implies for the current
// budget, or zero when there is no budget to divide. It is never below
// MinSyncInterval, which is what stops a fast writer from being told to flush
// several times a second on the strength of a barrier that costs 10 ms.
func (p *Policy) Interval() time.Duration {
	start := p.start.Load()
	if p.budget == 0 || start == 0 {
		return 0
	}

	elapsed := time.Duration(p.now().UnixNano() - start)

	total := float64(p.totalBytes.Load())
	if elapsed <= 0 || total <= 0 {
		return 0
	}

	rate := total / elapsed.Seconds() // bytes per second
	if rate <= 0 {
		return 0
	}
	// budget/rate is a count of seconds, and a time.Duration is a count of
	// nanoseconds. The multiply is the difference between the interval this
	// policy is supposed to compute and the MinSyncInterval floor, because
	// time.Duration(10) is ten nanoseconds rather than ten seconds.
	d := time.Duration(float64(p.budget) / rate * float64(time.Second))
	if d < MinSyncInterval {
		return MinSyncInterval
	}

	return d
}

// effectiveInterval is the shorter of the rate-derived interval and the caller's
// tolerance. A MaxInterval of zero means the caller expressed no time preference,
// so the rate-derived interval applies on its own.
//
// It is also what makes MaxInterval work before any rate exists: with one record
// appended the interval is zero, and zero would mean never due, when what the
// caller asked for was a minute.
func (p *Policy) effectiveInterval() time.Duration {
	derived := p.Interval()
	if p.maxInterval > 0 && (derived == 0 || p.maxInterval < derived) {
		return p.maxInterval
	}

	return derived
}

// Due reports whether a flush is due now. It allocates nothing and is safe to
// call on every append.
func (p *Policy) Due() bool {
	risk := p.atRisk.Load()
	if risk == 0 {
		return false
	}

	if p.budget > 0 && risk >= p.budget {
		return true
	}

	iv := p.effectiveInterval()
	if iv <= 0 {
		return false
	}

	last := p.lastFlush.Load()
	if last == 0 {
		last = p.start.Load()
	}

	return p.now().UnixNano()-last >= int64(iv)
}
