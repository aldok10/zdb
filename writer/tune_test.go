package writer

import (
	"testing"
	"time"

	"github.com/aldok10/zdb/internal/platform"
	"github.com/aldok10/zdb/reader"
	"github.com/aldok10/zdb/record"
)

// The policy is a calculation with one input this package cannot control, so the
// tests split in two. The formula is checked against hand-computed values through
// a Policy built directly, which is deterministic on any machine. NewPolicy is
// then checked against the real system, which is the part that varies.

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
}

// policy builds a Policy with a chosen budget, bypassing totalRAM so the formula
// is testable without depending on the host's memory.
func policy(budget uint64, maxInterval time.Duration, c *fakeClock) *Policy {
	return &Policy{
		budget:      budget,
		maxInterval: maxInterval,
		now:         c.Now,
		totalRAM:    budget,
		ramKnown:    true,
	}
}

// interval = budget / rate, with rate observed over the policy's lifetime.
// 10 MB of budget against 1 MB/s of appends is a 10 second interval.
func TestPolicyIntervalIsBudgetOverRate(t *testing.T) {
	c := newFakeClock()
	p := policy(10_000_000, 0, c)

	p.Append(1_000_000) // 1 MB, at t0
	c.advance(time.Second)

	if got, want := p.Interval(), 10*time.Second; got != want {
		t.Fatalf("interval: got %v, want %v (budget 10MB / rate 1MB/s)", got, want)
	}
}

// A burst of double the rate halves the interval, which is the whole point of
// observing the rate rather than declaring it.
func TestPolicyIntervalTracksTheRate(t *testing.T) {
	c := newFakeClock()
	p := policy(10_000_000, 0, c)

	p.Append(4_000_000) // 4 MB in 1 second
	c.advance(time.Second)

	if got, want := p.Interval(), 2500*time.Millisecond; got != want {
		t.Fatalf("interval: got %v, want %v (budget 10MB / rate 4MB/s)", got, want)
	}
}

// A budget far below what the store can write in a second must not produce an
// interval below MinSyncInterval. Sync costs about 10 ms whatever it flushes, so
// being told to flush every 100 us would spend all its time in barriers.
func TestPolicyIntervalIsFlooredAtMinSyncInterval(t *testing.T) {
	c := newFakeClock()
	p := policy(100, 0, c) // a 100 byte budget against 1 MB/s is 100 us

	p.Append(1_000_000)
	c.advance(time.Second)

	if got := p.Interval(); got != MinSyncInterval {
		t.Fatalf("interval: got %v, want the %v floor", got, MinSyncInterval)
	}
}

// The RAM ceiling binds before the interval does, which is the branch that keeps
// the dirty set clear of the kernel's own throttle.
func TestPolicyDueWhenBudgetReached(t *testing.T) {
	c := newFakeClock()
	p := policy(1000, 0, c)

	p.Append(999)

	if p.Due() {
		t.Fatal("due at 999 bytes against a 1000 byte budget")
	}

	p.Append(1)

	if !p.Due() {
		t.Fatal("not due at 1000 bytes against a 1000 byte budget")
	}
}

// With a budget nothing will reach, the caller's own tolerance decides. This is
// the recovery point objective in time form.
func TestPolicyDueWhenMaxIntervalElapsed(t *testing.T) {
	c := newFakeClock()
	p := policy(1_000_000_000, 5*time.Second, c)

	p.Append(100)
	c.advance(4 * time.Second)

	if p.Due() {
		t.Fatal("due after 4s with a 5s tolerance")
	}

	c.advance(1 * time.Second)

	if !p.Due() {
		t.Fatal("not due after 5s with a 5s tolerance")
	}
}

// A store with nothing appended has nothing to lose, so no wall clock reading can
// make a flush due. This is what stops an idle store from being flushed forever.
func TestPolicyIdleStoreIsNeverDue(t *testing.T) {
	c := newFakeClock()
	p := policy(1_000_000_000, time.Second, c)

	c.advance(24 * time.Hour)

	if p.Due() {
		t.Fatal("due after a day with nothing appended")
	}
}

// A failed flush has moved nothing, so the exposure must survive it. Flushed is
// therefore a separate call the caller makes after Sync returns nil.
func TestPolicyFlushedResetsExposure(t *testing.T) {
	c := newFakeClock()
	p := policy(1000, 0, c)

	p.Append(2000)

	if !p.Due() {
		t.Fatal("not due at 2000 bytes against a 1000 byte budget")
	}

	p.Flushed()

	if got := p.AtRiskBytes(); got != 0 {
		t.Fatalf("at risk after Flushed: got %d, want 0", got)
	}

	if p.Due() {
		t.Fatal("due immediately after a successful flush")
	}
}

// Rebase keeps the exposure and discards the rate measurement, so a caller can
// re-average after the write rate changes by a large factor.
func TestPolicyRebaseKeepsExposure(t *testing.T) {
	c := newFakeClock()
	p := policy(10_000_000, 0, c)

	p.Append(1_000_000)
	c.advance(time.Second)
	p.Rebase()

	if got := p.AtRiskBytes(); got != 1_000_000 {
		t.Fatalf("Rebase changed the exposure: got %d, want 1000000", got)
	}
	// Rate measurement is gone, so there is no interval to report yet.
	if got := p.Interval(); got != 0 {
		t.Fatalf("interval right after Rebase: got %v, want 0", got)
	}

	p.Append(1_000_000)
	c.advance(time.Second)

	if got, want := p.Interval(), 10*time.Second; got != want {
		t.Fatalf("interval after Rebase: got %v, want %v", got, want)
	}
}

// Asking for no RAM budget at all is legitimate and leaves MaxInterval in charge.
// The unsafe direction, a positive fraction on a platform whose memory cannot be
// read, is an error rather than a silent zero budget that would flush forever.
func TestPolicyRAMBudgetIsExplicit(t *testing.T) {
	if _, err := NewPolicy(TuneOptions{RAMFraction: -1}); err != nil {
		t.Fatalf("RAMFraction -1 should be allowed: %v", err)
	}

	if _, err := NewPolicy(TuneOptions{}); err != nil {
		t.Fatalf("the default policy needs RAM: %v", err)
	}
}

// The real host. This is the assertion that the native read works at all, and it
// fails loudly rather than yielding a budget derived from nothing.
func TestPolicyBudgetMatchesSystemMemory(t *testing.T) {
	ram, ok := platform.TotalRAM()
	if !ok {
		t.Skip("total memory is not readable on this platform; see internal/platform/ram_*.go")
	}

	if ram < 1<<30 {
		t.Fatalf("total memory %d is implausibly small, the native read is probably wrong", ram)
	}

	p, err := NewPolicy(TuneOptions{RAMFraction: 0.01})
	if err != nil {
		t.Fatal(err)
	}

	if !p.RAMKnown() {
		t.Fatal("RAMKnown is false on a platform that reported memory")
	}

	if want := uint64(float64(ram) * 0.01); p.BudgetBytes() != want {
		t.Fatalf("budget: got %d, want %d (1%% of %d)", p.BudgetBytes(), want, ram)
	}

	if p.RAMBytes() != ram {
		t.Fatalf("RAMBytes: got %d, want %d", p.RAMBytes(), ram)
	}
}

// The gate from AGENTS.md section 6.1 applies to the write path too. Append and
// Due are called on every bar, so an allocation here would be a hot-path
// regression that no benchmark would catch.
func TestPolicyHotPathIsZeroAllocation(t *testing.T) {
	p, err := NewPolicy(TuneOptions{})
	if err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		p.Append(record.BarSize)
		p.Due()
	})
	if allocs != 0 {
		t.Fatalf("Append and Due allocated %v times per bar, want 0", allocs)
	}
}

// A store opened without a Policy never flushes itself, so adopting one is not a
// change in behaviour for a caller that has not asked for it.
func TestSyncIfDueIsFalseWithoutAPolicy(t *testing.T) {
	db, err := Open[record.Bar](Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if db.Policy() != nil {
		t.Fatal("a store opened without Sync should have no Policy")
	}

	for i := range 1000 {
		if err := db.Append(testTuneKey, barAt(i)); err != nil {
			t.Fatal(err)
		}
	}

	flushed, err := db.SyncIfDue()
	if err != nil {
		t.Fatal(err)
	}

	if flushed {
		t.Fatal("SyncIfDue flushed a store with no Policy")
	}
}

// With a policy the same loop adapts. A budget of ten records against fifty
// appends means the flush count is predictable rather than incidental, which is
// what proves the accumulation is being counted and not merely guarded.
func TestSyncIfDueFlushesWhenTheBudgetIsReached(t *testing.T) {
	dir := t.TempDir()
	c := newFakeClock()

	const n = 50

	p := policy(10*record.BarSize, 0, c)

	db, err := Open[record.Bar](Options{Dir: dir, Sync: p})
	if err != nil {
		t.Fatal(err)
	}

	if db.Policy() != p {
		t.Fatal("the store did not take the installed Policy")
	}

	flushes := 0

	for i := range n {
		if err := db.Append(testTuneKey, barAt(i)); err != nil {
			t.Fatal(err)
		}

		flushed, err := db.SyncIfDue()
		if err != nil {
			t.Fatal(err)
		}

		if flushed {
			flushes++
			if got, want := p.AtRiskBytes(), uint64(0); got != want {
				t.Fatalf("at risk right after flush %d: got %d, want %d", flushes, got, want)
			}
		} else if (i+1)%10 == 0 {
			// Every tenth append crosses the budget, so those five must flush and
			// the forty-five in between must not. Getting the phase wrong here
			// would pass a test that never checked the accounting at all.
			t.Fatalf("append %d did not flush with the budget already reached", i)
		}
	}

	if want := n / 10; flushes != want {
		t.Fatalf("flushes: got %d, want %d for %d records under a ten-record budget", flushes, want, n)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := reader.Open[record.Bar](dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	bars, err := r.Range(testTuneKey, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(bars) != n {
		t.Fatalf("read back %d bars, want %d", len(bars), n)
	}
}

const testTuneKey = "tune/BTCUSDT"

func barAt(i int) record.Bar {
	const base = 1_704_067_200_000

	return record.Bar{
		Datetime: base + int64(i)*1000,
		Open:     100, High: 101, Low: 99, Close: 100.5,
		Volume: uint64(1000 + i),
	}
}
