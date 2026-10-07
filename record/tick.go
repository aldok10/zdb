package record

import (
	"fmt"
	"time"
)

// Tick is one bid/ask/last print as a caller sees it: prices as doubles,
// datetime signed. It is the only tick type this package exports, because it is
// the only form a caller needs. What actually goes in a file is tick, which is
// unexported and has no float in it at all.
//
// # What a tick record deliberately leaves out
//
// A print carries prices, volume and a validity mask, and nothing a feed repeats
// on every tick:
//
//   - Symbol is not a field. It is the shard key, already stored once in every
//     segment header, so a per-record copy is duplication of information the file
//     already has. A second dimension such as venue or account belongs in the key
//     path, so "binance/spot/BTCUSDT" carries both.
//   - Volume and Flags are both present even when a source reports only one of
//     them. A print that changed a single price carries all three prices, with
//     Flags naming which are current. That costs 8 bytes per tick and saves a
//     second record type, which is the cheaper trade at any tick rate worth
//     storing.
//   - Source-specific padding is not stored. Feeds pad their structs for their
//     own ABI, and ZDB is not binary-compatible with any source and does not
//     intend to be, so padding is bytes per record that nothing reads.
//
// None of this is dropped silently: a tick store is a different directory with a
// different record width, and the width in every segment header is checked against
// TickSize on open. A print feed and a per-session aggregate feed, which carries
// counters and greeks rather than prints, therefore cannot be confused for one
// another.
//
// # Timestamps are microseconds
//
// Datetime is unix **microseconds**, not the milliseconds a candle uses. Some
// sources declare a millisecond field in their published struct while the value
// that actually arrives carries microseconds, and the difference decides whether
// the data is storable at all: a shard requires strictly increasing timestamps,
// and a liquid symbol produces several ticks per millisecond, so a millisecond key
// rejects real ticks with ErrOutOfOrder. Microseconds is what the source actually
// provides.
//
// The consequence is that the header's firstTs and lastTs, like every timestamp in
// a tick store, are microseconds. There is no sentinel for "unbounded" in a query
// bound, and 0 is not a microsecond timestamp, so zero still means unbounded.
//
// # Prices are signed
//
// Bid, Ask and Last are float64 at the API boundary and fixed-point at
// PriceScale on disk, stored as an unsigned word holding a two's complement
// value. Signed, unlike bar: crude oil traded negative in April 2020,
// and a tick store that refuses to store a negative bid is a tick store with a
// hole in it exactly when it matters.
//
// The directive below writes Tick's schema instead of reflecting over it. It runs
// inside the record package, so the qualifier is empty, and unit=us matches the
// microsecond stamps the doc above spends three paragraphs justifying.
//
//go:generate go run github.com/aldok10/zdb/cmd/gen-schema-record -qualify= -type Tick
type Tick struct {
	// Datetime is unix microseconds and must be strictly greater than the last
	// tick stored for this key. It is the field the binary search orders on.
	Datetime int64 `zdb:"datetime,time,unit=us"`
	// Bid, Ask and Last are prices at PriceScale precision. All three are tagged
	// price: they are float64 at the boundary and fixed-point on disk, which is
	// the condition ScaleSignedPrice exists for, and a negative bid is a value a
	// tick store must not refuse.
	Bid  float64 `zdb:"bid,price"`
	Ask  float64 `zdb:"ask,price"`
	Last float64 `zdb:"last,price"`
	// Volume is the trade volume the tick reports, or the tick volume when the
	// source distinguishes them. TickVolume on a candle is tick count; here it
	// is whatever the source means, and 0 means "no volume reported".
	Volume uint64 `zdb:"volume"`
	// Flags names which of Bid, Ask, Last and Volume are valid on this tick.
	Flags uint64 `zdb:"flags"`
	// VolumeExt is the extended-accuracy volume some sources carry alongside
	// Volume. Zero when the source reports none.
	VolumeExt uint64 `zdb:"volume_ext"`
}

// tick is a print in its stored form. Every field is fixed-point at PriceScale
// and unsigned, so a record holds no float and the comparisons the binary search
// makes are integer comparisons. That is the same trade bar makes, and for the
// same reason: decode stays free of float work, and tick does not escape this
// package because there is no reason for a caller to hold one.
//
// Every field is uint64, including the three prices, even though a price is
// signed. The sign is carried in the word rather than in the field type, for two
// reasons. The segment header reads every record word as unsigned, so the stored
// form now matches how the file is actually read instead of needing a cast in
// each direction; and a stored record becomes one uniform block of uint64, with
// no mixed signedness for a reader of the bytes to reason about.
//
// A price is therefore two's complement in an unsigned field, and the sign is
// reapplied in exactly one place on the way out: signed, in float. Nothing else
// in this file may reinterpret these words as signed, which is what keeps the
// conversion auditable. The ceiling is math.MaxInt64 at PriceScale, so about 92
// billion, which is past any price that has traded.
type tick struct {
	Datetime  uint64
	Bid       uint64
	Ask       uint64
	Last      uint64
	Volume    uint64
	Flags     uint64
	VolumeExt uint64
}

// TickSize is the encoded width of one Tick: seven 8-byte fields, no padding.
const TickSize = 7 * 8

// Tick flags. They say which of the record's fields this tick actually carries,
// so a consumer does not read a stale Bid as a current one.
const (
	// TickFlagRaw means the tick's prices are unprocessed exchange values.
	TickFlagRaw uint64 = 0x01
	// TickFlagBid means Bid is valid.
	TickFlagBid uint64 = 0x02
	// TickFlagAsk means Ask is valid.
	TickFlagAsk uint64 = 0x04
	// TickFlagLast means Last is valid.
	TickFlagLast uint64 = 0x08
	// TickFlagVolume means Volume is valid.
	TickFlagVolume uint64 = 0x10
	// TickFlagBuy means the tick is a buy-side print.
	TickFlagBuy uint64 = 0x20
	// TickFlagSell means the tick is a sell-side print.
	TickFlagSell uint64 = 0x40
)

// Tick field offsets. Fixed at authoring time rather than computed, so a record
// position is addressable arithmetic and the seqlock's fixed-width publish holds.
const (
	tickOffDatetime  = 0
	tickOffBid       = 8
	tickOffAsk       = 16
	tickOffLast      = 24
	tickOffVolume    = 32
	tickOffFlags     = 40
	tickOffVolumeExt = 48
)

var _ Record[Tick] = Tick{}

// RecordSize is the encoded width of one Tick. It satisfies Record[Tick].
func (Tick) RecordSize() int { return TickSize }

// Stamp is Tick's Datetime, the value a shard orders on. It satisfies
// Record[Tick].
func (t Tick) Stamp() int64 { return t.Datetime }

// Time converts Datetime to a time.Time. It is for display and logging, not for
// the read path.
func (t Tick) Time() time.Time { return time.UnixMicro(t.Datetime).UTC() }

// EncodeInto writes the Tick into TickSize bytes at dst. It satisfies
// Record[Tick].
//
// Like Bar's, this is the boundary where a float enters the store: the writer
// holds a Tick and a Tick is what it writes. The conversion is not left to a
// blind cast, because int64(NaN) is undefined and differs by architecture, so a
// non-finite price and a price out of fixed-point range are both reported.
func (t Tick) EncodeInto(dst []byte) error {
	if t.Datetime < 0 {
		return fmt.Errorf("zdb: tick has negative datetime %d: %w", t.Datetime, ErrInvalid)
	}

	bid, err := scaleSigned(t.Bid)
	if err != nil {
		return fmt.Errorf("zdb: tick bid %v: %w", t.Bid, err)
	}

	ask, err := scaleSigned(t.Ask)
	if err != nil {
		return fmt.Errorf("zdb: tick ask %v: %w", t.Ask, err)
	}

	last, err := scaleSigned(t.Last)
	if err != nil {
		return fmt.Errorf("zdb: tick last %v: %w", t.Last, err)
	}

	encodeTick(dst, tick{
		Datetime:  uint64(t.Datetime),
		Bid:       uint64(bid),
		Ask:       uint64(ask),
		Last:      uint64(last),
		Volume:    t.Volume,
		Flags:     t.Flags,
		VolumeExt: t.VolumeExt,
	})

	return nil
}

// DecodeInto reads one Tick from TickSize bytes at src into dst. It satisfies
// Record[Tick] and allocates nothing: src is the mapping and dst is the
// destination the caller already holds.
func (Tick) DecodeInto(src []byte, dst *Tick) {
	*dst = decodeTick(src).float()
}

// float converts a stored print back to the exported form, undoing PriceScale. It
// costs three divisions, which is the price of exposing floats at all, exactly as
// bar.float costs four.
func (t tick) float() Tick {
	return Tick{
		Datetime:  int64(t.Datetime),
		Bid:       signed(int64(t.Bid)),
		Ask:       signed(int64(t.Ask)),
		Last:      signed(int64(t.Last)),
		Volume:    t.Volume,
		Flags:     t.Flags,
		VolumeExt: t.VolumeExt,
	}
}

// encodeTick writes t as TickSize little-endian bytes. Every field is a straight
// 8-byte store, so there is no conversion and no float arithmetic on the append
// path. A price is stored unsigned with its sign in the word, so no cast is
// needed here either: the words go out exactly as decodeTick reads them, which is
// what lets a record round-trip through the file as an uninterpreted block.
func encodeTick(dst []byte, t tick) {
	PutLe64(dst[tickOffDatetime:], t.Datetime)
	PutLe64(dst[tickOffBid:], t.Bid)
	PutLe64(dst[tickOffAsk:], t.Ask)
	PutLe64(dst[tickOffLast:], t.Last)
	PutLe64(dst[tickOffVolume:], t.Volume)
	PutLe64(dst[tickOffFlags:], t.Flags)
	PutLe64(dst[tickOffVolumeExt:], t.VolumeExt)
}

// decodeTick reads the TickSize bytes at src straight out of the mapping: seven
// 8-byte loads, no scaling, no float interpretation, no allocation.
func decodeTick(src []byte) tick {
	return tick{
		Datetime:  Le64(src[tickOffDatetime:]),
		Bid:       Le64(src[tickOffBid:]),
		Ask:       Le64(src[tickOffAsk:]),
		Last:      Le64(src[tickOffLast:]),
		Volume:    Le64(src[tickOffVolume:]),
		Flags:     Le64(src[tickOffFlags:]),
		VolumeExt: Le64(src[tickOffVolumeExt:]),
	}
}
