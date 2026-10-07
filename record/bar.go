package record

import (
	"errors"
	"time"
)

const (
	// BarSize is the encoded width of one record in bytes. It is packed rather
	// than left at Go's natural stride: sizeof(Bar) would be 72 because
	// of the uint32 spread's trailing padding, and every padding byte would be
	// read on every scan.
	BarSize = 60
)

// Bar is one bar as a caller sees it: prices as doubles, spread signed,
// datetime signed. It is the only bar type this package exports, because it is
// the only form a caller needs. What actually goes in a file is bar, which is
// unexported and has no float in it at all.
//
// Datetime is unix milliseconds, the same unit Range and Scan take. TickVolume
// counts individual ticks, Volume is base-currency volume, and Spread is in
// points.
//
// The four OHLC columns are tagged price, which says that they are float64 at
// this boundary and fixed-point at PriceScale on disk. That is what lets a query
// layer meet a condition and a record in fixed-point integers instead of turning
// either side back into a double, and it is why a column so tagged must be a
// float64 — the flag and the field type agree here by construction.
//
// The directive below writes Bar's schema instead of reflecting over it. It runs
// inside the record package, so the qualifier is empty: emitting record.MustSchema
// here would reference the package the file is being generated into.
//
//go:generate go run github.com/aldok10/zdb/cmd/gen-schema-record -qualify= -type Bar
type Bar struct {
	Datetime   int64   `zdb:"datetime,time,unit=ms"`
	Open       float64 `zdb:"open,price"`
	High       float64 `zdb:"high,price"`
	Low        float64 `zdb:"low,price"`
	Close      float64 `zdb:"close,price"`
	TickVolume uint64  `zdb:"tick_volume"`
	Spread     int32   `zdb:"spread"`
	Volume     uint64  `zdb:"volume"`
}

// bar is a bar in its stored form. Field order follows the OHLC convention every
// candle feed shares, but every field is widened to an unsigned integer and
// spread to uint32, so a record holds no float and no sign. That is what keeps
// decode free of float work and keeps a price exactly comparable, and it is why
// bar does not escape this package: there is no reason for a caller to hold one.
//
// Prices are fixed-point, scaled by PriceScale, so a price finer than 1e-8
// rounds rather than being stored.
type bar struct {
	Datetime   uint64
	Open       uint64
	High       uint64
	Low        uint64
	Close      uint64
	TickVolume uint64
	Spread     uint32
	Volume     uint64
}

var _ Record[Bar] = Bar{}

// RecordSize is the encoded width of one Bar. It satisfies Record[Bar].
func (Bar) RecordSize() int { return BarSize }

// Stamp is Bar's Datetime, the value a shard orders on. It satisfies Record[Bar].
func (b Bar) Stamp() int64 { return b.Datetime }

// DecodeInto reads one Bar from BarSize bytes at src and writes it to dst, both
// direct: src is the mapping, dst is the caller's record, and nothing is
// allocated or staged between them.
func (Bar) DecodeInto(src []byte, dst *Bar) {
	*dst = decodeBar(src).float()
}

// Time converts Datetime to a time.Time. It is for display and logging, not for
// the read path.
func (b Bar) Time() time.Time { return time.UnixMilli(b.Datetime).UTC() }

// EncodeInto scales b's prices by PriceScale and writes the packed record into
// dst, which must be at least BarSize bytes. It reports an error for a
// non-finite or negative price and for a negative datetime: int64(NaN) is
// undefined and differs by architecture, so none of those conversions can be
// left to a blind cast.
//
// This is the boundary where a float enters the store, which is why it is a
// method on the exported type rather than a constructor: the writer holds a Bar,
// and a Bar is what it writes.
func (b Bar) EncodeInto(dst []byte) error {
	if b.Datetime < 0 {
		return errors.New("zdb: negative bar datetime")
	}

	out := bar{
		Datetime:   uint64(b.Datetime),
		TickVolume: b.TickVolume,
		Spread:     uint32(b.Spread),
		Volume:     b.Volume,
	}
	for _, f := range []struct {
		dst *uint64
		val float64
	}{
		{&out.Open, b.Open},
		{&out.High, b.High},
		{&out.Low, b.Low},
		{&out.Close, b.Close},
	} {
		v, err := scalePrice(f.val)
		if err != nil {
			return err
		}

		*f.dst = v
	}

	encodeBar(dst, out)

	return nil
}

// float converts a stored bar back to the exported form, undoing PriceScale. It
// costs four divisions, which is the price of exposing floats at all: a caller
// that only needs the close pays for four anyway.
func (b bar) float() Bar {
	return Bar{
		Datetime:   int64(b.Datetime),
		Open:       price(b.Open),
		High:       price(b.High),
		Low:        price(b.Low),
		Close:      price(b.Close),
		TickVolume: b.TickVolume,
		Spread:     int32(b.Spread),
		Volume:     b.Volume,
	}
}

// Byte offsets of each field inside an encoded bar. Named once so the writer and
// the reader cannot drift apart, which is how an earlier version of this file
// silently produced garbage keys.
const (
	barOffDatetime   = 0
	barOffOpen       = 8
	barOffHigh       = 16
	barOffLow        = 24
	barOffClose      = 32
	barOffTickVolume = 40
	barOffSpread     = 48
	barOffVolume     = 52
)

// encodeBar writes b as BarSize little-endian bytes. Seven of the eight fields
// are a straight 8-byte store, so there is no conversion and no float arithmetic
// on the append path.
func encodeBar(dst []byte, b bar) {
	PutLe64(dst[barOffDatetime:], b.Datetime)
	PutLe64(dst[barOffOpen:], b.Open)
	PutLe64(dst[barOffHigh:], b.High)
	PutLe64(dst[barOffLow:], b.Low)
	PutLe64(dst[barOffClose:], b.Close)
	PutLe64(dst[barOffTickVolume:], b.TickVolume)
	PutLe32(dst[barOffSpread:], b.Spread)
	PutLe64(dst[barOffVolume:], b.Volume)
}

// decodeBar reads the BarSize bytes at src straight out of the mapping: seven
// 8-byte loads, one 4-byte load, no scaling, no float interpretation, no
// allocation.
func decodeBar(src []byte) bar {
	return bar{
		Datetime:   Le64(src[barOffDatetime:]),
		Open:       Le64(src[barOffOpen:]),
		High:       Le64(src[barOffHigh:]),
		Low:        Le64(src[barOffLow:]),
		Close:      Le64(src[barOffClose:]),
		TickVolume: Le64(src[barOffTickVolume:]),
		Spread:     Le32(src[barOffSpread:]),
		Volume:     Le64(src[barOffVolume:]),
	}
}
