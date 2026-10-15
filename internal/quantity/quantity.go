// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package quantity provides a lightweight representation and parser for
// non-negative resource quantities with optional SI (base-10) or IEC (base-2)
// unit suffixes.
package quantity

import (
	"errors"
	"math"
	"math/bits"
	"strconv"
)

// SuffixStyle specifies how a Qty interprets and formats unit suffixes.
type SuffixStyle uint8

const (
	// None indicates no unit suffix (plain decimal numbers).
	None SuffixStyle = iota
	// SI indicates base-10 SI suffixes (m, k, M, G, T, P, E).
	SI
	// IEC indicates base-2 IEC suffixes (Ki, Mi, Gi, Ti, Pi, Ei).
	IEC
)

// milliDivisor is the fractional precision per whole unit (currently 1000 for millis).
const milliDivisor = 1000

// ErrOverflow is returned when a quantity exceeds the range of a uint64.
var ErrOverflow = errors.New("quantity overflows uint64")

// Qty represents a non-negative quantity with millisecond (1/1000) fractional
// precision and a suffix style.
//
// The zero value of Qty is valid and represents 0 with style None.
type Qty struct {
	whole     uint64
	milli     uint64      // TODO: could become higher precision, if we track a divisor
	style     SuffixStyle // None by default
	orig      string      // if this was parsed input, this is the original string; else empty
	canonical string      // cached canonical representation
}

// New returns a Qty with the given whole-number value and suffix style.
func New(val uint64, style SuffixStyle) Qty {
	return clampIEC(Qty{
		whole: val,
		style: style,
	})
}

// NewMilli returns a Qty with the given milli-unit value (val / 1000) and
// suffix style.
func NewMilli(val uint64, style SuffixStyle) Qty {
	return clampIEC(Qty{
		whole: val / milliDivisor,
		milli: val % milliDivisor,
		style: style,
	})
}

// clampIEC caps IEC quantities at math.MaxInt64, as resource.Quantity does
// for BinarySI. SI and unsuffixed quantities are not clamped there, or here.
func clampIEC(q Qty) Qty {
	if q.style == IEC && (q.whole > math.MaxInt64 || (q.whole == math.MaxInt64 && q.milli > 0)) {
		return Qty{whole: math.MaxInt64, style: IEC}
	}
	return q
}

func hasIECSuffix(s string) bool {
	return len(s) > 0 && s[len(s)-1] == 'i'
}

// MustParse is like Parse but panics if the input cannot be parsed.
func MustParse(s string) Qty {
	q, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return q
}

// ErrInvalid is returned when a quantity string is syntactically invalid.
var ErrInvalid = errors.New("invalid quantity")

// Parse parses a quantity string into a Qty.
//
// Valid inputs consist of:
//   - A whole-number part in base 10 with no leading zeros (except "0" itself),
//     and no sign ("+" or "-").
//   - An optional fractional part of 1 to 3 digits (".0" to ".999").
//   - An optional SI suffix (m, k, M, G, T, P, E) or IEC suffix
//     (Ki, Mi, Gi, Ti, Pi, Ei).
//
// Scientific/exponent notation is not supported. The resulting value must fit
// within a uint64, except IEC values above math.MaxInt64 are clamped to it, as
// resource.Quantity does. If a fractional "m" suffix produces sub-milli precision
// (e.g. "0.5m"), it rounds up to the next milli.
func Parse(s string) (Qty, error) {
	q, err := parse(s)
	if hasIECSuffix(s) {
		if errors.Is(err, ErrOverflow) {
			return Qty{whole: math.MaxInt64, style: IEC}, nil
		}
		if err == nil {
			return clampIEC(q), nil
		}
	}
	return q, err
}

func parse(s string) (Qty, error) {
	end := len(s)
	if end == 0 {
		return Qty{}, ErrInvalid
	}

	style := None
	var base uint64 = 1
	var shift uint
	isMilli := false
	maxSafeDigits := 19

	last := s[end-1]
	if last < '0' || last > '9' {
		if last == 'i' {
			if end < 3 {
				return Qty{}, ErrInvalid
			}
			style = IEC
			switch s[end-2] {
			case 'K':
				shift, maxSafeDigits = 10, 16
			case 'M':
				shift, maxSafeDigits = 20, 13
			case 'G':
				shift, maxSafeDigits = 30, 10
			case 'T':
				shift, maxSafeDigits = 40, 7
			case 'P':
				shift, maxSafeDigits = 50, 4
			case 'E':
				shift, maxSafeDigits = 60, 1
			default:
				return Qty{}, ErrInvalid
			}
			base = uint64(1) << shift
			end -= 2
		} else {
			if end < 2 {
				return Qty{}, ErrInvalid
			}
			style = SI
			switch last {
			case 'm':
				isMilli = true
			case 'k':
				base, maxSafeDigits = 1e3, 16
			case 'M':
				base, maxSafeDigits = 1e6, 13
			case 'G':
				base, maxSafeDigits = 1e9, 10
			case 'T':
				base, maxSafeDigits = 1e12, 7
			case 'P':
				base, maxSafeDigits = 1e15, 4
			case 'E':
				base, maxSafeDigits = 1e18, 1
			default:
				return Qty{}, ErrInvalid
			}
			end--
		}
	}

	// Single-pass scan of the whole-number part.
	var wholeIn uint64
	i := 0
	for ; i < end; i++ {
		d := s[i] - '0'
		if d > 9 {
			break
		}
		wholeIn = wholeIn*10 + uint64(d)
	}

	wholeLen := i
	if wholeLen == 0 || (wholeLen > 1 && s[0] == '0') {
		return Qty{}, ErrInvalid
	}

	var fracIn uint64
	if i < end {
		if s[i] != '.' {
			return Qty{}, ErrInvalid
		}
		fracStart := i + 1
		fracLen := end - fracStart
		if fracLen == 0 || fracLen > 3 {
			return Qty{}, ErrInvalid
		}
		d0 := s[fracStart] - '0'
		if d0 > 9 {
			return Qty{}, ErrInvalid
		}
		switch fracLen {
		case 1:
			fracIn = uint64(d0) * 100
		case 2:
			d1 := s[fracStart+1] - '0'
			if d1 > 9 {
				return Qty{}, ErrInvalid
			}
			fracIn = (uint64(d0)*10 + uint64(d1)) * 10
		case 3:
			d1 := s[fracStart+1] - '0'
			d2 := s[fracStart+2] - '0'
			if d1 > 9 || d2 > 9 {
				return Qty{}, ErrInvalid
			}
			fracIn = uint64(d0)*100 + uint64(d1)*10 + uint64(d2)
		}
	}

	// Fast path: when wholeLen <= maxSafeDigits, neither wholeIn nor the
	// scaled result can ever overflow uint64.
	if wholeLen <= maxSafeDigits {
		var whole, milli uint64
		switch {
		case style == None:
			whole = wholeIn
			milli = fracIn
		case isMilli:
			whole = wholeIn / milliDivisor
			milli = wholeIn % milliDivisor
			if fracIn > 0 {
				milli++
				if milli == milliDivisor {
					whole++
					milli = 0
				}
			}
		case style == SI:
			whole = wholeIn*base + fracIn*(base/milliDivisor)
		default: // IEC
			whole = wholeIn << shift
			if fracIn > 0 {
				// For shift <= 50, fracIn << shift fits in uint64; for shift == 60
				// with wholeLen == 1, we use 128-bit division.
				if shift <= 50 {
					scaled := fracIn << shift
					whole += scaled / milliDivisor
					milli = scaled % milliDivisor
				} else {
					fHi, fLo := bits.Mul64(fracIn, base)
					extraWhole, rem := bits.Div64(fHi, fLo, milliDivisor)
					whole += extraWhole
					milli = rem
				}
			}
		}
		return Qty{
			whole: whole,
			milli: milli,
			style: style,
			orig:  s,
		}, nil
	}

	return parseSlowBoundary(s, wholeLen, wholeIn, fracIn, style, base, isMilli, maxSafeDigits)
}

const maxUint64Str = "18446744073709551615"

// parseSlowBoundary handles numbers whose whole-part digit length exceeds
// maxSafeDigits (e.g. 20-digit plain numbers or 2-digit E/Ei quantities).
func parseSlowBoundary(s string, wholeLen int, wholeIn, fracIn uint64, style SuffixStyle, base uint64, isMilli bool, maxSafeDigits int) (Qty, error) {
	if wholeLen > maxSafeDigits+1 {
		return Qty{}, ErrOverflow
	}
	if wholeLen == 20 && s[:20] > maxUint64Str {
		return Qty{}, ErrOverflow
	}

	var whole, milli uint64
	switch {
	case isMilli:
		whole = wholeIn / milliDivisor
		milli = wholeIn % milliDivisor
		if fracIn > 0 {
			milli++
			if milli == milliDivisor {
				whole++
				milli = 0
			}
		}
	case style == None:
		if wholeIn == math.MaxUint64 && fracIn > 0 {
			return Qty{}, ErrOverflow
		}
		whole = wholeIn
		milli = fracIn
	default:
		hi, w := bits.Mul64(wholeIn, base)
		if hi != 0 {
			return Qty{}, ErrOverflow
		}
		var m uint64
		if fracIn > 0 {
			fHi, fLo := bits.Mul64(fracIn, base)
			extraWhole, rem := bits.Div64(fHi, fLo, milliDivisor)
			var carry uint64
			w, carry = bits.Add64(w, extraWhole, 0)
			if carry != 0 {
				return Qty{}, ErrOverflow
			}
			m = rem
			if w == math.MaxUint64 && m > 0 {
				return Qty{}, ErrOverflow
			}
		}
		whole = w
		milli = m
	}

	return Qty{
		whole: whole,
		milli: milli,
		style: style,
		orig:  s,
	}, nil
}

// CanParse reports whether s is a valid quantity string that fits in a uint64,
// without constructing a Qty. Like Parse, any valid IEC value fits.
func CanParse(s string) error {
	if err := canParse(s); err != nil && !(errors.Is(err, ErrOverflow) && hasIECSuffix(s)) {
		return err
	}
	return nil
}

func canParse(s string) error {
	end := len(s)
	if end == 0 {
		return ErrInvalid
	}

	style := None
	var base uint64 = 1
	isMilli := false
	maxSafeDigits := 19

	last := s[end-1]
	if last < '0' || last > '9' {
		if last == 'i' {
			if end < 3 {
				return ErrInvalid
			}
			style = IEC
			switch s[end-2] {
			case 'K':
				base, maxSafeDigits = 1<<10, 16
			case 'M':
				base, maxSafeDigits = 1<<20, 13
			case 'G':
				base, maxSafeDigits = 1<<30, 10
			case 'T':
				base, maxSafeDigits = 1<<40, 7
			case 'P':
				base, maxSafeDigits = 1<<50, 4
			case 'E':
				base, maxSafeDigits = 1<<60, 1
			default:
				return ErrInvalid
			}
			end -= 2
		} else {
			if end < 2 {
				return ErrInvalid
			}
			style = SI
			switch last {
			case 'm':
				isMilli = true
			case 'k':
				base, maxSafeDigits = 1e3, 16
			case 'M':
				base, maxSafeDigits = 1e6, 13
			case 'G':
				base, maxSafeDigits = 1e9, 10
			case 'T':
				base, maxSafeDigits = 1e12, 7
			case 'P':
				base, maxSafeDigits = 1e15, 4
			case 'E':
				base, maxSafeDigits = 1e18, 1
			default:
				return ErrInvalid
			}
			end--
		}
	}

	i := 0
	for ; i < end; i++ {
		if s[i]-'0' > 9 {
			break
		}
	}

	wholeLen := i
	if wholeLen == 0 || (wholeLen > 1 && s[0] == '0') {
		return ErrInvalid
	}

	fracStart := -1
	fracLen := 0
	if i < end {
		if s[i] != '.' {
			return ErrInvalid
		}
		fracStart = i + 1
		fracLen = end - fracStart
		if fracLen == 0 || fracLen > 3 {
			return ErrInvalid
		}
		for j := fracStart; j < end; j++ {
			if s[j]-'0' > 9 {
				return ErrInvalid
			}
		}
	}

	// Fast path: guaranteed not to overflow uint64.
	if wholeLen <= maxSafeDigits {
		return nil
	}
	if wholeLen > maxSafeDigits+1 {
		return ErrOverflow
	}
	if wholeLen == 20 {
		if s[:20] > maxUint64Str {
			return ErrOverflow
		}
		if style == None && fracLen > 0 && s[:20] == maxUint64Str {
			for j := fracStart; j < end; j++ {
				if s[j] != '0' {
					return ErrOverflow
				}
			}
		}
		return nil
	}

	// Boundary digit length for suffixed quantities (e.g. 2-digit E/Ei):
	// compute numeric value to check for uint64 overflow.
	var wholeIn uint64
	for j := range wholeLen {
		wholeIn = wholeIn*10 + uint64(s[j]-'0')
	}

	var fracIn uint64
	switch fracLen {
	case 1:
		fracIn = uint64(s[fracStart]-'0') * 100
	case 2:
		fracIn = (uint64(s[fracStart]-'0')*10 + uint64(s[fracStart+1]-'0')) * 10
	case 3:
		fracIn = uint64(s[fracStart]-'0')*100 + uint64(s[fracStart+1]-'0')*10 + uint64(s[fracStart+2]-'0')
	}

	_, err := parseSlowBoundary(s, wholeLen, wholeIn, fracIn, style, base, isMilli, maxSafeDigits)
	return err
}

// AsUint64 returns the whole-number value of q, rounding up if q has a
// non-zero fractional part. It returns ErrOverflow if rounding up would
// overflow uint64.
func (q Qty) AsUint64() (uint64, error) {
	if q.milli == 0 {
		return q.whole, nil
	}
	if q.whole == math.MaxUint64 {
		return 0, ErrOverflow
	}
	return q.whole + 1, nil
}

// AsUint64Milli returns the value of q in milli-units (whole*1000 + milli).
// It returns ErrOverflow if the result overflows uint64.
func (q Qty) AsUint64Milli() (uint64, error) {
	hi, lo := bits.Mul64(q.whole, milliDivisor)
	if hi != 0 {
		return 0, ErrOverflow
	}
	sum, carry := bits.Add64(lo, q.milli, 0)
	if carry != 0 {
		return 0, ErrOverflow
	}
	return sum, nil
}

// String returns the original input string if q was created by Parse, or the
// canonical representation otherwise.
func (q *Qty) String() string {
	if q.orig != "" {
		return q.orig
	}
	return q.Canonical()
}

type siUnit struct {
	base   uint64
	suffix string
}

var siUnits = [...]siUnit{
	{1e18, "E"},
	{1e15, "P"},
	{1e12, "T"},
	{1e9, "G"},
	{1e6, "M"},
	{1e3, "k"},
}

var iecSuffixes = [...]string{
	1: "Ki",
	2: "Mi",
	3: "Gi",
	4: "Ti",
	5: "Pi",
	6: "Ei",
}

// Canonical returns the canonical string representation of q and caches it
// internally so repeated calls do not recompute it.
//
// Canonicalization rules:
//   - Zero is always "0".
//   - For SuffixStyle None, the value is formatted as a plain decimal without
//     trailing fractional zeros (e.g. "42", "3.5", "0.001").
//   - For SuffixStyle SI when the value is strictly between 0 and 1
//     (whole == 0 && milli > 0), the "m" suffix is used (e.g. "500m") because
//     whole units with a suffix are friendlier than a leading-zero float.
//   - For SuffixStyle SI (when whole >= 1000) and IEC (when whole >= 1024),
//     only the single largest suffix whose base is <= whole is considered.
//     If the value divided by that base is exact in at most 3 decimal places
//     AND the resulting suffixed string is strictly shorter than the
//     unsuffixed representation, the suffixed form is used (e.g. 1000 -> "1k",
//     3500000 -> "3.5M", 3005000 -> "3.005M", 1024 -> "1Ki").
//     Otherwise (if it needs more than 3 decimal places, such as 3000500 in SI,
//     or if the suffixed form is at least as long as the unsuffixed form, such
//     as 1001 -> "1.001k", 1500 -> "1.5k", or 1536 -> "1.5Ki"), the plain
//     unsuffixed form is used.
func (q *Qty) Canonical() string {
	if q.canonical != "" {
		return q.canonical
	}
	if q.whole == 0 && q.milli == 0 {
		q.canonical = "0"
		return "0"
	}

	var buf [32]byte
	var b []byte

	if q.whole == 0 {
		// q.milli > 0
		if q.style == SI {
			b = strconv.AppendUint(buf[:0], q.milli, 10)
			b = append(b, 'm')
		} else {
			b = append(buf[:0], '0')
			b = appendFrac(b, q.milli)
		}
		return q.cacheCanonical(b)
	}

	switch q.style {
	case SI:
		if q.whole >= 1e3 {
			for i := range siUnits {
				u := siUnits[i]
				if q.whole >= u.base {
					qWhole := q.whole / u.base
					remWhole := q.whole % u.base
					hi, lo := bits.Mul64(remWhole, milliDivisor)
					lo, carry := bits.Add64(lo, q.milli, 0)
					hi += carry
					qFrac, rem := bits.Div64(hi, lo, u.base)
					if rem == 0 {
						suffixedLen := digits10(qWhole) + fracLen(qFrac) + len(u.suffix)
						unsuffixedLen := digits10(q.whole) + fracLen(q.milli)
						if suffixedLen < unsuffixedLen {
							b = strconv.AppendUint(buf[:0], qWhole, 10)
							b = appendFrac(b, qFrac)
							b = append(b, u.suffix...)
							return q.cacheCanonical(b)
						}
					}
					break
				}
			}
		}
	case IEC:
		if q.whole >= 1<<10 {
			idx := (bits.Len64(q.whole) - 1) / 10
			shift := uint(idx * 10)
			base := uint64(1) << shift
			suffix := iecSuffixes[idx]

			qWhole := q.whole >> shift
			remWhole := q.whole & (base - 1)
			hi, lo := bits.Mul64(remWhole, milliDivisor)
			lo, carry := bits.Add64(lo, q.milli, 0)
			hi += carry
			if lo&(base-1) == 0 {
				qFrac := (hi << (64 - shift)) | (lo >> shift)
				suffixedLen := digits10(qWhole) + fracLen(qFrac) + len(suffix)
				unsuffixedLen := digits10(q.whole) + fracLen(q.milli)
				if suffixedLen < unsuffixedLen {
					b = strconv.AppendUint(buf[:0], qWhole, 10)
					b = appendFrac(b, qFrac)
					b = append(b, suffix...)
					return q.cacheCanonical(b)
				}
			}
		}
	}

	b = strconv.AppendUint(buf[:0], q.whole, 10)
	b = appendFrac(b, q.milli)
	return q.cacheCanonical(b)
}

func (q *Qty) cacheCanonical(b []byte) string {
	if q.orig != "" && string(b) == q.orig {
		q.canonical = q.orig
		return q.orig
	}
	s := string(b)
	q.canonical = s
	return s
}

func appendFrac(b []byte, frac uint64) []byte {
	if frac == 0 {
		return b
	}
	d1 := frac / 100
	rem := frac % 100
	d2 := rem / 10
	d3 := rem % 10
	b = append(b, '.', byte('0'+d1))
	if d2 != 0 || d3 != 0 {
		b = append(b, byte('0'+d2))
		if d3 != 0 {
			b = append(b, byte('0'+d3))
		}
	}
	return b
}

func fracLen(frac uint64) int {
	if frac == 0 {
		return 0
	}
	if frac%10 != 0 {
		return 4
	}
	if frac%100 != 0 {
		return 3
	}
	return 2
}

func digits10(v uint64) int {
	n := 1
	for v >= 10 {
		v /= 10
		n++
	}
	return n
}
