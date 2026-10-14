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

package quantity

import (
	"errors"
	"math"
	"testing"
)

func TestParseValid(t *testing.T) {
	tests := []struct {
		input     string
		wantWhole uint64
		wantMilli uint64
		wantStyle SuffixStyle
		wantCanon string
	}{
		// Plain integers (None)
		{input: "0", wantWhole: 0, wantMilli: 0, wantStyle: None, wantCanon: "0"},
		{input: "1", wantWhole: 1, wantMilli: 0, wantStyle: None, wantCanon: "1"},
		{input: "42", wantWhole: 42, wantMilli: 0, wantStyle: None, wantCanon: "42"},
		{input: "999", wantWhole: 999, wantMilli: 0, wantStyle: None, wantCanon: "999"},
		{input: "1000", wantWhole: 1000, wantMilli: 0, wantStyle: None, wantCanon: "1000"},
		{input: "1024", wantWhole: 1024, wantMilli: 0, wantStyle: None, wantCanon: "1024"},
		{input: "18446744073709551615", wantWhole: math.MaxUint64, wantMilli: 0, wantStyle: None, wantCanon: "18446744073709551615"},

		// Plain decimals (None)
		{input: "0.0", wantWhole: 0, wantMilli: 0, wantStyle: None, wantCanon: "0"},
		{input: "0.00", wantWhole: 0, wantMilli: 0, wantStyle: None, wantCanon: "0"},
		{input: "0.000", wantWhole: 0, wantMilli: 0, wantStyle: None, wantCanon: "0"},
		{input: "0.5", wantWhole: 0, wantMilli: 500, wantStyle: None, wantCanon: "0.5"},
		{input: "0.50", wantWhole: 0, wantMilli: 500, wantStyle: None, wantCanon: "0.5"},
		{input: "0.500", wantWhole: 0, wantMilli: 500, wantStyle: None, wantCanon: "0.5"},
		{input: "0.05", wantWhole: 0, wantMilli: 50, wantStyle: None, wantCanon: "0.05"},
		{input: "0.050", wantWhole: 0, wantMilli: 50, wantStyle: None, wantCanon: "0.05"},
		{input: "0.001", wantWhole: 0, wantMilli: 1, wantStyle: None, wantCanon: "0.001"},
		{input: "0.999", wantWhole: 0, wantMilli: 999, wantStyle: None, wantCanon: "0.999"},
		{input: "5.0", wantWhole: 5, wantMilli: 0, wantStyle: None, wantCanon: "5"},
		{input: "5.00", wantWhole: 5, wantMilli: 0, wantStyle: None, wantCanon: "5"},
		{input: "5.000", wantWhole: 5, wantMilli: 0, wantStyle: None, wantCanon: "5"},
		{input: "3.5", wantWhole: 3, wantMilli: 500, wantStyle: None, wantCanon: "3.5"},
		{input: "3.50", wantWhole: 3, wantMilli: 500, wantStyle: None, wantCanon: "3.5"},
		{input: "3.500", wantWhole: 3, wantMilli: 500, wantStyle: None, wantCanon: "3.5"},
		{input: "3.05", wantWhole: 3, wantMilli: 50, wantStyle: None, wantCanon: "3.05"},
		{input: "3.005", wantWhole: 3, wantMilli: 5, wantStyle: None, wantCanon: "3.005"},
		{input: "18446744073709551615.0", wantWhole: math.MaxUint64, wantMilli: 0, wantStyle: None, wantCanon: "18446744073709551615"},
		{input: "18446744073709551615.000", wantWhole: math.MaxUint64, wantMilli: 0, wantStyle: None, wantCanon: "18446744073709551615"},

		// SI "m" (milli) suffix
		{input: "0m", wantWhole: 0, wantMilli: 0, wantStyle: SI, wantCanon: "0"},
		{input: "0.0m", wantWhole: 0, wantMilli: 0, wantStyle: SI, wantCanon: "0"},
		{input: "1m", wantWhole: 0, wantMilli: 1, wantStyle: SI, wantCanon: "1m"},
		{input: "1.0m", wantWhole: 0, wantMilli: 1, wantStyle: SI, wantCanon: "1m"},
		{input: "50m", wantWhole: 0, wantMilli: 50, wantStyle: SI, wantCanon: "50m"},
		{input: "500m", wantWhole: 0, wantMilli: 500, wantStyle: SI, wantCanon: "500m"},
		{input: "500.00m", wantWhole: 0, wantMilli: 500, wantStyle: SI, wantCanon: "500m"},
		{input: "999m", wantWhole: 0, wantMilli: 999, wantStyle: SI, wantCanon: "999m"},
		{input: "1000m", wantWhole: 1, wantMilli: 0, wantStyle: SI, wantCanon: "1"},
		{input: "1500m", wantWhole: 1, wantMilli: 500, wantStyle: SI, wantCanon: "1.5"},
		{input: "1000000m", wantWhole: 1000, wantMilli: 0, wantStyle: SI, wantCanon: "1k"},
		// Sub-milli rounds up to next milli
		{input: "0.1m", wantWhole: 0, wantMilli: 1, wantStyle: SI, wantCanon: "1m"},
		{input: "0.5m", wantWhole: 0, wantMilli: 1, wantStyle: SI, wantCanon: "1m"},
		{input: "0.999m", wantWhole: 0, wantMilli: 1, wantStyle: SI, wantCanon: "1m"},
		{input: "1.5m", wantWhole: 0, wantMilli: 2, wantStyle: SI, wantCanon: "2m"},
		{input: "999.5m", wantWhole: 1, wantMilli: 0, wantStyle: SI, wantCanon: "1"},
		{input: "18446744073709551615m", wantWhole: 18446744073709551, wantMilli: 615, wantStyle: SI, wantCanon: "18446744073709551.615"},
		{input: "18446744073709551615.5m", wantWhole: 18446744073709551, wantMilli: 616, wantStyle: SI, wantCanon: "18446744073709551.616"},

		// SI suffixes (k, M, G, T, P, E)
		{input: "0k", wantWhole: 0, wantMilli: 0, wantStyle: SI, wantCanon: "0"},
		{input: "1k", wantWhole: 1000, wantMilli: 0, wantStyle: SI, wantCanon: "1k"},
		{input: "1.0k", wantWhole: 1000, wantMilli: 0, wantStyle: SI, wantCanon: "1k"},
		{input: "1.5k", wantWhole: 1500, wantMilli: 0, wantStyle: SI, wantCanon: "1500"},
		{input: "1.05k", wantWhole: 1050, wantMilli: 0, wantStyle: SI, wantCanon: "1050"},
		{input: "1.005k", wantWhole: 1005, wantMilli: 0, wantStyle: SI, wantCanon: "1005"},
		{input: "1.001k", wantWhole: 1001, wantMilli: 0, wantStyle: SI, wantCanon: "1001"},
		{input: "12k", wantWhole: 12000, wantMilli: 0, wantStyle: SI, wantCanon: "12k"},
		{input: "12.5k", wantWhole: 12500, wantMilli: 0, wantStyle: SI, wantCanon: "12500"},
		{input: "3M", wantWhole: 3_000_000, wantMilli: 0, wantStyle: SI, wantCanon: "3M"},
		{input: "3.5M", wantWhole: 3_500_000, wantMilli: 0, wantStyle: SI, wantCanon: "3.5M"},
		{input: "3.50M", wantWhole: 3_500_000, wantMilli: 0, wantStyle: SI, wantCanon: "3.5M"},
		{input: "3.05M", wantWhole: 3_050_000, wantMilli: 0, wantStyle: SI, wantCanon: "3.05M"},
		{input: "3.005M", wantWhole: 3_005_000, wantMilli: 0, wantStyle: SI, wantCanon: "3.005M"},
		{input: "1G", wantWhole: 1e9, wantMilli: 0, wantStyle: SI, wantCanon: "1G"},
		{input: "2.5G", wantWhole: 2.5e9, wantMilli: 0, wantStyle: SI, wantCanon: "2.5G"},
		{input: "1T", wantWhole: 1e12, wantMilli: 0, wantStyle: SI, wantCanon: "1T"},
		{input: "1.25T", wantWhole: 1.25e12, wantMilli: 0, wantStyle: SI, wantCanon: "1.25T"},
		{input: "1P", wantWhole: 1e15, wantMilli: 0, wantStyle: SI, wantCanon: "1P"},
		{input: "1.5P", wantWhole: 1.5e15, wantMilli: 0, wantStyle: SI, wantCanon: "1.5P"},
		{input: "1E", wantWhole: 1e18, wantMilli: 0, wantStyle: SI, wantCanon: "1E"},
		{input: "1.5E", wantWhole: 1.5e18, wantMilli: 0, wantStyle: SI, wantCanon: "1.5E"},
		{input: "18E", wantWhole: 18e18, wantMilli: 0, wantStyle: SI, wantCanon: "18E"},
		{input: "18.446E", wantWhole: 18_446_000_000_000_000_000, wantMilli: 0, wantStyle: SI, wantCanon: "18.446E"},

		// IEC suffixes (Ki, Mi, Gi, Ti, Pi, Ei)
		{input: "0Ki", wantWhole: 0, wantMilli: 0, wantStyle: IEC, wantCanon: "0"},
		{input: "1Ki", wantWhole: 1024, wantMilli: 0, wantStyle: IEC, wantCanon: "1Ki"},
		{input: "1.0Ki", wantWhole: 1024, wantMilli: 0, wantStyle: IEC, wantCanon: "1Ki"},
		{input: "1.5Ki", wantWhole: 1536, wantMilli: 0, wantStyle: IEC, wantCanon: "1536"},
		{input: "1.25Ki", wantWhole: 1280, wantMilli: 0, wantStyle: IEC, wantCanon: "1280"},
		{input: "1.125Ki", wantWhole: 1152, wantMilli: 0, wantStyle: IEC, wantCanon: "1152"},
		{input: "1.1Ki", wantWhole: 1126, wantMilli: 400, wantStyle: IEC, wantCanon: "1.1Ki"},
		{input: "1.10Ki", wantWhole: 1126, wantMilli: 400, wantStyle: IEC, wantCanon: "1.1Ki"},
		{input: "1.001Ki", wantWhole: 1025, wantMilli: 24, wantStyle: IEC, wantCanon: "1.001Ki"},
		{input: "10Ki", wantWhole: 10240, wantMilli: 0, wantStyle: IEC, wantCanon: "10Ki"},
		{input: "3Mi", wantWhole: 3 << 20, wantMilli: 0, wantStyle: IEC, wantCanon: "3Mi"},
		{input: "3.5Mi", wantWhole: 3<<20 + 512<<10, wantMilli: 0, wantStyle: IEC, wantCanon: "3.5Mi"},
		{input: "3.25Mi", wantWhole: 3<<20 + 256<<10, wantMilli: 0, wantStyle: IEC, wantCanon: "3.25Mi"},
		// "3.125Mi" is 7 chars, equal to "3276800" (7 chars), so integer form wins.
		{input: "3.125Mi", wantWhole: 3<<20 + 128<<10, wantMilli: 0, wantStyle: IEC, wantCanon: "3276800"},
		{input: "1Gi", wantWhole: 1 << 30, wantMilli: 0, wantStyle: IEC, wantCanon: "1Gi"},
		{input: "3.125Gi", wantWhole: 3<<30 + 128<<20, wantMilli: 0, wantStyle: IEC, wantCanon: "3.125Gi"},
		{input: "1Ti", wantWhole: 1 << 40, wantMilli: 0, wantStyle: IEC, wantCanon: "1Ti"},
		{input: "1.5Ti", wantWhole: 1<<40 + 512<<30, wantMilli: 0, wantStyle: IEC, wantCanon: "1.5Ti"},
		{input: "1Pi", wantWhole: 1 << 50, wantMilli: 0, wantStyle: IEC, wantCanon: "1Pi"},
		{input: "1.5Pi", wantWhole: 1<<50 + 512<<40, wantMilli: 0, wantStyle: IEC, wantCanon: "1.5Pi"},
		{input: "1Ei", wantWhole: 1 << 60, wantMilli: 0, wantStyle: IEC, wantCanon: "1Ei"},
		{input: "1.5Ei", wantWhole: 1<<60 + 512<<50, wantMilli: 0, wantStyle: IEC, wantCanon: "1.5Ei"},
		{input: "15Ei", wantWhole: 15 << 60, wantMilli: 0, wantStyle: IEC, wantCanon: "15Ei"},
		{input: "15.999Ei", wantWhole: 18445591152204944769, wantMilli: 24, wantStyle: IEC, wantCanon: "15.999Ei"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			if err := CanParse(tc.input); err != nil {
				t.Fatalf("CanParse(%q) unexpected error: %v", tc.input, err)
			}
			q, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tc.input, err)
			}
			if q.whole != tc.wantWhole || q.milli != tc.wantMilli {
				t.Errorf("Parse(%q) = (whole=%d, milli=%d), want (whole=%d, milli=%d)",
					tc.input, q.whole, q.milli, tc.wantWhole, tc.wantMilli)
			}
			if q.style != tc.wantStyle {
				t.Errorf("Parse(%q) style = %v, want %v", tc.input, q.style, tc.wantStyle)
			}
			if got := q.String(); got != tc.input {
				t.Errorf("Parse(%q).String() = %q, want %q", tc.input, got, tc.input)
			}
			if got := q.Canonical(); got != tc.wantCanon {
				t.Errorf("Parse(%q).Canonical() = %q, want %q", tc.input, got, tc.wantCanon)
			}
			// After Canonical() is called, String() still returns the original input.
			if got := q.String(); got != tc.input {
				t.Errorf("Parse(%q).String() after Canonical() = %q, want %q", tc.input, got, tc.input)
			}
		})
	}
}

func TestParseInvalid(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantOverflow bool
	}{
		{name: "empty", input: ""},
		{name: "space only", input: " "},
		{name: "leading space", input: " 1"},
		{name: "trailing space", input: "1 "},
		{name: "space before suffix", input: "1 k"},
		{name: "tab", input: "\t1"},
		{name: "newline", input: "1\n"},

		// Leading zeros
		{name: "double zero", input: "00"},
		{name: "leading zero int", input: "01"},
		{name: "multiple leading zeros", input: "007"},
		{name: "leading zero float", input: "00.5"},
		{name: "leading zero nonzero float", input: "01.5"},
		{name: "leading zero zero float", input: "00.0"},
		{name: "leading zero SI", input: "00k"},
		{name: "leading zero nonzero SI", input: "01k"},
		{name: "leading zero IEC", input: "00Ki"},
		{name: "leading zero milli", input: "01m"},

		// Malformed decimals
		{name: "missing leading zero before dot", input: ".5"},
		{name: "missing leading zero before dot zero", input: ".0"},
		{name: "missing leading zero with suffix", input: ".5k"},
		{name: "trailing dot", input: "5."},
		{name: "zero trailing dot", input: "0."},
		{name: "trailing dot before SI suffix", input: "5.k"},
		{name: "trailing dot before IEC suffix", input: "5.Ki"},
		{name: "bare dot", input: "."},
		{name: "dot and suffix", input: ".k"},
		{name: "double dot", input: "1..0"},
		{name: "two dots", input: "1.2.3"},
		{name: "4 fractional digits", input: "0.0001"},
		{name: "4 fractional trailing zeros", input: "1.0000"},
		{name: "4 fractional digits with suffix", input: "1.0001M"},
		{name: "5 fractional digits", input: "3.14159"},

		// Signs and negatives
		{name: "negative int", input: "-1"},
		{name: "positive sign int", input: "+1"},
		{name: "negative zero", input: "-0"},
		{name: "positive zero", input: "+0"},
		{name: "negative float", input: "-0.5"},
		{name: "positive float", input: "+0.5"},
		{name: "negative with suffix", input: "-1k"},
		{name: "positive with suffix", input: "+1Ki"},
		{name: "minus in fraction", input: "1.-5"},

		// Scientific / exponent notation
		{name: "lowercase e exponent", input: "1e3"},
		{name: "uppercase E exponent", input: "1E3"},
		{name: "negative exponent", input: "1e-3"},
		{name: "positive exponent", input: "1E+3"},
		{name: "float with exponent", input: "1.5e2"},
		{name: "zero exponent", input: "1e0"},

		// Invalid suffixes
		{name: "uppercase K", input: "1K"},
		{name: "lowercase ki", input: "1ki"},
		{name: "lowercase mi", input: "1mi"},
		{name: "lowercase gi", input: "1gi"},
		{name: "lowercase ti", input: "1ti"},
		{name: "lowercase pi", input: "1pi"},
		{name: "lowercase ei", input: "1ei"},
		{name: "lowercase g", input: "1g"},
		{name: "lowercase t", input: "1t"},
		{name: "lowercase p", input: "1p"},
		{name: "lowercase e", input: "1e"},
		{name: "bytes B", input: "1B"},
		{name: "KB", input: "1KB"},
		{name: "KiB", input: "1KiB"},
		{name: "MiB", input: "1MiB"},
		{name: "double suffix", input: "1Mk"},
		{name: "IEC then SI suffix", input: "1Kik"},
		{name: "bare SI suffix", input: "k"},
		{name: "bare IEC suffix", input: "Ki"},
		{name: "bare m suffix", input: "m"},
		{name: "bare E suffix", input: "E"},
		{name: "word", input: "banana"},

		// Overflows
		{name: "MaxUint64 + 1", input: "18446744073709551616", wantOverflow: true},
		{name: "MaxUint64 + 0.001", input: "18446744073709551615.001", wantOverflow: true},
		{name: "MaxUint64 + 0.5", input: "18446744073709551615.5", wantOverflow: true},
		{name: "20 nines", input: "99999999999999999999", wantOverflow: true},
		{name: "21 digits", input: "100000000000000000000", wantOverflow: true},
		{name: "milli with overflowing whole part", input: "18446744073709551616m", wantOverflow: true},
		{name: "19E overflows", input: "19E", wantOverflow: true},
		{name: "18.447E overflows", input: "18.447E", wantOverflow: true},
		{name: "16Ei overflows", input: "16Ei", wantOverflow: true},
		{name: "k overflow", input: "18446744073709552k", wantOverflow: true},
		{name: "Ki overflow", input: "18014398509481984Ki", wantOverflow: true},
		{name: "fractional overflow after base multiply", input: "18446744073709551.616k", wantOverflow: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := CanParse(tc.input); err == nil {
				t.Fatalf("CanParse(%q) = nil, want error", tc.input)
			} else if tc.wantOverflow && !errors.Is(err, ErrOverflow) {
				t.Errorf("CanParse(%q) error = %v, want errors.Is(err, ErrOverflow)", tc.input, err)
			}
			q, err := Parse(tc.input)
			if err == nil {
				t.Fatalf("Parse(%q) = %+v, want error", tc.input, q)
			}
			if tc.wantOverflow && !errors.Is(err, ErrOverflow) {
				t.Errorf("Parse(%q) error = %v, want errors.Is(err, ErrOverflow)", tc.input, err)
			}
		})
	}
}

func TestMustParsePanicsOnInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustParse(invalid) did not panic")
		}
	}()
	MustParse("invalid")
}

func TestAsUint64AndAsUint64Milli(t *testing.T) {
	tests := []struct {
		input        string
		wantUint64   uint64
		wantMilli    uint64
		wantMilliErr bool
	}{
		{input: "0", wantUint64: 0, wantMilli: 0},
		{input: "0.000", wantUint64: 0, wantMilli: 0},
		{input: "0.001", wantUint64: 1, wantMilli: 1},
		{input: "0.5", wantUint64: 1, wantMilli: 500},
		{input: "0.999", wantUint64: 1, wantMilli: 999},
		{input: "1", wantUint64: 1, wantMilli: 1000},
		{input: "3.5", wantUint64: 4, wantMilli: 3500},
		{input: "3.50", wantUint64: 4, wantMilli: 3500},
		{input: "5.0", wantUint64: 5, wantMilli: 5000},
		{input: "1m", wantUint64: 1, wantMilli: 1},
		{input: "500m", wantUint64: 1, wantMilli: 500},
		{input: "1000m", wantUint64: 1, wantMilli: 1000},
		{input: "1001m", wantUint64: 2, wantMilli: 1001},
		{input: "1500m", wantUint64: 2, wantMilli: 1500},
		{input: "0.5m", wantUint64: 1, wantMilli: 1},
		{input: "1.1Ki", wantUint64: 1127, wantMilli: 1126400},
		{input: "1.5Ki", wantUint64: 1536, wantMilli: 1536000},
		{input: "18446744073709551615m", wantUint64: 18446744073709552, wantMilli: math.MaxUint64},
		{input: "1E", wantUint64: 1e18, wantMilliErr: true},
		{input: "18446744073709551615", wantUint64: math.MaxUint64, wantMilliErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			q := MustParse(tc.input)
			gotU, err := q.AsUint64()
			if err != nil {
				t.Fatalf("AsUint64() unexpected error: %v", err)
			}
			if gotU != tc.wantUint64 {
				t.Errorf("AsUint64() = %d, want %d", gotU, tc.wantUint64)
			}

			gotM, err := q.AsUint64Milli()
			if tc.wantMilliErr {
				if !errors.Is(err, ErrOverflow) {
					t.Errorf("AsUint64Milli() = %d, %v, want ErrOverflow", gotM, err)
				}
			} else {
				if err != nil {
					t.Fatalf("AsUint64Milli() unexpected error: %v", err)
				}
				if gotM != tc.wantMilli {
					t.Errorf("AsUint64Milli() = %d, want %d", gotM, tc.wantMilli)
				}
			}
		})
	}

	t.Run("AsUint64 overflow when rounding MaxUint64", func(t *testing.T) {
		q := Qty{whole: math.MaxUint64, milli: 1}
		if _, err := q.AsUint64(); !errors.Is(err, ErrOverflow) {
			t.Errorf("AsUint64() error = %v, want ErrOverflow", err)
		}
	})
}

func TestCanonicalRules(t *testing.T) {
	tests := []struct {
		name string
		qty  Qty
		want string
	}{
		// Zero values
		{name: "zero value Qty{}", qty: Qty{}, want: "0"},
		{name: "zero None", qty: New(0, None), want: "0"},
		{name: "zero SI", qty: New(0, SI), want: "0"},
		{name: "zero IEC", qty: New(0, IEC), want: "0"},
		{name: "zero milli SI", qty: NewMilli(0, SI), want: "0"},

		// SuffixStyle None
		{name: "None 1000", qty: New(1000, None), want: "1000"},
		{name: "None 3000000", qty: New(3_000_000, None), want: "3000000"},
		{name: "None 500m", qty: NewMilli(500, None), want: "0.5"},
		{name: "None 50m", qty: NewMilli(50, None), want: "0.05"},
		{name: "None 5m", qty: NewMilli(5, None), want: "0.005"},
		{name: "None 3500m", qty: NewMilli(3500, None), want: "3.5"},

		// SuffixStyle SI
		{name: "SI 1m", qty: NewMilli(1, SI), want: "1m"},
		{name: "SI 50m", qty: NewMilli(50, SI), want: "50m"},
		{name: "SI 500m", qty: NewMilli(500, SI), want: "500m"},
		{name: "SI 999m", qty: NewMilli(999, SI), want: "999m"},
		{name: "SI 1000m -> 1", qty: NewMilli(1000, SI), want: "1"},
		{name: "SI 1500m -> 1.5", qty: NewMilli(1500, SI), want: "1.5"},
		{name: "SI 999", qty: New(999, SI), want: "999"},
		{name: "SI 1000 -> 1k", qty: New(1000, SI), want: "1k"},
		{name: "SI 1001 -> 1001 (1.001k is longer)", qty: New(1001, SI), want: "1001"},
		{name: "SI 1050 -> 1050 (1.05k is longer)", qty: New(1050, SI), want: "1050"},
		{name: "SI 1500 -> 1500 (1.5k is same length)", qty: New(1500, SI), want: "1500"},
		{name: "SI 12000 -> 12k", qty: New(12000, SI), want: "12k"},
		{name: "SI 12500 -> 12500 (12.5k is same length)", qty: New(12500, SI), want: "12500"},
		{name: "SI 3000000 -> 3M", qty: New(3_000_000, SI), want: "3M"},
		{name: "SI 3500000 -> 3.5M", qty: New(3_500_000, SI), want: "3.5M"},
		{name: "SI 3050000 -> 3.05M", qty: New(3_050_000, SI), want: "3.05M"},
		{name: "SI 3005000 -> 3.005M", qty: New(3_005_000, SI), want: "3.005M"},
		{name: "SI 3000500 -> 3000500 (exceeds 3 decimals of M)", qty: New(3_000_500, SI), want: "3000500"},
		{name: "SI 5G", qty: New(5e9, SI), want: "5G"},
		{name: "SI 5.005G", qty: New(5_005_000_000, SI), want: "5.005G"},
		{name: "SI 5.0005G -> integer", qty: New(5_000_500_000, SI), want: "5000500000"},
		{name: "SI 7T", qty: New(7e12, SI), want: "7T"},
		{name: "SI 8P", qty: New(8e15, SI), want: "8P"},
		{name: "SI 9E", qty: New(9e18, SI), want: "9E"},

		// SuffixStyle IEC
		{name: "IEC 500m -> 0.5", qty: NewMilli(500, IEC), want: "0.5"},
		{name: "IEC 1023", qty: New(1023, IEC), want: "1023"},
		{name: "IEC 1024 -> 1Ki", qty: New(1024, IEC), want: "1Ki"},
		{name: "IEC 1025 -> 1025", qty: New(1025, IEC), want: "1025"},
		{name: "IEC 1536 -> 1536 (1.5Ki is longer)", qty: New(1536, IEC), want: "1536"},
		{name: "IEC 10240 -> 10Ki", qty: New(10240, IEC), want: "10Ki"},
		{name: "IEC 15872 -> 15872 (15.5Ki is longer)", qty: New(15872, IEC), want: "15872"},
		{name: "IEC 3Mi", qty: New(3<<20, IEC), want: "3Mi"},
		{name: "IEC 3.5Mi", qty: New(3<<20+512<<10, IEC), want: "3.5Mi"},
		{name: "IEC 3.25Mi", qty: New(3<<20+256<<10, IEC), want: "3.25Mi"},
		{name: "IEC 3.125Mi -> 3276800 (same length)", qty: New(3<<20+128<<10, IEC), want: "3276800"},
		{name: "IEC 3Gi", qty: New(3<<30, IEC), want: "3Gi"},
		{name: "IEC 3.125Gi", qty: New(3<<30+128<<20, IEC), want: "3.125Gi"},
		{name: "IEC 4Ti", qty: New(4<<40, IEC), want: "4Ti"},
		{name: "IEC 5Pi", qty: New(5<<50, IEC), want: "5Pi"},
		{name: "IEC 6Ei", qty: New(6<<60, IEC), want: "6Ei"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.qty
			if got := q.Canonical(); got != tc.want {
				t.Errorf("Canonical() = %q, want %q", got, tc.want)
			}
			if q.canonical != tc.want {
				t.Errorf("cached canonical = %q, want %q", q.canonical, tc.want)
			}
			// For New / NewMilli quantities, String() returns Canonical().
			if got := q.String(); got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCanonicalRoundTripExhaustive(t *testing.T) {
	styles := []SuffixStyle{None, SI, IEC}

	checkRoundTrip := func(t *testing.T, q Qty) {
		t.Helper()
		canon := q.Canonical()
		if canon == "" {
			t.Fatalf("Canonical() returned empty string for %+v", q)
		}
		// Calling Canonical() a second time must return the cached value.
		if canon2 := q.Canonical(); canon2 != canon {
			t.Fatalf("second Canonical() = %q, want %q", canon2, canon)
		}

		parsed, err := Parse(canon)
		if err != nil {
			t.Fatalf("Parse(Canonical()=%q) failed for %+v: %v", canon, q, err)
		}
		if parsed.whole != q.whole || parsed.milli != q.milli {
			t.Fatalf("round-trip mismatch for %+v (canon=%q): got (whole=%d, milli=%d), want (whole=%d, milli=%d)",
				q, canon, parsed.whole, parsed.milli, q.whole, q.milli)
		}
		if canonAgain := parsed.Canonical(); canonAgain != canon {
			t.Fatalf("idempotence failure for %+v: Canonical()=%q, Parse().Canonical()=%q",
				q, canon, canonAgain)
		}
	}

	// 1. All milli values from 0 to 25000 across all styles.
	for _, style := range styles {
		for m := uint64(0); m <= 25000; m++ {
			checkRoundTrip(t, NewMilli(m, style))
		}
	}

	// 2. Every SI base (10^3..10^18) with various whole multipliers and all
	// thousandths (0..999) of that base, plus off-by-one neighbors.
	siBases := []uint64{1e3, 1e6, 1e9, 1e12, 1e15, 1e18}
	multipliers := []uint64{1, 2, 3, 9, 10, 12, 15, 18, 99, 100, 999}
	for _, base := range siBases {
		step := base / 1000
		for _, mult := range multipliers {
			if mult > math.MaxUint64/base {
				continue
			}
			baseVal := mult * base
			for frac := uint64(0); frac < 1000; frac++ {
				extra := frac * step
				if baseVal > math.MaxUint64-extra {
					continue
				}
				v := baseVal + extra
				for _, style := range styles {
					checkRoundTrip(t, New(v, style))
					if v > 0 {
						checkRoundTrip(t, New(v-1, style))
					}
					if v < math.MaxUint64 {
						checkRoundTrip(t, New(v+1, style))
					}
				}
			}
		}
	}

	// 3. Every IEC base (2^10..2^60) with various whole multipliers and all
	// thousandths (0..999) of that base (via Parse), plus integer neighbors.
	iecSuffixList := []string{"Ki", "Mi", "Gi", "Ti", "Pi", "Ei"}
	for idx, sfx := range iecSuffixList {
		shift := uint((idx + 1) * 10)
		base := uint64(1) << shift
		for _, mult := range []uint64{0, 1, 2, 3, 10, 15, 100, 1023} {
			if mult > math.MaxUint64/base {
				continue
			}
			w := mult * base
			// Test via New around exact multiples of base/8 (exact binary decimals).
			for eighths := uint64(0); eighths < 8; eighths++ {
				part := (base / 8) * eighths
				if w <= math.MaxUint64-part {
					v := w + part
					for _, style := range styles {
						checkRoundTrip(t, New(v, style))
						if v > 0 {
							checkRoundTrip(t, New(v-1, style))
						}
						if v < math.MaxUint64 {
							checkRoundTrip(t, New(v+1, style))
						}
					}
				}
			}
		}
		// Test decimal IEC strings "<mult>.<frac><sfx>" for all frac 0..999.
		for _, mult := range []string{"0", "1", "2", "15"} {
			for frac := 0; frac < 1000; frac++ {
				var num string
				switch {
				case frac == 0:
					num = mult + sfx
				case frac%100 == 0:
					num = mult + "." + string(rune('0'+frac/100)) + sfx
				case frac%10 == 0:
					num = mult + "." + string(rune('0'+frac/100)) + string(rune('0'+(frac/10)%10)) + sfx
				default:
					num = mult + "." + string(rune('0'+frac/100)) + string(rune('0'+(frac/10)%10)) + string(rune('0'+frac%10)) + sfx
				}
				q, err := Parse(num)
				if err != nil {
					continue
				}
				checkRoundTrip(t, q)
			}
		}
	}

	// 4. Boundary values near math.MaxUint64.
	for delta := uint64(0); delta < 1000; delta++ {
		v := math.MaxUint64 - delta
		for _, style := range styles {
			checkRoundTrip(t, New(v, style))
			checkRoundTrip(t, NewMilli(v, style))
		}
	}
}

var (
	benchQtySink  Qty
	benchStrSink  string
	benchUintSink uint64
)

func BenchmarkParse(b *testing.B) {
	cases := []struct {
		name  string
		input string
	}{
		{name: "Int", input: "3000000"},
		{name: "Float", input: "3.005"},
		{name: "SI_Int", input: "3M"},
		{name: "SI_Float", input: "3.005M"},
		{name: "SI_Milli", input: "500m"},
		{name: "IEC_Int", input: "8Gi"},
		{name: "IEC_Float", input: "3.125Gi"},
		{name: "MaxUint64", input: "18446744073709551615"},
	}

	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q, err := Parse(bc.input)
				if err != nil {
					b.Fatal(err)
				}
				benchQtySink = q
			}
		})
	}
}

func BenchmarkCanParse(b *testing.B) {
	cases := []struct {
		name  string
		input string
	}{
		{name: "Int", input: "3000000"},
		{name: "Float", input: "3.005"},
		{name: "SI_Int", input: "3M"},
		{name: "SI_Float", input: "3.005M"},
		{name: "SI_Milli", input: "500m"},
		{name: "IEC_Int", input: "8Gi"},
		{name: "IEC_Float", input: "3.125Gi"},
		{name: "MaxUint64", input: "18446744073709551615"},
	}

	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := CanParse(bc.input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCanonicalUncached(b *testing.B) {
	cases := []struct {
		name string
		qty  Qty
	}{
		{name: "None_Int", qty: New(3_000_000, None)},
		{name: "None_Float", qty: NewMilli(3500, None)},
		{name: "SI_Exact", qty: New(3_000_000, SI)},
		{name: "SI_Frac", qty: New(3_005_000, SI)},
		{name: "SI_Fallback", qty: New(3_000_500, SI)},
		{name: "SI_Milli", qty: NewMilli(500, SI)},
		{name: "IEC_Exact", qty: New(8<<30, IEC)},
		{name: "IEC_Frac", qty: New(3<<30+128<<20, IEC)},
		{name: "IEC_Fallback", qty: New((8<<30)+1, IEC)},
		{name: "FromParse_AlreadyCanonical", qty: MustParse("3.5M")},
	}

	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q := bc.qty
				benchStrSink = q.Canonical()
			}
		})
	}
}

func BenchmarkCanonicalCached(b *testing.B) {
	q := New(3_005_000, SI)
	_ = q.Canonical()
	b.ReportAllocs()
	for b.Loop() {
		benchStrSink = q.Canonical()
	}
}

func BenchmarkStringFromParse(b *testing.B) {
	q := MustParse("3.50M")
	b.ReportAllocs()
	for b.Loop() {
		benchStrSink = q.String()
	}
}

func BenchmarkAsUint64(b *testing.B) {
	q := MustParse("3.5M")
	b.ReportAllocs()
	for b.Loop() {
		v, err := q.AsUint64()
		if err != nil {
			b.Fatal(err)
		}
		benchUintSink = v
	}
}

func BenchmarkAsUint64Milli(b *testing.B) {
	q := MustParse("3.5M")
	b.ReportAllocs()
	for b.Loop() {
		v, err := q.AsUint64Milli()
		if err != nil {
			b.Fatal(err)
		}
		benchUintSink = v
	}
}
