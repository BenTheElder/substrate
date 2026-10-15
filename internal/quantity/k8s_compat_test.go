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
	"math"
	"math/big"
	"math/rand"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

// checkK8sSubset fails if s parses here but not as a Kubernetes
// resource.Quantity, or parses to a different value. Anything we accept must
// mean the same thing to existing Kubernetes clients.
func checkK8sSubset(t *testing.T, s string) {
	t.Helper()
	q, err := Parse(s)
	if err != nil {
		return
	}
	k, err := resource.ParseQuantity(s)
	if err != nil {
		t.Fatalf("Parse(%q) accepted, but resource.ParseQuantity rejected it: %v", s, err)
	}
	// we keep millis and round sub-milli values up, so compare against the
	// k8s value rounded up to the next milli
	if got, want := milliValue(q), ceilMilli(k); got.Cmp(want) != 0 {
		t.Fatalf("Parse(%q) = %s millis, resource.ParseQuantity = %s millis", s, got, want)
	}
	// what we print has to be just as compatible
	checkK8sCanonical(t, q)
}

// checkK8sCanonical fails if q prints as something Kubernetes rejects or
// reads as a different value
func checkK8sCanonical(t *testing.T, q Qty) {
	t.Helper()
	canon := q.Canonical()
	kc, err := resource.ParseQuantity(canon)
	if err != nil {
		t.Fatalf("Canonical() of %+v = %q, which resource.ParseQuantity rejects: %v", q, canon, err)
	}
	if got, want := milliValue(q), ceilMilli(kc); got.Cmp(want) != 0 {
		t.Fatalf("Canonical() of %+v = %q, which resource.ParseQuantity reads as %s millis, want %s", q, canon, want, got)
	}
}

func TestK8sSubsetConstructors(t *testing.T) {
	var vals []uint64
	for _, v := range []uint64{0, 1, 999, 1000, 1023, 1024, 1e18, 8 << 60, math.MaxInt64, math.MaxUint64} {
		for d := uint64(0); d < 3; d++ {
			vals = append(vals, v+d, v-d)
		}
	}
	for shift := 10; shift <= 63; shift++ {
		vals = append(vals, 1<<shift, 3<<(shift-1), (1<<shift)-1)
	}
	for _, v := range vals {
		for _, style := range []SuffixStyle{None, SI, IEC} {
			checkK8sCanonical(t, New(v, style))
			checkK8sCanonical(t, NewMilli(v, style))
		}
	}
}

func milliValue(q Qty) *big.Int {
	v := new(big.Int).SetUint64(q.whole)
	v.Mul(v, big.NewInt(milliDivisor))
	return v.Add(v, new(big.Int).SetUint64(q.milli))
}

// ceilMilli returns k in millis, rounded up
func ceilMilli(k resource.Quantity) *big.Int {
	d := k.AsDec() // value = unscaled * 10^-scale
	v := new(big.Int).Mul(d.UnscaledBig(), big.NewInt(milliDivisor))
	scale := int64(d.Scale())
	if scale <= 0 {
		return v.Mul(v, new(big.Int).Exp(big.NewInt(10), big.NewInt(-scale), nil))
	}
	div := new(big.Int).Exp(big.NewInt(10), big.NewInt(scale), nil)
	quo, rem := new(big.Int).QuoRem(v, div, new(big.Int))
	if rem.Sign() > 0 {
		quo.Add(quo, big.NewInt(1))
	}
	return quo
}

var k8sSubsetSuffixes = []string{
	"", "m", "k", "M", "G", "T", "P", "E",
	"Ki", "Mi", "Gi", "Ti", "Pi", "Ei",
	// not valid here, but close enough to be worth probing
	"K", "g", "mi", "ki", "u", "n", "i", "Ki ", "e3", "E3", "e-3",
}

func TestK8sSubsetGrammar(t *testing.T) {
	wholes := []string{
		"", "0", "1", "2", "7", "9", "10", "15", "99", "100", "512", "999", "1000",
		"1023", "1024", "1536", "4096", "999999", "1000000",
		"9223372036854775807", "9223372036854775808",
		"18446744073709551615", "18446744073709551616", "99999999999999999999",
		"00", "01", "007", "+1", "-1", " 1",
	}
	fracs := []string{"", ".", ".0", ".00", ".000", ".0000", ".1", ".5", ".05", ".001", ".009", ".25", ".999", ".1234", ".-1", ".a"}
	for _, w := range wholes {
		for _, f := range fracs {
			for _, sfx := range k8sSubsetSuffixes {
				checkK8sSubset(t, w+f+sfx)
			}
		}
	}
}

// every SI and IEC multiple near the uint64 boundary, where our overflow
// checks and the k8s inf.Dec path both get exercised
func TestK8sSubsetBoundaries(t *testing.T) {
	for _, sfx := range k8sSubsetSuffixes[:14] {
		for w := 1; w <= 20000; w++ {
			for _, f := range []string{"", ".5", ".999"} {
				checkK8sSubset(t, itoa(w)+f+sfx)
			}
		}
	}
}

func TestK8sSubsetRandom(t *testing.T) {
	const alphabet = "0123456789.mkMGTPEKi+-e "
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 200000; i++ {
		b := make([]byte, 1+r.Intn(24))
		for j := range b {
			b[j] = alphabet[r.Intn(len(alphabet))]
		}
		checkK8sSubset(t, string(b))
	}
}

func FuzzK8sSubset(f *testing.F) {
	for _, s := range []string{"0", "500m", "1.5", "512Mi", "1.5Gi", "18446744073709551615", "16Ei", "0.5m"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		checkK8sSubset(t, s)
	})
}

func itoa(n int) string {
	return big.NewInt(int64(n)).String()
}
