//go:build linux

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

package actornet

import (
	"strings"
	"testing"
)

// The default has to keep producing exactly what the hardcoded scheme did, or
// moving to a plan silently renumbers every worker's actors.
func TestDefaultPodSidePlanMatchesTheOriginalScheme(t *testing.T) {
	plan := DefaultPodSidePlan()
	if got, want := plan.Slots(), 4094; got != want {
		t.Errorf("Slots() = %d, want %d", got, want)
	}
	for _, tc := range []struct {
		slot int
		want string
	}{
		{0, "169.254.32.1"},
		{1, "169.254.32.2"},
		{254, "169.254.32.255"},
		{255, "169.254.33.0"},
		{256, "169.254.33.1"},
		{4093, "169.254.47.254"},
	} {
		if got := plan.IP(tc.slot).String(); got != tc.want {
			t.Errorf("IP(%d) = %s, want %s", tc.slot, got, tc.want)
		}
	}
}

// Every address a plan hands out has to fall inside the prefix the nftables
// rules match on, or traffic is silently unmatched.
func TestPodSidePlanStaysInsideItsPrefix(t *testing.T) {
	for _, cidr := range []string{"169.254.32.0/20", "10.80.0.0/22", "192.168.5.0/24"} {
		plan, err := NewPodSidePlan(cidr)
		if err != nil {
			t.Fatalf("NewPodSidePlan(%s): %v", cidr, err)
		}
		prefix := plan.prefix
		for _, slot := range []int{0, 1, plan.Slots() / 2, plan.Slots() - 1} {
			ip := plan.IP(slot)
			addr, ok := addrFromIP(ip)
			if !ok || !prefix.Contains(addr) {
				t.Errorf("%s: IP(%d) = %s is outside the prefix", cidr, slot, ip)
			}
			if addr == prefix.Addr() {
				t.Errorf("%s: IP(%d) handed out the network address", cidr, slot)
			}
		}
	}
}

// The prefix decides the ceiling, which is what the ateom reports as capacity.
func TestPodSidePlanSlotsFollowThePrefix(t *testing.T) {
	for _, tc := range []struct {
		cidr string
		want int
	}{
		{"169.254.32.0/20", 4094},
		{"10.0.0.0/24", 254},
		{"10.0.0.0/30", 2},
		{"10.0.0.0/16", 65534},
		// Larger than the cap: bounded rather than overflowing the reported
		// capacity.
		{"10.0.0.0/8", 65534},
	} {
		plan, err := NewPodSidePlan(tc.cidr)
		if err != nil {
			t.Fatalf("NewPodSidePlan(%s): %v", tc.cidr, err)
		}
		if got := plan.Slots(); got != tc.want {
			t.Errorf("%s: Slots() = %d, want %d", tc.cidr, got, tc.want)
		}
	}
}

func TestNewPodSidePlanRejects(t *testing.T) {
	for _, tc := range []struct{ name, cidr, wantErr string }{
		{"not a prefix", "169.254.32.0", "parsing pod-side subnet"},
		{"bare address", "not-an-address", "parsing pod-side subnet"},
		{"too small", "10.0.0.0/31", "too small"},
		// IPv6 is shaped for but not implemented: the ruleset is an IPv4
		// nftables table and the actor's own address is IPv4 and frozen into
		// every snapshot. Rejected loudly rather than half-working.
		{"ipv6", "fd00::/64", "IPv6 actor addressing is not implemented"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewPodSidePlan(tc.cidr)
			if err == nil {
				t.Fatalf("NewPodSidePlan(%s) = nil error, want one", tc.cidr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// A prefix given with host bits set is the same plan as its masked form, so a
// deployment writing 10.0.0.5/24 gets 10.0.0.0/24 rather than a silent offset.
func TestNewPodSidePlanMasks(t *testing.T) {
	plan, err := NewPodSidePlan("10.0.0.5/24")
	if err != nil {
		t.Fatalf("NewPodSidePlan: %v", err)
	}
	if got, want := plan.Subnet(), "10.0.0.0/24"; got != want {
		t.Errorf("Subnet() = %s, want %s", got, want)
	}
	if got, want := plan.IP(0).String(), "10.0.0.1"; got != want {
		t.Errorf("IP(0) = %s, want %s", got, want)
	}
}
