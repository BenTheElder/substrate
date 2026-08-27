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
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/roottest"
	"github.com/google/nftables"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
)

// withTestPodNetNS runs fn with the calling thread inside a throwaway namespace
// standing in for the worker pod's, so every link, route, and nftables table the
// code under test creates disappears with the test rather than landing on the
// machine running it.
func withTestPodNetNS(t *testing.T, fn func()) {
	t.Helper()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	orig, err := netns.Get()
	if err != nil {
		t.Fatalf("getting current netns: %v", err)
	}
	defer orig.Close()
	defer func() {
		if err := netns.Set(orig); err != nil {
			t.Errorf("restoring original netns: %v", err)
		}
	}()
	pod, err := netns.New() // also switches this thread into it
	if err != nil {
		t.Fatalf("creating pod netns: %v", err)
	}
	defer pod.Close()

	fn()
}

func requireNftables(t *testing.T) {
	t.Helper()
	if _, err := (&nftables.Conn{}).ListTablesOfFamily(nftables.TableFamilyIPv4); err != nil {
		t.Skipf("nftables unavailable in this environment: %v", err)
	}
}

// actorTable returns the worker's actor table, or nil when it is absent.
func actorTable(t *testing.T) *nftables.Table {
	t.Helper()
	tables, err := (&nftables.Conn{}).ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		t.Fatalf("listing tables: %v", err)
	}
	for _, table := range tables {
		if table.Name == nftTableName {
			return table
		}
	}
	return nil
}

// countRules totals the rules across every chain of the actor table. With the
// per-actor state in a map this should not move as actors come and go, which is
// what TestRulesAreFixedRegardlessOfActorCount asserts.
func countRules(t *testing.T) int {
	t.Helper()
	table := actorTable(t)
	if table == nil {
		return 0
	}
	c := &nftables.Conn{}
	chains, err := c.ListChainsOfTableFamily(table.Family)
	if err != nil {
		t.Fatalf("listing chains: %v", err)
	}
	total := 0
	for _, chain := range chains {
		if chain.Table == nil || chain.Table.Name != table.Name {
			continue
		}
		rules, err := c.GetRules(table, chain)
		if err != nil {
			t.Fatalf("listing rules of %q: %v", chain.Name, err)
		}
		total += len(rules)
	}
	return total
}

// mapKeys returns the veth names the actor address map is keyed by, which is
// the per-actor state the fixed rules read. Counting rules would no longer say
// anything: there are five of them whether one actor is hosted or a thousand.
func mapKeys(t *testing.T, name string) []string {
	t.Helper()
	table := actorTable(t)
	if table == nil {
		return nil
	}
	c := &nftables.Conn{}
	elems, err := c.GetSetElements(&nftables.Set{Table: table, Name: name})
	if err != nil {
		t.Fatalf("listing elements of %q: %v", name, err)
	}
	var out []string
	for _, e := range elems {
		out = append(out, strings.TrimRight(string(e.Key), "\x00"))
	}
	sort.Strings(out)
	return out
}

// setAddrs returns the addresses in a plain (non-map) set of the actor table.
func setAddrs(t *testing.T, name string) []string {
	t.Helper()
	table := actorTable(t)
	if table == nil {
		return nil
	}
	c := &nftables.Conn{}
	elems, err := c.GetSetElements(&nftables.Set{Table: table, Name: name})
	if err != nil {
		t.Fatalf("listing elements of %q: %v", name, err)
	}
	var out []string
	for _, e := range elems {
		out = append(out, net.IP(e.Key).String())
	}
	sort.Strings(out)
	return out
}

// countRulesInChain totals the rules in one chain of the actor table.
func countRulesInChain(t *testing.T, name string) int {
	t.Helper()
	table := actorTable(t)
	if table == nil {
		return 0
	}
	c := &nftables.Conn{}
	chains, err := c.ListChainsOfTableFamily(table.Family)
	if err != nil {
		t.Fatalf("listing chains: %v", err)
	}
	for _, chain := range chains {
		if chain.Table == nil || chain.Table.Name != table.Name || chain.Name != name {
			continue
		}
		rules, err := c.GetRules(table, chain)
		if err != nil {
			t.Fatalf("listing rules of %q: %v", name, err)
		}
		return len(rules)
	}
	return 0
}

// TestEgressRedirectIsPerActor is the property that keeps actors from imposing
// their egress configuration on each other. Redirecting the whole pod-side
// range would need no per-actor state at all, but it would also capture an
// actor with no egress gateway -- and atunnel closes any connection it cannot
// attribute to an activation, so that actor's TCP would be dropped rather than
// masqueraded out. Membership of the tunneled set is what makes it per actor.
// testPlan is the default addressing; these tests assert on concrete
// addresses, so they need the plan the production default produces.
var testPlan = DefaultPodSidePlan()

func TestEgressRedirectIsPerActor(t *testing.T) {
	roottest.Require(t, "installing nftables rules")

	withTestPodNetNS(t, func() {
		requireNftables(t)

		tunneled := &ActorNetwork{ActorUID: "actor-a", Slot: 0, PodSideIP: testPlan.IP(0), NetNS: noNetNS, TunneledEgress: true}
		direct := &ActorNetwork{ActorUID: "actor-b", Slot: 1, PodSideIP: testPlan.IP(1), NetNS: noNetNS}

		if err := ApplyActorNetworkRules(testPlan, []*ActorNetwork{tunneled, direct}, 15001); err != nil {
			t.Fatalf("apply(tunneled,direct): %v", err)
		}
		// Both actors are addressable...
		if got, want := mapKeys(t, "actor_podside"), []string{"ate0", "ate1"}; !slices.Equal(got, want) {
			t.Errorf("actor address map keyed by %v, want %v", got, want)
		}
		// ...but only the one that asked for it is redirected.
		if got, want := setAddrs(t, "tunneled_egress"), []string{testPlan.IP(0).String()}; !slices.Equal(got, want) {
			t.Errorf("tunneled-egress set = %v, want just the tunneled actor (%v)", got, want)
		}
		// The redirect itself is one rule regardless, and the masquerade that
		// carries the direct actor's traffic is still there.
		if got := countRulesInChain(t, "prerouting"); got != 1 {
			t.Errorf("redirect rules = %d, want 1 (it is gated on the set, not repeated per actor)", got)
		}
		if got := countRulesInChain(t, "postrouting"); got != 1 {
			t.Errorf("masquerade rules = %d, want 1", got)
		}

		// An actor that did not ask for it, alone: no set membership at all.
		if err := ApplyActorNetworkRules(testPlan, []*ActorNetwork{direct}, 15001); err != nil {
			t.Fatalf("apply(direct): %v", err)
		}
		if got := setAddrs(t, "tunneled_egress"); len(got) != 0 {
			t.Errorf("tunneled-egress set = %v, want empty", got)
		}
	})
}

// TestRulesAreFixedRegardlessOfActorCount is the scaling property the map
// exists for: an activation writes set elements, not rules. If this ever starts
// failing because the count grows with the actor list, the per-actor rule
// emission has come back and with it an O(actors) cost per activation, paid
// under the lock that serializes every activation on the worker.
func TestRulesAreFixedRegardlessOfActorCount(t *testing.T) {
	roottest.Require(t, "installing nftables rules")

	withTestPodNetNS(t, func() {
		requireNftables(t)

		one := []*ActorNetwork{{ActorUID: "a", Slot: 0, PodSideIP: testPlan.IP(0), NetNS: noNetNS}}
		many := make([]*ActorNetwork, 0, 50)
		for i := range 50 {
			many = append(many, &ActorNetwork{
				ActorUID: fmt.Sprintf("actor-%d", i), Slot: i,
				PodSideIP: testPlan.IP(i), NetNS: noNetNS, TunneledEgress: true,
			})
		}

		if err := ApplyActorNetworkRules(testPlan, one, 15001); err != nil {
			t.Fatalf("apply(one): %v", err)
		}
		withOne := countRules(t)

		if err := ApplyActorNetworkRules(testPlan, many, 15001); err != nil {
			t.Fatalf("apply(many): %v", err)
		}
		if got := countRules(t); got != withOne {
			t.Errorf("rules with 50 actors = %d, with 1 = %d; want them equal", got, withOne)
		}
		if got := len(mapKeys(t, "actor_podside")); got != len(many) {
			t.Errorf("actor address map holds %d entries, want %d", got, len(many))
		}
		if got := len(setAddrs(t, "tunneled_egress")); got != len(many) {
			t.Errorf("tunneled-egress set holds %d entries, want %d", got, len(many))
		}
	})
}

// TestSetElementsPerMessageStaysUnderTheAttributeLimit pins the arithmetic that
// decides how many elements ride in one netlink message. It is a pure
// calculation, but getting it wrong is invisible: the length field wraps, the
// kernel keeps the fraction the truncated length describes, and the surplus
// elements are dropped without an error anywhere.
func TestSetElementsPerMessageStaysUnderTheAttributeLimit(t *testing.T) {
	// Mirrors the wire encoding in nftables' makeElemList: each element is a
	// nest holding a key attribute, plus a data attribute when the set is a map,
	// with every value in its own NFTA_DATA_VALUE.
	encodedBytes := func(set *nftables.Set, n int) int {
		value := func(width int) int { return 4 + 4 + ((int(width) + 3) &^ 3) }
		per := 4 + 4 + value(int(set.KeyType.Bytes))
		if set.IsMap {
			per += 4 + value(int(set.DataType.Bytes))
		}
		return 4 + per*n // the enclosing NFTA_SET_ELEM_LIST_ELEMENTS attribute
	}

	for _, tc := range []struct {
		name string
		set  *nftables.Set
	}{
		{"actor address map", &nftables.Set{IsMap: true, KeyType: nftables.TypeIFName, DataType: nftables.TypeIPAddr}},
		{"tunneled egress set", &nftables.Set{KeyType: nftables.TypeIPAddr}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := setElementsPerMessage(tc.set)
			if n < 1 {
				t.Fatalf("setElementsPerMessage = %d, want at least 1", n)
			}
			if got := encodedBytes(tc.set, n); got > 65535 {
				t.Errorf("%d elements encode to %d bytes, over the uint16 attribute limit", n, got)
			}
			// And it is not so conservative as to be pointless: one more must be
			// what actually overruns.
			if got := encodedBytes(tc.set, n+1); got <= 65535 {
				t.Errorf("%d elements still encode to %d bytes; the chunk is smaller than it needs to be", n+1, got)
			}
		})
	}
}

// TestActorAddressMapHoldsEveryAddressableSlot is the regression test for the
// ceiling this found in practice. A worker filled past 1638 actors stopped
// wiring new ones -- no error, no log, the map simply stopped growing and the
// activation hung with its veth never created, because the elements overran a
// uint16 length on the way to the kernel.
//
// It asserts the whole addressable range rather than a number just past the old
// limit: the point is that the map, not the encoding, is what bounds a worker.
func TestActorAddressMapHoldsEveryAddressableSlot(t *testing.T) {
	roottest.Require(t, "installing nftables rules")

	withTestPodNetNS(t, func() {
		requireNftables(t)

		actors := make([]*ActorNetwork, 0, testPlan.Slots())
		for i := range testPlan.Slots() {
			actors = append(actors, &ActorNetwork{
				ActorUID: fmt.Sprintf("actor-%d", i), Slot: i,
				PodSideIP: testPlan.IP(i), NetNS: noNetNS, TunneledEgress: true,
			})
		}
		if err := ApplyActorNetworkRules(testPlan, actors, 15001); err != nil {
			t.Fatalf("apply(%d actors): %v", len(actors), err)
		}
		if got := len(mapKeys(t, "actor_podside")); got != len(actors) {
			t.Errorf("actor address map holds %d entries, want %d", got, len(actors))
		}
		if got := len(setAddrs(t, "tunneled_egress")); got != len(actors) {
			t.Errorf("tunneled-egress set holds %d entries, want %d", got, len(actors))
		}
	})
}

// TestEveryActorCanResolveItsGateway is the regression test for the martian
// source floods a packed worker produced.
//
// Every actor holds the same frozen address behind its own veth, so the pod
// netns has one identical connected route per actor and cannot say which
// interface 169.254.17.2 is on. With reverse-path validation on -- which is how
// a GKE node comes -- everything but the one veth the route happens to name is
// a martian source. IP survives that, because the nftables rewrite runs at raw
// prerouting and has already replaced the address by the time routing looks.
// ARP does not: arp_process runs the same check on the sender address, and a
// failed check means the kernel drops the request without replying, so the
// actor never learns its gateway's MAC.
//
// Hence two actors, and an assertion on the SECOND: the first resolves either
// way, because its veth is the one the duplicate route names.
func TestEveryActorCanResolveItsGateway(t *testing.T) {
	roottest.Require(t, "creating network namespaces and veth pairs")
	ctx := context.Background()

	withTestPodNetNS(t, func() {
		requireNftables(t)

		// A fresh namespace inherits rp_filter from the host, so the setting this
		// test is about depends on the machine it runs on. Pin it to what a
		// worker pod actually gets.
		for _, scope := range []string{"all", "default"} {
			path := "/proc/sys/net/ipv4/conf/" + scope + "/rp_filter"
			if err := os.WriteFile(path, []byte("1\n"), 0o644); err != nil {
				t.Skipf("cannot set %s in the test namespace: %v", path, err)
			}
		}

		var networks []*ActorNetwork
		for i := range 2 {
			network, err := SetupActorNetwork(ctx, ActorNetworkConfig{
				ActorUID: fmt.Sprintf("actor-%d", i), Slot: i,
			})
			if err != nil {
				t.Fatalf("SetupActorNetwork(slot %d): %v", i, err)
			}
			t.Cleanup(func() { _ = CleanupActorNetwork(ctx, network) })
			networks = append(networks, network)
		}
		if err := ApplyActorNetworkRules(testPlan, networks, 0); err != nil {
			t.Fatalf("ApplyActorNetworkRules: %v", err)
		}

		for i, network := range networks {
			if err := resolveGateway(ctx, network); err != nil {
				t.Errorf("actor in slot %d: %v", i, err)
			}
		}
	})
}

// resolveGateway provokes an ARP for the actor's gateway from inside the actor's
// namespace and reports whether the worker side answered it.
func resolveGateway(ctx context.Context, network *ActorNetwork) error {
	return ateomnet.NetNSDo(ctx, network.NetNS, func(context.Context) error {
		// Any datagram will do. It is not delivered anywhere and does not need
		// to be; the point is the neighbor lookup that sending it forces.
		conn, err := net.Dial("udp", net.JoinHostPort(ateomnet.ActorVethGateway, "9"))
		if err != nil {
			return fmt.Errorf("dialing the gateway: %w", err)
		}
		defer conn.Close()
		link, err := netlink.LinkByName(ateomnet.ActorVethName)
		if err != nil {
			return fmt.Errorf("acquiring the actor veth: %w", err)
		}

		const resolved = netlink.NUD_REACHABLE | netlink.NUD_STALE | netlink.NUD_DELAY |
			netlink.NUD_PROBE | netlink.NUD_PERMANENT
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := conn.Write([]byte("x")); err != nil {
				return fmt.Errorf("sending to the gateway: %w", err)
			}
			neighbors, err := netlink.NeighList(link.Attrs().Index, netlink.FAMILY_V4)
			if err != nil {
				return fmt.Errorf("listing neighbors: %w", err)
			}
			for _, n := range neighbors {
				if n.IP.Equal(ateomnet.ActorVethGwIP) && n.HardwareAddr != nil && n.State&resolved != 0 {
					return nil
				}
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("gateway %s never resolved; its ARP is being dropped", ateomnet.ActorVethGateway)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

// TestSetupActorNetworkIsRepeatable pins that an actor can be set up and torn
// down over and over in the same slot, which is what a worker cycling actors
// through a slot actually does. Cleanup is idempotent too: the extra call on an
// already-clean network must still succeed, because the failure paths run it
// speculatively.
func TestSetupActorNetworkIsRepeatable(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestPodNetNS(t, func() {
		requireNftables(t)

		for i := range 3 {
			network, err := SetupActorNetwork(ctx, ActorNetworkConfig{ActorUID: "actor-a", Slot: 0})
			if err != nil {
				t.Fatalf("SetupActorNetwork (activation %d): %v", i, err)
			}
			if _, err := netlink.LinkByName(network.HostVeth()); err != nil {
				t.Fatalf("host veth %q missing after activation %d: %v", network.HostVeth(), i, err)
			}
			if err := CleanupActorNetwork(ctx, network); err != nil {
				t.Fatalf("CleanupActorNetwork (activation %d): %v", i, err)
			}
			if _, err := netlink.LinkByName(network.HostVeth()); err == nil {
				t.Errorf("host veth %q survived cleanup of activation %d", network.HostVeth(), i)
			}
			// Idempotent: the actor is already gone, and saying so again is not
			// an error.
			if err := CleanupActorNetwork(ctx, network); err != nil {
				t.Fatalf("CleanupActorNetwork on an already-clean network: %v", err)
			}
		}
	})
}

// TestSetupActorNetworkHostVethHWAddr covers the micro-VM requirement: a CH
// snapshot freezes the guest's ARP entry for its gateway, so the worker-side
// veth MAC has to be exactly the one the caller asked for, on every pod. Left
// to the kernel it would be random, and a restored guest would blackhole its
// egress until the frozen entry expired.
func TestSetupActorNetworkHostVethHWAddr(t *testing.T) {
	roottest.Require(t, "creating network namespaces and veth pairs")
	ctx := context.Background()

	withTestPodNetNS(t, func() {
		want := ateomnet.MustParseMAC("02:a8:1e:00:00:01")
		network, err := SetupActorNetwork(ctx, ActorNetworkConfig{
			ActorUID:           "actor-a",
			Slot:               0,
			HostVethHWAddr:     want,
			SweepInteriorLinks: true,
		})
		if err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}
		t.Cleanup(func() { _ = CleanupActorNetwork(ctx, network) })

		host, err := netlink.LinkByName(network.HostVeth())
		if err != nil {
			t.Fatalf("host veth %q missing from the pod netns: %v", network.HostVeth(), err)
		}
		if got := host.Attrs().HardwareAddr.String(); got != want.String() {
			t.Errorf("host veth MAC = %s, want %s", got, want)
		}
	})
}

// TestSetupActorNetworkSweepsInteriorLinks covers the other half of the
// micro-VM path: SweepInteriorLinks clears a previous activation's leftovers
// (kata's tap device) out of the actor's namespace before the new pair is
// created, and must not take the freshly created actor veth with it.
func TestSetupActorNetworkSweepsInteriorLinks(t *testing.T) {
	roottest.Require(t, "creating network namespaces and veth pairs")
	ctx := context.Background()

	withTestPodNetNS(t, func() {
		// Create the actor's namespace by setting it up once, then plant a
		// leftover in it the way a previous activation's kata tap would be.
		first, err := SetupActorNetwork(ctx, ActorNetworkConfig{ActorUID: "actor-a", Slot: 0})
		if err != nil {
			t.Fatalf("SetupActorNetwork (first): %v", err)
		}
		const leftover = "stale-tap0"
		if err := ateomnet.NetNSDo(ctx, first.NetNS, func(context.Context) error {
			return netlink.LinkAdd(&netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: leftover}})
		}); err != nil {
			t.Fatalf("planting a leftover interior link: %v", err)
		}

		network, err := SetupActorNetwork(ctx, ActorNetworkConfig{
			ActorUID:           "actor-a",
			Slot:               0,
			SweepInteriorLinks: true,
		})
		if err != nil {
			t.Fatalf("SetupActorNetwork (second): %v", err)
		}
		t.Cleanup(func() { _ = CleanupActorNetwork(ctx, network) })

		if err := ateomnet.NetNSDo(ctx, network.NetNS, func(context.Context) error {
			if _, err := netlink.LinkByName(leftover); err == nil {
				t.Errorf("leftover interior link %q was not swept", leftover)
			}
			if _, err := netlink.LinkByName(ateomnet.ActorVethName); err != nil {
				t.Errorf("actor veth %q missing after the sweep: %v", ateomnet.ActorVethName, err)
			}
			return nil
		}); err != nil {
			t.Fatalf("inspecting the actor's netns: %v", err)
		}
	})
}

// TestSetupActorNetworkBuildsAnActorsSide pins what one actor ends up with: its
// own namespace holding the address every actor holds, and a pod-side address
// that is its alone.
func TestSetupActorNetworkBuildsAnActorsSide(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestPodNetNS(t, func() {
		network, err := SetupActorNetwork(ctx, ActorNetworkConfig{ActorUID: "actor-a", Slot: 0})
		if err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}
		t.Cleanup(func() { _ = CleanupActorNetwork(ctx, network) })

		if got, want := network.PodSideIP.String(), "169.254.32.1"; got != want {
			t.Errorf("pod-side address = %s, want %s", got, want)
		}
		if _, err := netlink.LinkByName(network.HostVeth()); err != nil {
			t.Errorf("host veth %q missing: %v", network.HostVeth(), err)
		}
		// The route is what makes this actor reachable unambiguously, which a
		// route to the shared actor address could never be.
		routes, err := netlink.RouteList(nil, netlink.FAMILY_V4)
		if err != nil {
			t.Fatalf("listing routes: %v", err)
		}
		var found bool
		for _, route := range routes {
			if route.Dst != nil && route.Dst.IP.Equal(network.PodSideIP) {
				found = true
			}
		}
		if !found {
			t.Errorf("no route to the actor's pod-side address %s", network.PodSideIP)
		}

		// Inside, the actor sees the address it was checkpointed with.
		if err := ateomnet.NetNSDo(ctx, network.NetNS, func(context.Context) error {
			link, err := netlink.LinkByName(ateomnet.ActorVethName)
			if err != nil {
				return err
			}
			addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
			if err != nil {
				return err
			}
			for _, addr := range addrs {
				if addr.IP.String() == ateomnet.ActorVethIP {
					return nil
				}
			}
			t.Errorf("actor interface addresses = %v, want %s", addrs, ateomnet.ActorVethIP)
			return nil
		}); err != nil {
			t.Fatalf("inspecting the actor's netns: %v", err)
		}
	})
}

// TestTwoActorsHoldTheSameAddress is the property the whole design rests on:
// two actors on one worker, each in its own namespace at the same address, told
// apart only by their pod-side addresses.
func TestTwoActorsHoldTheSameAddress(t *testing.T) {
	roottest.Require(t, "creating network namespaces and veth pairs")
	ctx := context.Background()

	withTestPodNetNS(t, func() {
		a, err := SetupActorNetwork(ctx, ActorNetworkConfig{ActorUID: "actor-a", Slot: 0})
		if err != nil {
			t.Fatalf("SetupActorNetwork(a): %v", err)
		}
		t.Cleanup(func() { _ = CleanupActorNetwork(ctx, a) })
		b, err := SetupActorNetwork(ctx, ActorNetworkConfig{ActorUID: "actor-b", Slot: 1})
		if err != nil {
			t.Fatalf("SetupActorNetwork(b): %v", err)
		}
		t.Cleanup(func() { _ = CleanupActorNetwork(ctx, b) })

		if a.PodSideIP.Equal(b.PodSideIP) {
			t.Fatalf("both actors got pod-side address %s; they must differ", a.PodSideIP)
		}
		if a.HostVeth() == b.HostVeth() {
			t.Fatalf("both actors got veth %q; they must differ", a.HostVeth())
		}
		// Same address inside both, which is the point: it is frozen into every
		// actor's snapshot and cannot be allocated per actor.
		for _, network := range []*ActorNetwork{a, b} {
			if err := ateomnet.NetNSDo(ctx, network.NetNS, func(context.Context) error {
				link, err := netlink.LinkByName(ateomnet.ActorVethName)
				if err != nil {
					return err
				}
				addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
				if err != nil {
					return err
				}
				if len(addrs) != 1 || addrs[0].IP.String() != ateomnet.ActorVethIP {
					t.Errorf("actor %s sees %v, want just %s", network.ActorUID, addrs, ateomnet.ActorVethIP)
				}
				return nil
			}); err != nil {
				t.Fatalf("inspecting %s: %v", network.ActorUID, err)
			}
		}
	})
}

// TestApplyActorNetworkRulesRebuild covers the ruleset tracking the hosted set
// as actors come and go. The reconcile is one transaction precisely so an actor
// already running never loses its addressing mid-flight; this checks the states
// either side of it, which is what a test can observe.
func TestApplyActorNetworkRulesRebuild(t *testing.T) {
	roottest.Require(t, "installing nftables rules")

	withTestPodNetNS(t, func() {
		requireNftables(t)

		a := &ActorNetwork{ActorUID: "actor-a", Slot: 0, PodSideIP: testPlan.IP(0), NetNS: noNetNS}
		b := &ActorNetwork{ActorUID: "actor-b", Slot: 1, PodSideIP: testPlan.IP(1), NetNS: noNetNS}

		if err := ApplyActorNetworkRules(testPlan, []*ActorNetwork{a}, 15001); err != nil {
			t.Fatalf("apply(a): %v", err)
		}
		if got, want := mapKeys(t, "actor_podside"), []string{"ate0"}; !slices.Equal(got, want) {
			t.Errorf("map keys = %v, want %v", got, want)
		}

		if err := ApplyActorNetworkRules(testPlan, []*ActorNetwork{a, b}, 15001); err != nil {
			t.Fatalf("apply(a,b): %v", err)
		}
		if got, want := mapKeys(t, "actor_podside"), []string{"ate0", "ate1"}; !slices.Equal(got, want) {
			t.Errorf("map keys = %v, want %v", got, want)
		}

		// Removing an actor takes its entry with it and leaves the rest, rather
		// than leaving one that would misdirect the next actor into the slot.
		if err := ApplyActorNetworkRules(testPlan, []*ActorNetwork{b}, 15001); err != nil {
			t.Fatalf("apply(b): %v", err)
		}
		if got, want := mapKeys(t, "actor_podside"), []string{"ate1"}; !slices.Equal(got, want) {
			t.Errorf("map keys after removing an actor = %v, want %v", got, want)
		}

		// An empty set removes the table outright: with no actors there is
		// nothing to rewrite, redirect, or masquerade.
		if err := ApplyActorNetworkRules(testPlan, nil, 15001); err != nil {
			t.Fatalf("apply(none): %v", err)
		}
		if table := actorTable(t); table != nil {
			t.Errorf("actor table still present with no actors hosted")
		}
	})
}
