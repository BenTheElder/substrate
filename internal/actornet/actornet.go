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

// Package actornet builds the per-actor networking a worker uses when it hosts
// more than one actor at a time. It supersedes the single-actor wiring in
// internal/ateomnet, which will go once every ateom has moved over.
package actornet

// Networking for a worker that hosts more than one actor at a time.
//
// Every actor sees the same address. It has to: a restored guest's network
// configuration is frozen in its memory snapshot (ateom-microvm configures the
// guest only on cold boot), so an actor must find itself at ActorVethIP on every
// worker it ever resumes on. Per-actor addressing on the actor's side is
// therefore not available, and N actors in one worker pod are, from the pod's
// point of view, indistinguishable.
//
// They are made distinguishable by a stateless 1:1 rewrite at each actor's veth:
// inbound, before conntrack, the actor's source becomes a pod-side address
// unique to it; outbound, after routing, the reverse. The actor never observes
// this -- it keeps its frozen address and its frozen gateway -- while
// everything in the pod netns deals in
// an address that identifies exactly one actor. Routing, conntrack, the TCP
// redirect to atunnel, and DNS masquerade all then work without any of them
// having to know that actors share an address.
//
// Two things this deliberately does NOT do, both tried and measured:
//
//   - Handle traffic inside the actor's netns. gVisor removes the addresses from
//     that namespace and injects frames with AF_PACKET, so nothing there can
//     route and its netfilter hooks never see actor traffic. The pod netns is
//     the only place with a working host stack.
//   - Separate actors with conntrack zones. Zones keep identical 5-tuples in
//     separate entries, but a reply still un-NATs to the shared actor address
//     and the pod netns has N equally-good routes to it, so one actor's answer
//     is delivered to another. Zones also break their own return path: a reply
//     arrives on the pod's uplink, where there is no input interface to key the
//     zone off.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	// DefaultPodSideSubnet is the range actors are known by inside the worker
	// pod when nothing overrides it. It is pod-local and never persisted, so
	// unlike the actor's own frozen address it is free to be chosen per
	// deployment, and it is link-local so it cannot escape the pod.
	//
	// Link-local is unadministered, which is why no allocator has to be asked
	// for it -- and also why it can collide: anything else on the node is
	// equally free to squat here. That is what PodSidePlan exists to let a
	// deployment resolve.
	DefaultPodSideSubnet = "169.254.32.0/20"

	// maxPodSideSlots caps how many slots any prefix yields. A /20 is already
	// far more actors than a worker will host, and the cap keeps a careless
	// prefix (or, later, an IPv6 one) from producing a slot count that overflows
	// the capacity it is reported as.
	maxPodSideSlots = 65534
)

// PodSidePlan is how a worker addresses the actors it hosts: a prefix, and the
// slot-to-address mapping over it.
//
// The plan is per worker process and never leaves it, so two workers may use
// different prefixes and nothing has to agree. What the plan does decide is how
// many actors the worker can address at all, which is why the ateom reports
// Slots() as its capacity rather than a constant -- change the prefix and the
// ceiling the control plane places against follows, with nothing to configure
// on the other side.
type PodSidePlan struct {
	prefix netip.Prefix
	slots  int
}

// NewPodSidePlan builds a plan over cidr.
//
// IPv6 is rejected rather than mishandled. The addressing here would extend to
// it, but the ruleset this package builds is an IPv4 nftables table operating on
// IPv4 header offsets, and the actor's own frozen address is IPv4 and baked into
// every existing snapshot. So the shape is family-agnostic and the guard is one
// place to remove, but the work is real and is not done.
func NewPodSidePlan(cidr string) (*PodSidePlan, error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return nil, fmt.Errorf("parsing pod-side subnet %q: %w", cidr, err)
	}
	prefix = prefix.Masked()
	if !prefix.Addr().Is4() {
		return nil, fmt.Errorf("pod-side subnet %q is not IPv4: IPv6 actor addressing is not implemented", cidr)
	}
	// Usable hosts, less the network and broadcast addresses.
	hostBits := prefix.Addr().BitLen() - prefix.Bits()
	if hostBits < 2 {
		return nil, fmt.Errorf("pod-side subnet %q is too small to address any actor", cidr)
	}
	slots := maxPodSideSlots
	if hostBits < 32 {
		if n := (1 << hostBits) - 2; n < slots {
			slots = n
		}
	}
	return &PodSidePlan{prefix: prefix, slots: slots}, nil
}

// addrFromIP converts a net.IP for prefix containment checks.
func addrFromIP(ip net.IP) (netip.Addr, bool) {
	if v4 := ip.To4(); v4 != nil {
		return netip.AddrFrom4([4]byte(v4)), true
	}
	return netip.Addr{}, false
}

// MustPodSidePlan is NewPodSidePlan for a prefix known good at compile time.
func MustPodSidePlan(cidr string) *PodSidePlan {
	plan, err := NewPodSidePlan(cidr)
	if err != nil {
		panic(err)
	}
	return plan
}

// DefaultPodSidePlan is the plan a worker uses when nothing overrides it.
func DefaultPodSidePlan() *PodSidePlan { return MustPodSidePlan(DefaultPodSideSubnet) }

// Slots is how many actors this plan can address, and so the ceiling the ateom
// reports as its capacity.
func (p *PodSidePlan) Slots() int { return p.slots }

// Subnet is the prefix in CIDR form, as the nftables rules match on it.
func (p *PodSidePlan) Subnet() string { return p.prefix.String() }

// IP is the address the pod netns knows the actor in slot by. Slots start at
// zero and map to the first usable address upward, skipping the network address.
func (p *PodSidePlan) IP(slot int) net.IP {
	addr := p.prefix.Addr()
	for range slot + 1 {
		addr = addr.Next()
	}
	return net.IP(addr.AsSlice())
}

// HostVethName is the worker-pod end of the actor in slot's veth pair. Named by
// slot rather than by actor so it fits the 15-character interface name limit
// whatever the actor is called.
func HostVethName(slot int) string { return fmt.Sprintf("ate%d", slot) }

// ActorNetNSName is the named network namespace an actor's sandbox runs in. One
// per actor, not one per worker pod, since each actor needs its own address
// space to hold the same ActorVethIP.
//
// Defined in ateompath because atelet writes the matching path into the OCI
// bundle before ateom has created the namespace; the two must agree, so there
// is one definition rather than a convention repeated in both.
func ActorNetNSName(actorUID string) string { return ateompath.ActorNetNSName(actorUID) }

// ActorNetwork is one actor's network on this worker.
type ActorNetwork struct {
	// ActorUID identifies the actor; it names the netns.
	ActorUID string
	// Slot is the worker-local index that fixes the veth name and pod-side
	// address. Stable for the life of the activation and reusable afterwards.
	Slot int
	// NetNS is the actor's namespace, where its sandbox runs. Unset is
	// noNetNS, NOT the zero value: netns.NsHandle is a file descriptor, and its
	// zero value is 0, which is stdin rather than "nothing".
	NetNS netns.NsHandle
	// PodSideIP is what the pod netns addresses this actor by; the actor itself
	// never sees it. This is what atunnel dials for ingress, and what an actor's
	// egress appears to come from.
	PodSideIP net.IP
	// TunneledEgress arms the transparent TCP redirect into atunnel for THIS
	// actor. It is per actor rather than per worker because actors here do not
	// agree: one with no egress gateway has no atunnel activation, and a
	// redirect it never asked for would send its TCP to a listener that closes
	// every connection it cannot attribute. Its egress belongs on the
	// masquerade path instead.
	TunneledEgress bool
}

// nftTableName is the worker's actor table. Unchanged from when ateomnet owned
// it, so an ateom rolling onto this code still recognizes and clears the table
// its predecessor left in the pod netns.
const nftTableName = "ateom_actor"

// noNetNS is an ActorNetwork that holds no namespace. Spelled out because the
// zero value of a file descriptor is a real one.
const noNetNS = netns.NsHandle(-1)

// holdsNetNS reports whether this ActorNetwork owns a namespace handle worth
// closing. Standard descriptors are excluded: nothing here should ever hold one,
// so seeing one means a handle was never set, and closing it would take out the
// process's own stdin, stdout, or stderr.
func (n *ActorNetwork) holdsNetNS() bool {
	return int(n.NetNS) > 2
}

// HostVeth is the worker-pod end of this actor's veth pair.
func (n *ActorNetwork) HostVeth() string { return HostVethName(n.Slot) }

// ActorNetworkConfig describes the network to build for one actor.
type ActorNetworkConfig struct {
	ActorUID string
	Slot     int

	// Plan is the worker's pod-side addressing. Nil uses the default, so a
	// caller that has no opinion is not forced to state one.
	Plan *PodSidePlan

	// HostVethHWAddr fixes the worker-side MAC. The micro-VM runtime needs it:
	// a snapshot freezes the guest's ARP entry for its gateway, so a restored
	// guest only reaches the network if the gateway answers with the same MAC on
	// every pod. Zero leaves the kernel's random MAC, which is fine for gVisor.
	HostVethHWAddr net.HardwareAddr

	// SweepInteriorLinks deletes leftover links in the actor's namespace before
	// wiring it. Used by the micro-VM runtime to clear a previous activation's
	// tap device. A namespace this call just created has nothing to sweep, so it
	// only matters when reusing one.
	SweepInteriorLinks bool

	// TunneledEgress arms this actor's transparent TCP redirect into atunnel.
	// Set it when the activation supplies an egress gateway; see
	// ActorNetwork.TunneledEgress.
	TunneledEgress bool
}

// SetupActorNetwork builds one actor's network: its own namespace, a veth pair
// into it, and the address rewrite that makes it distinguishable from the other
// actors on this worker.
//
// It does NOT install the shared rules (redirect, masquerade). Those describe
// the whole actor set rather than one actor, so the caller applies them with
// ApplyActorNetworkRules once the set has changed.
func SetupActorNetwork(ctx context.Context, cfg ActorNetworkConfig) (_ *ActorNetwork, retErr error) {
	if cfg.ActorUID == "" {
		return nil, fmt.Errorf("actor UID is required")
	}
	plan := cfg.Plan
	if plan == nil {
		plan = DefaultPodSidePlan()
	}
	if cfg.Slot < 0 || cfg.Slot >= plan.Slots() {
		return nil, fmt.Errorf("slot %d is outside 0..%d", cfg.Slot, plan.Slots()-1)
	}

	network := &ActorNetwork{
		ActorUID:       cfg.ActorUID,
		Slot:           cfg.Slot,
		PodSideIP:      plan.IP(cfg.Slot),
		NetNS:          noNetNS,
		TunneledEgress: cfg.TunneledEgress,
	}

	// Clear anything a previous activation in this slot left behind before
	// building on top of it. Idempotent, so a retry after a partial failure
	// starts from the same place a first attempt does.
	if err := CleanupActorNetwork(ctx, network); err != nil {
		return nil, fmt.Errorf("while clearing stale actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if err := CleanupActorNetwork(ctx, network); err != nil {
				slog.WarnContext(ctx, "Failed to clean up partially configured actor network",
					slog.String("actorUID", cfg.ActorUID), slog.Any("err", err))
			}
		}
	}()

	// Before the veth exists, not after: an interface takes its own rp_filter
	// from conf.default when it is created, and the effective setting is the max
	// of that and conf.all. Setting this afterwards would leave the veth built by
	// the first activation validating reverse paths it cannot satisfy.
	if err := ateomnet.DisableReversePathFilter(); err != nil {
		return nil, err
	}

	ns, err := ateomnet.CreateNetNSWithoutSwitching(ActorNetNSName(cfg.ActorUID))
	if err != nil {
		return nil, fmt.Errorf("while creating actor netns: %w", err)
	}
	network.NetNS = ns

	if cfg.SweepInteriorLinks {
		if err := sweepInteriorLinks(ctx, ns); err != nil {
			return nil, err
		}
	}

	// The peer is born in the actor's namespace under its final name: moving and
	// renaming a netdev each cost an RCU grace period under the global RTNL lock,
	// which is most of what actor network setup used to spend.
	veth := &netlink.Veth{
		LinkAttrs:     netlink.LinkAttrs{Name: network.HostVeth()},
		PeerName:      ateomnet.ActorVethName,
		PeerNamespace: netlink.NsFd(int(ns)),
	}
	if len(cfg.HostVethHWAddr) > 0 {
		veth.LinkAttrs.HardwareAddr = cfg.HostVethHWAddr
	}
	if err := netlink.LinkAdd(veth); err != nil {
		return nil, fmt.Errorf("while creating actor veth pair: %w", err)
	}

	host, err := netlink.LinkByName(network.HostVeth())
	if err != nil {
		return nil, fmt.Errorf("while getting host veth: %w", err)
	}
	// The worker end carries the gateway address the actor's frozen config
	// points at. Every actor's veth carries the same one, which Linux permits;
	// nothing routes by it, because the rewrite below means the pod netns
	// addresses actors by their pod-side address instead.
	if err := netlink.AddrReplace(host, ateomnet.HostVethAddr); err != nil {
		return nil, fmt.Errorf("while assigning host veth address: %w", err)
	}
	if err := netlink.LinkSetUp(host); err != nil {
		return nil, fmt.Errorf("while bringing up host veth: %w", err)
	}
	// The route that makes this actor reachable, and unambiguously so.
	if err := netlink.RouteReplace(&netlink.Route{
		LinkIndex: host.Attrs().Index,
		Dst:       &net.IPNet{IP: network.PodSideIP, Mask: net.CIDRMask(32, 32)},
		Scope:     netlink.SCOPE_LINK,
	}); err != nil {
		return nil, fmt.Errorf("while routing %s to %s: %w", network.PodSideIP, network.HostVeth(), err)
	}

	if err := ateomnet.NetNSDo(ctx, ns, func(context.Context) error {
		return configureActorInterior()
	}); err != nil {
		return nil, fmt.Errorf("while configuring actor veth in its netns: %w", err)
	}

	if err := ateomnet.EnableIPv4Forwarding(); err != nil {
		return nil, err
	}
	return network, nil
}

// configureActorInterior sets up what the actor sees, which is the same on every
// worker it ever runs on. Assumes it is already inside the actor's namespace.
//
// Nothing pins a neighbor entry for the gateway. Several veths in the worker
// pod carry that address, which looks like it should need pinning, but each veth
// pair is its own layer-2 domain: an ARP leaving this actor can only arrive on
// its own pod-side veth, and is answered there with that veth's MAC. Neither
// runtime would read such an entry anyway -- gVisor runs its own netstack over
// AF_PACKET and ARPs on the wire, and the micro-VM guest pins its own entry
// internally via the agent (see ateom-microvm's configureGuestNetwork), frozen
// into its snapshot along with the rest of its network configuration.
func configureActorInterior() error {
	lo, err := netlink.LinkByName("lo")
	if err != nil {
		return fmt.Errorf("while acquiring lo: %w", err)
	}
	if err := netlink.LinkSetUp(lo); err != nil {
		return fmt.Errorf("while bringing up lo: %w", err)
	}
	actorLink, err := netlink.LinkByName(ateomnet.ActorVethName)
	if err != nil {
		return fmt.Errorf("while acquiring actor veth: %w", err)
	}
	if err := netlink.AddrReplace(actorLink, ateomnet.ActorVethAddr); err != nil {
		return fmt.Errorf("while assigning actor veth address: %w", err)
	}
	if err := netlink.LinkSetUp(actorLink); err != nil {
		return fmt.Errorf("while bringing up actor veth: %w", err)
	}
	return netlink.RouteReplace(&netlink.Route{
		LinkIndex: actorLink.Attrs().Index,
		Gw:        ateomnet.ActorVethGwIP,
	})
}

func sweepInteriorLinks(ctx context.Context, ns netns.NsHandle) error {
	return ateomnet.NetNSDo(ctx, ns, func(ctx context.Context) error {
		links, err := netlink.LinkList()
		if err != nil {
			return fmt.Errorf("while listing interior netns links: %w", err)
		}
		for _, l := range links {
			if l.Attrs().Name == "lo" {
				continue
			}
			if err := netlink.LinkDel(l); err != nil {
				slog.WarnContext(ctx, "Failed to delete leftover interior link",
					slog.String("link", l.Attrs().Name), slog.Any("err", err))
			}
		}
		return nil
	})
}

// CleanupActorNetwork removes one actor's network. Intentionally idempotent, so
// it can run before setup, after a checkpoint, and from a failed setup without
// the caller having to know how far things got.
//
// Deleting the worker-side veth takes its peer with it, and deleting the
// namespace takes anything left inside. The shared rules are not touched: they
// describe the whole actor set, and the caller reapplies them with
// ApplyActorNetworkRules once this actor is out of it.
func CleanupActorNetwork(ctx context.Context, network *ActorNetwork) error {
	var cleanupErr error

	if link, err := netlink.LinkByName(HostVethName(network.Slot)); err == nil {
		if err := netlink.LinkDel(link); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while deleting host veth: %w", err))
		}
	} else if _, notFound := errors.AsType[netlink.LinkNotFoundError](err); !notFound {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while looking up host veth: %w", err))
	}

	// Drop the route explicitly: it survives the link it pointed at only in
	// unusual cases, but a stale route to a reused pod-side address would send
	// the next actor's traffic nowhere.
	if err := netlink.RouteDel(&netlink.Route{
		Dst: &net.IPNet{IP: network.PodSideIP, Mask: net.CIDRMask(32, 32)},
	}); err != nil && !errors.Is(err, unix.ESRCH) && !errors.Is(err, unix.ENOENT) {
		slog.WarnContext(ctx, "Failed to delete pod-side route",
			slog.String("ip", network.PodSideIP.String()), slog.Any("err", err))
	}

	// Guarded on holdsNetNS, not on IsOpen: IsOpen only asks whether the handle
	// is not -1, and an ActorNetwork built to describe a network that does not
	// exist yet has the zero value, which is fd 0. Closing that closes stdin --
	// quietly, the first time, and then EBADF -- and worse, once fd 0 is free
	// the next namespace opened can land on it and be closed out from under the
	// actor using it.
	if network.holdsNetNS() {
		if err := network.NetNS.Close(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while closing actor netns handle: %w", err))
		}
		network.NetNS = noNetNS
	}
	if network.ActorUID != "" {
		if err := netns.DeleteNamed(ActorNetNSName(network.ActorUID)); err != nil && !errors.Is(err, unix.ENOENT) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while deleting actor netns: %w", err))
		}
	}
	return cleanupErr
}

// ApplyActorNetworkRules reconciles the worker's nftables table with the actors
// it is currently hosting. egressRedirectPort is atunnel's local egress
// listener; zero installs no redirect and leaves actor egress on the masquerade
// path.
//
// The rules are FIXED -- five of them, whether the worker hosts two actors or
// four thousand. Everything that varies per actor lives in an nftables map and
// a set, so an activation writes elements rather than rule expression trees.
//
// Rules are emptied and refilled in ONE transaction: committing an empty state
// separately would take addressing away from every running actor for as long as
// the rebuild takes. AddTable, AddChain and AddSet are idempotent creates, so a
// reconcile changes only what differs.
//
// The map and set are diffed against what the kernel holds and only the
// difference written, so an activation costs the change rather than the
// worker's occupancy. See reconcileSetElements for the wire limits a full
// refill hits.
func ApplyActorNetworkRules(plan *PodSidePlan, actors []*ActorNetwork, egressRedirectPort uint16) error {
	if plan == nil {
		plan = DefaultPodSidePlan()
	}
	c := &nftables.Conn{}
	if len(actors) == 0 {
		return removeActorTable(c)
	}

	table := c.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: nftTableName})

	// podSideByVeth maps an actor's host-side veth to the pod-side address that
	// names it. This is the entirety of the per-actor inbound state.
	podSideByVeth := &nftables.Set{
		Table:    table,
		Name:     "actor_podside",
		IsMap:    true,
		KeyType:  nftables.TypeIFName,
		DataType: nftables.TypeIPAddr,
	}
	// tunneledEgress holds the pod-side addresses whose TCP is redirected into
	// atunnel. Only actors that armed it are in here: atunnel closes any egress
	// connection it cannot attribute to an activation, so redirecting an actor
	// that never asked would drop its TCP rather than masquerade it out.
	tunneledEgress := &nftables.Set{
		Table:   table,
		Name:    "tunneled_egress",
		KeyType: nftables.TypeIPAddr,
	}
	if err := c.AddSet(podSideByVeth, nil); err != nil {
		return fmt.Errorf("while declaring the actor address map: %w", err)
	}
	if err := c.AddSet(tunneledEgress, nil); err != nil {
		return fmt.Errorf("while declaring the tunneled-egress set: %w", err)
	}

	// Drop the rules before adding them back. Ordering inside the batch is the
	// order these calls are made in, so this has to precede every AddRule below.
	c.FlushTable(table)

	// Inbound rewrite, ahead of the conntrack lookup so the entry is created
	// against an address unique to this actor. Without this every actor's flows
	// share one 5-tuple space and collide. The map lookup is both the match and
	// the value: a packet arriving on an interface that is not an actor veth
	// finds nothing and falls through.
	rewriteIn := c.AddChain(&nftables.Chain{
		Name: "actor-rewrite-in", Table: table, Type: nftables.ChainTypeFilter,
		Hooknum: nftables.ChainHookPrerouting, Priority: nftables.ChainPriorityRaw,
	})
	c.AddRule(&nftables.Rule{Table: table, Chain: rewriteIn, Exprs: concat(
		[]expr.Any{
			&expr.Meta{Key: expr.MetaKeyIIFNAME, Register: 1},
			&expr.Lookup{
				SourceRegister: 1,
				DestRegister:   2,
				IsDestRegSet:   true,
				SetName:        podSideByVeth.Name,
				SetID:          podSideByVeth.ID,
			},
		},
		// Only the frozen actor address is rewritten. Register 1 is reused here;
		// the lookup has already put what it needs in register 2.
		ipFieldEqual(ipHeaderSrc, ateomnet.ActorVethIP),
		setIPFieldFromRegister(ipHeaderSrc, 2),
	)})

	// Outbound rewrite, after routing has already chosen the actor's veth from
	// the pod-side address, so the frozen address goes back on at the last
	// moment.
	//
	// One rule with no per-actor state, because the address it writes is the
	// same for every actor -- they all hold it. Routing has already
	// disambiguated by the time this runs: a packet only reaches postrouting
	// with a pod-side destination if a /32 route sent it out that actor's veth.
	rewriteOut := c.AddChain(&nftables.Chain{
		Name: "actor-rewrite-out", Table: table, Type: nftables.ChainTypeFilter,
		Hooknum: nftables.ChainHookPostrouting, Priority: nftables.ChainPriorityMangle,
	})
	c.AddRule(&nftables.Rule{Table: table, Chain: rewriteOut, Exprs: concat(
		ipPrefixEqual(ipHeaderDst, plan.Subnet()),
		setIPField(ipHeaderDst, ActorVethGwAddrFor(ateomnet.ActorVethIP)),
	)})

	prerouting := c.AddChain(&nftables.Chain{
		Name: "prerouting", Table: table, Type: nftables.ChainTypeNAT,
		Hooknum: nftables.ChainHookPrerouting, Priority: nftables.ChainPriorityNATDest,
	})
	if egressRedirectPort != 0 {
		c.AddRule(&nftables.Rule{Table: table, Chain: prerouting, Exprs: concat(
			[]expr.Any{
				&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: ipHeaderSrc, Len: 4},
				&expr.Lookup{SourceRegister: 1, SetName: tunneledEgress.Name, SetID: tunneledEgress.ID},
			},
			ateomnet.TCPProtocol(),
			[]expr.Any{
				&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(egressRedirectPort)},
				&expr.Redir{RegisterProtoMin: 1},
			},
		)})
	}

	postrouting := c.AddChain(&nftables.Chain{
		Name: "postrouting", Table: table, Type: nftables.ChainTypeNAT,
		Hooknum: nftables.ChainHookPostrouting, Priority: nftables.ChainPriorityNATSource,
	})
	// Masquerade is what carries DNS and anything else the TCP tunnel does not
	// handle, straight out of the pod with no proxy in the path. It matches the
	// pod-side range, so it sees one address per actor and conntrack can tell
	// the replies apart even when the actors' own tuples are identical.
	c.AddRule(&nftables.Rule{Table: table, Chain: postrouting, Exprs: concat(
		ipPrefixEqual(ipHeaderSrc, plan.Subnet()),
		[]expr.Any{&expr.Masq{}},
	)})

	acceptPolicy := nftables.ChainPolicyAccept
	forward := c.AddChain(&nftables.Chain{
		Name: "forward", Table: table, Type: nftables.ChainTypeFilter,
		Hooknum: nftables.ChainHookForward, Priority: nftables.ChainPriorityFilter,
		Policy: &acceptPolicy,
	})
	c.AddRule(&nftables.Rule{Table: table, Chain: forward,
		Exprs: []expr.Any{&expr.Verdict{Kind: expr.VerdictAccept}}})

	if err := c.Flush(); err != nil {
		return fmt.Errorf("while installing actor nftables rules: %w", err)
	}

	// The per-actor state, after the sets above exist to hold it.
	vethElems := make([]nftables.SetElement, 0, len(actors))
	var tunneledElems []nftables.SetElement
	for _, a := range actors {
		key := make([]byte, unix.IFNAMSIZ)
		copy(key, a.HostVeth())
		vethElems = append(vethElems, nftables.SetElement{Key: key, Val: a.PodSideIP.To4()})
		if a.TunneledEgress {
			tunneledElems = append(tunneledElems, nftables.SetElement{Key: a.PodSideIP.To4()})
		}
	}
	if err := reconcileSetElements(c, podSideByVeth, vethElems); err != nil {
		return fmt.Errorf("while populating the actor address map: %w", err)
	}
	if err := reconcileSetElements(c, tunneledEgress, tunneledElems); err != nil {
		return fmt.Errorf("while populating the tunneled-egress set: %w", err)
	}
	return nil
}

// reconcileSetElements brings set to exactly want, writing only the difference.
//
// Rewriting the whole set instead hits two limits, both of which fail SILENTLY:
//
//   - One SetAddElements puts every element in a single
//     NFTA_SET_ELEM_LIST_ELEMENTS attribute, whose length is a uint16. At 40
//     bytes an element, 4+40*1639 is the first size past 65535: the length
//     wraps and the kernel keeps only the fraction it describes, so the rest of
//     the actors vanish from the map.
//   - Splitting across messages clears that, and then the transaction outgrows
//     what one sendmsg takes (EMSGSIZE) past about three of them.
//
// A diff avoids both, and removes a window where the flush and the repopulate
// straddled the commit.
func reconcileSetElements(c *nftables.Conn, set *nftables.Set, want []nftables.SetElement) error {
	have, err := c.GetSetElements(set)
	if err != nil {
		return fmt.Errorf("while reading set %q: %w", set.Name, err)
	}

	// Keyed on the value too, so an element whose data changed is replaced
	// rather than left stale -- a map cannot hold two entries for one key, and
	// the delete below has to happen for the add to be accepted.
	type entry struct{ key, val string }
	index := func(elems []nftables.SetElement) map[entry]nftables.SetElement {
		m := make(map[entry]nftables.SetElement, len(elems))
		for _, e := range elems {
			m[entry{string(e.Key), string(e.Val)}] = e
		}
		return m
	}
	haveIdx, wantIdx := index(have), index(want)

	var add, remove []nftables.SetElement
	for k, e := range wantIdx {
		if _, ok := haveIdx[k]; !ok {
			add = append(add, e)
		}
	}
	for k, e := range haveIdx {
		if _, ok := wantIdx[k]; !ok {
			remove = append(remove, e)
		}
	}

	// Removals first: a replaced key needs its old entry gone before the new one
	// is accepted.
	if err := applySetElements(c, set, remove, c.SetDeleteElements); err != nil {
		return fmt.Errorf("while removing from set %q: %w", set.Name, err)
	}
	if err := applySetElements(c, set, add, c.SetAddElements); err != nil {
		return fmt.Errorf("while adding to set %q: %w", set.Name, err)
	}
	return nil
}

// applySetElements commits elems ONE netlink message at a time, which puts both
// limits on reconcileSetElements out of reach by construction.
//
// A cold rebuild therefore spans several transactions. These are additions for
// actors whose veths already exist, so a partial apply leaves some briefly
// unwired -- the state being recovered from, not a regression from it.
func applySetElements(c *nftables.Conn, set *nftables.Set, elems []nftables.SetElement, queue func(*nftables.Set, []nftables.SetElement) error) error {
	perMessage := setElementsPerMessage(set)
	for len(elems) > 0 {
		n := min(perMessage, len(elems))
		if err := queue(set, elems[:n]); err != nil {
			return err
		}
		if err := c.Flush(); err != nil {
			return err
		}
		elems = elems[n:]
	}
	return nil
}

// setElementsPerMessage is how many of set's elements fit in one netlink
// message, derived from the encoding so a change to the key or value width
// cannot quietly walk back over the limit.
//
// Per element the library emits a nest holding a key attribute and, for a map,
// a data attribute; each value sits in its own NFTA_DATA_VALUE, and every
// nlattr costs a 4-byte header and rounds its payload up to 4 bytes.
func setElementsPerMessage(set *nftables.Set) int {
	const (
		attrHeader = 4
		// Attribute lengths are uint16, and the elements share the attribute
		// with nothing else, so this is the whole budget less its own header.
		maxPayload = 65535 - attrHeader
	)
	align4 := func(n int) int { return (n + 3) &^ 3 }
	nested := func(payload int) int { return attrHeader + payload }

	// Key sizes come from the type rather than from an element, so an empty set
	// still gets a sane answer.
	perElem := nested(nested(attrHeader + align4(int(set.KeyType.Bytes))))
	if set.IsMap {
		perElem += nested(nested(attrHeader + align4(int(set.DataType.Bytes))))
	}
	perElem += attrHeader // the element's own nest header

	if n := maxPayload / perElem; n > 0 {
		return n
	}
	return 1
}

// removeActorTable deletes the worker's actor table if it is there.
func removeActorTable(c *nftables.Conn) error {
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("while listing nftables tables: %w", err)
	}
	for _, table := range tables {
		if table.Name != nftTableName {
			continue
		}
		c.DelTable(table)
		if err := c.Flush(); err != nil {
			return fmt.Errorf("while deleting actor nftables table: %w", err)
		}
		return nil
	}
	return nil
}

// Offsets of the source and destination address in the IPv4 header, and of its
// checksum, which a write to either has to fix up.
const (
	ipHeaderSrc      = 12
	ipHeaderDst      = 16
	ipHeaderChecksum = 10
)

// ActorVethGwAddrFor is the actor-side address a pod-side address maps back to.
// Constant today because every actor holds the same one; a function so callers
// read as though it were per actor, which is what it would become if the frozen
// address ever stopped being universal.
func ActorVethGwAddrFor(actorIP string) net.IP { return ateomnet.MustParseIP(actorIP) }

func concat(groups ...[]expr.Any) []expr.Any {
	var out []expr.Any
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

func ipFieldEqual(offset uint32, ip string) []expr.Any {
	return []expr.Any{
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: offset, Len: 4},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: ateomnet.MustParseIP(ip)},
	}
}

func ipPrefixEqual(offset uint32, cidr string) []expr.Any {
	_, prefix, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(fmt.Sprintf("parsing constant CIDR %q: %v", cidr, err))
	}
	return []expr.Any{
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: offset, Len: 4},
		&expr.Bitwise{SourceRegister: 1, DestRegister: 1, Len: 4,
			Mask: prefix.Mask, Xor: []byte{0, 0, 0, 0}},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: prefix.IP.To4()},
	}
}

// setIPField writes an address into the IPv4 header, fixing the header checksum
// and the L4 pseudo-header checksums that depend on it. Without the pseudo-header
// flag the addresses would be rewritten correctly and every TCP and UDP payload
// would then be discarded as corrupt.
// setIPFieldFromRegister writes whatever a previous expression left in reg into
// an IPv4 header field, fixing up both checksums. The register form is what
// lets one rule serve every actor: the value comes from the map lookup.
func setIPFieldFromRegister(offset uint32, reg uint32) []expr.Any {
	return []expr.Any{
		&expr.Payload{
			OperationType:  expr.PayloadWrite,
			SourceRegister: reg,
			Base:           expr.PayloadBaseNetworkHeader,
			Offset:         offset,
			Len:            4,
			CsumType:       expr.CsumTypeInet,
			CsumOffset:     ipHeaderChecksum,
			CsumFlags:      unix.NFT_PAYLOAD_L4CSUM_PSEUDOHDR,
		},
	}
}

func setIPField(offset uint32, ip net.IP) []expr.Any {
	return []expr.Any{
		&expr.Immediate{Register: 1, Data: ip.To4()},
		&expr.Payload{
			OperationType:  expr.PayloadWrite,
			SourceRegister: 1,
			Base:           expr.PayloadBaseNetworkHeader,
			Offset:         offset,
			Len:            4,
			CsumType:       expr.CsumTypeInet,
			CsumOffset:     ipHeaderChecksum,
			CsumFlags:      unix.NFT_PAYLOAD_L4CSUM_PSEUDOHDR,
		},
	}
}
