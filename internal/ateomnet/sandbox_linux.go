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

package ateomnet

import (
	"context"
	"errors"
	"fmt"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strconv"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// SandboxNetwork is one sandbox's private networking: the namespace the runtime
// is given, and the namespace atunnel serves it from. They are the same handle
// unless the runtime needs them split.
type SandboxNetwork struct {
	// ActorUID names the namespaces, so a teardown can find them again.
	ActorUID string
	// RuntimeNetNS is what the sandbox runs in. gVisor claims every interface
	// here and moves their addresses into its own stack.
	RuntimeNetNS netns.NsHandle
	// GatewayNetNS holds the kernel end of the link and atunnel's sockets. The
	// same namespace as RuntimeNetNS when the runtime leaves the kernel on the
	// packet path, as a micro-VM does.
	GatewayNetNS netns.NsHandle
	// PodSideIP is the address the sandbox is reached on. Every sandbox holds
	// the same one; which namespace answers is what tells them apart.
	PodSideIP net.IP
}

func (n *SandboxNetwork) holdsNetNS() bool { return n.RuntimeNetNS > 0 }

// SandboxNetworkConfig describes one actor's private networking.
type SandboxNetworkConfig struct {
	ActorUID string

	// Veth builds a veth pair and hands eth0 to an inner namespace, leaving the
	// peer and atunnel's sockets one namespace out. Required for gVisor, which
	// claims every interface in the namespace it is given. A micro-VM does not
	// need it: its tap is already the boundary, and the namespace keeps the end
	// the kernel owns.
	Veth bool

	// EgressPort is where atunnel serves this actor. Every TCP connection the
	// actor makes is redirected to it, whatever port it was aimed at.
	EgressPort uint16

	// GatewayHWAddr fixes the MAC the actor's gateway answers with. A micro-VM
	// snapshot freezes the guest's ARP entry for it, so a random MAC would
	// blackhole guest egress until that entry expired. gVisor re-ARPs and can
	// leave it unset.
	GatewayHWAddr net.HardwareAddr
}

// SetupSandboxNetwork gives an actor private networking and nothing that any
// other actor shares: no address from a worker-wide plan and no nftables.
//
//	actor netns:   what the workload sees. Under Veth, lo and eth0 holding
//	               ActorVethIP with a default route at ActorVethGwIP -- the same
//	               view an actor has under every other design, so a restored
//	               guest still finds itself where its snapshot expects.
//	atunnel netns: the gateway address, a local default route, and the sockets
//	               atunnel serves this actor on. The outer namespace under Veth,
//	               otherwise the actor's own.
//
// gVisor needs the two split because it claims EVERY interface in the namespace
// it is given -- measured, see hack/experiments/gvisor-netns-probe.sh -- and
// moves their addresses into its own stack. A peer beside eth0 would be
// swallowed with it, leaving nothing able to terminate TCP. One namespace out,
// the kernel still owns the peer, so atunnel serves the actor with ordinary
// sockets and no userspace network stack is needed.
//
// A single nftables redirect in the atunnel namespace puts atunnel in front of
// every TCP connection the actor makes, and SO_ORIGINAL_DST recovers where it
// was headed. Nothing about the actor's addressing is per-actor: every actor
// holds the same /30, so no address has to be allocated or tracked.
func SetupSandboxNetwork(ctx context.Context, cfg SandboxNetworkConfig) (_ *SandboxNetwork, retErr error) {
	actorUID := cfg.ActorUID
	if actorUID == "" {
		return nil, fmt.Errorf("actornet: actor UID is required")
	}

	actorName := ateompath.ActorNetNSName(actorUID)
	actorNS, err := CreateNetNSWithoutSwitching(actorName)
	if err != nil {
		return nil, fmt.Errorf("while creating the actor netns %s: %w", actorName, err)
	}
	defer func() {
		if retErr != nil {
			actorNS.Close()
			_ = removeNamedNetNS(actorName)
		}
	}()

	// Without a veth the actor's own namespace is where atunnel listens: the tap
	// the runtime adds later is the boundary, and the kernel keeps this end.
	atunnelNS := actorNS
	if cfg.Veth {
		outerName := SandboxGatewayNetNSName(actorUID)
		outer, err := CreateNetNSWithoutSwitching(outerName)
		if err != nil {
			return nil, fmt.Errorf("while creating the outer netns %s: %w", outerName, err)
		}
		defer func() {
			if retErr != nil {
				outer.Close()
				_ = removeNamedNetNS(outerName)
			}
		}()
		atunnelNS = outer

		// Built in the outer namespace, then eth0 is handed to the actor, so the
		// pair never exists anywhere both ends are visible to gVisor.
		if err := NetNSDo(ctx, outer, func(context.Context) error {
			veth := &netlink.Veth{
				LinkAttrs: netlink.LinkAttrs{Name: gatewayVethName},
				PeerName:  ActorVethName,
			}
			if cfg.GatewayHWAddr != nil {
				veth.LinkAttrs.HardwareAddr = cfg.GatewayHWAddr
			}
			if err := netlink.LinkAdd(veth); err != nil {
				return fmt.Errorf("while creating the veth pair: %w", err)
			}
			atSide, err := netlink.LinkByName(gatewayVethName)
			if err != nil {
				return err
			}
			if err := netlink.AddrReplace(atSide, HostVethAddr); err != nil {
				return fmt.Errorf("while assigning the atunnel-side address: %w", err)
			}
			if err := netlink.LinkSetUp(atSide); err != nil {
				return err
			}
			peer, err := netlink.LinkByName(ActorVethName)
			if err != nil {
				return err
			}
			return netlink.LinkSetNsFd(peer, int(actorNS))
		}); err != nil {
			return nil, err
		}

		// The actor side is what gVisor reads its addresses and routes off
		// before taking them into its own stack, so it has to look like an
		// ordinary gateway attachment.
		if err := NetNSDo(ctx, actorNS, func(context.Context) error {
			// The actor reaches its own address over loopback, so lo has to be
			// up even though nothing else here uses it.
			if err := linkUp("lo"); err != nil {
				return err
			}
			eth0, err := netlink.LinkByName(ActorVethName)
			if err != nil {
				return err
			}
			if err := netlink.AddrReplace(eth0, ActorVethAddr); err != nil {
				return fmt.Errorf("while assigning the actor address: %w", err)
			}
			if err := netlink.LinkSetUp(eth0); err != nil {
				return err
			}
			return netlink.RouteReplace(&netlink.Route{
				LinkIndex: eth0.Attrs().Index,
				Gw:        ActorVethGwIP,
			})
		}); err != nil {
			return nil, err
		}
	}

	if err := setupGatewaySide(ctx, atunnelNS, cfg.EgressPort); err != nil {
		return nil, err
	}

	return &SandboxNetwork{
		ActorUID:     actorUID,
		RuntimeNetNS: actorNS,
		GatewayNetNS: atunnelNS,
		// Every actor holds the same address; the namespace is the identity.
		PodSideIP: net.ParseIP(ActorVethIP),
	}, nil
}

func linkUp(name string) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		return err
	}
	return netlink.LinkSetUp(link)
}

// setupGatewaySide brings up lo and puts atunnel in front of the actor's TCP.
func setupGatewaySide(ctx context.Context, ns netns.NsHandle, egressPort uint16) error {
	if err := NetNSDo(ctx, ns, func(context.Context) error {
		// atunnel answers the actor's DNS on 53, and the worker holds no
		// CAP_NET_BIND_SERVICE.
		if err := AllowUnprivilegedPorts(); err != nil {
			return err
		}
		return linkUp("lo")
	}); err != nil {
		return err
	}
	return installEgressRedirect(ns, egressPort)
}

// installEgressRedirect sends every TCP connection the actor makes to atunnel,
// whatever port it was aimed at, so which ports an actor may reach is the
// actor's business and nothing has to be declared on the worker.
//
// One rule and no sets, because exactly one actor's traffic crosses this
// namespace. That is the difference from the worker-wide ruleset it replaces,
// which held an element per actor and is what put a ceiling on how many a
// worker could hold. The actor's own /30 is excluded so atunnel can still dial
// back in to serve ingress.
func installEgressRedirect(ns netns.NsHandle, egressPort uint16) error {
	if egressPort == 0 {
		return fmt.Errorf("actornet: atunnel egress port is required")
	}
	c, err := nftables.New(nftables.WithNetNSFd(int(ns)))
	if err != nil {
		return fmt.Errorf("while opening nftables in the actor namespace: %w", err)
	}
	defer func() { _ = c.CloseLasting() }()

	table := c.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: "ateom-actor"})
	prerouting := c.AddChain(&nftables.Chain{
		Name: "prerouting", Table: table, Type: nftables.ChainTypeNAT,
		Hooknum: nftables.ChainHookPrerouting, Priority: nftables.ChainPriorityNATDest,
	})

	exprs := []expr.Any{
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: ipv4HeaderDst, Len: 4},
		&expr.Bitwise{
			SourceRegister: 1, DestRegister: 1, Len: 4,
			Mask: actorSubnetMask(), Xor: []byte{0, 0, 0, 0},
		},
		&expr.Cmp{Op: expr.CmpOpNeq, Register: 1, Data: actorSubnetBase()},
	}
	exprs = append(exprs, l4ProtocolEqual(unix.IPPROTO_TCP)...)
	exprs = append(exprs,
		&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(egressPort)},
		&expr.Redir{RegisterProtoMin: 1},
	)
	c.AddRule(&nftables.Rule{Table: table, Chain: prerouting, Exprs: exprs})

	if err := c.Flush(); err != nil {
		return fmt.Errorf("while installing the actor egress redirect: %w", err)
	}
	return nil
}

// ipv4HeaderDst is the offset of the destination address in an IPv4 header.
const ipv4HeaderDst = 16

func actorSubnet() *net.IPNet {
	_, subnet, err := net.ParseCIDR(ActorVethSubnet)
	if err != nil {
		panic(fmt.Sprintf("parsing constant actor subnet %q: %v", ActorVethSubnet, err))
	}
	return subnet
}

func actorSubnetBase() []byte { return actorSubnet().IP.To4() }
func actorSubnetMask() []byte { return []byte(actorSubnet().Mask) }

// gatewayVethName is the peer's name in the outer namespace. It never
// appears in the sandbox, so it does not have to look like anything.
const gatewayVethName = "atside"

// SandboxGatewayNetNSName names the namespace holding the veth peer and atunnel's
// sockets for one actor.
func SandboxGatewayNetNSName(actorUID string) string {
	return ateompath.ActorNetNSName(actorUID) + "-at"
}

// CleanupSandboxNetwork releases the namespace. Nothing else was built, so
// nothing else has to be taken apart.
func CleanupSandboxNetwork(network *SandboxNetwork) error {
	if network == nil {
		return nil
	}
	var errs error
	// Recorded before the first Close, which zeroes the handle it is called on:
	// without a veth both name the same namespace, and closing a descriptor
	// twice would land on whatever reused the number.
	separate := network.GatewayNetNS != network.RuntimeNetNS
	if network.holdsNetNS() {
		if err := network.RuntimeNetNS.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("while closing the sandbox netns: %w", err))
		}
	}
	if separate && network.GatewayNetNS > 0 {
		if err := network.GatewayNetNS.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("while closing the gateway netns: %w", err))
		}
	}
	// Deleting the namespaces takes any veth pair with them.
	for _, name := range []string{ateompath.ActorNetNSName(network.ActorUID), SandboxGatewayNetNSName(network.ActorUID)} {
		if err := removeNamedNetNS(name); err != nil {
			errs = errors.Join(errs, fmt.Errorf("while deleting netns %s: %w", name, err))
		}
	}
	return errs
}

// ListenInNetNS opens a TCP listener per port inside ns, bound to the
// wildcard address so any destination the local default route delivers is
// accepted. A socket keeps the namespace it was created in, so the caller
// serves these from wherever it likes.
func ListenInNetNS(ctx context.Context, ns netns.NsHandle, ports []uint16) (_ []net.Listener, retErr error) {
	var listeners []net.Listener
	defer func() {
		if retErr != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
		}
	}()
	if err := NetNSDo(ctx, ns, func(context.Context) error {
		for _, port := range ports {
			l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
			if err != nil {
				return fmt.Errorf("while listening on port %d: %w", port, err)
			}
			listeners = append(listeners, l)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return listeners, nil
}

// EgressServer serves one actor's captured connections. Satisfied by
// atunnel.Egress; an interface so this package does not depend on it.
type EgressServer interface {
	ServeFor(ctx context.Context, actorKey string, listener net.Listener) error
}

// ServeSandboxEgress puts the egress server's sockets inside the actor's own
// namespace, where the local default route delivers everything it sends. The
// listener is the actor's identity: they all hold the same address, so nothing
// about a connection distinguishes them.
//
// Only ports gets captured. A port with no listener is refused rather than
// escaping, which is the fail-closed half of routing everything through the
// tunnel. Closing the returned listeners stops the actor's egress.
func ServeSandboxEgress(ctx context.Context, e EgressServer, actorKey string, ns netns.NsHandle, ports []uint16) ([]net.Listener, error) {
	listeners, err := ListenInNetNS(ctx, ns, ports)
	if err != nil {
		return nil, fmt.Errorf("while opening actor egress listeners: %w", err)
	}
	for _, l := range listeners {
		go func(l net.Listener) {
			// Background rather than the caller's context: these outlive the
			// activation and are stopped by closing the listener.
			if err := e.ServeFor(context.Background(), actorKey, l); err != nil {
				slog.WarnContext(ctx, "Actor egress listener stopped",
					slog.String("actorUID", actorKey), slog.Any("err", err))
			}
		}(l)
	}
	return listeners, nil
}

// DNSServer answers an actor's DNS. Satisfied by atunnel.DNSRelay; an interface
// so this package does not depend on it.
type DNSServer interface {
	ServePacket(ctx context.Context, pc net.PacketConn) error
	Serve(ctx context.Context, listener net.Listener) error
}

// ServeSandboxDNS answers the actor's DNS on its gateway address, from inside the
// namespace atunnel serves it in.
//
// The address is the same in every actor on every worker, so an actor's
// resolv.conf can name it and still be right after a snapshot is restored
// somewhere else -- which is what lets DNS work without allocating anything per
// actor. Both transports are served: a resolver falls back to TCP when an
// answer does not fit in a datagram.
//
// Closing the returned sockets stops answering for this actor.
func ServeSandboxDNS(ctx context.Context, relay DNSServer, ns netns.NsHandle, port uint16) (_ []io.Closer, retErr error) {
	// The wildcard rather than the gateway address: a micro-VM's gateway lives
	// on the tap, which the runtime creates when it boots the guest, so the
	// address does not exist yet when the actor is hosted. Binding the wildcard
	// answers on it either way, and only lo and the actor's own device are ever
	// in this namespace.
	address := net.JoinHostPort("0.0.0.0", strconv.Itoa(int(port)))

	var packet net.PacketConn
	var stream net.Listener
	if err := NetNSDo(ctx, ns, func(context.Context) error {
		pc, err := net.ListenPacket("udp", address)
		if err != nil {
			return fmt.Errorf("while opening the actor DNS socket: %w", err)
		}
		packet = pc
		l, err := net.Listen("tcp", address)
		if err != nil {
			_ = pc.Close()
			return fmt.Errorf("while opening the actor DNS listener: %w", err)
		}
		stream = l
		return nil
	}); err != nil {
		return nil, err
	}

	// Detached from the caller's context, which ends with the activation RPC,
	// but cancelable: the relay's capacity is the worker's, so a query still in
	// flight has to be dropped when the actor goes away rather than held until
	// it times out.
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		if err := relay.ServePacket(serveCtx, packet); err != nil {
			slog.WarnContext(ctx, "Actor DNS socket stopped", slog.Any("err", err))
		}
	}()
	go func() {
		if err := relay.Serve(serveCtx, stream); err != nil {
			slog.WarnContext(ctx, "Actor DNS listener stopped", slog.Any("err", err))
		}
	}()
	// Cancel first: closing the sockets alone leaves the queries already being
	// resolved holding the relay.
	return []io.Closer{closerFunc(func() error { stopServing(); return nil }), packet, stream}, nil
}

// closerFunc adapts a cancel function to io.Closer, so a caller takes a
// sandbox's sockets and the work behind them down as one list.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// NetNSDialer dials from inside ns.
//
// The connection is made on a thread pinned into the namespace for the whole
// dial, and on its own goroutine so the pin cannot leak: a network namespace is
// a property of an OS thread, and Go moves goroutines between threads freely.
// http.Transport in particular dials on a background goroutine, so a dialer
// that did not pin would connect from the worker's namespace and reach either
// nothing or the wrong actor.
func NetNSDialer(ns netns.NsHandle) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		type result struct {
			conn net.Conn
			err  error
		}
		done := make(chan result, 1)
		go func() {
			runtime.LockOSThread()
			// Deliberately not unlocked on the failure path below: a thread
			// whose namespace could not be restored must not be reused.
			host, err := netns.Get()
			if err != nil {
				done <- result{err: fmt.Errorf("while reading the current netns: %w", err)}
				return
			}
			defer host.Close()
			if err := netns.Set(ns); err != nil {
				runtime.UnlockOSThread()
				done <- result{err: fmt.Errorf("while entering the actor netns: %w", err)}
				return
			}
			conn, dialErr := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err := netns.Set(host); err != nil {
				if conn != nil {
					conn.Close()
				}
				done <- result{err: fmt.Errorf("while restoring the worker netns: %w", err)}
				return
			}
			runtime.UnlockOSThread()
			done <- result{conn: conn, err: dialErr}
		}()
		completed := <-done
		if err := ctx.Err(); err != nil {
			if completed.conn != nil {
				_ = completed.conn.Close()
			}
			return nil, err
		}
		return completed.conn, completed.err
	}
}
