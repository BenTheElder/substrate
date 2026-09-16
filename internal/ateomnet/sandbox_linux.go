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
	"net/netip"
	"runtime"
	"strconv"
	"sync"
	"syscall"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// SandboxNetwork holds a sandbox's runtime and gateway namespaces.
type SandboxNetwork struct {
	// ActorUID names the namespaces, so a teardown can find them again.
	ActorUID string
	// RuntimeNetNS is what the sandbox runs in. gVisor claims every interface
	// here and moves their addresses into its own stack.
	RuntimeNetNS netns.NsHandle
	// GatewayNetNS holds atunnel's sockets and the kernel end of the link.
	// For microVMs it shares RuntimeNetNS.
	GatewayNetNS netns.NsHandle
	// PodSideIP is identical across sandboxes, isolated by namespace.
	PodSideIP net.IP
}

func (n *SandboxNetwork) holdsNetNS() bool { return n.RuntimeNetNS > 0 }

// SandboxNetworkConfig describes one actor's private networking.
type SandboxNetworkConfig struct {
	ActorUID string

	// Veth separates gVisor's interfaces from the kernel-owned gateway.
	// MicroVMs use a tap in a single namespace instead.
	Veth bool

	// EgressPort is where atunnel serves this actor. Every TCP connection the
	// actor makes is redirected to it, whatever port it was aimed at.
	EgressPort uint16

	// DNSPort is where the actor's resolver answers, on the gateway address.
	DNSPort uint16

	// GatewayHWAddr fixes the MAC the actor's gateway answers with. A micro-VM
	// snapshot freezes the guest's ARP entry for it, so a random MAC would
	// blackhole guest egress until that entry expired. gVisor re-ARPs and can
	// leave it unset.
	GatewayHWAddr net.HardwareAddr
}

// SetupSandboxNetwork creates isolated networking with fixed sandbox addresses.
// gVisor uses a veth pair across runtime and gateway namespaces because it takes
// over every interface in its namespace. MicroVMs use a tap in one namespace.
// Gateway nftables redirect TCP egress to atunnel, preserving SO_ORIGINAL_DST.
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

	// Without a veth, atunnel shares the namespace with the runtime's tap.
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

		// Keep the kernel-owned peer outside gVisor's namespace.
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

		// gVisor imports these addresses and routes into its network stack.
		if err := NetNSDo(ctx, actorNS, func(context.Context) error {
			// Loopback lets the actor reach its own address.
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
		PodSideIP:    net.ParseIP(ActorVethIP),
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

// installEgressRedirect redirects TCP egress to atunnel, excluding the sandbox's
// own /30 so replies to ingress connections are not redirected.
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

// gatewayVethName is the veth peer in the gateway namespace.
const gatewayVethName = "atside"

// SandboxGatewayNetNSName names the namespace holding the veth peer and atunnel's
// sockets for one actor.
func SandboxGatewayNetNSName(actorUID string) string {
	return ateompath.ActorNetNSName(actorUID) + "-at"
}

// CleanupSandboxNetwork closes namespace handles and removes their names.
func CleanupSandboxNetwork(network *SandboxNetwork) error {
	if network == nil {
		return nil
	}
	var errs error
	// Compare before Close sets the handle to -1; microVMs share one descriptor.
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

// ListenInNetNS opens wildcard TCP listeners inside ns.
// Sockets retain their namespace and can be served from another namespace.
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
	Serve(ctx context.Context, listener net.Listener) error
}

// ServeSandboxEgress serves redirected TCP in the gateway namespace.
// Closing the returned listeners stops accepting new connections.
func ServeSandboxEgress(ctx context.Context, e EgressServer, ns netns.NsHandle, ports []uint16) ([]net.Listener, error) {
	listeners, err := ListenInNetNS(ctx, ns, ports)
	if err != nil {
		return nil, fmt.Errorf("while opening actor egress listeners: %w", err)
	}
	for _, l := range listeners {
		go func(l net.Listener) {
			// Background rather than the caller's context: these outlive the
			// activation and are stopped by closing the listener.
			if err := e.Serve(context.Background(), l); err != nil {
				slog.WarnContext(ctx, "Sandbox egress listener stopped", slog.Any("err", err))
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

// ServeSandboxDNS serves UDP and TCP DNS in the gateway namespace.
func ServeSandboxDNS(ctx context.Context, relay DNSServer, ns netns.NsHandle, port uint16) (_ []io.Closer, retErr error) {
	// Bind the wildcard because the microVM tap's gateway address is added later.
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

// NetNSDialer dials TCP or UDP IP literals in ns, pinning a thread only until
// the socket is created.
func NetNSDialer(ns netns.NsHandle) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch network {
		case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
		default:
			return nil, net.UnknownNetworkError(network)
		}
		hostname, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if _, err := netip.ParseAddr(hostname); err != nil {
			return nil, fmt.Errorf("sandbox dial requires an IP literal: %w", err)
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
			locked := true
			restore := func() error {
				if !locked {
					return nil
				}
				if err := netns.Set(host); err != nil {
					return fmt.Errorf("while restoring the worker netns: %w", err)
				}
				runtime.UnlockOSThread()
				locked = false
				return nil
			}
			dialer := &net.Dialer{ControlContext: func(context.Context, string, string, syscall.RawConn) error {
				if !locked {
					return fmt.Errorf("sandbox dial cannot recreate its socket outside the namespace")
				}
				return restore()
			}}
			conn, dialErr := dialer.DialContext(ctx, network, addr)
			if err := restore(); err != nil {
				if conn != nil {
					conn.Close()
				}
				done <- result{err: err}
				return
			}
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

// SandboxSession owns a sandbox's network and serving sockets.
type SandboxSession struct {
	Network *SandboxNetwork

	mu      sync.Mutex
	sockets []io.Closer
}

// ServeSandbox builds a sandbox's network and serves it from the gateway
// namespace: egress on the redirect's port, DNS on 53. Either server may be nil
// to leave that unserved, which fails the sandbox closed rather than letting it
// out.
func ServeSandbox(ctx context.Context, cfg SandboxNetworkConfig, egress EgressServer, dns DNSServer) (_ *SandboxSession, retErr error) {
	network, err := SetupSandboxNetwork(ctx, cfg)
	if err != nil {
		return nil, err
	}
	session := &SandboxSession{Network: network}
	defer func() {
		if retErr != nil {
			_ = session.Close(ctx)
		}
	}()

	if egress != nil {
		listeners, err := ServeSandboxEgress(ctx, egress, network.GatewayNetNS, []uint16{cfg.EgressPort})
		if err != nil {
			return nil, err
		}
		for _, l := range listeners {
			session.sockets = append(session.sockets, l)
		}
	}
	if dns != nil {
		served, err := ServeSandboxDNS(ctx, dns, network.GatewayNetNS, cfg.DNSPort)
		if err != nil {
			return nil, err
		}
		session.sockets = append(session.sockets, served...)
	}
	return session, nil
}

// Close stops serving the sandbox and removes its namespaces.
func (s *SandboxSession) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.sockets {
		_ = c.Close()
	}
	s.sockets = nil
	if s.Network == nil {
		return nil
	}
	network := s.Network
	s.Network = nil
	return CleanupSandboxNetwork(network)
}

// Dialer reaches the sandbox from the gateway namespace, the only place its
// address is routable.
func (s *SandboxSession) Dialer() func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		s.mu.Lock()
		if s.Network == nil {
			s.mu.Unlock()
			return nil, net.ErrClosed
		}
		fd, err := unix.FcntlInt(uintptr(s.Network.GatewayNetNS), unix.F_DUPFD_CLOEXEC, 0)
		s.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("while retaining the sandbox namespace: %w", err)
		}
		ns := netns.NsHandle(fd)
		defer ns.Close()
		return NetNSDialer(ns)(ctx, network, address)
	}
}
