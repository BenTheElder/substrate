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

package ateomnet

import (
	"context"
	"errors"
	"fmt"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/vishvananda/netlink"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/roottest"
)

// Two actors, each in its own namespace, both holding the same address, both
// reached through their own dialer. This is the whole POC3 premise in one test.
// testEgressPort stands in for atunnel's egress listener, which the ateom
// passes in from its own flag.
const testEgressPort = 15001

func TestNetNSDialerCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NetNSDialer(-1)(ctx, "tcp", "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dial: got %v, want cancellation", err)
	}
}

func TestSetupSandboxNetwork(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	type actor struct {
		net  *SandboxNetwork
		body string
	}
	actors := map[string]*actor{}
	for _, uid := range []string{"11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"} {
		n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{ActorUID: uid, Veth: true, EgressPort: testEgressPort})
		if err != nil {
			t.Fatalf("SetupSandboxNetwork(%s): %v", uid, err)
		}
		t.Cleanup(func() {
			if err := CleanupSandboxNetwork(n); err != nil {
				t.Errorf("cleanup %s: %v", uid, err)
			}
		})
		if got := n.PodSideIP.String(); got != ActorVethIP {
			t.Errorf("actor address = %s, want the same %s every actor holds", got, ActorVethIP)
		}

		// The actor's app, bound where a real one binds, inside its namespace.
		var lis net.Listener
		if err := NetNSDo(ctx, n.RuntimeNetNS, func(context.Context) error {
			l, err := net.Listen("tcp", net.JoinHostPort(ActorVethIP, "80"))
			lis = l
			return err
		}); err != nil {
			t.Fatalf("actor %s listen: %v", uid, err)
		}
		t.Cleanup(func() { lis.Close() })
		body := "i-am-" + uid[:8]
		go http.Serve(lis, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, body)
		}))
		actors[uid] = &actor{net: n, body: body}
	}

	// Reaching each actor is a matter of which namespace the dial is made from.
	for uid, a := range actors {
		client := &http.Client{Transport: &http.Transport{DialContext: NetNSDialer(a.net.RuntimeNetNS)}, Timeout: 5 * time.Second}
		resp, err := client.Get((&url.URL{Scheme: "http", Host: net.JoinHostPort(ActorVethIP, "80")}).String())
		if err != nil {
			t.Fatalf("reaching actor %s: %v", uid, err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(got) != a.body {
			t.Errorf("actor %s answered %q, want %q", uid, got, a.body)
		}
	}

	// The worker's own namespace must not reach any of them: the address is
	// meaningless outside a namespace, which is what makes reuse safe.
	direct := &http.Client{Timeout: 2 * time.Second}
	if _, err := direct.Get("http://" + net.JoinHostPort(ActorVethIP, "80")); err == nil {
		t.Error("the worker namespace reached an actor directly; addresses are not isolated")
	}
}

// With atunnel not listening, nothing escapes: the redirect sends the actor's
// TCP to a port with nothing behind it, so the connection is refused rather
// than reaching the internet. Which ports the actor may use is no longer the
// policy -- whether atunnel is there to carry it is.
func TestActorEgressIsFailClosedWithoutAtunnel(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{
		ActorUID: "33333333-3333-3333-3333-333333333333", Veth: true, EgressPort: testEgressPort,
	})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	for _, destination := range []string{"93.184.216.34:443", "93.184.216.34:8080"} {
		if err := NetNSDo(ctx, n.RuntimeNetNS, func(context.Context) error {
			c, err := net.DialTimeout("tcp", destination, 3*time.Second)
			if err != nil {
				return err
			}
			c.Close()
			return nil
		}); err == nil {
			t.Errorf("the actor reached %s with no atunnel listening; egress is not fail-closed", destination)
		}
	}
}

// atunnel dials the actor from the outer namespace, so ingress has to cross the
// pair while the local default route is catching everything else. The connected
// /30 wins for in-subnet destinations; the negative control is what proves the
// dial reached the sandbox rather than being answered locally.
func TestIngressCrossesThePairWhileEgressIsCaptured(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{ActorUID: "44444444-4444-4444-4444-444444444444", Veth: true, EgressPort: testEgressPort})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	var app net.Listener
	if err := NetNSDo(ctx, n.RuntimeNetNS, func(context.Context) error {
		l, e := net.Listen("tcp", net.JoinHostPort(ActorVethIP, "80"))
		app = l
		return e
	}); err != nil {
		t.Fatalf("actor listen: %v", err)
	}
	defer app.Close()
	go func() {
		for {
			c, e := app.Accept()
			if e != nil {
				return
			}
			io.WriteString(c, "the-actor")
			c.Close()
		}
	}()

	dial := NetNSDialer(n.GatewayNetNS)
	c, err := dial(ctx, "tcp", net.JoinHostPort(ActorVethIP, "80"))
	if err != nil {
		t.Fatalf("ingress dial: %v", err)
	}
	got, _ := io.ReadAll(c)
	c.Close()
	if string(got) != "the-actor" {
		t.Errorf("ingress reached %q, want %q", got, "the-actor")
	}

	// A port the actor is not serving must be refused by the sandbox, not
	// accepted by the local default route.
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if c2, e := dial(cctx, "tcp", net.JoinHostPort(ActorVethIP, "81")); e == nil {
		c2.Close()
		t.Error("a port with no listener was accepted, so ingress is not reaching the sandbox")
	}
}

// The whole egress contract in one test: whatever port the actor aims at, the
// connection arrives on atunnel's one listener and still says where it was
// headed. The port is deliberately not one anybody configured -- that is the

// The micro-VM shape: one namespace, no veth. The tap ateom builds carries the
// guest's traffic INTO the namespace, so the kernel keeps the interface side
// and terminates it. Only the namespace shape is asserted here -- a process
// dialing from inside takes the output path, which a prerouting redirect never
// sees, so it is not a stand-in for a guest behind a tap.
func TestSetupSandboxNetworkWithoutVeth(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{
		ActorUID:   "77777777-7777-7777-7777-777777777777",
		EgressPort: testEgressPort,
	})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	if n.GatewayNetNS != n.RuntimeNetNS {
		t.Errorf("got a second namespace (%v vs %v); this shape needs one", n.GatewayNetNS, n.RuntimeNetNS)
	}

	// No veth was built, so nothing but lo is here until the tap arrives.
	if err := NetNSDo(ctx, n.RuntimeNetNS, func(context.Context) error {
		links, err := netlink.LinkList()
		if err != nil {
			return err
		}
		for _, l := range links {
			if l.Attrs().Name != "lo" {
				t.Errorf("unexpected interface %q in the actor namespace", l.Attrs().Name)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("listing links: %v", err)
	}
}

// A micro-VM snapshot freezes the guest's ARP entry for its gateway, so the
// gateway has to answer with the same MAC on every worker.
func TestGatewayHardwareAddressIsFixedWhenAsked(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	want, err := net.ParseMAC("02:00:00:00:17:01")
	if err != nil {
		t.Fatal(err)
	}
	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{
		ActorUID:      "88888888-8888-8888-8888-888888888888",
		Veth:          true,
		EgressPort:    testEgressPort,
		GatewayHWAddr: want,
	})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	if err := NetNSDo(ctx, n.GatewayNetNS, func(context.Context) error {
		l, err := netlink.LinkByName("atside")
		if err != nil {
			return err
		}
		if got := l.Attrs().HardwareAddr.String(); got != want.String() {
			t.Errorf("gateway MAC is %s, want %s", got, want)
		}
		return nil
	}); err != nil {
		t.Fatalf("reading the gateway link: %v", err)
	}
}

// A teardown that fails leaves the actor's namespace names behind. The kernel
// creates them with O_EXCL, so without removing the leftovers first the same
// actor could never be hosted on this worker again.
func TestSetupSucceedsOverALeftoverNamespace(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()
	const uid = "aaaaaaaa-0000-0000-0000-00000000000a"
	cfg := SandboxNetworkConfig{ActorUID: uid, Veth: true, EgressPort: testEgressPort}

	first, err := SetupSandboxNetwork(ctx, cfg)
	if err != nil {
		t.Fatalf("first SetupSandboxNetwork: %v", err)
	}
	// Drop the handles WITHOUT deleting the names, which is what a worker that
	// failed or died mid-teardown leaves on disk.
	first.RuntimeNetNS.Close()
	if first.GatewayNetNS != first.RuntimeNetNS {
		first.GatewayNetNS.Close()
	}
	for _, name := range []string{ateompath.ActorNetNSName(uid), SandboxGatewayNetNSName(uid)} {
		if _, err := os.Stat("/var/run/netns/" + name); err != nil {
			t.Fatalf("expected leftover netns %s: %v", name, err)
		}
	}

	second, err := SetupSandboxNetwork(ctx, cfg)
	if err != nil {
		t.Fatalf("the actor is wedged by its own leftover namespace: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(second) })

	// And it is a working namespace, not just a created one.
	if err := NetNSDo(ctx, second.RuntimeNetNS, func(context.Context) error {
		if _, err := netlink.LinkByName(ActorVethName); err != nil {
			return fmt.Errorf("actor interface missing after reuse: %w", err)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

// The worker-wide ruleset drops actor UDP to any port but DNS, because there it
// would otherwise escape through the compatibility masquerade. Namespace-only
// actors have no masquerade, and the property is worth pinning down because
// QUIC on 443 is exactly how an actor would dodge a TCP-only tunnel.
//
// Asserted in the namespace the datagram lands in, not from the actor: UDP is
// unacknowledged, so an actor's send succeeds on the strength of its default
// route whether or not anything ever carries the packet further.
func TestActorUDPHasNowhereToGoBeyondTheNamespacePair(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{
		ActorUID: "bbbbbbbb-0000-0000-0000-00000000000b", Veth: true, EgressPort: testEgressPort,
	})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	// The actor's UDP arrives here on the veth peer. With no route beyond the
	// pair and no masquerade, there is nothing to forward it out of.
	for _, destination := range []string{"93.184.216.34:443", "93.184.216.34:53"} {
		if err := NetNSDo(ctx, n.GatewayNetNS, func(context.Context) error {
			c, err := net.Dial("udp", destination)
			if err != nil {
				return err
			}
			defer c.Close()
			_, err = c.Write([]byte("probe"))
			return err
		}); err == nil {
			t.Errorf("the atunnel namespace can reach %s over UDP; actor UDP could follow it out", destination)
		}
	}
}

// DNS end to end in the namespace pair: the actor asks its gateway, atunnel
// answers from the namespace next door by re-asking the worker pod's resolver.
// The gateway address is identical in every actor, which is what lets a

// Without a veth both handles name the same namespace. Closing a descriptor
// twice would close whatever reused the number -- another actor's namespace, or
// any socket the worker happens to open next.
func TestCleanupClosesEachDescriptorOnce(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	network, err := SetupSandboxNetwork(context.Background(), SandboxNetworkConfig{
		ActorUID:   "close-once",
		EgressPort: 15001,
	})
	if err != nil {
		t.Fatal(err)
	}
	if network.RuntimeNetNS != network.GatewayNetNS {
		t.Fatalf("expected one namespace without a veth, got %d and %d", network.RuntimeNetNS, network.GatewayNetNS)
	}

	// A second close of the same descriptor reports EBADF.
	if err := CleanupSandboxNetwork(network); err != nil {
		t.Fatalf("CleanupSandboxNetwork closed a descriptor twice: %v", err)
	}
}

// stoppableDNS records that its serving contexts were canceled.
type stoppableDNS struct{ packet, stream chan struct{} }

func (d *stoppableDNS) ServePacket(ctx context.Context, pc net.PacketConn) error {
	<-ctx.Done()
	close(d.packet)
	return pc.Close()
}

func (d *stoppableDNS) Serve(ctx context.Context, l net.Listener) error {
	<-ctx.Done()
	close(d.stream)
	return l.Close()
}

// The relay is the worker's, shared by every actor it hosts. Closing a
// sandbox's sockets has to stop the work behind them too, or an actor's
// queries keep holding capacity the next one needs.
func TestClosingSandboxDNSStopsServing(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	network, err := SetupSandboxNetwork(context.Background(), SandboxNetworkConfig{
		ActorUID:   "dns-teardown",
		EgressPort: 15001,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = CleanupSandboxNetwork(network) }()

	relay := &stoppableDNS{packet: make(chan struct{}), stream: make(chan struct{})}
	closers, err := ServeSandboxDNS(context.Background(), relay, network.GatewayNetNS, 53)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range closers {
		_ = c.Close()
	}

	for _, tc := range []struct {
		name    string
		stopped chan struct{}
	}{{"UDP", relay.packet}, {"TCP", relay.stream}} {
		select {
		case <-tc.stopped:
		case <-time.After(5 * time.Second):
			t.Errorf("%s serving outlived the sandbox's sockets", tc.name)
		}
	}
}
