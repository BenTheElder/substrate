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

package atunnel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// DNSPort is where the relay answers. An actor's resolver is the gateway
	// address, which is the same in every actor on every worker, so a snapshot
	// may freeze it and still find the relay when it is restored elsewhere.
	DNSPort = 53

	// maxDNSDatagram is the largest UDP payload there is. The relay parses no
	// DNS, so it cannot know what buffer size a query advertised: anything
	// smaller would chop a datagram the actor and its resolver had agreed on,
	// and forward the remains as if they were the whole answer.
	maxDNSDatagram = 65535

	// maxInFlightDNS bounds the queries being resolved at once. A sandbox is
	// untrusted and can ask as fast as it likes; past this, queries are dropped
	// rather than turned into goroutines and upstream sockets, which is what a
	// loaded resolver does and what a client retries.
	maxInFlightDNS = 64

	// maxDNSConnections bounds concurrent TCP queries, for the same reason.
	maxDNSConnections = 16

	// dnsTCPTimeout bounds one TCP query's whole life, so a sandbox cannot hold
	// connections open by never sending anything. RFC 7766 leaves the timeout
	// to the server and expects clients to reconnect.
	dnsTCPTimeout = 30 * time.Second

	// dnsExchangeTimeout bounds one upstream query, so a dead resolver costs the
	// actor a retry against the next one rather than a hang.
	dnsExchangeTimeout = 5 * time.Second
)

// DNSRelay answers an actor's DNS by forwarding it to the worker pod's own
// resolvers.
//
// The actor addresses a resolver it has no route to; the relay holds the socket
// it lands on, in the actor's namespace, and re-asks the question from the pod's
// namespace where cluster DNS is reachable. Messages are forwarded verbatim --
// the relay parses no DNS -- so IDs, EDNS options and anything else survive
// untouched.
//
// This is the DNS half of routing everything through atunnel: without it an
// actor's resolution either fails or escapes unseen.
//
// Deliberately not sent through the actor's egress tunnel. Doing so would need
// a DNS parser, UDP-to-TCP translation, and an egress policy that admits the
// resolver, and would make readiness depend on the egress path and on the
// actor's certificate already being minted. The cost is that a name lookup is
// not checked against egress policy the way a connection is; the actor cannot
// choose its resolver, so what remains is which names it may resolve, and this
// relay is where such a check would go.
type DNSRelay struct {
	upstreams []string
	// dial reaches the upstream resolver. The default dials from wherever the
	// relay runs, which is the pod's namespace: the listening socket is the only
	// thing that belongs to the actor.
	dial func(ctx context.Context, network, address string) (net.Conn, error)

	// inFlight and connections bound the work one sandbox can make the worker
	// do. Both are held for the life of a query rather than a rate.
	inFlight    chan struct{}
	connections chan struct{}
}

// NewDNSRelay forwards to upstreams, each "host:port".
func NewDNSRelay(upstreams []string) (*DNSRelay, error) {
	if len(upstreams) == 0 {
		return nil, fmt.Errorf("atunnel: at least one upstream resolver is required")
	}
	for _, u := range upstreams {
		if _, _, err := net.SplitHostPort(u); err != nil {
			return nil, fmt.Errorf("atunnel: invalid upstream resolver %q: %w", u, err)
		}
	}
	d := &net.Dialer{Timeout: dnsExchangeTimeout}
	return &DNSRelay{
		upstreams:   upstreams,
		dial:        d.DialContext,
		inFlight:    make(chan struct{}, maxInFlightDNS),
		connections: make(chan struct{}, maxDNSConnections),
	}, nil
}

// ResolvConfNameservers reads the nameservers out of a resolv.conf, as
// "host:port". The worker pod's own file is the intended argument: an actor
// then resolves exactly what the pod resolves.
func ResolvConfNameservers(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("atunnel: reading resolv.conf: %w", err)
	}
	defer f.Close()

	var out []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if i := strings.IndexAny(line, "#;"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		rest, ok := strings.CutPrefix(line, "nameserver")
		if !ok {
			continue
		}
		address := strings.TrimSpace(rest)
		if address == "" || net.ParseIP(address) == nil {
			continue
		}
		out = append(out, net.JoinHostPort(address, "53"))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("atunnel: reading resolv.conf: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("atunnel: %s names no usable nameserver", path)
	}
	return out, nil
}

// ServePacket answers UDP queries until ctx is canceled or the socket fails.
func (r *DNSRelay) ServePacket(ctx context.Context, pc net.PacketConn) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = pc.Close()
		case <-done:
		}
	}()
	defer close(done)

	var wg sync.WaitGroup
	defer wg.Wait()

	buf := make([]byte, maxDNSDatagram)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("atunnel: reading actor DNS query: %w", err)
		}
		// Copied: the buffer is reused by the next read.
		query := make([]byte, n)
		copy(query, buf[:n])

		select {
		case r.inFlight <- struct{}{}:
		default:
			slog.DebugContext(ctx, "atunnel dropped a DNS query; too many in flight")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-r.inFlight }()
			answer, err := r.exchangeUDP(ctx, query)
			if err != nil {
				slog.WarnContext(ctx, "atunnel could not resolve an actor DNS query", slog.Any("err", err))
				return
			}
			if _, err := pc.WriteTo(answer, from); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "atunnel could not return a DNS answer", slog.Any("err", err))
			}
		}()
	}
}

// Serve answers TCP queries, which is where a resolver goes when an answer does
// not fit in a datagram.
func (r *DNSRelay) Serve(ctx context.Context, listener net.Listener) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	defer close(done)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("atunnel: accepting actor DNS connection: %w", err)
		}
		select {
		case r.connections <- struct{}{}:
		default:
			slog.DebugContext(ctx, "atunnel refused a DNS connection; too many open")
			_ = conn.Close()
			continue
		}
		go func() {
			defer func() { <-r.connections }()
			r.relayTCP(ctx, conn)
		}()
	}
}

func (r *DNSRelay) exchangeUDP(ctx context.Context, query []byte) ([]byte, error) {
	var errs error
	for _, upstream := range r.upstreams {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := r.dial(ctx, "udp", upstream)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		answer, err := func() ([]byte, error) {
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
			defer stop()
			if err := conn.SetDeadline(time.Now().Add(dnsExchangeTimeout)); err != nil {
				return nil, err
			}
			if _, err := conn.Write(query); err != nil {
				return nil, err
			}
			buf := make([]byte, maxDNSDatagram)
			n, err := conn.Read(buf)
			if err != nil {
				return nil, err
			}
			return buf[:n], nil
		}()
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("upstream %s: %w", upstream, err))
			continue
		}
		return answer, nil
	}
	return nil, fmt.Errorf("atunnel: no upstream resolver answered: %w", errs)
}

// relayTCP pipes one DNS connection to an upstream. DNS over TCP is a
// length-prefixed stream, and copying it whole means the relay never has to
// know that.
func (r *DNSRelay) relayTCP(ctx context.Context, downstream net.Conn) {
	defer downstream.Close()

	var upstream net.Conn
	var errs error
	for _, address := range r.upstreams {
		conn, err := r.dial(ctx, "tcp", address)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		upstream = conn
		break
	}
	if upstream == nil {
		slog.WarnContext(ctx, "atunnel could not reach any resolver for an actor DNS connection", slog.Any("err", errs))
		return
	}
	defer upstream.Close()

	// A copy in progress does not watch ctx, and the connection slot it holds
	// belongs to the worker rather than to the sandbox that opened it. Teardown
	// must therefore take the connection down rather than wait out the
	// deadline, or the next actor finds the slots still taken.
	relayDone := make(chan struct{})
	defer close(relayDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = downstream.Close()
			_ = upstream.Close()
		case <-relayDone:
		}
	}()

	deadline := time.Now().Add(dnsTCPTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = downstream.SetDeadline(deadline)
	_ = upstream.SetDeadline(deadline)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upstream, downstream)
		if c, ok := upstream.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(downstream, upstream)
		if c, ok := downstream.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
	}()
	wg.Wait()
}
