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
	// DNSPort is the relay port on the sandbox's gateway.
	DNSPort = 53

	// Read complete UDP datagrams without truncating EDNS responses.
	maxDNSDatagram = 65535

	// Drop excess UDP queries to bound goroutines and upstream sockets.
	maxInFlightDNS = 64

	// maxDNSConnections bounds open TCP connections.
	maxDNSConnections = 16

	// dnsTCPTimeout limits connection lifetime, including idle clients.
	dnsTCPTimeout = 30 * time.Second

	// dnsExchangeTimeout bounds each upstream attempt.
	dnsExchangeTimeout = 5 * time.Second
)

// DNSRelay forwards UDP and TCP DNS unchanged to the worker pod's resolvers.
// It listens in the sandbox's gateway namespace and dials from the worker's.
// DNS bypasses the egress tunnel and is not checked against egress policy.
type DNSRelay struct {
	upstreams []string
	// dial reaches upstream resolvers from the worker namespace.
	dial func(ctx context.Context, network, address string) (net.Conn, error)

	// Limits are shared across all sandboxes using this relay.
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

// ResolvConfNameservers reads nameservers from resolv.conf as "host:53".
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

// Serve relays TCP DNS connections until ctx is canceled or the listener closes.
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

	// Wait for the relays to drain before returning, so a closed listener
	// leaves no goroutine still holding a connection slot.
	var wg sync.WaitGroup
	defer wg.Wait()

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
		wg.Add(1)
		go func() {
			defer wg.Done()
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

// relayTCP copies a DNS stream without parsing its length-prefixed messages.
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

	// Cancel active copies on teardown to release the worker's connection slots.
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
