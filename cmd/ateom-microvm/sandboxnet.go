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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/vishvananda/netns"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/readyz"
)

// prepareSandboxNetwork builds the actor's network and starts serving it.
func (s *AteomService) prepareSandboxNetwork(ctx context.Context, actorUID string) error {
	session, err := ateomnet.ServeSandbox(ctx, ateomnet.SandboxNetworkConfig{
		ActorUID:      actorUID,
		EgressPort:    s.atunnelEgressPort,
		DNSPort:       atunnel.DNSPort,
		GatewayHWAddr: gatewayHWAddr,
	}, s.atunnelEgress, s.dnsRelay)
	if err != nil {
		return fmt.Errorf("while setting up the sandbox network: %w", err)
	}

	s.netMu.Lock()
	s.network = session
	s.netMu.Unlock()
	return nil
}

// releaseSandboxNetwork stops serving the actor and takes its network down.
func (s *AteomService) releaseSandboxNetwork(ctx context.Context) error {
	s.netMu.Lock()
	session := s.network
	s.network = nil
	s.netMu.Unlock()

	if session == nil {
		return nil
	}
	if err := session.Close(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to remove the sandbox network", slog.Any("err", err))
		return err
	}
	return nil
}

// sandboxNetNS is where the actor's tap and atunnel's sockets live, or -1
// between activations.
func (s *AteomService) sandboxNetNS() netns.NsHandle {
	s.netMu.Lock()
	defer s.netMu.Unlock()
	if s.network == nil {
		return -1
	}
	return s.network.Network.GatewayNetNS
}

// sandboxDialer reaches the actor from its tap's namespace.
func (s *AteomService) sandboxDialer() func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		s.netMu.Lock()
		session := s.network
		s.netMu.Unlock()
		if session == nil {
			return nil, fmt.Errorf("no actor is active on this worker")
		}
		return session.Dialer()(ctx, network, address)
	}
}

func (s *AteomService) readyzDialer() readyz.DialFunc { return s.sandboxDialer() }

// writeActorResolvConf points the guest resolver at its fixed gateway address.
func writeActorResolvConf(rootfs string) error {
	pod, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return fmt.Errorf("reading the worker pod resolv.conf: %w", err)
	}
	return ateomnet.WriteRootfsResolvConf(rootfs, ateomnet.SandboxResolvConf(pod))
}

// attachAtunnel completes setup after atunnel receives the service's dialer.
func (s *AteomService) attachAtunnel(ingress *atunnel.Server, egress *atunnel.Egress, egressPort uint16) {
	s.atunnelIngress = ingress
	s.atunnelEgress = egress
	s.atunnelEgressPort = egressPort
}

// egressPortOf extracts the port to bind in each sandbox's gateway namespace.
func egressPortOf(listenAddress string) (uint16, error) {
	_, port, err := net.SplitHostPort(listenAddress)
	if err != nil {
		return 0, fmt.Errorf("atunnel egress listen address %q: %w", listenAddress, err)
	}
	p, ok := atunnel.ParsePort(port)
	if !ok {
		return 0, fmt.Errorf("atunnel egress listen address %q has no usable port", listenAddress)
	}
	return uint16(p), nil
}
