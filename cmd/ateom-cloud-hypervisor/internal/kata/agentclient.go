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

package kata

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-cloud-hypervisor/internal/kata/agentpb"
	"github.com/containerd/ttrpc"
)

// agentVsockPort is the guest port the kata-agent's ttrpc server listens on.
const agentVsockPort = 1024

// AgentClient is a thin ttrpc client for the kata-agent RPCs ateom drives
// directly (network reconfiguration after a CH snapshot restore). It dials the
// agent through CH's hybrid-vsock unix socket — the same channel the kata shim
// uses — so it works on a restored VM with no shim present.
type AgentClient struct {
	conn   net.Conn
	client *ttrpc.Client
}

// DialAgent connects to the kata-agent through the hybrid-vsock socket at
// vsockPath (VsockSocketPath(id)): plain-text "CONNECT <port>" handshake with
// the VMM, then ttrpc over the stream.
func DialAgent(ctx context.Context, vsockPath string) (*AgentClient, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", vsockPath)
	if err != nil {
		return nil, fmt.Errorf("dialing hybrid vsock %q: %w", vsockPath, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", agentVsockPort); err != nil {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT: %w", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT response: %w", err)
	}
	if !strings.HasPrefix(line, "OK ") {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT refused: %q", strings.TrimSpace(line))
	}
	_ = conn.SetDeadline(time.Time{}) // ttrpc manages its own timeouts via ctx
	return &AgentClient{conn: conn, client: ttrpc.NewClient(conn)}, nil
}

// Close shuts the ttrpc client and underlying connection.
func (a *AgentClient) Close() error {
	err := a.client.Close()
	_ = a.conn.Close()
	return err
}

// UpdateInterface replaces the addresses (and mtu/name binding) of the guest
// interface named by iface.Name/Device with the provided spec, returning the
// resulting interface state. Mirrors grpc.AgentService/UpdateInterface.
func (a *AgentClient) UpdateInterface(ctx context.Context, iface *agentpb.Interface) (*agentpb.Interface, error) {
	resp := &agentpb.Interface{}
	if err := a.client.Call(ctx, "grpc.AgentService", "UpdateInterface",
		&agentpb.UpdateInterfaceRequest{Interface: iface}, resp); err != nil {
		return nil, fmt.Errorf("agent UpdateInterface: %w", err)
	}
	return resp, nil
}

// UpdateRoutes replaces the guest routing table with the provided routes,
// returning the resulting routes. Mirrors grpc.AgentService/UpdateRoutes.
func (a *AgentClient) UpdateRoutes(ctx context.Context, routes []*agentpb.Route) ([]*agentpb.Route, error) {
	resp := &agentpb.Routes{}
	if err := a.client.Call(ctx, "grpc.AgentService", "UpdateRoutes",
		&agentpb.UpdateRoutesRequest{Routes: &agentpb.Routes{Routes: routes}}, resp); err != nil {
		return nil, fmt.Errorf("agent UpdateRoutes: %w", err)
	}
	return resp.GetRoutes(), nil
}
