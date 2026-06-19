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

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata/agentpb"
	"github.com/containerd/ttrpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// agentVsockPort is the guest port the kata-agent's ttrpc server listens on.
const agentVsockPort = 1024

// AgentClient is a thin ttrpc client for the kata-agent RPCs ateom drives
// directly. Resurrected + expanded from tag ateom-chv-pre-rebase (which only
// mirrored UpdateInterface/UpdateRoutes for post-restore re-IP): this version
// adds CreateContainer/StartContainer so ateom can assemble the container rootfs
// itself ("be your own hook scheduler") instead of relying on the kata runtime's
// ShareRootFilesystem to emit the storages. It dials the agent through CH's
// hybrid-vsock unix socket — the same channel the kata shim uses.
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

// CreateContainer asks the agent to create a container: mount its storages (in
// order) and build the rootfs, then fork the parked init process. This is the
// hook point — the agent mounts storages[] (here: a bind of the virtio-fs lower
// followed by the tmpfs-upper overlay) before init_rootfs consumes the rootfs.
// Mirrors grpc.AgentService/CreateContainer (returns google.protobuf.Empty).
func (a *AgentClient) CreateContainer(ctx context.Context, req *agentpb.CreateContainerRequest) error {
	if err := a.client.Call(ctx, "grpc.AgentService", "CreateContainer", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent CreateContainer: %w", err)
	}
	return nil
}

// StartContainer execs the container's init process (pivots into the rootfs the
// storages assembled). Mirrors grpc.AgentService/StartContainer.
func (a *AgentClient) StartContainer(ctx context.Context, containerID string) error {
	req := &agentpb.StartContainerRequest{ContainerId: containerID}
	if err := a.client.Call(ctx, "grpc.AgentService", "StartContainer", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent StartContainer: %w", err)
	}
	return nil
}
