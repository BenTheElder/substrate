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
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"go.ate.dev/substrate/internal/ateworker/procman"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ExecBackend adapts kata.AgentClient (vsock 1024, no debug_console required)
// to procman.Backend.
type ExecBackend struct {
	ac          *AgentClient
	defaultCID  string
	baseProcess map[string]*agentpb.Process
	// fsHelperCmd is the in-container helper command prefix (e.g. ["/.substrate/fshelper"])
	// used for ReadFile/WriteFile without tar.
	fsHelperCmd []string
	nextExecID  atomic.Uint64
}

// ExecBackendOptions configures NewExecBackend.
type ExecBackendOptions struct {
	DefaultContainerID string
	// BaseProcesses maps container ID -> OCI Process template so ExecProcess
	// inherits User, Capabilities, NoNewPrivileges, Rlimits, Cwd, and base Env.
	BaseProcesses map[string]*agentpb.Process
	// FSHelperCmd is the helper argv prefix inside the container for ReadFile/WriteFile.
	FSHelperCmd []string
}

// NewExecBackend creates a procman.Backend backed by ac.
func NewExecBackend(ac *AgentClient, opts ExecBackendOptions) *ExecBackend {
	return &ExecBackend{
		ac:          ac,
		defaultCID:  opts.DefaultContainerID,
		baseProcess: opts.BaseProcesses,
		fsHelperCmd: slices.Clone(opts.FSHelperCmd),
	}
}

func (b *ExecBackend) resolveContainer(name string) (string, error) {
	if name != "" {
		return name, nil
	}
	if b.defaultCID != "" {
		return b.defaultCID, nil
	}
	return "", status.Error(codes.InvalidArgument, "container_name is required")
}

func (b *ExecBackend) buildProcess(cid string, spec procman.ExecSpec) *agentpb.Process {
	var p *agentpb.Process
	if base := b.baseProcess[cid]; base != nil {
		p = proto.Clone(base).(*agentpb.Process)
	} else {
		p = &agentpb.Process{}
	}
	p.Args = slices.Clone(spec.Command)
	if spec.Cwd != "" {
		p.Cwd = spec.Cwd
	} else if p.Cwd == "" {
		p.Cwd = "/"
	}
	p.Env = mergeEnv(p.Env, spec.Env)
	p.Terminal = false
	return p
}

func mergeEnv(base []string, overrides map[string]string) []string {
	if len(overrides) == 0 {
		return slices.Clone(base)
	}
	m := make(map[string]string, len(base)+len(overrides))
	for _, kv := range base {
		k, v, ok := strings.Cut(kv, "=")
		if ok {
			m[k] = v
		}
	}
	maps.Copy(m, overrides)
	keys := slices.Sorted(maps.Keys(m))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+m[k])
	}
	return out
}

// Exec starts a process via grpc.AgentService/ExecProcess.
func (b *ExecBackend) Exec(ctx context.Context, spec procman.ExecSpec) (procman.ExecHandle, error) {
	cid, err := b.resolveContainer(spec.ContainerName)
	if err != nil {
		return nil, err
	}
	execID := fmt.Sprintf("exec-%d", b.nextExecID.Add(1))
	req := &agentpb.ExecProcessRequest{
		ContainerId: cid,
		ExecId:      execID,
		Process:     b.buildProcess(cid, spec),
	}
	if err := b.ac.ExecProcess(ctx, req); err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	return &kataExecHandle{
		ac:     b.ac,
		cid:    cid,
		execID: execID,
		stdout: NewStdioReader(streamCtx, b.ac, cid, execID, false),
		stderr: NewStdioReader(streamCtx, b.ac, cid, execID, true),
		cancel: cancel,
	}, nil
}

// ReadFile streams raw file bytes via the configured in-container fshelper.
func (b *ExecBackend) ReadFile(ctx context.Context, containerName, path string, mode uint32, emit func([]byte) error) error {
	if len(b.fsHelperCmd) == 0 {
		return status.Error(codes.Unimplemented, "kata-agent has no native ReadFile RPC and no fshelper is configured")
	}
	cmd := append(slices.Clone(b.fsHelperCmd), "read", path, strconv.FormatUint(uint64(mode), 8))
	h, err := b.Exec(ctx, procman.ExecSpec{
		ContainerName: containerName,
		Command:       cmd,
		Stdin:         false,
	})
	if err != nil {
		return err
	}
	return procman.ReadFileOverExec(ctx, h, emit)
}

// WriteFile streams raw file bytes via the configured in-container fshelper.
func (b *ExecBackend) WriteFile(ctx context.Context, containerName, path string, mode uint32, seekOffset int64, r io.Reader) (int64, error) {
	if len(b.fsHelperCmd) == 0 {
		return 0, status.Error(codes.Unimplemented, "kata-agent has no container-scoped WriteFile RPC and no fshelper is configured")
	}
	cmd := append(slices.Clone(b.fsHelperCmd), "write", path, strconv.FormatUint(uint64(mode), 8), strconv.FormatInt(seekOffset, 10))
	h, err := b.Exec(ctx, procman.ExecSpec{
		ContainerName: containerName,
		Command:       cmd,
		Stdin:         true,
	})
	if err != nil {
		return 0, err
	}
	return procman.WriteFileOverExec(ctx, h, r)
}

type kataExecHandle struct {
	ac     *AgentClient
	cid    string
	execID string
	stdout io.Reader
	stderr io.Reader
	cancel context.CancelFunc
}

// PID returns 0 because kata-agent's ExecProcess returns google.protobuf.Empty.
func (h *kataExecHandle) PID() int32        { return 0 }
func (h *kataExecHandle) Stdout() io.Reader { return h.stdout }
func (h *kataExecHandle) Stderr() io.Reader { return h.stderr }

func (h *kataExecHandle) WriteStdin(ctx context.Context, data []byte) (int, error) {
	n, err := h.ac.WriteStdin(ctx, h.cid, h.execID, data)
	return int(n), err
}

func (h *kataExecHandle) CloseStdin(ctx context.Context) error {
	return h.ac.CloseStdin(ctx, h.cid, h.execID)
}

// Signal sends sig to the exec'd PID.
// Note: kata-agent 4.3.1 calls libc::kill(pid, sig) rather than kill(-pgid, sig).
func (h *kataExecHandle) Signal(ctx context.Context, sig syscall.Signal) error {
	return h.ac.SignalProcess(ctx, h.cid, h.execID, uint32(sig))
}

func (h *kataExecHandle) Wait(ctx context.Context) (int32, error) {
	return h.ac.WaitProcess(ctx, h.cid, h.execID)
}

func (h *kataExecHandle) Close() error {
	h.cancel()
	return nil
}
