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

// Package runtime defines the sandbox-runtime abstraction the harness drives.
//
// A Runtime is a thing that can boot a sandbox from a guest image, freeze/thaw it
// (pause/resume), snapshot/restore it to a directory, and tear it down — plus
// expose the VMM/sandbox PID and the per-instance cgroup so the swap mechanism
// can reclaim its memory and account for it. Implementations: ch (cloud-hypervisor)
// and gvisor (runsc).
package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/suspend-bench/internal/metrics"
	"github.com/agent-substrate/substrate/suspend-bench/internal/vsock"
)

// BootSpec describes the sandbox to create.
type BootSpec struct {
	ID          string          // unique per instance (used for sockets, cgroup leaf, paths)
	KernelPath  string          // guest vmlinux (ch only)
	RootfsPath  string          // ext4 virtio-blk image (ch) — gvisor uses an OCI bundle
	BundlePath  string          // OCI bundle dir (gvisor only)
	MemoryBytes int64           // guest RAM
	VCPUs       int             // guest vCPUs
	VsockCID    uint32          // guest vsock context id (>=3), unique per instance
	BackingFile bool            // back guest RAM with a file
	SharedMem   bool            // memfd-backed guest RAM (CH shared=true); enables PR #8113 sparse snapshots
	Cgroup      *metrics.Cgroup // leaf to launch the VMM/sandbox directly into
	WorkDir     string          // per-instance scratch (api sockets, vsock sockets, logs)
}

// Handle is a live (or paused) sandbox.
type Handle struct {
	ID     string
	VMMPid int             // PID to account/reclaim (CH process or runsc sentry)
	Cgroup *metrics.Cgroup // the leaf it lives in
	Dial   vsock.DialFunc  // open a fresh control channel to the workload
	Spec   BootSpec
	priv   any // runtime-private state
}

// SetPriv / Priv let a Runtime stash its own state on the Handle.
func (h *Handle) SetPriv(v any) { h.priv = v }
func (h *Handle) Priv() any     { return h.priv }

// Runtime is the sandbox-runtime plugin interface.
type Runtime interface {
	Name() string

	// Boot creates and starts the sandbox; the returned Handle has a working
	// Dial for the control channel.
	Boot(ctx context.Context, spec BootSpec) (*Handle, error)

	// Pause / Resume freeze and thaw the sandbox in place (no teardown).
	Pause(ctx context.Context, h *Handle) error
	Resume(ctx context.Context, h *Handle) error

	// Snapshot writes a portable image of the (paused) sandbox into destDir.
	Snapshot(ctx context.Context, h *Handle, destDir string) error

	// Restore re-creates the sandbox from srcDir into a fresh VMM, updating h
	// (VMMPid, Dial, cgroup). mode is "copy" or "ondemand" for CH; ignored
	// elsewhere. The restored sandbox is left paused — call Resume.
	Restore(ctx context.Context, h *Handle, srcDir, mode string) error

	// Teardown kills/deletes the sandbox and frees its memory.
	Teardown(ctx context.Context, h *Handle) error

	// SupportsRestoreMode reports whether mode is meaningful for this runtime.
	SupportsRestoreMode(mode string) bool

	// RestoreLeavesPaused reports whether Restore leaves the sandbox paused
	// (true for cloud-hypervisor, requiring an explicit Resume) or running
	// (false for gVisor/runsc, which restores running).
	RestoreLeavesPaused() bool
}

// FirstPing dials the control channel and issues PING, retrying the WHOLE cycle
// (dial + ping) until a valid PONG or the deadline. Retrying the ping too — not
// just the dial — matters for workloads behind a vsock bridge (the Node workload
// uses socat): there, CONNECT succeeds as soon as socat listens, but a PING can
// EOF until the backend (node) is actually accepting. This is the building block
// for resume-to-first-ping timing.
func FirstPing(h *Handle, deadline time.Duration) (*vsock.Client, vsock.StateToken, error) {
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		c, err := h.Dial(time.Second)
		if err != nil {
			lastErr = err
			time.Sleep(5 * time.Millisecond)
			continue
		}
		tok, err := c.Ping()
		if err != nil {
			c.Close()
			lastErr = err
			time.Sleep(5 * time.Millisecond)
			continue
		}
		return c, tok, nil
	}
	return nil, vsock.StateToken{}, fmt.Errorf("first ping timed out after %s: %w", deadline, lastErr)
}
