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

// Package mechanism implements the suspend/resume strategies under test:
// checkpoint_local (snapshot to SSD + restore), swap (cgroup v2 reclaim + resume
// in place), and coldstart (teardown + fresh boot baseline).
//
// The primary comparison is swap vs checkpoint_local. Compression and CH restore
// mode are Stage-2 diagnostic knobs threaded through SuspendInput/ResumeInput.
package mechanism

import (
	"context"
	"time"

	"github.com/agent-substrate/substrate/suspend-bench/internal/runtime"
	"github.com/agent-substrate/substrate/suspend-bench/internal/vsock"
)

// SuspendInput carries the per-run knobs into Suspend.
type SuspendInput struct {
	ImageDir    string // where checkpoint_local writes the snapshot
	Compression string // none | zstd | lz4 (checkpoint_local only)
}

// SuspendResult holds suspend-side timings and accounting.
type SuspendResult struct {
	SuspendMs          float64
	ImageApparentBytes int64
	ImageActualBytes   int64
	HostRSSFreedBytes  int64
	SwapCurrentBytes   int64
	ZswapCurrentBytes  int64
}

// ResumeInput carries the per-run knobs into Resume.
type ResumeInput struct {
	ImageDir       string
	Compression    string
	RestoreMode    string        // copy | ondemand (CH checkpoint_local)
	ResumeDeadline time.Duration // max wait for first ping
}

// ResumeResult holds resume-side timings plus the open control channel and the
// first-ping token (the driver uses these for correctness + the page walk).
type ResumeResult struct {
	ResumeCallMs        float64
	ResumeToFirstPingMs float64
	Client              *vsock.Client
	Token               vsock.StateToken
}

// Mechanism is a suspend/resume strategy.
type Mechanism interface {
	Name() string

	// Suspend takes a running sandbox at its target working set to a suspended
	// state (snapshotted, or paged-to-swap, or torn down).
	Suspend(ctx context.Context, rt runtime.Runtime, h *runtime.Handle, in SuspendInput) (SuspendResult, error)

	// Resume brings it back and returns the first-ping result. It may replace
	// *h (restore / coldstart create a fresh VMM).
	Resume(ctx context.Context, rt runtime.Runtime, h *runtime.Handle, in ResumeInput) (ResumeResult, error)

	// PreservesState is false for coldstart (a fresh boot loses workload state),
	// so the driver skips correctness assertions for it.
	PreservesState() bool

	// NeedsImageDir reports whether this mechanism uses an on-disk snapshot dir.
	NeedsImageDir() bool
}
