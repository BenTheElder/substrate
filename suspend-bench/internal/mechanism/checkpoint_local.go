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

package mechanism

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/suspend-bench/internal/metrics"
	"github.com/agent-substrate/substrate/suspend-bench/internal/runtime"
)

// CheckpointLocal is the snapshot-to-local-SSD foil: pause → snapshot → (optional
// compress) → teardown (frees node RAM), then restore (copy or ondemand) → resume.
// This is the cross-node-capable path the poc2 findings recommend; the benchmark
// measures whether swap beats it.
type CheckpointLocal struct{}

func (CheckpointLocal) Name() string         { return "checkpoint_local" }
func (CheckpointLocal) PreservesState() bool { return true }
func (CheckpointLocal) NeedsImageDir() bool  { return true }

func (CheckpointLocal) Suspend(ctx context.Context, rt runtime.Runtime, h *runtime.Handle, in SuspendInput) (SuspendResult, error) {
	var res SuspendResult
	// All node RAM held by this VMM is freed once we tear it down post-snapshot,
	// so the "freed" figure is its full resident set at suspend time.
	preRSS := metrics.RSSBytes(h.VMMPid)

	start := time.Now()
	if err := rt.Pause(ctx, h); err != nil {
		return res, fmt.Errorf("pause: %w", err)
	}
	if err := rt.Snapshot(ctx, h, in.ImageDir); err != nil {
		return res, fmt.Errorf("snapshot: %w", err)
	}
	if _, err := compressMemoryRanges(ctx, in.ImageDir, in.Compression); err != nil {
		return res, fmt.Errorf("compress: %w", err)
	}
	res.SuspendMs = metrics.Ms(start)

	res.ImageApparentBytes = metrics.DirApparentBytes(in.ImageDir)
	res.ImageActualBytes = metrics.DirActualBytes(in.ImageDir)
	res.HostRSSFreedBytes = preRSS
	res.SwapCurrentBytes = -1
	res.ZswapCurrentBytes = -1

	// Free the node: the image is on disk now.
	if err := rt.Teardown(ctx, h); err != nil {
		return res, fmt.Errorf("teardown after snapshot: %w", err)
	}
	return res, nil
}

func (CheckpointLocal) Resume(ctx context.Context, rt runtime.Runtime, h *runtime.Handle, in ResumeInput) (ResumeResult, error) {
	var res ResumeResult
	start := time.Now()
	// Decompress (if needed) is part of the work required to resume.
	if err := decompressMemoryRanges(ctx, in.ImageDir, in.Compression); err != nil {
		return res, fmt.Errorf("decompress: %w", err)
	}
	if err := rt.Restore(ctx, h, in.ImageDir, in.RestoreMode); err != nil {
		return res, fmt.Errorf("restore: %w", err)
	}
	// CH restores paused (needs an explicit resume); gVisor restores running.
	if rt.RestoreLeavesPaused() {
		if err := rt.Resume(ctx, h); err != nil {
			return res, fmt.Errorf("resume: %w", err)
		}
	}
	res.ResumeCallMs = metrics.Ms(start)

	client, tok, err := runtime.FirstPing(h, in.ResumeDeadline)
	if err != nil {
		return res, fmt.Errorf("first ping after restore: %w", err)
	}
	res.ResumeToFirstPingMs = metrics.Ms(start)
	res.Client = client
	res.Token = tok
	return res, nil
}
