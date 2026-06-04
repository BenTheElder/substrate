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

// Swap is the candidate mechanism: pause the sandbox, reclaim its guest memory to
// host swap/zswap (charged to its own leaf cgroup), and resume in place — no
// teardown, no image. The VMM stays alive, so resume is just thaw + lazy swap-in.
//
// This is the same-node fast path from the poc2 findings; the question is whether
// its resume latency beats checkpoint_local enough to justify keeping memory on
// the node.
type Swap struct{}

func (Swap) Name() string         { return "swap" }
func (Swap) PreservesState() bool { return true }
func (Swap) NeedsImageDir() bool  { return false }

func (Swap) Suspend(ctx context.Context, rt runtime.Runtime, h *runtime.Handle, _ SuspendInput) (SuspendResult, error) {
	var res SuspendResult
	if h.Cgroup == nil {
		return res, fmt.Errorf("swap mechanism requires the sandbox to be in a leaf cgroup")
	}
	preRSS := metrics.RSSBytes(h.VMMPid)

	start := time.Now()
	if err := rt.Pause(ctx, h); err != nil {
		return res, fmt.Errorf("pause: %w", err)
	}
	// Allow this cgroup to swap, then drive reclaim of its resident footprint.
	// NOTE: a single oversized write to memory.reclaim can block/spin retrying an
	// unreachable target, so reclaim iteratively — ask for the *current* resident
	// amount each round, tolerate EAGAIN near the floor, and stop when it stops
	// shrinking. This pages out the whole cgroup (incl. the guest-RAM mapping).
	if err := h.Cgroup.SetSwapMax("max"); err != nil {
		return res, fmt.Errorf("set swap.max=max: %w", err)
	}
	// Reclaim in small fixed chunks under a wall-clock budget. A single large
	// memory.reclaim write BLOCKS (uninterruptibly) when the cgroup keeps making
	// partial progress toward an unreachable target — the worst case is the last
	// tens of MB of CH/KVM bookkeeping that trickle out a few KB per retry,
	// resetting the kernel's retry cap and spinning forever. So: (1) stop at a
	// floor instead of chasing zero, (2) cap each write with its own timeout (it
	// runs on a goroutine we abandon if it spins — the page-out it already did
	// still counts), and (3) cap total time.
	const chunk = int64(256 << 20)
	const floor = int64(32 << 20)
	// Generous cap so multi-GB working sets can be fully evicted (the progress
	// stall check below exits early once there's nothing left to reclaim).
	budget := time.Now().Add(90 * time.Second)
	for time.Now().Before(budget) {
		cur := h.Cgroup.Current()
		if cur <= floor {
			break
		}
		ask := chunk
		if ask > cur {
			ask = cur
		}
		done := make(chan error, 1)
		go func() { done <- h.Cgroup.Reclaim(ask) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			// Write is spinning on unreclaimable pages; stop (goroutine abandoned,
			// returns when the cgroup is torn down at cell end).
		}
		if h.Cgroup.Current()+(4<<20) >= cur {
			break // < 4 MiB progress this round — nothing more worth reclaiming
		}
	}
	res.SuspendMs = metrics.Ms(start)

	postRSS := metrics.RSSBytes(h.VMMPid)
	res.HostRSSFreedBytes = preRSS - postRSS
	res.SwapCurrentBytes = h.Cgroup.SwapCurrent()
	res.ZswapCurrentBytes = h.Cgroup.ZswapCurrent()
	res.ImageApparentBytes = -1
	res.ImageActualBytes = -1
	return res, nil
}

func (Swap) Resume(ctx context.Context, rt runtime.Runtime, h *runtime.Handle, in ResumeInput) (ResumeResult, error) {
	var res ResumeResult
	start := time.Now()
	if err := rt.Resume(ctx, h); err != nil {
		return res, fmt.Errorf("resume: %w", err)
	}
	res.ResumeCallMs = metrics.Ms(start)

	// First ping faults the working set back in from swap/zswap on demand.
	client, tok, err := runtime.FirstPing(h, in.ResumeDeadline)
	if err != nil {
		return res, fmt.Errorf("first ping after resume: %w", err)
	}
	res.ResumeToFirstPingMs = metrics.Ms(start)
	res.Client = client
	res.Token = tok
	return res, nil
}
