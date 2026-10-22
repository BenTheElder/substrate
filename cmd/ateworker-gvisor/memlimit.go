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
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/ocispec"
)

// With runc 1.5.2+ on systemd 253-258 the worker container's scope has
// OOMPolicy=kill, so any kernel OOM kill under it stops the whole worker, even
// one confined to a single actor's leaf. So the sandbox leaf gets memory.high
// at the actor's limit, which throttles without invoking the OOM killer, and
// we kill a sandbox that stays over with cgroup.kill, which systemd ignores.
// https://github.com/opencontainers/cgroups/blob/19a241326c83a5100dff94732733791a0c9e4720/systemd/v2.go#L366-L382

// memoryWatchInterval is how often sandbox leaves are checked.
const memoryWatchInterval = 250 * time.Millisecond

// throttleSandboxMemory swaps the memory.max runsc set on the sandbox leaf for
// a memory.high at the same limit. runsc reads memory.max back to size the
// sentry's --total-memory when create launches the sandbox, so this runs after
// the sandbox's create. On failure the leaf keeps its memory.max.
func (s *AteWorkerService) throttleSandboxMemory(ctx context.Context, hosted *hostedActor) {
	if !s.cgroupDelegated {
		return
	}
	limit, err := swapMemoryMaxForHigh(s.sandboxLeaf(hosted.attribution.UID))
	if err != nil {
		attrs := append(ateattr.ActorLogAttrs(hosted.attribution), slog.Any("err", err))
		slog.LogAttrs(ctx, slog.LevelWarn, "Sandbox keeps its memory.max, an OOM kill in it will restart the worker", attrs...)
		return
	}
	hosted.memoryLimit.Store(limit)
}

func (s *AteWorkerService) sandboxLeaf(actorUID string) string {
	return filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(actorUID, sandboxCgroupContainer))
}

// swapMemoryMaxForHigh returns the limit it moved, or 0 if the leaf has none.
func swapMemoryMaxForHigh(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, "memory.max"))
	if errors.Is(err, fs.ErrNotExist) {
		// memory controller not delegated
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	v := strings.TrimSpace(string(b))
	if v == "max" {
		return 0, nil
	}
	limit, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing memory.max %q: %w", v, err)
	}
	// high first, so the leaf is never unlimited
	if err := os.WriteFile(filepath.Join(dir, "memory.high"), []byte(v), 0o644); err != nil {
		return 0, err
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max"), 0o644); err != nil {
		return 0, err
	}
	return limit, nil
}

// watchMemory kills sandboxes that stay over their memory limit, until ctx is
// done.
func (s *AteWorkerService) watchMemory(ctx context.Context) {
	ticker := time.NewTicker(memoryWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, h := range s.hostedActors() {
				s.checkMemory(ctx, h, now)
			}
		}
	}
}

func (s *AteWorkerService) checkMemory(ctx context.Context, h *hostedActor, now time.Time) {
	limit := h.memoryLimit.Load()
	// a stopping sandbox may already be in its checkpoint cgroup
	if limit == 0 || h.stopping.Load() || h.oomKilled.Load() {
		return
	}
	dir := s.sandboxLeaf(h.attribution.UID)
	over, current, err := h.memoryWatch.Check(dir, limit, now)
	if err != nil || !over {
		// a read fails when the sandbox is going away
		return
	}
	h.oomKilled.Store(true)
	attrs := append(ateattr.ActorLogAttrs(h.attribution),
		slog.Uint64("memoryLimitBytes", limit),
		slog.Uint64(string(ateattr.StatsMemoryUsageKey), current))
	slog.LogAttrs(ctx, slog.LevelWarn, "Killing a sandbox over its memory limit", attrs...)
	killCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cgroup.KillLeafUnder(killCtx, s.cgroupRoot, filepath.Base(dir)); err != nil {
		attrs = append(attrs, slog.Any("err", err))
		slog.LogAttrs(ctx, slog.LevelError, "Failed to kill a sandbox over its memory limit", attrs...)
	}
}
