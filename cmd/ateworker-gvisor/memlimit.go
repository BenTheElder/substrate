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

// throttleSandboxMemory swaps the memory.max runsc set on the sandbox leaf for
// a memory.high at the same limit (see cgroup.MemoryGuard). runsc reads
// memory.max back to size the sentry's --total-memory when create launches the
// sandbox, so this runs after the sandbox's create. On failure the leaf keeps
// its memory.max.
func (s *AteWorkerService) throttleSandboxMemory(ctx context.Context, hosted *hostedActor) {
	if !s.throttleMemory {
		return
	}
	limit, err := swapMemoryMaxForHigh(s.sandboxLeaf(hosted.attribution.UID))
	if err != nil {
		attrs := append(ateattr.ActorLogAttrs(hosted.attribution), slog.Any("err", err))
		slog.LogAttrs(ctx, slog.LevelWarn, "Sandbox keeps its memory.max, an OOM kill in it will restart the worker", attrs...)
		return
	}
	hosted.memory.Resume(limit)
}

// removeStaleSandboxLeaf removes a sandbox leaf an earlier activation of the
// actor left behind when its runsc delete never ran. runsc joins an existing
// cgroup as is, so the new sandbox would get no memory.max, a sentry sized to
// the host's RAM, and no watch.
// https://github.com/google/gvisor/blob/0068142bdeb9de91fe72bf9794204de6dec92cb4/runsc/cgroup/cgroup_v2.go#L138-L153
func (s *AteWorkerService) removeStaleSandboxLeaf(ctx context.Context, actorUID string) error {
	dir := s.sandboxLeaf(actorUID)
	killCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cgroup.KillLeafUnder(killCtx, s.cgroupRoot, filepath.Base(dir)); err != nil {
		return fmt.Errorf("while clearing a leftover sandbox cgroup: %w", err)
	}
	if err := os.Remove(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("while removing leftover sandbox cgroup %q: %w", dir, err)
	}
	return nil
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

// MemoryTargets returns the sandboxes for cgroup.WatchMemory to guard.
func (s *AteWorkerService) MemoryTargets() []cgroup.MemoryTarget {
	hosted := s.hostedActors()
	targets := make([]cgroup.MemoryTarget, 0, len(hosted))
	for _, h := range hosted {
		if !h.stopping.Load() {
			targets = append(targets, memoryTarget{s: s, h: h})
		}
	}
	return targets
}

// memoryTarget is an actor's sandbox cgroup.
type memoryTarget struct {
	s *AteWorkerService
	h *hostedActor
}

func (t memoryTarget) MemoryGuard() *cgroup.MemoryGuard {
	return &t.h.memory
}

func (t memoryTarget) MemoryCgroup() (root, name string) {
	return t.s.cgroupRoot, filepath.Base(t.s.sandboxLeaf(t.h.attribution.UID))
}

func (t memoryTarget) MemoryKilled(kill cgroup.Kill) {
	ctx := context.Background()
	attrs := append(ateattr.ActorLogAttrs(t.h.attribution),
		slog.Uint64("memoryLimitBytes", kill.Limit),
		slog.Uint64(string(ateattr.StatsMemoryUsageKey), kill.Current))
	slog.LogAttrs(ctx, slog.LevelWarn, "Killing a sandbox over its memory limit", attrs...)
	go func() {
		if err := <-kill.Done; err != nil {
			attrs = append(attrs, slog.Any("err", err))
			slog.LogAttrs(ctx, slog.LevelError, "Failed to kill a sandbox over its memory limit", attrs...)
		}
	}()
}
