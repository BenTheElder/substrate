//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"log/slog"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
)

// leafMemoryHigh is the memory.high for an actor's cgroup, or 0 for none: what
// the actor books on the worker. The guest can't use more than its memory
// limit, so past that it's cloud-hypervisor or virtiofsd growing, which would
// otherwise take the whole worker pod to its limit (see cgroup.MemoryGuard).
func (s *AteWorkerService) leafMemoryHigh(memoryBytes int64) int64 {
	if !s.throttleMemory || memoryBytes <= 0 {
		return 0
	}
	return actorBooking(memoryBytes)
}

// MemoryTargets returns the running microVMs for cgroup.WatchMemory to guard.
func (s *AteWorkerService) MemoryTargets() []cgroup.MemoryTarget {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	targets := make([]cgroup.MemoryTarget, 0, len(s.actors))
	for _, h := range s.actors {
		if h.vm != nil && !h.vm.stopping.Load() {
			targets = append(targets, memoryTarget{h: h, ra: h.vm})
		}
	}
	return targets
}

// memoryTarget is an actor's cgroup, which holds its VMM and virtiofsd. When
// its guard kills them, watchVM reports the actor crashed.
type memoryTarget struct {
	h  *hostedActor
	ra *runningActor
}

func (t memoryTarget) MemoryGuard() *cgroup.MemoryGuard {
	return &t.ra.memory
}

func (t memoryTarget) MemoryCgroup() (root, name string) {
	return cgroup.Root, t.h.attribution.UID
}

func (t memoryTarget) MemoryKilled(kill cgroup.Kill) {
	ctx := context.Background()
	attrs := append(ateattr.ActorLogAttrs(t.h.attribution),
		slog.Uint64("memoryLimitBytes", kill.Limit),
		slog.Uint64(string(ateattr.StatsMemoryUsageKey), kill.Current))
	slog.LogAttrs(ctx, slog.LevelWarn, "Killing a microVM over its memory limit", attrs...)
	go func() {
		if err := <-kill.Done; err != nil {
			attrs = append(attrs, slog.Any("err", err))
			slog.LogAttrs(ctx, slog.LevelError, "Failed to kill a microVM over its memory limit", attrs...)
		}
	}()
}
