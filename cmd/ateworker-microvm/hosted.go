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
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateworker-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateworker/cgroup"
	"github.com/agent-substrate/substrate/internal/sizing"
)

// actorLeaf opens the actor's cgroup, or returns nil when the worker has none.
func (s *microvmRuntime) actorLeaf(actorUID string, size sizing.SandboxSize) (*cgroup.ActorLeaf, error) {
	if !s.actorCgroups {
		return nil, nil
	}
	return cgroup.OpenActorLeaf(actorUID, size.MilliCPU)
}

// cleanupSandboxState kills what is left of the actor's host processes and
// clears its sandbox directories.
func (s *microvmRuntime) cleanupSandboxState(ctx context.Context, actorUID string) {
	if s.actorCgroups {
		killCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := cgroup.KillActorLeaf(killCtx, actorUID); err != nil {
			slog.WarnContext(ctx, "Failed to kill the actor's leftover processes", slog.Any("err", err))
		}
		cancel()
	}
	kata.CleanupSandboxState(ctx, actorUID)
}

// removeActorLeaf deletes the actor's cgroup. Its processes must be gone.
func (s *microvmRuntime) removeActorLeaf(ctx context.Context, actorUID string) {
	if !s.actorCgroups {
		return
	}
	if err := cgroup.RemoveActorLeaf(actorUID); err != nil {
		slog.WarnContext(ctx, "Failed to remove the actor's cgroup", slog.Any("err", err))
	}
}
