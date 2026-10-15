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

package ateworker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// gracefulShutdown propagates SIGTERM into every running sandbox and waits for
// the workloads to exit. It takes no actor's lifecycle lock, so a suspend can
// still land mid-drain.
func (s *service) gracefulShutdown(ctx context.Context) {
	// Set first so an RPC that arrives while we drain is turned away.
	s.shuttingDown.Store(true)
	// Aborting a boot is less disruptive than waiting for it only to SIGTERM
	// what it produced.
	s.cancelStartups(ctx)

	// One deadline covers the whole drain, so waiting for in-flight RPCs and
	// waiting out SIGTERM split a single grace period.
	deadline := time.Now().Add(workloadGracePeriod)

	// Let checkpoints finish saving state before stopping workloads.
	waitCtx, waitCancel := context.WithDeadline(ctx, deadline)
	defer waitCancel()
	if !s.inFlight.WaitIdle(waitCtx) {
		slog.ErrorContext(ctx, "Giving up waiting for in-flight RPCs during graceful shutdown",
			slog.Any("rpcs", s.inFlight.Names()))
	}

	type target struct {
		hosted  *hostedActor
		sandbox Sandbox
	}
	var targets []target
	for _, h := range s.hostedActors() {
		if sb := s.sandboxOf(h); sb != nil {
			targets = append(targets, target{h, sb})
		}
	}
	if len(targets) == 0 {
		slog.InfoContext(ctx, "No active workload at shutdown; exiting cleanly")
		return
	}

	// Drained concurrently: they share one deadline, so in series the first
	// actor's wait would come out of every later one's allowance.
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Go(func() {
			// The drain ends the activation: read it while the sandbox runs,
			// and write its final record once the workloads are down.
			s.readFinal(ctx, t.hosted)
			s.runtime.Drain(ctx, t.hosted.attribution, t.sandbox, deadline)
			s.recordFinal(ctx, t.hosted)
		})
	}
	wg.Wait()

	slog.InfoContext(ctx, "Shutting down")
}
