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
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/workerstats"
)

// Sweeps and initial readings measure up to statsFanOut actors at once. A
// sweep gives up on the rest after statsSweepBudget, so it ends inside one
// sample interval. An actor not reached in time reports as pending.
const (
	statsFanOut      = 32
	statsSweepBudget = 45 * time.Second
)

// GetWorkloadStats implements ateworkerpb.Worker/GetWorkloadStats.
//
// It must not take the actor's lifecycle lock, which is held across whole
// boots and checkpoints, so polls would go silent through the phases whose
// usage matters most.
func (s *service) GetWorkloadStats(ctx context.Context, req *ateworkerpb.GetWorkloadStatsRequest) (*ateworkerpb.GetWorkloadStatsResponse, error) {
	if req.GetActorUid() == "" {
		return nil, apierror.InvalidArgument("actor_uid is required")
	}

	// NOT_FOUND rather than FAILED_PRECONDITION: the requested actor is not
	// here, which no amount of retrying on the same timer will change.
	hosted := s.lookupActor(req.GetActorUid())
	if hosted == nil {
		return nil, apierror.NotFound("worker is not executing actor %q", req.GetActorUid())
	}
	reading, err := s.measure(ctx, hosted)
	if err != nil {
		// The actor is here but has no numbers right now, most often a poll
		// landing in its boot. The caller should take the next sample.
		if errors.Is(err, ErrNoSample) {
			return nil, apierror.FailedPrecondition("%v", err)
		}
		return nil, apierror.Internal("reading usage: %v", err)
	}

	// The read holds no lifecycle lock, so a checkpoint plus a fresh run can
	// land underneath it. Pointer identity catches that: hostActor stores a new
	// record every time.
	if s.lookupActor(req.GetActorUid()) != hosted {
		return nil, apierror.NotFound("worker stopped executing actor %q while the sample was being taken", req.GetActorUid())
	}
	// The reading advanced the activation's CPU, so the cache keeps up with it.
	hosted.usage.Store(reading.Sample)

	return &ateworkerpb.GetWorkloadStatsResponse{Sample: reading.Sample}, nil
}

// GetActiveWorkloadStats implements ateworkerpb.Worker/GetActiveWorkloadStats,
// the discovery read. It serves each actor's latest sample from the sampler,
// so a poll costs no reading of its own. An actor not sampled yet is pending.
func (s *service) GetActiveWorkloadStats(ctx context.Context, req *ateworkerpb.GetActiveWorkloadStatsRequest) (*ateworkerpb.GetActiveWorkloadStatsResponse, error) {
	hosted := s.hostedActors()
	samples := make([]*ateworkerpb.WorkloadStatsSample, 0, len(hosted))
	for _, h := range hosted {
		sample := h.usage.Latest()
		if sample == nil {
			sample = s.pendingSample(h)
		}
		samples = append(samples, sample)
	}
	// An empty list is "available", per the proto, not an error.
	return &ateworkerpb.GetActiveWorkloadStatsResponse{Samples: samples}, nil
}

// sweepUsage samples every hosted actor between its initial reading and its
// final record, and stores each sample and writes its periodic record. Same
// lock discipline as GetWorkloadStats.
func (s *service) sweepUsage(ctx context.Context) {
	sweepCtx, cancel := context.WithTimeout(ctx, statsSweepBudget)
	defer cancel()

	var wg sync.WaitGroup
	for _, h := range s.hostedActors() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The readings run here, outside the sampler's own recover.
			defer func() {
				if r := recover(); r != nil {
					slog.ErrorContext(ctx, "Usage sample panicked", slog.String(string(ateattr.ActorUIDKey), h.attribution.UID),
						slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
				}
			}()
			s.sweepActor(sweepCtx, h)
		}()
	}
	wg.Wait()
}

// sweepActor samples one actor for the sweep. An actor that cannot be measured
// is pending, so it stays attributable.
func (s *service) sweepActor(ctx context.Context, h *hostedActor) {
	if !h.usage.Sampling() {
		return
	}
	sample := s.pendingSample(h)
	reading, err := s.measureInSlot(ctx, h)
	switch {
	case err == nil:
		sample = reading.Sample
	case !errors.Is(err, ErrNoSample) && !errors.Is(err, context.DeadlineExceeded):
		attrs := append(ateattr.ActorLogAttrs(h.attribution), slog.Any("err", err))
		slog.LogAttrs(ctx, slog.LevelWarn, "Failed to measure actor usage", attrs...)
	}
	// The sandbox is found by UID alone. If the actor was re-hosted meanwhile,
	// the numbers are the new activation's: drop them, and let it start with
	// its own initial reading.
	if s.lookupActor(h.attribution.UID) != h {
		return
	}
	if reading.Dead {
		s.warnDeadSandbox(ctx, h, reading.DeadAttrs)
	}
	h.usage.Periodic(sample, func() { s.usage.Emit(ctx, ateattr.StatsKindPeriodic, sample) })
}

// recordInitial samples a new activation and writes its initial record,
// bounded like one sweep. It runs on its own goroutine, so it recovers from
// panics the way a sweep does; the activation is then sampled without an
// initial record.
func (s *service) recordInitial(ctx context.Context, h *hostedActor) {
	actorUID := h.attribution.UID
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Initial usage sample panicked", slog.String(string(ateattr.ActorUIDKey), actorUID),
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			h.usage.Initial(nil, nil)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, statsSweepBudget)
	defer cancel()
	var sample *ateworkerpb.WorkloadStatsSample
	reading, err := s.measureInSlot(ctx, h)
	if err != nil {
		slog.WarnContext(ctx, "No initial usage sample", slog.String(string(ateattr.ActorUIDKey), actorUID), slog.Any("err", err))
	} else {
		sample = reading.Sample
	}
	h.usage.Initial(sample, func() { s.usage.Emit(ctx, ateattr.StatsKindInitial, sample) })
}

// readFinal samples an activation while its sandbox still answers, for the
// final record a checkpoint, terminate or drain writes. A failed reading
// leaves the newest measured sample to stand in.
func (s *service) readFinal(ctx context.Context, h *hostedActor) {
	if s.cfg.FinalReadTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.FinalReadTimeout)
		defer cancel()
	}
	reading, err := s.measure(ctx, h)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelDebug, "Final usage reading failed; using the newest sample",
			append(ateattr.ActorLogAttrs(h.attribution), slog.Any("err", err))...)
		return
	}
	h.usage.Store(reading.Sample)
}

// recordFinalIfEnded writes h's final record if a checkpoint or terminate tore
// its activation down, which unhosts the actor even when a later step fails.
// The caller holds the actor's lock.
func (s *service) recordFinalIfEnded(ctx context.Context, h *hostedActor) {
	if h != nil && s.lookupActor(h.attribution.UID) != h {
		s.recordFinal(ctx, h)
	}
}

// recordFinal writes the final record of an ended activation from its newest
// measured sample.
func (s *service) recordFinal(ctx context.Context, h *hostedActor) {
	pending := s.pendingSample(h)
	h.usage.Final(func(measured *ateworkerpb.WorkloadStatsSample) {
		if measured == nil {
			measured = pending
		}
		s.usage.Emit(ctx, ateattr.StatsKindFinal, measured)
	})
}

// measure reads h's usage as a reading of its activation.
func (s *service) measure(ctx context.Context, h *hostedActor) (Reading, error) {
	var reading Reading
	sample, err := h.usage.Measure(ctx, func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
		r, err := s.runtime.Sample(ctx, h.attribution, s.sandboxOf(h))
		reading = r
		return r.Sample, r.CPUByContainer, err
	})
	if err != nil {
		return Reading{}, err
	}
	reading.Sample = sample
	return reading, nil
}

// measureInSlot is measure while holding one of s.sampleSlots. The slot is
// taken inside the reading, so a sweep waiting on another reading of the same
// actor holds no slot.
func (s *service) measureInSlot(ctx context.Context, h *hostedActor) (Reading, error) {
	var reading Reading
	sample, err := h.usage.Measure(ctx, func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
		select {
		case s.sampleSlots <- struct{}{}:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		defer func() { <-s.sampleSlots }()
		r, err := s.runtime.Sample(ctx, h.attribution, s.sandboxOf(h))
		reading = r
		return r.Sample, r.CPUByContainer, err
	})
	if err != nil {
		return Reading{}, err
	}
	reading.Sample = sample
	return reading, nil
}

// pendingSample is h with no numbers yet.
func (s *service) pendingSample(h *hostedActor) *ateworkerpb.WorkloadStatsSample {
	return h.usage.WithEpoch(workerstats.PendingSample(&h.attribution, s.cfg.SandboxClass))
}

// warnDeadSandbox logs, once per activation, that h's sandbox has no process
// left. Nothing else notices this: the actor stays RUNNING and its stats read
// like an idle actor's.
//
// A lifecycle RPC empties the sandbox on purpose when it tears it down, and
// holds the actor's lock while it does, so a busy actor is skipped. h is
// looked up again after the reading, so a teardown that starts or finishes in
// between is never reported.
func (s *service) warnDeadSandbox(ctx context.Context, h *hostedActor, extra []slog.Attr) {
	if h.deadReported.Load() {
		return
	}
	if s.locks.Busy(h.attribution.UID) || s.lookupActor(h.attribution.UID) != h {
		return
	}
	if !h.deadReported.CompareAndSwap(false, true) {
		return
	}
	attrs := append(ateattr.ActorLogAttrs(h.attribution), extra...)
	slog.LogAttrs(ctx, slog.LevelWarn, "Sandbox has no processes left while the actor is hosted", attrs...)
}
