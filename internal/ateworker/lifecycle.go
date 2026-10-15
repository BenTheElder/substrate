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
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/internal/ateworker/actornet"
	"github.com/agent-substrate/substrate/internal/ateworker/phaselog"
	"github.com/agent-substrate/substrate/internal/ateworker/tunnel"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/sizing"
	"github.com/agent-substrate/substrate/internal/wakeupprobe"
	"github.com/agent-substrate/substrate/internal/workerstats"
)

// The phases the worker times itself. Runtimes add their own between
// net_setup and wakeup_probe.
const (
	PhasePrep          = "prep"
	phaseEgressPrepare = "egress_prepare"
	phaseNetSetup      = "net_setup"
	phaseWakeupProbe   = "wakeup_probe"
	phaseActivate      = "activate"
	phaseTeardown      = "teardown"
)

// containerCountKey carries the number of application containers on the
// restore record, since a runtime's per-container phases are sums over them.
const containerCountKey = "ate.actor.container.count"

// startAttempts is how many times Start is tried when it fails with
// ErrRetryStart. One retry covers a transient failure, beyond that the caller
// should hear about it.
const startAttempts = 2

func (s *service) RunWorkload(ctx context.Context, req *ateworkerpb.RunWorkloadRequest) (*ateworkerpb.RunWorkloadResponse, error) {
	a := &Actor{
		Attribution: workerstats.ActorAttributionFromRequest(req),
		Dirs:        req.GetActorDirs(),
		Spec:        req.GetSpec(),
		Size:        sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
		RunscPath:   req.GetRunscPath(),
		AssetPaths:  req.GetRuntimeAssetPaths(),
	}
	if err := s.validate(a); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, a.Attribution.UID) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(a.Attribution.UID)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(a.Attribution.UID, rpcRunWorkload, cancel)
	if err != nil {
		return nil, err
	}
	defer release()

	s.actorLogger.EmitLifecycleLog(ctx, "Actor starting", a.Attribution)
	act := &Activation{Actor: *a, Timer: NewTimer(time.Now())}
	if err := s.start(ctx, act, req.GetEgressGateway(), false); err != nil {
		return nil, err
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor started", a.Attribution)
	return &ateworkerpb.RunWorkloadResponse{}, nil
}

func (s *service) RestoreWorkload(ctx context.Context, req *ateworkerpb.RestoreWorkloadRequest) (_ *ateworkerpb.RestoreWorkloadResponse, retErr error) {
	// Taken before the lock, so prep covers the wait for it.
	tStart := time.Now()
	a := &Actor{
		Attribution: workerstats.ActorAttributionFromRequest(req),
		Dirs:        req.GetActorDirs(),
		Spec:        req.GetSpec(),
		Size:        sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
		RunscPath:   req.GetRunscPath(),
		AssetPaths:  req.GetRuntimeAssetPaths(),
	}
	fidelity := req.GetFidelity()
	if err := s.validate(a); err != nil {
		return nil, err
	}
	if err := s.validateFidelity(fidelity); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, a.Attribution.UID) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(a.Attribution.UID)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Recorded on the way out, so a failed restore still reports the phases it
	// completed. On failure the total includes the cleanup.
	timer := NewTimer(tStart)
	defer func() {
		attrs := phaselog.SnapshotPhaseAttrs(a.Attribution, fidelity, phaselog.RestoreDurationKey, retErr, timer.Phases())
		attrs = append(attrs, slog.Int(containerCountKey, len(a.Containers())))
		slog.LogAttrs(ctx, slog.LevelInfo, "Restore timing breakdown", attrs...)
	}()

	release, err := s.beginRPC(a.Attribution.UID, rpcRestoreWorkload, cancel)
	if err != nil {
		return nil, err
	}
	defer release()

	s.actorLogger.EmitLifecycleLog(ctx, "Actor restoring", a.Attribution)
	act := &Activation{
		Actor:              *a,
		Fidelity:           fidelity,
		PreserveRestoreDir: req.GetPreserveRestoreDir(),
		Timer:              timer,
	}
	keepsCounters := s.cfg.MemoryRestoreKeepsCounters && fidelity == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	if err := s.start(ctx, act, req.GetEgressGateway(), keepsCounters); err != nil {
		return nil, err
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor restored", a.Attribution)
	return &ateworkerpb.RestoreWorkloadResponse{}, nil
}

// start hosts the actor, starts its sandbox, and opens it to traffic once its
// containers answer their wakeup probes. On error nothing is left running.
func (s *service) start(ctx context.Context, act *Activation, gateway *ateworkerpb.EgressGateway, keepsCounters bool) (retErr error) {
	attribution := act.Attribution
	if err := s.tunnel.Deactivate(ctx, attribution); err != nil {
		return err
	}
	if err := s.endPreviousActivation(ctx, &act.Actor); err != nil {
		return err
	}
	act.Timer.Lap(PhasePrep)

	var sb Sandbox
	defer func() {
		if retErr == nil {
			return
		}
		// Detached: the RPC's context may be what failed it.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := s.tunnel.Deactivate(cleanupCtx, attribution); err != nil {
			slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after start failure", slog.Any("err", err))
		}
		if sb != nil {
			if err := s.runtime.Stop(cleanupCtx, &act.Actor, sb); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to stop the sandbox after start failure", slog.Any("err", err))
			}
		}
		if err := s.unhostActor(cleanupCtx, attribution.UID); err != nil {
			slog.WarnContext(cleanupCtx, "Failed to clean up actor network after start failure", slog.Any("err", err))
		}
	}()

	act.Dial = s.sandboxDialer(attribution.UID)
	var hosted *hostedActor
	var egress *tunnel.ActorEgress
	for attempt := 1; ; attempt++ {
		var err error
		egress, err = s.tunnel.PrepareEgress(ctx, attribution, gateway)
		act.Timer.Lap(phaseEgressPrepare)
		if err != nil {
			return err
		}
		// Published before the sandbox starts, so stats include startup usage.
		hosted, err = s.hostActor(ctx, attribution, keepsCounters)
		act.Timer.Lap(phaseNetSetup)
		if err != nil {
			return err
		}
		act.Network = hosted.network
		act.Attempt = attempt
		sb, err = s.runtime.Start(ctx, act)
		if err == nil {
			break
		}
		if attempt >= startAttempts || !errors.Is(err, ErrRetryStart) {
			return err
		}
		slog.WarnContext(ctx, "Sandbox stopped before the actor started; retrying",
			slog.String("actorUID", attribution.UID), slog.Int("attempt", attempt), slog.Any("err", err))
		// Deactivating retires the egress listener bound to the old network,
		// so the network is rebuilt. The actor keeps its slot.
		if err := s.tunnel.Deactivate(ctx, attribution); err != nil {
			return err
		}
	}

	err := wakeupprobe.WaitAll(ctx, act.Containers(), actornet.ActorVethIP, wakeupprobe.DialFunc(act.Dial))
	act.Timer.Lap(phaseWakeupProbe)
	if err != nil {
		return fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}
	err = s.tunnel.Activate(attribution, act.Dial, egress)
	act.Timer.Lap(phaseActivate)
	if err != nil {
		return err
	}

	s.setSandbox(hosted, sb)
	// Off the RPC path, since a reading can take seconds.
	go s.recordInitial(context.WithoutCancel(ctx), hosted)
	return nil
}

// endPreviousActivation ends the activation a re-host is about to replace. A
// sandbox still running would be dropped from tracking and left running, so it
// is stopped, read first for the final record. This happens after a checkpoint
// that failed before its teardown, or on a retried Run or Restore.
func (s *service) endPreviousActivation(ctx context.Context, a *Actor) error {
	old := s.lookupActor(a.Attribution.UID)
	if old == nil {
		return nil
	}
	if sb := s.sandboxOf(old); sb != nil {
		s.readFinal(ctx, old)
		s.setSandbox(old, nil)
		if err := s.runtime.Stop(ctx, a, sb); err != nil {
			return fmt.Errorf("while stopping the actor's previous sandbox: %w", err)
		}
	}
	s.recordFinal(ctx, old)
	return nil
}

// CheckpointWorkload runs even while the worker drains, so an actor can still
// be suspended on shutdown.
func (s *service) CheckpointWorkload(ctx context.Context, req *ateworkerpb.CheckpointWorkloadRequest) (_ *ateworkerpb.CheckpointWorkloadResponse, retErr error) {
	// Taken before the lock, so prep covers the wait for it.
	tStart := time.Now()
	a := &Actor{
		Attribution: workerstats.ActorAttributionFromRequest(req),
		Dirs:        req.GetActorDirs(),
		Spec:        req.GetSpec(),
		RunscPath:   req.GetRunscPath(),
		AssetPaths:  req.GetRuntimeAssetPaths(),
	}
	fidelity := req.GetFidelity()
	if err := s.validate(a); err != nil {
		return nil, err
	}
	if err := s.validateFidelity(fidelity); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, a.Attribution.UID) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(a.Attribution.UID)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Not cancelable: a checkpoint is saving the actor's state.
	defer s.inFlight.Add(a.Attribution.UID, rpcCheckpointWorkload, nil)()

	// Recorded on the way out, so a failed checkpoint still reports the phases
	// it completed.
	timer := NewTimer(tStart)
	defer func() {
		phaselog.LogSnapshotPhases(ctx, "Checkpoint timing breakdown", a.Attribution, fidelity,
			phaselog.CheckpointDurationKey, retErr, timer.Phases())
	}()

	if err := s.tunnel.Deactivate(ctx, a.Attribution); err != nil {
		return nil, err
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointing", a.Attribution)

	// Read while the sandbox still answers. The final record waits for the
	// teardown.
	hosted := s.lookupActor(a.Attribution.UID)
	if hosted != nil {
		s.readFinal(ctx, hosted)
	}

	resp, err := s.runtime.Checkpoint(ctx, &Checkpoint{
		Actor:    *a,
		Fidelity: fidelity,
		Sandbox:  s.sandboxOf(hosted),
		Timer:    timer,
	})
	if err != nil {
		return nil, err
	}

	// Best-effort: the snapshot is already on disk for atelet to ship.
	if err := s.terminate(ctx, a); err != nil {
		slog.WarnContext(ctx, "failed to terminate workload after checkpoint",
			slog.String("actor", a.Attribution.Ref.String()),
			slog.String("actorUID", a.Attribution.UID),
			slog.Any("err", err))
	}
	timer.Lap(phaseTeardown)
	s.recordFinalIfEnded(ctx, hosted)

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointed", a.Attribution)
	return resp, nil
}

func (s *service) TerminateWorkload(ctx context.Context, req *ateworkerpb.TerminateWorkloadRequest) (*ateworkerpb.TerminateWorkloadResponse, error) {
	a := &Actor{
		Attribution: workerstats.ActorAttributionFromRequest(req),
		Dirs:        req.GetActorDirs(),
		Spec:        req.GetSpec(),
		RunscPath:   req.GetRunscPath(),
	}
	if err := s.validate(a); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, a.Attribution.UID) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(a.Attribution.UID)

	hosted := s.lookupActor(a.Attribution.UID)
	if hosted != nil {
		s.readFinal(ctx, hosted)
	}
	err := s.terminate(ctx, a)
	s.recordFinalIfEnded(ctx, hosted)
	if err != nil {
		return nil, fmt.Errorf("failed to terminate workload: %w", err)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor terminated", a.Attribution)
	return &ateworkerpb.TerminateWorkloadResponse{}, nil
}

// terminate closes the actor to traffic, stops its sandbox, and unhosts it.
// The actor is unhosted even when a step fails.
func (s *service) terminate(ctx context.Context, a *Actor) error {
	var errs []error
	if err := s.tunnel.Deactivate(ctx, a.Attribution); err != nil {
		errs = append(errs, fmt.Errorf("while deactivating actor networking: %w", err))
	}
	// Cleared first, so a stats read that lands mid-teardown reports no
	// numbers instead of a failed read.
	hosted := s.lookupActor(a.Attribution.UID)
	sb := s.sandboxOf(hosted)
	if hosted != nil {
		s.setSandbox(hosted, nil)
	}
	if err := s.runtime.Stop(ctx, a, sb); err != nil {
		errs = append(errs, fmt.Errorf("while stopping the sandbox: %w", err))
	}
	if err := s.unhostActor(ctx, a.Attribution.UID); err != nil {
		errs = append(errs, fmt.Errorf("while cleaning up actor network: %w", err))
	}
	return errors.Join(errs...)
}
