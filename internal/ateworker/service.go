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

// Package ateworker implements the worker service both sandbox runtimes share.
// Each cmd/ateworker-* binary supplies a Runtime and calls Main.
package ateworker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateworker/tunnel"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/workerstats"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Config describes a runtime to the worker.
type Config struct {
	// ServiceName names the worker in its telemetry.
	ServiceName string
	// SandboxClass is the atev1alpha1 sandbox class the runtime provides.
	SandboxClass string
	// Fidelities are the snapshot fidelities the runtime serves.
	Fidelities []ateworkerpb.SnapshotFidelity
	// FinalReadTimeout bounds the usage reading taken for an activation's
	// final record, which sits on the checkpoint path. Zero means no bound.
	FinalReadTimeout time.Duration
	// Veth gives the actor a veth pair to a gateway namespace. Without it the
	// runtime builds its own link, like a micro-VM tap, in a single namespace.
	Veth bool
	// MemoryRestoreKeepsCounters is set when a MEMORY restore brings the sandbox's
	// CPU counters back with it, instead of starting them at zero.
	MemoryRestoreKeepsCounters bool
}

// workloadGracePeriod is the whole budget for draining the worker on shutdown.
// It needs to stay well under the pod's termination grace period, so the
// escalation to SIGKILL happens here and not as a kubelet SIGKILL of the
// worker.
const workloadGracePeriod = 30 * time.Minute

const (
	rpcRunWorkload        = "RunWorkload"
	rpcRestoreWorkload    = "RestoreWorkload"
	rpcCheckpointWorkload = "CheckpointWorkload"
)

// service implements ateworkerpb.WorkerServer on top of a Runtime.
type service struct {
	ateworkerpb.UnimplementedWorkerServer

	cfg     Config
	runtime Runtime

	// Serializes lifecycle RPCs per actor.
	locks *actorlock.Locks
	// Tracks lifecycle RPCs for shutdown cancellation and draining.
	inFlight *actorlock.InFlight

	// shuttingDown is set once SIGTERM has been received. While true, new
	// workload RPCs are rejected with codes.Unavailable so the control plane
	// reschedules. It is set before shutdown waits on anything, so an RPC that
	// arrives meanwhile is turned away instead of queueing.
	shuttingDown atomic.Bool

	actorLogger *actorlog.ActorLogger
	tunnel      *tunnel.Tunnel
	// usage writes the usage records. Nil writes none.
	usage *workerstats.UsageEmitter

	// Guards actors, draining, and the mutable hostedActor fields.
	actorsMu sync.RWMutex
	// Keyed by actor UID.
	actors map[string]*hostedActor
	// Actors undergoing network cleanup still count against capacity.
	draining  int
	maxActors int
	// sampleSlots bounds the usage readings of sweeps and initial records
	// together to statsFanOut at once.
	sampleSlots chan struct{}
}

var _ ateworkerpb.WorkerServer = (*service)(nil)

func newService(cfg Config, runtime Runtime, tunnel *tunnel.Tunnel, actorLogger *actorlog.ActorLogger, maxActors int) *service {
	return &service{
		cfg:         cfg,
		runtime:     runtime,
		locks:       actorlock.New(),
		inFlight:    actorlock.NewInFlight(),
		actors:      map[string]*hostedActor{},
		maxActors:   maxActors,
		sampleSlots: make(chan struct{}, statsFanOut),
		tunnel:      tunnel,
		actorLogger: actorLogger,
	}
}

// beginRPC registers before checking the drain flag so shutdown cannot miss it.
func (s *service) beginRPC(actorUID, name string, cancel context.CancelFunc) (func(), error) {
	release := s.inFlight.Add(actorUID, name, cancel)
	if err := s.rejectIfDraining(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// rejectIfDraining returns a codes.Unavailable error once shutdown has begun,
// so the control plane reschedules the actor onto a live worker.
func (s *service) rejectIfDraining() error {
	if s.shuttingDown.Load() {
		return apierror.Unavailable("worker draining: not accepting new workloads")
	}
	return nil
}

// cancelStartups cancels all boots and restores, leaving checkpoints running.
func (s *service) cancelStartups(ctx context.Context) {
	for _, actorUID := range s.inFlight.CancelStartups() {
		slog.InfoContext(ctx, "Cancelling in-progress workload startup RPC due to graceful shutdown",
			slog.String("actorUID", actorUID))
	}
}

// validate rejects a request before the actor's lock is taken.
func (s *service) validate(a *Actor) error {
	if errs := resources.ValidateActorDirs(a.Dirs, field.NewPath("actor_dirs")); len(errs) > 0 {
		return apierror.InvalidArgument("%v", errs.ToAggregate())
	}
	return s.runtime.Validate(a)
}

// validateFidelity rejects a checkpoint or restore the runtime cannot serve.
func (s *service) validateFidelity(fidelity ateworkerpb.SnapshotFidelity) error {
	if errs := resources.ValidateSnapshotFidelity(fidelity, s.cfg.Fidelities, field.NewPath("fidelity")); len(errs) > 0 {
		return apierror.InvalidArgument("%v", errs.ToAggregate())
	}
	return nil
}
