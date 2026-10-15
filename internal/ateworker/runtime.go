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
	"time"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateworker/actornet"
	"github.com/agent-substrate/substrate/internal/atunnel/ingress"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sizing"
)

// Runtime runs actors in one kind of sandbox. The worker owns everything
// else: request validation and locking, actor networking, usage records,
// shutdown.
//
// The worker serializes Start, Checkpoint and Stop per actor. Sample and Drain
// run without the actor's lock.
type Runtime interface {
	// Validate rejects a request the runtime cannot serve, before the worker
	// takes the actor's lock.
	Validate(a *Actor) error
	// Start boots the actor, or restores it when a.Fidelity is set. On error it
	// cleans up whatever it started. An error wrapping ErrRetryStart asks the
	// worker to rebuild the actor's network and start again.
	Start(ctx context.Context, a *Activation) (Sandbox, error)
	// Checkpoint snapshots the actor into its checkpoint dir and reports the
	// files it wrote. The worker tears the actor down afterwards.
	Checkpoint(ctx context.Context, c *Checkpoint) (*ateworkerpb.CheckpointWorkloadResponse, error)
	// Stop tears down the actor's sandbox. sb is nil when the worker has no
	// record of one, so Stop must find leftovers from a alone. Repeated calls
	// are safe.
	Stop(ctx context.Context, a *Actor, sb Sandbox) error
	// Sample measures the actor's resource usage. sb is nil until Start
	// returns. Routine failures wrap ErrNoSample.
	Sample(ctx context.Context, actor resources.ActorAttribution, sb Sandbox) (Reading, error)
	// Drain stops the actor's workloads on shutdown, giving them until
	// deadline to exit before killing them.
	Drain(ctx context.Context, actor resources.ActorAttribution, sb Sandbox, deadline time.Time)
}

// Sandbox is the per-activation state a Runtime returns from Start and gets
// back on later calls. The worker never looks inside it.
type Sandbox any

// Env is what a Runtime gets from the worker at startup.
type Env struct {
	PodUID string
	// ActorLogger forwards actor output to the pod's stdout.
	ActorLogger *actorlog.ActorLogger
	// ActorCgroups is set when the worker's cgroup was delegated, so the
	// runtime can create a cgroup per actor.
	ActorCgroups bool
}

// Actor is what a lifecycle request says about the actor it acts on.
type Actor struct {
	Attribution resources.ActorAttribution
	Dirs        *ateworkerpb.ActorDirs
	Spec        *ateworkerpb.WorkloadSpec
	// Size is the actor's declared limits. Zero on Checkpoint and Terminate.
	Size sizing.SandboxSize
	// RunscPath and AssetPaths locate the sandbox binaries on the node.
	RunscPath  string
	AssetPaths map[string]string
}

// Containers is the actor's containers.
func (a *Actor) Containers() []*ateworkerpb.Container {
	return a.Spec.GetContainers()
}

// Activation is a Start.
type Activation struct {
	Actor
	// Fidelity is unset on a cold boot.
	Fidelity ateworkerpb.SnapshotFidelity
	// PreserveRestoreDir asks the runtime to leave the restore dir as atelet
	// staged it.
	PreserveRestoreDir bool
	// Network is the actor's network, set up before Start.
	Network *actornet.SandboxSession
	// Dial connects to the actor inside its network.
	Dial ingress.DialFunc
	// Timer times the phases of a restore.
	Timer *Timer
	// Attempt counts the Starts of this activation from 1. Start is retried
	// after ErrRetryStart.
	Attempt int
}

// Checkpoint is a Checkpoint.
type Checkpoint struct {
	Actor
	Fidelity ateworkerpb.SnapshotFidelity
	// Sandbox is nil when the worker has no record of one.
	Sandbox Sandbox
	Timer   *Timer
}

// Reading is one usage measurement.
type Reading struct {
	Sample *ateworkerpb.WorkloadStatsSample
	// CPUByContainer is each container's CPU, when the runtime reads them
	// separately, so a container it could not read is not taken for one
	// whose usage fell.
	CPUByContainer map[string]uint64
	// Dead reports a sandbox with no process left. DeadAttrs say more.
	Dead      bool
	DeadAttrs []slog.Attr
}

var (
	// ErrRetryStart marks a Start that failed before the actor ran anything,
	// so starting again is safe.
	ErrRetryStart = errors.New("sandbox stopped before the actor started")
	// ErrNoSample marks a routine failure to measure an actor: it is booting,
	// tearing down, or not answering.
	ErrNoSample = errors.New("no usage sample")
)
