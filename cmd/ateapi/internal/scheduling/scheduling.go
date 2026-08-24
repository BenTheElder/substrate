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

// Package scheduling decides which worker should host an actor.
package scheduling

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/apimachinery/pkg/labels"
)

// Constraints describes what a worker must satisfy to host an actor.
type Constraints struct {
	// SandboxClass must equal the worker's sandbox class. Snapshots are not
	// portable across sandbox classes, so this is never relaxed.
	SandboxClass string

	// TemplateSelector and ActorSelector must both match the worker's labels.
	TemplateSelector labels.Selector
	ActorSelector    labels.Selector

	// RequiredNodes, when non-empty, restricts placement to workers running
	// on one of these nodes. Used when the actor's latest snapshot is local
	// to specific node VMs.
	RequiredNodes []string

	// CPUMilli and MemoryBytes are the actor's declared resource limits, from
	// the ActorTemplate. A worker has room only if its capacity, less what its
	// existing assignments already took, is >= these. Zero means "unconstrained"
	// for that dimension (the actor did not declare a limit), and a worker that
	// reports zero capacity for a dimension is treated as unconstrained too, so
	// placement is never blocked by missing data (matching the pre-capacity
	// behavior).
	CPUMilli    int64
	MemoryBytes int64
}

// ErrNoCapacity is returned by Schedule when no free worker satisfies the
// constraints.
var ErrNoCapacity = errors.New("no free workers satisfy the constraints")

// Scheduler answers placement questions against the current worker fleet.
type Scheduler interface {
	// Schedule returns a free worker satisfying constraints.
	// Returns ErrNoCapacity when no free worker satisfies the requested constraints.
	Schedule(ctx context.Context, constraints Constraints) (*ateapipb.Worker, error)

	// Applies reports whether worker satisfies the constraints that do not
	// depend on what it is already hosting: sandbox class, state, selectors,
	// and node.
	//
	// Deliberately excludes whether there is room, because callers use it for
	// two different questions. Schedule asks "may this worker take one more",
	// and pairs it with HasRoom. A caller re-validating a worker that already
	// holds the actor asks only "is this still a legal placement" -- and for
	// that one, room is the wrong question: the actor's own resources are
	// already counted against the worker, so a full worker holding the actor
	// would report itself ineligible and the actor would be evicted from a
	// placement that is perfectly valid.
	Applies(worker *ateapipb.Worker, constraints Constraints) bool

	// HasRoom reports whether worker's remaining capacity admits one more actor
	// of this size, in every dimension. Independent of Applies.
	HasRoom(worker *ateapipb.Worker, constraints Constraints) bool
}

// WorkerSource provides the whole fleet of workers.
type WorkerSource interface {
	Workers() ([]*ateapipb.Worker, error)
}

type scheduler struct {
	source WorkerSource
	// intn returns a uniformly distributed random value in [0,n).
	// Defaults to the global math/rand source
	intn func(n int) int
	// Records the number of eligible workers available during scheduling.
	eligibleWorkers metric.Int64Histogram
}

// Option configures the Scheduler returned by New.
type Option func(*scheduler)

// WithIntn overrides the random source used to pick among equally suitable
// workers. n is always >= 1.
func WithIntn(intn func(n int) int) Option {
	return func(s *scheduler) { s.intn = intn }
}

// New returns a Scheduler placing onto workers reported by source.
func New(source WorkerSource, opts ...Option) Scheduler {
	s := &scheduler{source: source, intn: rand.Intn}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Schedule filters the current worker fleet to find unassigned candidates matching the given constraints.
func (s *scheduler) Schedule(ctx context.Context, constraints Constraints) (*ateapipb.Worker, error) {
	workers, err := s.source.Workers()
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}

	// Filter for candidate workers that meet all scheduling constraints and
	// still have room for one more actor.
	matching := make([]*ateapipb.Worker, 0, len(workers))
	var candidates []*ateapipb.Worker
	for _, worker := range workers {
		if !s.Applies(worker, constraints) {
			continue
		}
		matching = append(matching, worker)
		if s.HasRoom(worker, constraints) {
			candidates = append(candidates, worker)
		}
	}

	// Record telemetry on the number of eligible workers per pool/namespace before returning
	s.recordEligibleWorkers(ctx, matching, constraints)

	if len(candidates) == 0 {
		return nil, ErrNoCapacity
	}

	return candidates[s.intn(len(candidates))], nil
}

func (s *scheduler) Applies(worker *ateapipb.Worker, constraints Constraints) bool {
	if worker.GetSandboxClass() != constraints.SandboxClass {
		return false
	}

	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		return false
	}

	set := labels.Set(worker.GetLabels())
	if constraints.TemplateSelector != nil && !constraints.TemplateSelector.Matches(set) {
		return false
	}
	if constraints.ActorSelector != nil && !constraints.ActorSelector.Matches(set) {
		return false
	}

	return len(constraints.RequiredNodes) == 0 || slices.Contains(constraints.RequiredNodes, worker.GetNodeName())
}

// HasRoom reports whether what the worker has left admits one more actor of
// this size. A zero constraint (the actor declared no limit) or zero worker
// capacity (capacity unknown) is treated as unconstrained in that dimension, so
// placement is never blocked by missing data.
func (s *scheduler) HasRoom(worker *ateapipb.Worker, constraints Constraints) bool {
	capacity := worker.GetCapacity()
	used := Allocated(worker)

	// The actor count is the one dimension with no per-actor size to compare:
	// every assignment costs exactly one, so a worker at its limit has no room
	// regardless of how small the next actor is.
	//
	// Unset means one here, not unconstrained as it does for cpu and memory;
	// see resources.WorkerMaxActors.
	if used.GetActors() >= resources.WorkerMaxActors(capacity) {
		return false
	}
	if constraints.CPUMilli > 0 && capacity.GetCpuMilli() > 0 &&
		capacity.GetCpuMilli()-used.GetCpuMilli() < constraints.CPUMilli {
		return false
	}
	if constraints.MemoryBytes > 0 && capacity.GetMemoryBytes() > 0 &&
		capacity.GetMemoryBytes()-used.GetMemoryBytes() < constraints.MemoryBytes {
		return false
	}
	return true
}

// Allocated is what a worker's current assignments took from it: the running
// total the control plane maintains as it binds and releases them.
//
// Stored rather than summed per call because Schedule reads it for every worker
// on every placement, which would otherwise cost the fleet's whole actor count
// each time.
//
// An assignment that records no resources contributes only to the actor count:
// the actor declared no limits, so nothing is known to have been reserved,
// matching how a zero constraint is unconstrained at placement.
func Allocated(worker *ateapipb.Worker) *ateapipb.WorkerCapacity {
	status := worker.GetStatus()
	if allocated := status.GetAllocated(); allocated != nil || len(status.GetAssignments()) == 0 {
		return allocated
	}
	// Assignments with no total should not happen, but trusting one would read a
	// full worker as empty and keep placing actors on it. Summing is slower and
	// correct.
	total := &ateapipb.WorkerCapacity{}
	for _, assignment := range status.GetAssignments() {
		total.Actors++
		total.CpuMilli += assignment.GetResources().GetCpuMilli()
		total.MemoryBytes += assignment.GetResources().GetMemoryBytes()
	}
	return total
}
