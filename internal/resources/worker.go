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

package resources

import (
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// AddToAllocated adjusts allocation by an assignment; sign is 1 or -1.
// It returns nil when allocation reaches zero.
func AddToAllocated(total *ateapipb.WorkerCapacity, assignment *ateapipb.ActorAssignment, sign int64) (*ateapipb.WorkerCapacity, error) {
	held, err := ParseQuantities(total.GetResources())
	if err != nil {
		return nil, fmt.Errorf("allocated: %w", err)
	}
	booked, err := ParseQuantities(assignment.GetResources())
	if err != nil {
		return nil, fmt.Errorf("assignment for actor %s: %w", assignment.GetActorUid(), err)
	}
	if held == nil {
		held = Quantities{}
	}
	if sign > 0 {
		held.Add(booked)
	} else {
		held.Sub(booked)
	}

	actors := total.GetActors() + int32(sign)
	resources := held.Proto()
	if actors == 0 && resources == nil {
		return nil, nil
	}
	return &ateapipb.WorkerCapacity{Actors: actors, Resources: resources}, nil
}

// SumAllocated is what a set of assignments takes from a Worker, or nil for
// none. Rebuilds the total rather than adjusting it, which is what the checks
// holding AddToAllocated to the assignments it counts compare against.
func SumAllocated(assignments []*ateapipb.ActorAssignment) (*ateapipb.WorkerCapacity, error) {
	if len(assignments) == 0 {
		return nil, nil
	}
	total := Quantities{}
	for _, assignment := range assignments {
		booked, err := ParseQuantities(assignment.GetResources())
		if err != nil {
			return nil, fmt.Errorf("assignment for actor %s: %w", assignment.GetActorUid(), err)
		}
		total.Add(booked)
	}
	return &ateapipb.WorkerCapacity{Actors: int32(len(assignments)), Resources: total.Proto()}, nil
}

// WorkerAssignmentFor returns the Worker's assignment for actorUID, or nil if
// it is not hosting that Actor.
func WorkerAssignmentFor(worker *ateapipb.Worker, actorUID string) *ateapipb.ActorAssignment {
	for _, assignment := range worker.GetStatus().GetAssignments() {
		if assignment.GetActorUid() == actorUID {
			return assignment
		}
	}
	return nil
}

// BindAssignment records an Actor as hosted by a Worker, replacing any
// assignment for the same Actor UID. Replacing, not appending: a retried claim
// would otherwise book the Actor against the Worker's capacity twice.
func BindAssignment(worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment) error {
	if worker.GetStatus() == nil {
		worker.Status = &ateapipb.WorkerStatus{}
	}
	replaced := false
	for i, existing := range worker.Status.GetAssignments() {
		if existing.GetActorUid() == assignment.GetActorUid() {
			worker.Status.Assignments[i] = assignment
			replaced = true
			break
		}
	}
	if !replaced {
		worker.Status.Assignments = append(worker.Status.GetAssignments(), assignment)
	}
	allocated, err := SumAllocated(worker.Status.GetAssignments())
	if err != nil {
		return err
	}
	worker.Status.Allocated = allocated
	return nil
}

// ReleaseAssignment drops a Worker's assignment for actorUID, reporting whether
// it held one. Release runs on paths that retry, so already-free is not an
// error.
func ReleaseAssignment(worker *ateapipb.Worker, actorUID string) (bool, error) {
	kept := make([]*ateapipb.ActorAssignment, 0, len(worker.GetStatus().GetAssignments()))
	for _, assignment := range worker.GetStatus().GetAssignments() {
		if assignment.GetActorUid() != actorUID {
			kept = append(kept, assignment)
		}
	}
	if len(kept) == len(worker.GetStatus().GetAssignments()) {
		return false, nil
	}
	worker.Status.Assignments = kept
	allocated, err := SumAllocated(kept)
	if err != nil {
		return false, err
	}
	worker.Status.Allocated = allocated
	return true, nil
}
