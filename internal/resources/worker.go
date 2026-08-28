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

import "github.com/agent-substrate/substrate/pkg/proto/ateapipb"

// WorkerMaxActors is how many Actors a Worker admits. Unset capacity reads as
// one, not unconstrained: a Worker that has not said it can hold more is
// assumed not to be able to. Shared by the scheduler and the CLI so they cannot
// disagree about what silence means.
func WorkerMaxActors(capacity *ateapipb.WorkerCapacity) int32 {
	if capacity.GetActors() < 1 {
		return 1
	}
	return capacity.GetActors()
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
func BindAssignment(worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment) {
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
	worker.Status.Allocated = SumAllocated(worker.Status.GetAssignments())
}

// ReleaseAssignment drops a Worker's assignment for actorUID, reporting whether
// it held one. Release runs on paths that retry, so already-free is not an
// error.
func ReleaseAssignment(worker *ateapipb.Worker, actorUID string) bool {
	kept := make([]*ateapipb.ActorAssignment, 0, len(worker.GetStatus().GetAssignments()))
	for _, assignment := range worker.GetStatus().GetAssignments() {
		if assignment.GetActorUid() != actorUID {
			kept = append(kept, assignment)
		}
	}
	if len(kept) == len(worker.GetStatus().GetAssignments()) {
		return false
	}
	worker.Status.Assignments = kept
	worker.Status.Allocated = SumAllocated(kept)
	return true
}

// SumAllocated is what a set of assignments takes from a Worker, or nil for
// none, so an idle Worker carries no allocation rather than a zeroed message.
// status.allocated is kept as this sum so placement never has to compute it.
func SumAllocated(assignments []*ateapipb.ActorAssignment) *ateapipb.WorkerCapacity {
	if len(assignments) == 0 {
		return nil
	}
	total := &ateapipb.WorkerCapacity{Actors: int32(len(assignments))}
	for _, assignment := range assignments {
		total.CpuMilli += assignment.GetResources().GetCpuMilli()
		total.MemoryBytes += assignment.GetResources().GetMemoryBytes()
	}
	return total
}
