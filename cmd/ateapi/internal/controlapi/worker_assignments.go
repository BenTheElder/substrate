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

package controlapi

import (
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// A Worker hosts a set of Actors rather than at most one, so binding and
// releasing are list operations. They live here, together, because the
// invariants that matter span them.
//
// The first is at most one assignment per Actor UID. Two entries for one Actor
// would count its resources against the Worker's capacity twice and leave one
// behind on release, which is a silent capacity leak rather than a visible
// error.
//
// The second is that status.allocated equals the sum of the assignments. It is
// stored rather than derived because the scheduler reads it for every worker on
// every placement, and summing there makes a single placement cost the whole
// fleet's actor count. Keeping a total in step with a list is the kind of thing
// that drifts, so the list is only ever changed through the two functions
// below, and anything that sets it wholesale calls recomputeAllocated.

// bindActorToWorker records an Actor as hosted by a Worker, replacing any
// existing entry for the same Actor UID.
//
// Replacing rather than appending is what makes a retried assignment safe: the
// resume workflow can reach this more than once for the same Actor (a version
// conflict, a re-driven step), and appending would double-book the Actor
// against its Worker's capacity.
func bindActorToWorker(worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment) {
	if worker.GetStatus() == nil {
		worker.Status = &ateapipb.WorkerStatus{}
	}
	for i, existing := range worker.Status.Assignments {
		if existing.GetActorUid() == assignment.GetActorUid() {
			// A replacement is a subtract and an add, not an add: the Actor was
			// already counted, and its declared size may have changed.
			addAllocated(worker, existing, -1)
			worker.Status.Assignments[i] = assignment
			addAllocated(worker, assignment, +1)
			return
		}
	}
	worker.Status.Assignments = append(worker.Status.Assignments, assignment)
	addAllocated(worker, assignment, +1)
}

// releaseActorFromWorker drops the Worker's assignment for actorUID and reports
// whether one was there. A false return means the release is already done,
// which callers treat as success: release runs on paths that retry (suspend,
// crash, a pod vanishing), and each must converge rather than fail the second
// time through.
func releaseActorFromWorker(worker *ateapipb.Worker, actorUID string) bool {
	assignments := worker.GetStatus().GetAssignments()
	for i, existing := range assignments {
		if existing.GetActorUid() != actorUID {
			continue
		}
		worker.Status.Assignments = append(assignments[:i:i], assignments[i+1:]...)
		addAllocated(worker, existing, -1)
		return true
	}
	return false
}

// addAllocated moves the Worker's running total by one assignment's worth.
// sign is +1 to bind and -1 to release.
func addAllocated(worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment, sign int64) {
	if worker.Status.Allocated == nil {
		worker.Status.Allocated = &ateapipb.WorkerCapacity{}
	}
	total := worker.Status.Allocated
	total.Actors += int32(sign)
	total.CpuMilli += sign * assignment.GetResources().GetCpuMilli()
	total.MemoryBytes += sign * assignment.GetResources().GetMemoryBytes()
}

// recomputeAllocated rebuilds the running total from the assignments, for the
// paths that set the list rather than binding and releasing through it -- the
// syncer adopting a worker it just discovered, and tests constructing one.
// Cheap to call and the only correct thing to do after touching the list
// directly.
func recomputeAllocated(worker *ateapipb.Worker) {
	if worker.GetStatus() == nil {
		return
	}
	total := &ateapipb.WorkerCapacity{}
	for _, assignment := range worker.GetStatus().GetAssignments() {
		total.Actors++
		total.CpuMilli += assignment.GetResources().GetCpuMilli()
		total.MemoryBytes += assignment.GetResources().GetMemoryBytes()
	}
	worker.Status.Allocated = total
}

// workerAssignmentForActor returns the Worker's assignment for actorUID, or nil
// when it is not hosting that Actor.
func workerAssignmentForActor(worker *ateapipb.Worker, actorUID string) *ateapipb.ActorAssignment {
	if actorUID == "" {
		return nil
	}
	for _, assignment := range worker.GetStatus().GetAssignments() {
		if assignment.GetActorUid() == actorUID {
			return assignment
		}
	}
	return nil
}
