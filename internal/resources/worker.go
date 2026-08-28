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

// WorkerMaxActors is how many Actors a Worker admits, reading unset capacity as
// one rather than as unconstrained.
//
// It lives here, rather than in the scheduler that enforces it, because the
// scheduler is not the only reader: the CLI reports occupancy against the same
// limit, and if the two disagreed about what an unset capacity means, workers
// would be shown as having room the scheduler would never use, or the reverse.
//
// Unset means one because that is the safe reading. Unlike an unknown cpu or
// memory limit -- where the envelope could not be determined and refusing to
// place would be worse than allowing it -- a Worker that has not said it can
// host more than one Actor almost certainly cannot, and treating silence as
// "no limit" would let any Worker built without capacity accept Actors without
// bound.
func WorkerMaxActors(capacity *ateapipb.WorkerCapacity) int32 {
	if capacity.GetActors() < 1 {
		return 1
	}
	return capacity.GetActors()
}

// AddToAllocated moves a Worker's running allocation total by one assignment's
// worth, with sign +1 to bind and -1 to release, and returns it. A Worker back
// to holding nothing gets nil rather than a zeroed message, so that a Worker
// that emptied and one that was never filled are the same record.
//
// An assignment that records no resources moves only the Actor count: the Actor
// declared no limits, so nothing is known to have been reserved, matching how a
// zero constraint is unconstrained at placement.
func AddToAllocated(total *ateapipb.WorkerCapacity, assignment *ateapipb.ActorAssignment, sign int64) *ateapipb.WorkerCapacity {
	if total == nil {
		total = &ateapipb.WorkerCapacity{}
	}
	total.Actors += int32(sign)
	total.CpuMilli += sign * assignment.GetResources().GetCpuMilli()
	total.MemoryBytes += sign * assignment.GetResources().GetMemoryBytes()
	if total.Actors == 0 && total.CpuMilli == 0 && total.MemoryBytes == 0 {
		return nil
	}
	return total
}

// SumAllocated totals what a set of assignments takes from a Worker, or nil for
// none. Rebuilds the total rather than adjusting it, which is what the checks
// that hold AddToAllocated to the assignments it counts compare against.
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
