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

// AddToAllocated adjusts allocation by an assignment; sign is 1 or -1.
// It returns nil when allocation reaches zero.
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

// SumAllocated is what a set of assignments takes from a Worker, or nil for
// none. Rebuilds the total rather than adjusting it, which is what the checks
// holding AddToAllocated to the assignments it counts compare against.
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
