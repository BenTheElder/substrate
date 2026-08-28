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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func assignment(uid string, cpu, mem int64) *ateapipb.ActorAssignment {
	return &ateapipb.ActorAssignment{
		Actor:     &ateapipb.ObjectRef{Atespace: "demo", Name: uid},
		ActorUid:  uid,
		Resources: &ateapipb.WorkerCapacity{CpuMilli: cpu, MemoryBytes: mem},
	}
}

// Unset capacity has to read as one, not as unconstrained: a Worker built
// before anything reported a ceiling must not accept Actors without bound.
func TestWorkerMaxActors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity *ateapipb.WorkerCapacity
		want     int32
	}{
		{"nil capacity", nil, 1},
		{"capacity with no actors dimension", &ateapipb.WorkerCapacity{CpuMilli: 4000}, 1},
		{"negative is still one", &ateapipb.WorkerCapacity{Actors: -3}, 1},
		{"a reported ceiling is used", &ateapipb.WorkerCapacity{Actors: 4094}, 4094},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := WorkerMaxActors(tc.capacity); got != tc.want {
				t.Errorf("WorkerMaxActors(%v) = %d, want %d", tc.capacity, got, tc.want)
			}
		})
	}
}

func TestBindAssignmentTracksAllocation(t *testing.T) {
	worker := &ateapipb.Worker{}

	BindAssignment(worker, assignment("a", 1000, 1<<30))
	BindAssignment(worker, assignment("b", 500, 2<<30))

	if got, want := len(worker.GetStatus().GetAssignments()), 2; got != want {
		t.Fatalf("assignments = %d, want %d", got, want)
	}
	allocated := worker.GetStatus().GetAllocated()
	if got, want := allocated.GetActors(), int32(2); got != want {
		t.Errorf("allocated.actors = %d, want %d", got, want)
	}
	if got, want := allocated.GetCpuMilli(), int64(1500); got != want {
		t.Errorf("allocated.cpu_milli = %d, want %d", got, want)
	}
	if got, want := allocated.GetMemoryBytes(), int64(3<<30); got != want {
		t.Errorf("allocated.memory_bytes = %d, want %d", got, want)
	}
}

// A claim can be retried for one Actor, and appending would book it against the
// Worker's capacity twice.
func TestBindAssignmentReplacesTheSameActor(t *testing.T) {
	worker := &ateapipb.Worker{}
	BindAssignment(worker, assignment("a", 1000, 1<<30))
	BindAssignment(worker, assignment("a", 2000, 1<<30))

	if got, want := len(worker.GetStatus().GetAssignments()), 1; got != want {
		t.Fatalf("assignments = %d, want %d", got, want)
	}
	if got, want := worker.GetStatus().GetAllocated().GetActors(), int32(1); got != want {
		t.Errorf("allocated.actors = %d, want %d", got, want)
	}
	if got, want := worker.GetStatus().GetAllocated().GetCpuMilli(), int64(2000); got != want {
		t.Errorf("allocated.cpu_milli = %d, want %d after replacement", got, want)
	}
}

func TestReleaseAssignment(t *testing.T) {
	worker := &ateapipb.Worker{}
	BindAssignment(worker, assignment("a", 1000, 1<<30))
	BindAssignment(worker, assignment("b", 500, 2<<30))

	if !ReleaseAssignment(worker, "a") {
		t.Fatal("ReleaseAssignment() = false for an Actor the Worker holds, want true")
	}
	if got, want := worker.GetStatus().GetAllocated().GetCpuMilli(), int64(500); got != want {
		t.Errorf("allocated.cpu_milli = %d, want %d", got, want)
	}
	if WorkerAssignmentFor(worker, "a") != nil {
		t.Error("released Actor is still assigned")
	}
	if WorkerAssignmentFor(worker, "b") == nil {
		t.Error("releasing one Actor dropped another")
	}

	// Release runs on paths that retry; the second pass converges rather than
	// failing.
	if ReleaseAssignment(worker, "a") {
		t.Error("ReleaseAssignment() = true for an Actor already released, want false")
	}
}

// An idle Worker carries no allocation at all, rather than an all-zero message
// that says the same thing in more bytes on every record and every event.
func TestReleasingTheLastAssignmentClearsAllocation(t *testing.T) {
	worker := &ateapipb.Worker{}
	BindAssignment(worker, assignment("a", 1000, 1<<30))
	ReleaseAssignment(worker, "a")

	if got := worker.GetStatus().GetAllocated(); got != nil {
		t.Errorf("allocated = %v for an idle Worker, want nil", got)
	}
}

// An Actor that declared no limits reserves nothing but still costs a slot.
func TestAssignmentWithoutResourcesCountsOnlyTheActor(t *testing.T) {
	worker := &ateapipb.Worker{}
	BindAssignment(worker, &ateapipb.ActorAssignment{ActorUid: "a"})

	allocated := worker.GetStatus().GetAllocated()
	if got, want := allocated.GetActors(), int32(1); got != want {
		t.Errorf("allocated.actors = %d, want %d", got, want)
	}
	if got := allocated.GetCpuMilli(); got != 0 {
		t.Errorf("allocated.cpu_milli = %d, want 0", got)
	}
}
