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

// The total moves by one assignment's worth in each direction, and a Worker
// back to holding nothing carries no allocation at all rather than a zeroed
// message: emptied and never-filled have to be the same record.
func TestAddToAllocated(t *testing.T) {
	var total *ateapipb.WorkerCapacity
	total = AddToAllocated(total, assignment("a", 1000, 1<<30), +1)
	total = AddToAllocated(total, assignment("b", 500, 2<<30), +1)

	if got, want := total.GetActors(), int32(2); got != want {
		t.Errorf("actors = %d, want %d", got, want)
	}
	if got, want := total.GetCpuMilli(), int64(1500); got != want {
		t.Errorf("cpu_milli = %d, want %d", got, want)
	}
	if got, want := total.GetMemoryBytes(), int64(3<<30); got != want {
		t.Errorf("memory_bytes = %d, want %d", got, want)
	}

	total = AddToAllocated(total, assignment("b", 500, 2<<30), -1)
	if got, want := total.GetCpuMilli(), int64(1000); got != want {
		t.Errorf("cpu_milli = %d after release, want %d", got, want)
	}
	if total = AddToAllocated(total, assignment("a", 1000, 1<<30), -1); total != nil {
		t.Errorf("allocated = %v for a Worker holding nothing, want nil", total)
	}
}

// An Actor that declared no limits reserves nothing but still costs a slot.
func TestAddToAllocatedCountsAnActorWithoutResources(t *testing.T) {
	total := AddToAllocated(nil, &ateapipb.ActorAssignment{ActorUid: "a"}, +1)
	if got, want := total.GetActors(), int32(1); got != want {
		t.Errorf("actors = %d, want %d", got, want)
	}
	if got := total.GetCpuMilli(); got != 0 {
		t.Errorf("cpu_milli = %d, want 0", got)
	}
}

// SumAllocated rebuilds what AddToAllocated adjusts; the contract tests hold the
// two to each other, so they have to agree on the empty case as well.
func TestSumAllocated(t *testing.T) {
	if got := SumAllocated(nil); got != nil {
		t.Errorf("SumAllocated(nil) = %v, want nil", got)
	}
	got := SumAllocated([]*ateapipb.ActorAssignment{assignment("a", 1000, 1<<30), assignment("b", 500, 0)})
	if got.GetActors() != 2 || got.GetCpuMilli() != 1500 || got.GetMemoryBytes() != 1<<30 {
		t.Errorf("SumAllocated() = %v, want 2 actors / 1500 milli / 1GiB", got)
	}
}
