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
	"fmt"
	"math/rand/v2"
	"testing"

	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func assignment(uid string, cpu, mem int64) *ateapipb.ActorAssignment {
	return &ateapipb.ActorAssignment{
		Actor:     &ateapipb.ObjectRef{Atespace: "s", Name: uid},
		ActorUid:  uid,
		Resources: &ateapipb.WorkerCapacity{CpuMilli: cpu, MemoryBytes: mem},
	}
}

// sumOfAssignments is what the running total is supposed to equal, computed the
// slow way the scheduler used to.
func sumOfAssignments(worker *ateapipb.Worker) *ateapipb.WorkerCapacity {
	total := &ateapipb.WorkerCapacity{}
	for _, a := range worker.GetStatus().GetAssignments() {
		total.Actors++
		total.CpuMilli += a.GetResources().GetCpuMilli()
		total.MemoryBytes += a.GetResources().GetMemoryBytes()
	}
	return total
}

// TestAllocatedTracksAssignmentsUnderRandomChurn is the test the stored total
// needs, because the risk of storing it is not that the arithmetic is wrong
// once -- it is that some sequence of binds and releases leaves it disagreeing
// with the list it summarizes, quietly, forever after.
//
// So: churn a worker through thousands of random binds, rebinds and releases,
// and after every single one require the total to equal the list.
func TestAllocatedTracksAssignmentsUnderRandomChurn(t *testing.T) {
	worker := &ateapipb.Worker{Status: &ateapipb.WorkerStatus{}}
	rng := rand.New(rand.NewPCG(1, 2))

	live := map[string]bool{}
	for step := range 4000 {
		uid := fmt.Sprintf("actor-%d", rng.IntN(120))
		switch rng.IntN(3) {
		case 0, 1:
			// Bind, sometimes over an actor already there and with a different
			// size, which is the case that has to subtract before it adds.
			bindActorToWorker(worker, assignment(uid, int64(rng.IntN(4)+1)*500, int64(rng.IntN(4)+1)<<24))
			live[uid] = true
		default:
			releaseActorFromWorker(worker, uid)
			delete(live, uid)
		}

		if diff := cmp.Diff(sumOfAssignments(worker), worker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
			t.Fatalf("after step %d (actor %s) the running total no longer matches the assignments (-want +got):\n%s", step, uid, diff)
		}
		if got, want := len(worker.GetStatus().GetAssignments()), len(live); got != want {
			t.Fatalf("after step %d the worker holds %d assignments, want %d", step, got, want)
		}
	}

	// And it must come back to exactly zero, not merely to something small:
	// a total that drifts by a rounding error per cycle is the failure this
	// guards against.
	for uid := range live {
		releaseActorFromWorker(worker, uid)
	}
	if got := worker.GetStatus().GetAllocated(); got.GetActors() != 0 || got.GetCpuMilli() != 0 || got.GetMemoryBytes() != 0 {
		t.Errorf("after releasing everything the total is %v, want all zero", got)
	}
}

// TestAllocatedIsWhatTheSchedulerReads pins that the scheduler sees the stored
// total, so the two cannot be changed apart.
func TestAllocatedIsWhatTheSchedulerReads(t *testing.T) {
	worker := &ateapipb.Worker{Status: &ateapipb.WorkerStatus{}}
	bindActorToWorker(worker, assignment("a", 2000, 1<<26))
	bindActorToWorker(worker, assignment("b", 500, 1<<24))

	if diff := cmp.Diff(sumOfAssignments(worker), scheduling.Allocated(worker), protocmp.Transform()); diff != "" {
		t.Errorf("scheduling.Allocated disagrees with the assignments (-want +got):\n%s", diff)
	}
}

// TestRecomputeAllocatedRepairsDrift covers the self-heal the syncer relies on:
// whatever put the total out of step, recomputing puts it back.
func TestRecomputeAllocatedRepairsDrift(t *testing.T) {
	worker := &ateapipb.Worker{Status: &ateapipb.WorkerStatus{}}
	bindActorToWorker(worker, assignment("a", 2000, 1<<26))
	// Reach past the helpers, which is exactly what recompute exists to survive.
	worker.Status.Assignments = append(worker.Status.Assignments, assignment("b", 500, 1<<24))

	if cmp.Diff(sumOfAssignments(worker), worker.GetStatus().GetAllocated(), protocmp.Transform()) == "" {
		t.Fatal("expected the direct edit to leave the total out of step; the test is not testing anything")
	}
	recomputeAllocated(worker)
	if diff := cmp.Diff(sumOfAssignments(worker), worker.GetStatus().GetAllocated(), protocmp.Transform()); diff != "" {
		t.Errorf("recomputeAllocated did not repair the total (-want +got):\n%s", diff)
	}
}
