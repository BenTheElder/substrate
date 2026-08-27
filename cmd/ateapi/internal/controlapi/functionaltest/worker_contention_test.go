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

package functionaltest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// TestResumeActor_ConcurrentOntoOneWorker is the regression test for a refusal
// that only concurrency produces, and that a worker hosting one actor could
// never have shown.
//
// Claiming a worker rewrites that worker's whole record, so actors activating
// onto the SAME worker are N writers compare-and-swapping one row. All but one
// lose each round. That is fine and expected -- the loser re-reads and tries
// again -- but only if the retry budget is sized for how many writers there
// are. It was five steps from 10ms, and on a real cluster that rejected 21% of
// activations at 12 in flight and 67% at 24, every one of them failing at
// ~235ms with "timed out waiting for the condition".
//
// Nothing sequential catches it: the same activations one at a time all
// succeed, which is exactly what the pre-existing resume tests do.
func TestResumeActor_ConcurrentOntoOneWorker(t *testing.T) {
	const actors = 128

	ns := namespaceForTest("ns-resume-contention")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	// createTemplate makes pool1 for us; widen it so one worker has room for all
	// of them. The point is that they contend, not that they are turned away for
	// lack of capacity.
	createTemplate(t, tc, ns)
	podUID := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	setWorkerActorCapacity(t, tc, podUID, actors)

	for i := range actors {
		if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("id%d", i)},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		}}); err != nil {
			t.Fatalf("CreateActor %d: %v", i, err)
		}
	}

	// Resume them all at once. Starting the goroutines is not enough to make
	// them race -- release them together so they arrive at the claim inside the
	// same window.
	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	errs := make([]error, actors)
	for i := range actors {
		wg.Go(func() {
			start.Wait()
			_, errs[i] = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: fmt.Sprintf("id%d", i)},
			})
		})
	}
	start.Done()
	wg.Wait()

	var failed int
	for i, err := range errs {
		if err != nil {
			failed++
			if failed <= 3 {
				t.Errorf("ResumeActor id%d: %v", i, err)
			}
		}
	}
	if failed > 0 {
		t.Fatalf("%d of %d concurrent activations onto one worker were refused; "+
			"they contend on the worker record and the retry budget has to absorb that", failed, actors)
	}

	// And every one of them is really on the worker: a claim that was lost but
	// reported as won would show up here rather than as an error above. Found by
	// pod rather than by taking the only worker in the list -- the list is
	// process-wide, so a -count>1 run sees the workers earlier iterations made.
	listed, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	var worker *ateapipb.Worker
	for _, candidate := range listed.GetWorkers() {
		if candidate.GetWorkerNamespace() == ns && candidate.GetWorkerPod() == "worker-1" {
			worker = candidate
			break
		}
	}
	if worker == nil {
		t.Fatalf("worker %s/worker-1 not in the list of %d", ns, len(listed.GetWorkers()))
	}
	if got := worker.GetStatus().GetAllocated().GetActors(); got != int32(actors) {
		t.Errorf("worker holds %d actors, want %d", got, actors)
	}
}

// setWorkerActorCapacity gives one worker room for several actors, standing in
// for the report its ateom would make. The ceiling is the worker's own, so this
// is where a test that wants a crowd on one worker has to say so.
func setWorkerActorCapacity(t *testing.T, tc *testContext, workerName string, actors int32) {
	t.Helper()
	observed, err := tc.client.GetWorker(context.Background(), &ateapipb.GetWorkerRequest{
		Worker: &ateapipb.ObjectRef{Name: workerName},
	})
	if err != nil {
		t.Fatalf("get worker %s: %v", workerName, err)
	}
	updated := proto.Clone(observed).(*ateapipb.Worker)
	if updated.Capacity == nil {
		updated.Capacity = &ateapipb.WorkerCapacity{}
	}
	updated.Capacity.Actors = actors
	if _, err := tc.client.UpdateWorker(context.Background(), &ateapipb.UpdateWorkerRequest{Worker: updated}); err != nil {
		t.Fatalf("widening worker %s to %d actors: %v", workerName, actors, err)
	}

	// The scheduler reads workers through a watch-fed cache, so wait for the
	// widened ceiling rather than merely for the write to return.
	if err := wait.PollUntilContextTimeout(context.Background(), 50*time.Millisecond, 5*time.Second, true,
		func(ctx context.Context) (bool, error) {
			got, err := tc.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{
				Worker: &ateapipb.ObjectRef{Name: workerName},
			})
			return err == nil && got.GetCapacity().GetActors() == actors, nil
		}); err != nil {
		t.Fatalf("worker %s did not reach %d actors: %v", workerName, actors, err)
	}
}
