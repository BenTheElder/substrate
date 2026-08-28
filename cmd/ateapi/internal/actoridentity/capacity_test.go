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

package actoridentity

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	capWorkerName = "8f1c2d34-5e6a-4b7c-9d8e-0f1a2b3c4d5e"
	capNode       = "node-1"
)

// seedCapacityWorker registers a Worker on nodeName carrying capacity, the way
// the syncer does from the pod's limits and the pool's ceiling.
func seedCapacityWorker(t *testing.T, st store.Interface, nodeName string, capacity *ateapipb.WorkerCapacity) *ateapipb.Worker {
	t.Helper()
	created, err := st.CreateWorker(context.Background(), &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: capWorkerName},
		WorkerNamespace: "ate-system",
		WorkerPool:      "pool-1",
		WorkerPod:       "worker-pod-1",
		WorkerPodUid:    capWorkerName,
		NodeName:        nodeName,
		Ip:              "10.1.2.3",
		SandboxClass:    "gvisor",
		Capacity:        capacity,
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
	})
	if err != nil {
		t.Fatalf("seeding worker: %v", err)
	}
	return created
}

func reportRequest(actors int32) *ateapipb.ReportWorkerCapacityRequest {
	return &ateapipb.ReportWorkerCapacityRequest{
		Worker:   &ateapipb.ObjectRef{Name: capWorkerName},
		Capacity: &ateapipb.WorkerCapacity{Actors: actors},
	}
}

// The point of the whole path: a Worker starts at the ceiling the control plane
// assumed and moves to the one its ateom actually reports.
func TestReportWorkerCapacity(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := newTestServer(t, st)
	seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 1, CpuMilli: 2000})

	got, err := s.ReportWorkerCapacity(ctxWithCert(ateletCertOn(t, capNode)), reportRequest(4094))
	if err != nil {
		t.Fatalf("ReportWorkerCapacity() failed: %v", err)
	}
	if want := int32(4094); got.GetWorker().GetCapacity().GetActors() != want {
		t.Errorf("capacity.actors = %d, want %d", got.GetWorker().GetCapacity().GetActors(), want)
	}
	// The reporter knows its slots and nothing about the pod's limits, so the
	// compute dimensions it did not speak to must survive.
	if want := int64(2000); got.GetWorker().GetCapacity().GetCpuMilli() != want {
		t.Errorf("capacity.cpu_milli = %d, want %d preserved", got.GetWorker().GetCapacity().GetCpuMilli(), want)
	}
}

// An atelet speaks for the Workers it herds and no others. A Worker on another
// node is reported as absent rather than forbidden, so a caller learns nothing
// about what runs elsewhere.
func TestReportWorkerCapacity_OtherNodeIsNotFound(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := newTestServer(t, st)
	seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 1})

	_, err := s.ReportWorkerCapacity(ctxWithCert(ateletCertOn(t, "some-other-node")), reportRequest(4094))
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %v (err %v), want NotFound", got, err)
	}

	// And the report must not have landed.
	after, err := st.GetWorker(context.Background(), capWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got := after.GetCapacity().GetActors(); got != 1 {
		t.Errorf("capacity.actors = %d, want 1 unchanged", got)
	}
}

// Re-sending the same capacity is not an update: the reporter runs on a timer,
// so a no-op report must not churn the Worker's version.
func TestReportWorkerCapacity_UnchangedDoesNotWrite(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := newTestServer(t, st)
	seeded := seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 4094})

	for range 3 {
		if _, err := s.ReportWorkerCapacity(ctxWithCert(ateletCertOn(t, capNode)), reportRequest(4094)); err != nil {
			t.Fatalf("ReportWorkerCapacity() failed: %v", err)
		}
	}
	after, err := st.GetWorker(context.Background(), capWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got, want := after.GetMetadata().GetVersion(), seeded.GetMetadata().GetVersion(); got != want {
		t.Errorf("version = %d after three identical reports, want %d unchanged", got, want)
	}
}

func TestReportWorkerCapacity_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := newTestServer(t, st)
	seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 1})
	authed := ctxWithCert(ateletCertOn(t, capNode))

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.ReportWorkerCapacityRequest
		want codes.Code
	}{
		{"unauthenticated", ctxWithCert(nil), reportRequest(2), codes.Unauthenticated},
		{"no worker ref", authed, &ateapipb.ReportWorkerCapacityRequest{
			Capacity: &ateapipb.WorkerCapacity{Actors: 2},
		}, codes.InvalidArgument},
		{"no capacity", authed, &ateapipb.ReportWorkerCapacityRequest{
			Worker: &ateapipb.ObjectRef{Name: capWorkerName},
		}, codes.InvalidArgument},
		{"absent worker", authed, &ateapipb.ReportWorkerCapacityRequest{
			Worker:   &ateapipb.ObjectRef{Name: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"},
			Capacity: &ateapipb.WorkerCapacity{Actors: 2},
		}, codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.ReportWorkerCapacity(tc.ctx, tc.req)
			if got := status.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}
