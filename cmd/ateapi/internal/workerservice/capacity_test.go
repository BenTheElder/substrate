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

package workerservice

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	capWorkerName = "8f1c2d34-5e6a-4b7c-9d8e-0f1a2b3c4d5e"
	capNode       = "node-1"
)

// seedCapacityWorker registers a Worker on nodeName carrying capacity, the way
// the syncer does: identity from the pod, and only the ceiling CreateWorker
// reifies.
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
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: capacity,
		},
	})
	if err != nil {
		t.Fatalf("seeding worker: %v", err)
	}
	return created
}

func setRequest(actors int32) *ateapipb.SetWorkerCapacityRequest {
	return &ateapipb.SetWorkerCapacityRequest{
		Worker:   &ateapipb.ObjectRef{Name: capWorkerName},
		Capacity: &ateapipb.WorkerCapacity{Actors: actors},
	}
}

// The point of the whole path: a Worker starts at the ceiling CreateWorker
// reified and moves to the one its ateom actually reports.
func TestSetWorkerCapacity(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st)
	seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 1, Resources: resources.CPUMemory(2000, 0)})

	got, err := s.SetWorkerCapacity(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, capNode)), setRequest(4094))
	if err != nil {
		t.Fatalf("SetWorkerCapacity() failed: %v", err)
	}
	if want := int32(4094); got.GetWorker().GetStatus().GetCapacity().GetActors() != want {
		t.Errorf("capacity.actors = %d, want %d", got.GetWorker().GetStatus().GetCapacity().GetActors(), want)
	}
	// A report replaces what is recorded. The Worker reports everything it has,
	// so a dimension this one leaves out is one it no longer supplies -- keeping
	// the old value would advertise compute nothing claims to have.
	if got := got.GetWorker().GetStatus().GetCapacity().GetResources(); got != nil {
		t.Errorf("capacity resources = %v, want the report's own (none)", got)
	}
}

// An atelet speaks for the Workers it herds and no others. A Worker on another
// node is reported as absent rather than forbidden, so a caller learns nothing
// about what runs elsewhere.
func TestSetWorkerCapacity_OtherNodeIsNotFound(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st)
	seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 1})

	_, err := s.SetWorkerCapacity(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "some-other-node")), setRequest(4094))
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %v (err %v), want NotFound", got, err)
	}

	// And the report must not have landed.
	after, err := st.GetWorker(context.Background(), capWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got := after.GetStatus().GetCapacity().GetActors(); got != 1 {
		t.Errorf("capacity.actors = %d, want 1 unchanged", got)
	}
}

// Re-sending the same capacity is not an update: the reporter runs on a timer,
// so a no-op report must not churn the Worker's version.
func TestSetWorkerCapacity_UnchangedDoesNotWrite(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st)
	seeded := seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 4094})

	for range 3 {
		if _, err := s.SetWorkerCapacity(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, capNode)), setRequest(4094)); err != nil {
			t.Fatalf("SetWorkerCapacity() failed: %v", err)
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

func TestSetWorkerCapacity_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st)
	seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 1})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, capNode))

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.SetWorkerCapacityRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), setRequest(2), codes.Unauthenticated},
		{"no worker ref", authed, &ateapipb.SetWorkerCapacityRequest{
			Capacity: &ateapipb.WorkerCapacity{Actors: 2},
		}, codes.InvalidArgument},
		{"no capacity", authed, &ateapipb.SetWorkerCapacityRequest{
			Worker: &ateapipb.ObjectRef{Name: capWorkerName},
		}, codes.InvalidArgument},
		{"absent worker", authed, &ateapipb.SetWorkerCapacityRequest{
			Worker:   &ateapipb.ObjectRef{Name: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"},
			Capacity: &ateapipb.WorkerCapacity{Actors: 2},
		}, codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.SetWorkerCapacity(tc.ctx, tc.req)
			if got := status.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

// A report goes straight to the store, so nothing else checks it. A negative
// ceiling is the case that matters: placement asks whether allocated is below
// capacity, so the Worker would take no Actor ever again.
func TestSetWorkerCapacity_RejectsNonsense(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := New(st)
	seeded := seedCapacityWorker(t, st, capNode, &ateapipb.WorkerCapacity{Actors: 4094})
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, capNode))

	for _, tc := range []struct {
		name     string
		capacity *ateapipb.WorkerCapacity
	}{
		{"negative ceiling", &ateapipb.WorkerCapacity{Actors: -1}},
		{"int32 underflow", &ateapipb.WorkerCapacity{Actors: -2147483648}},
		{"negative quantity", &ateapipb.WorkerCapacity{Resources: resources.CPUMemory(-1, 0)}},
		{"unparseable quantity", &ateapipb.WorkerCapacity{
			Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "lots"}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.SetWorkerCapacity(authed, &ateapipb.SetWorkerCapacityRequest{
				Worker:   &ateapipb.ObjectRef{Name: capWorkerName},
				Capacity: tc.capacity,
			})
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("code = %v (err %v), want %v", got, err, codes.InvalidArgument)
			}
		})
	}

	after, err := st.GetWorker(context.Background(), capWorkerName)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if diff := cmp.Diff(seeded.GetStatus().GetCapacity(), after.GetStatus().GetCapacity(), protocmp.Transform()); diff != "" {
		t.Errorf("capacity changed despite every report being refused (-want +got):\n%s", diff)
	}
}
