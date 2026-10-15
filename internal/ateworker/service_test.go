//go:build linux

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

package ateworker

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

var testDirs = &ateworkerpb.ActorDirs{
	RootDir:                   "/node/actors/actor-a",
	OciBundleDir:              "/node/actors/actor-a/bundle",
	CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
	RestoreDir:                "/node/actors/actor-a/restore",
	DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
	SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
	VolumesDir:                "/node/actors/actor-a/volumes",
}

// lifecycleCalls calls every lifecycle RPC for actor-a with dirs and fidelity.
func lifecycleCalls(s *service, dirs *ateworkerpb.ActorDirs, fidelity ateworkerpb.SnapshotFidelity) map[string]func() error {
	ctx := context.Background()
	return map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateworkerpb.RunWorkloadRequest{ActorUid: "actor-a", ActorDirs: dirs})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateworkerpb.RestoreWorkloadRequest{ActorUid: "actor-a", ActorDirs: dirs, Fidelity: fidelity})
			return err
		},
		"CheckpointWorkload": func() error {
			_, err := s.CheckpointWorkload(ctx, &ateworkerpb.CheckpointWorkloadRequest{ActorUid: "actor-a", ActorDirs: dirs, Fidelity: fidelity})
			return err
		},
		"TerminateWorkload": func() error {
			_, err := s.TerminateWorkload(ctx, &ateworkerpb.TerminateWorkloadRequest{ActorUid: "actor-a", ActorDirs: dirs})
			return err
		},
	}
}

// Every RPC rejects a request without usable ActorDirs before touching any
// state. A zero service has no locks, network, or runtime to touch.
func TestRPCsRejectMissingActorDirs(t *testing.T) {
	noRestoreDir := proto.Clone(testDirs).(*ateworkerpb.ActorDirs)
	noRestoreDir.RestoreDir = ""
	for name, dirs := range map[string]*ateworkerpb.ActorDirs{"missing": nil, "no restore dir": noRestoreDir} {
		s := &service{}
		memory := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
		for rpc, call := range lifecycleCalls(s, dirs, memory) {
			if got := apierror.Code(call()); got != codes.InvalidArgument {
				t.Errorf("%s: %s() code = %v, want %v", name, rpc, got, codes.InvalidArgument)
			}
		}
	}
}

// The runtime's own validation runs before the actor's lock.
func TestRPCsRejectWhatTheRuntimeRejects(t *testing.T) {
	s := &service{runtime: &fakeRuntime{validateErr: apierror.InvalidArgument("untrusted runsc path")}}
	memory := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	for rpc, call := range lifecycleCalls(s, testDirs, memory) {
		if err := call(); apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "runsc") {
			t.Errorf("%s() = %v, want the runtime's InvalidArgument", rpc, err)
		}
	}
}

// A fidelity the runtime does not serve fails validation up front, before the
// actor lock or any file system work. Refusing ROOTFS names what it does serve.
func TestRPCsRejectUnsupportedFidelity(t *testing.T) {
	for _, fidelity := range []ateworkerpb.SnapshotFidelity{0, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, 99} {
		s := &service{runtime: &fakeRuntime{}, cfg: newTestService(nil).cfg}
		calls := lifecycleCalls(s, testDirs, fidelity)
		for _, rpc := range []string{"RestoreWorkload", "CheckpointWorkload"} {
			err := calls[rpc]()
			if apierror.Code(err) != codes.InvalidArgument {
				t.Errorf("%s(%v) = %v, want InvalidArgument", rpc, fidelity, err)
				continue
			}
			if fidelity != ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS {
				continue
			}
			for _, want := range []string{"SNAPSHOT_FIDELITY_VOLUMES", "SNAPSHOT_FIDELITY_MEMORY"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s(%v) rejection %q does not list %s as supported", rpc, fidelity, err, want)
				}
			}
		}
	}
}

func TestAdmitActorEnforcesTheCeilingConcurrently(t *testing.T) {
	const ceiling = 8
	s := newTestService(&fakeRuntime{})
	s.maxActors = ceiling

	var wg sync.WaitGroup
	for i := range ceiling * 4 {
		wg.Go(func() {
			_, _, _ = s.admitActor(resources.ActorAttribution{UID: fmt.Sprintf("actor-%d", i)}, false)
		})
	}
	wg.Wait()

	if got := len(s.hostedActors()); got != ceiling {
		t.Errorf("admitted %d actors, want the ceiling of %d", got, ceiling)
	}
}

// Re-admitting an actor that is already hosted keeps its slot, so it succeeds
// on a full worker and a retry cannot lose its place to another actor.
func TestAdmitActorKeepsAHostedActorsSlot(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	s.maxActors = 1
	first, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-a"}, false)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-a"}, false)
	if err != nil {
		t.Fatalf("re-admitting a hosted actor on a full worker: %v", err)
	}
	if again == first {
		t.Error("re-admission kept the old record; readers could not tell the incarnations apart")
	}
	// ResourceExhausted, so the control plane treats it as a capacity miss.
	if _, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-b"}, false); apierror.Code(err) != codes.ResourceExhausted {
		t.Errorf("admitting past the ceiling: got %v, want ResourceExhausted", err)
	}
	if got := len(s.hostedActors()); got != 1 {
		t.Errorf("hosting %d actors, want 1", got)
	}
}

func TestDrainingActorsStillCountAgainstTheCeiling(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	s.maxActors = 1
	s.draining = 1
	if _, err := s.hostActor(context.Background(), resources.ActorAttribution{UID: "actor-a"}, false); err == nil {
		t.Error("admitted an actor while a draining one still held the only place")
	}
}

func TestGracefulShutdownWaitsForInFlightRPCs(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	release := s.inFlight.Add("actor-a", rpcCheckpointWorkload, nil)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		s.gracefulShutdown(context.Background())
	}()

	select {
	case <-returned:
		t.Fatal("gracefulShutdown returned while a checkpoint was still in flight")
	case <-time.After(250 * time.Millisecond):
	}

	release()
	select {
	case <-returned:
	case <-time.After(30 * time.Second):
		t.Error("gracefulShutdown did not return after the checkpoint finished")
	}
}

func TestGracefulShutdownCancelsEveryStartup(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	canceled := make(chan string, 2)
	for _, uid := range []string{"actor-a", "actor-b"} {
		defer s.inFlight.Add(uid, rpcRunWorkload, func() { canceled <- uid })()
	}

	s.cancelStartups(context.Background())

	seen := map[string]bool{}
	for range 2 {
		select {
		case uid := <-canceled:
			seen[uid] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %v were canceled", seen)
		}
	}
}

// The drain reaches every actor with a sandbox and ends each activation with a
// final record, read before its workloads are stopped.
func TestGracefulShutdownDrainsSandboxes(t *testing.T) {
	rt := &fakeRuntime{}
	s := newTestService(rt)
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)
	s.setSandbox(h, "sandbox")

	s.gracefulShutdown(context.Background())
	if !slices.Equal(rt.drained, []string{testActor.UID}) {
		t.Errorf("drained %v, want %v", rt.drained, []string{testActor.UID})
	}
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Fatalf("records = %v, want one final", got)
	}
	if got := rec.Sources(); got[0] != ateattr.StatsSourceCgroup {
		t.Errorf("final record source = %q, want measured", got[0])
	}
}

// An actor still booting has no sandbox to drain; its startup is canceled
// instead.
func TestGracefulShutdownSkipsActorsWithoutSandbox(t *testing.T) {
	rt := &fakeRuntime{}
	s := newTestService(rt)
	hostWithEpoch(s, time.Now(), false)

	s.gracefulShutdown(context.Background())
	if len(rt.drained) != 0 {
		t.Errorf("drained %v, want nothing", rt.drained)
	}
}

func TestBeginRPCRegistersBeforeCheckingForADrain(t *testing.T) {
	s := newTestService(&fakeRuntime{})

	release, err := s.beginRPC("actor-a", rpcRunWorkload, func() {})
	if err != nil {
		t.Fatalf("beginRPC on a live worker: %v", err)
	}
	if _, ok := s.inFlight.Names()["actor-a"]; !ok {
		t.Error("an admitted RPC is not visible to shutdown")
	}
	release()

	// Draining: refused, and nothing left behind for shutdown to wait on.
	s.shuttingDown.Store(true)
	if _, err := s.beginRPC("actor-b", rpcRunWorkload, func() {}); apierror.Code(err) != codes.Unavailable {
		t.Errorf("beginRPC while draining = %v, want Unavailable", err)
	}
	if got := s.inFlight.Names(); len(got) != 0 {
		t.Errorf("a refused RPC stayed registered: %v", got)
	}
}
