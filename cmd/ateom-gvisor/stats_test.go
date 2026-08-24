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

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The lifecycle transitions that maintain the hosted set — set by RunWorkload and
// RestoreWorkload, cleared by CheckpointWorkload — have no unit test, because
// those three RPCs each reach for netlink, runsc, and the worker pod's netns
// within a few lines of entry and cannot be driven from `go test`. The mapping
// they use is covered in internal/ateomstats; the transitions are verified end
// to end. What is testable here is everything GetWorkloadStats does with the
// result, which is where the polling loop will actually live.

var testActor = resources.ActorAttribution{
	Ref:              resources.ActorRef{Atespace: "space-a", Name: "actor-a"},
	UID:              "uid-a",
	TemplateAtespace: "ns-a",
	TemplateName:     "template-a",
}

var healthyCgroup = map[string]string{
	"memory.current": "157286400\n",
	"memory.peak":    "209715200\n",
	"memory.stat":    "anon 104857600\ninactive_file 20971520\nactive_file 31457280\n",
	"cpu.stat":       "usage_usec 1234567\nuser_usec 1000000\n",
}

// writeCgroupFixture lays down one actor's cgroup leaf under root.
func writeCgroupFixture(t *testing.T, root, leaf string, files map[string]string) {
	t.Helper()
	dir := filepath.Join(root, leaf)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("creating fixture cgroup dir: %v", err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("writing fixture %q: %v", name, err)
		}
	}
}

// newStatsService builds a service whose cgroup root is a fixture tree. A nil
// files map leaves the sandbox cgroup directory absent entirely, which is what
// a torn-down sandbox looks like.
//
// The leaf is named for testActor because leaves are per actor, not per
// container: two actors of one template share a container name, so the actor
// UID is what keeps their accounting apart.
func newStatsService(t *testing.T, files map[string]string) *AteomService {
	t.Helper()
	root := t.TempDir()
	if files != nil {
		dir := filepath.Join(root, actorCgroupLeaf(testActor.UID, sandboxCgroupContainer))
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatalf("creating fixture cgroup dir: %v", err)
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
				t.Fatalf("writing fixture %q: %v", name, err)
			}
		}
	}
	return &AteomService{
		actorLocks: actorlock.New(),
		cgroupRoot: root,
	}
}

func TestGetWorkloadStats(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	setHostedActor(s, &testActor)

	before := time.Now().UnixNano()
	got, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	after := time.Now().UnixNano()
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}

	if got.GetSample().GetObservedAtUnixNano() < before || got.GetSample().GetObservedAtUnixNano() > after {
		t.Errorf("GetWorkloadStats() observed_at_unix_nano = %d, want within [%d, %d]", got.GetSample().GetObservedAtUnixNano(), before, after)
	}
	// Checked above; zeroed so the rest can be compared as a whole.
	got.GetSample().ObservedAtUnixNano = 0

	want := &ateompb.GetWorkloadStatsResponse{Sample: &ateompb.WorkloadStatsSample{
		Atespace:              "space-a",
		ActorName:             "actor-a",
		ActorUid:              "uid-a",
		ActorTemplateAtespace: "ns-a",
		ActorTemplateName:     "template-a",
		SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_GVISOR,
		Source:                ateompb.StatsSource_STATS_SOURCE_CGROUP,
		MemoryCurrentBytes:    157286400,
		MemoryPeakBytes:       209715200,
		MemoryWorkingSetBytes: 136314880,
		CpuUsageUsec:          1234567,
	}}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetWorkloadStats() mismatch (-want +got):\n%s", diff)
	}
}

func TestGetWorkloadStatsErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		// files is the fixture sandbox cgroup; nil means the directory is absent.
		files map[string]string
		// active is stored into activeActor when non-nil; nil leaves the ateom
		// "available".
		active   *resources.ActorAttribution
		actorUID string
		want     codes.Code
	}{
		{
			// A required field the caller left off: a client bug, distinct from the
			// races below, so it gets a distinct code.
			name:     "empty actor_uid",
			files:    healthyCgroup,
			active:   &testActor,
			actorUID: "",
			want:     codes.InvalidArgument,
		},
		{
			// Not here at all. NOT_FOUND rather than FAILED_PRECONDITION, because
			// what the caller should do about it is re-resolve, not retry.
			name:     "ateom is available",
			files:    healthyCgroup,
			active:   nil,
			actorUID: "uid-a",
			want:     codes.NotFound,
		},
		{
			// The worker was recycled between the caller's view of the world and
			// this call. Reporting anyway would file one actor's numbers under
			// another's name, and it is the same "not here" as the case above.
			name:     "actor_uid does not match the executing workload",
			files:    healthyCgroup,
			active:   &testActor,
			actorUID: "uid-b",
			want:     codes.NotFound,
		},
		{
			// The requested actor is the one here, but there is nothing to read yet:
			// a poll landing in the boot, or a sandbox torn down between the
			// attribution check and the read. The one transient case, so the one
			// FAILED_PRECONDITION.
			name:     "no sandbox cgroup to measure",
			files:    nil,
			active:   &testActor,
			actorUID: "uid-a",
			want:     codes.FailedPrecondition,
		},
		{
			// The cgroup is there but does not parse: not a routine race, so it
			// must not be reported as one.
			name:     "sandbox cgroup is malformed",
			files:    map[string]string{"memory.current": "max\n"},
			active:   &testActor,
			actorUID: "uid-a",
			want:     codes.Internal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newStatsService(t, tc.files)
			if tc.active != nil {
				setHostedActor(s, tc.active)
			}

			resp, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: tc.actorUID})
			if resp != nil {
				t.Errorf("GetWorkloadStats() returned response %v, want nil", resp)
			}
			if got := status.Code(err); got != tc.want {
				t.Errorf("GetWorkloadStats() error code = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}
}

// TestGetWorkloadStatsDoesNotTakeLock is the regression test for the property
// the design turns on: a stats poll must not queue behind a lifecycle RPC. The
// actor's lock is held for the duration of the call here, so a handler that
// reached for it would deadlock and fail this test by timing out rather than by
// assertion.
func TestGetWorkloadStatsDoesNotTakeLock(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	setHostedActor(s, &testActor)

	// Stands in for a RunWorkload or CheckpointWorkload in flight against this
	// actor, which holds its lock across the entire body.
	if !s.actorLocks.Lock(context.Background(), testActor.UID) {
		t.Fatal("could not take the actor lock")
	}
	defer s.actorLocks.Unlock(testActor.UID)

	if _, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"}); err != nil {
		t.Errorf("GetWorkloadStats() error = %v, want nil", err)
	}
}

// TestAteomServiceStartsAvailable checks that a freshly constructed service
// retains no attribution. GetWorkloadStats's NOT_FOUND-when-available behavior
// is built on this: a non-nil zero value here would make an idle ateom report
// an empty actor's usage instead of refusing.
func TestAteomServiceStartsAvailable(t *testing.T) {
	if got := (&AteomService{}).hostedActors(); len(got) != 0 {
		t.Errorf("new AteomService hosts %v, want nothing", got)
	}
}

func TestGetActiveWorkloadStats(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	setHostedActor(s, &testActor)

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if len(got.GetSamples()) != 1 {
		t.Fatalf("GetActiveWorkloadStats() = %v, want exactly one sample", got)
	}

	// The keyed read against the same fixture is the reference: the discovery
	// read must produce the identical sample, since both are the same
	// measurement with a different addressing mode.
	want, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}
	sample := got.GetSamples()[0]
	sample.ObservedAtUnixNano = 0
	want.GetSample().ObservedAtUnixNano = 0
	if diff := cmp.Diff(want.GetSample(), sample, protocmp.Transform()); diff != "" {
		t.Errorf("discovery sample differs from keyed sample (-keyed +discovery):\n%s", diff)
	}
}

// TestGetActiveWorkloadStatsAvailable pins the contract that makes the
// discovery read scrapeable: an idle ateom is a reason, never an error.
func TestGetActiveWorkloadStatsAvailable(t *testing.T) {
	s := newStatsService(t, healthyCgroup)

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() on an available ateom: error = %v, want nil", err)
	}
	if got.GetNoSampleReason() != ateompb.NoSampleReason_NO_SAMPLE_REASON_NO_WORKLOAD {
		t.Errorf("GetActiveWorkloadStats() = %v, want NO_WORKLOAD reason", got)
	}
}

// TestGetActiveWorkloadStatsBooting: executing but nothing to measure yet is
// a NOT_MEASURABLE_YET reason, not an error, unlike the keyed read's
// FAILED_PRECONDITION. A blind caller finds boots as routinely as idle
// workers.
func TestGetActiveWorkloadStatsBooting(t *testing.T) {
	s := newStatsService(t, nil) // no cgroup directory: a poll landing mid-boot
	setHostedActor(s, &testActor)

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() mid-boot: error = %v, want nil", err)
	}
	if got.GetNoSampleReason() != ateompb.NoSampleReason_NO_SAMPLE_REASON_NOT_MEASURABLE_YET {
		t.Errorf("GetActiveWorkloadStats() mid-boot = %v, want NOT_MEASURABLE_YET reason", got)
	}
}

// The transition tests cover the re-check that runs after the lock-free
// measurement, via the readSandboxCgroup seam: flipping activeActor inside the
// read lands in exactly the window a checkpoint plus a fresh run (or a
// checkpoint alone) can land in.

// TestGetActiveWorkloadStatsTransition covers the hosted set changing while the
// discovery read is measuring.
//
// The cgroup leaf is named for the actor, and sampleSandbox builds its path
// from the attribution it is about to stamp on the sample, so a read can only
// ever return that actor's own numbers. Misattribution is not possible to
// express, and the sample is kept.
func TestGetActiveWorkloadStatsTransition(t *testing.T) {
	otherActor := testActor
	otherActor.UID = "uid-b"

	tests := []struct {
		name string
		to   *resources.ActorAttribution
		// wantSampleFor is the actor the surviving sample must be attributed
		// to, or empty when there should be no sample at all.
		wantSampleFor string
		wantReason    ateompb.NoSampleReason
	}{
		// A different actor took over mid-read. The numbers already read came
		// from the original actor's own leaf, so they are its numbers and are
		// reported as such.
		{name: "to another actor", to: &otherActor, wantSampleFor: testActor.UID},
		// The set emptied mid-read. Same reasoning: the sample belongs to the
		// actor whose leaf it was read from, even though that actor has since
		// left.
		{name: "to available", to: nil, wantSampleFor: testActor.UID},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newStatsService(t, healthyCgroup)
			setHostedActor(s, &testActor)
			s.readSandboxCgroup = func(dir string) (cgroupstats.Sample, error) {
				setHostedActor(s, tc.to)
				return cgroupstats.Read(dir)
			}

			got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
			if err != nil {
				t.Fatalf("GetActiveWorkloadStats() during transition: error = %v, want nil", err)
			}
			if tc.wantSampleFor == "" {
				if len(got.GetSamples()) != 0 {
					t.Errorf("GetActiveWorkloadStats() during transition returned %v, want none", got.GetSamples())
				}
				if got.GetNoSampleReason() != tc.wantReason {
					t.Errorf("GetActiveWorkloadStats() reason = %v, want %v", got.GetNoSampleReason(), tc.wantReason)
				}
				return
			}
			if len(got.GetSamples()) != 1 {
				t.Fatalf("GetActiveWorkloadStats() = %v, want exactly one sample", got)
			}
			if uid := got.GetSamples()[0].GetActorUid(); uid != tc.wantSampleFor {
				t.Errorf("sample attributed to %q, want %q", uid, tc.wantSampleFor)
			}
		})
	}
}

// TestGetWorkloadStatsTransition pins the keyed read's side of the same
// window: the caller asserted an actor that is gone by the time the sample
// exists, so the answer is NOT_FOUND -- its mapping wants re-resolving.
func TestGetWorkloadStatsTransition(t *testing.T) {
	s := newStatsService(t, healthyCgroup)
	setHostedActor(s, &testActor)
	s.readSandboxCgroup = func(dir string) (cgroupstats.Sample, error) {
		setHostedActor(s, nil)
		return cgroupstats.Read(dir)
	}

	_, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("GetWorkloadStats() during transition: code = %v, want %v (err: %v)", got, codes.NotFound, err)
	}
}

// setHostedActor makes the ateom host exactly this actor, the way RunWorkload
// would, or nothing when attribution is nil.
func setHostedActor(s *AteomService, attribution *resources.ActorAttribution) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	s.actors = map[string]*hostedActor{}
	if attribution != nil {
		s.actors[attribution.UID] = &hostedActor{attribution: *attribution}
	}
}

// TestGetActiveWorkloadStatsSeveralActors is why the discovery read returns a
// list: a caller that took the first sample would under-report a worker hosting
// more than one, silently and in the direction that looks healthy.
func TestGetActiveWorkloadStatsSeveralActors(t *testing.T) {
	second := testActor
	second.UID = "uid-b"

	s := newStatsService(t, healthyCgroup)
	// newStatsService lays the fixture leaf down for testActor only, so give the
	// second actor its own -- the leaf is per actor, which is what lets both be
	// measured independently.
	writeCgroupFixture(t, s.cgroupRoot, actorCgroupLeaf(second.UID, sandboxCgroupContainer), healthyCgroup)

	s.actorsMu.Lock()
	s.actors = map[string]*hostedActor{
		testActor.UID: {attribution: testActor},
		second.UID:    {attribution: second},
	}
	s.actorsMu.Unlock()

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if len(got.GetSamples()) != 2 {
		t.Fatalf("GetActiveWorkloadStats() returned %d samples, want 2: %v", len(got.GetSamples()), got)
	}
	seen := map[string]bool{}
	for _, sample := range got.GetSamples() {
		seen[sample.GetActorUid()] = true
	}
	for _, uid := range []string{testActor.UID, second.UID} {
		if !seen[uid] {
			t.Errorf("no sample for actor %q; got %v", uid, seen)
		}
	}
}

// TestGetActiveWorkloadStatsSkipsUnmeasurableActor covers the mixed state a
// multi-actor worker is usually in: one actor serving, another still booting.
// The one that is measurable must still be reported.
func TestGetActiveWorkloadStatsSkipsUnmeasurableActor(t *testing.T) {
	booting := testActor
	booting.UID = "uid-booting"

	s := newStatsService(t, healthyCgroup)
	s.actorsMu.Lock()
	s.actors = map[string]*hostedActor{
		testActor.UID: {attribution: testActor},
		// No cgroup leaf: accepted, but runsc has not created it yet.
		booting.UID: {attribution: booting},
	}
	s.actorsMu.Unlock()

	got, err := s.GetActiveWorkloadStats(context.Background(), &ateompb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	if len(got.GetSamples()) != 1 {
		t.Fatalf("GetActiveWorkloadStats() returned %d samples, want 1: %v", len(got.GetSamples()), got)
	}
	if uid := got.GetSamples()[0].GetActorUid(); uid != testActor.UID {
		t.Errorf("sample attributed to %q, want the measurable actor %q", uid, testActor.UID)
	}
}
