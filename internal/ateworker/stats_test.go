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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// failing is a runtime whose readings all fail with err.
func failing(err error) *fakeRuntime {
	return &fakeRuntime{sample: func(context.Context, resources.ActorAttribution, Sandbox) (Reading, error) {
		return Reading{}, err
	}}
}

var errNotReady = fmt.Errorf("%w: no sandbox yet", ErrNoSample)

func TestGetWorkloadStats(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	setHostedActor(s, &testActor)

	got, err := s.GetWorkloadStats(context.Background(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: testActor.UID})
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}
	want := healthyReading(testActor).Sample
	if diff := cmp.Diff(want, got.GetSample(), protocmp.Transform(), protocmp.IgnoreFields(want, "observed_at_unix_nano")); diff != "" {
		t.Errorf("GetWorkloadStats() mismatch (-want +got):\n%s", diff)
	}
}

func TestGetWorkloadStatsErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		rt   *fakeRuntime
		// active is hosted when non-nil; nil leaves the worker available.
		active   *resources.ActorAttribution
		actorUID string
		want     codes.Code
	}{
		{
			// A client bug, distinct from the races below.
			name:     "empty actor_uid",
			rt:       &fakeRuntime{},
			active:   &testActor,
			actorUID: "",
			want:     codes.InvalidArgument,
		},
		{
			// Not here at all. NOT_FOUND, because the caller should re-resolve,
			// not retry.
			name:     "worker is available",
			rt:       &fakeRuntime{},
			actorUID: testActor.UID,
			want:     codes.NotFound,
		},
		{
			// Reporting anyway would file one actor's numbers under another's.
			name:     "actor_uid does not match the executing workload",
			rt:       &fakeRuntime{},
			active:   &testActor,
			actorUID: "uid-b",
			want:     codes.NotFound,
		},
		{
			// The actor is here with nothing to read yet. The one transient case.
			name:     "no sample yet",
			rt:       failing(errNotReady),
			active:   &testActor,
			actorUID: testActor.UID,
			want:     codes.FailedPrecondition,
		},
		{
			// Not a routine race, so it must not be reported as one.
			name:     "broken reading",
			rt:       failing(errors.New("memory.current: max")),
			active:   &testActor,
			actorUID: testActor.UID,
			want:     codes.Internal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestService(tc.rt)
			if tc.active != nil {
				setHostedActor(s, tc.active)
			}
			resp, err := s.GetWorkloadStats(context.Background(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: tc.actorUID})
			if resp != nil {
				t.Errorf("GetWorkloadStats() returned response %v, want nil", resp)
			}
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("GetWorkloadStats() error code = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}
}

// A stats poll must not queue behind a lifecycle RPC, which holds the actor's
// lock throughout.
func TestGetWorkloadStatsDoesNotTakeLock(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	setHostedActor(s, &testActor)
	if !s.locks.Lock(context.Background(), testActor.UID) {
		t.Fatal("could not take the actor lock")
	}
	defer s.locks.Unlock(testActor.UID)

	if _, err := s.GetWorkloadStats(context.Background(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: testActor.UID}); err != nil {
		t.Errorf("GetWorkloadStats() error = %v, want nil", err)
	}
}

// The runtime sees the sandbox Start returned, and nothing before it.
func TestSampleGetsTheSandbox(t *testing.T) {
	var got []Sandbox
	rt := &fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, sb Sandbox) (Reading, error) {
		got = append(got, sb)
		return healthyReading(a), nil
	}}
	s := newTestService(rt)
	h := setHostedActor(s, &testActor)
	s.sweepUsage(context.Background())
	s.setSandbox(h, "sandbox")
	s.sweepUsage(context.Background())
	if len(got) != 2 || got[0] != nil || got[1] != "sandbox" {
		t.Errorf("Sample got sandboxes %v, want [<nil> sandbox]", got)
	}
}

func TestGetActiveWorkloadStats(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	setHostedActor(s, &testActor)

	got := sweepAndList(t, s)
	if len(got.GetSamples()) != 1 {
		t.Fatalf("GetActiveWorkloadStats() = %v, want one sample", got)
	}
	// The keyed read is the reference: both are the same measurement with a
	// different addressing mode.
	want, err := s.GetWorkloadStats(context.Background(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: testActor.UID})
	if err != nil {
		t.Fatalf("GetWorkloadStats() error = %v, want nil", err)
	}
	if diff := cmp.Diff(want.GetSample(), got.GetSamples()[0], protocmp.Transform(), protocmp.IgnoreFields(want.GetSample(), "observed_at_unix_nano")); diff != "" {
		t.Errorf("discovery sample differs from keyed sample (-keyed +discovery):\n%s", diff)
	}
}

// An idle worker is an empty list, never an error, so the read is scrapeable.
func TestGetActiveWorkloadStatsAvailable(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	if got := sweepAndList(t, s); len(got.GetSamples()) != 0 {
		t.Errorf("GetActiveWorkloadStats() on an available worker = %v, want no samples", got)
	}
}

// A hosted actor with nothing to read is a pending entry, attribution without
// measurements, not an error.
func TestGetActiveWorkloadStatsPending(t *testing.T) {
	for name, err := range map[string]error{"not ready": errNotReady, "broken": errors.New("broken")} {
		t.Run(name, func(t *testing.T) {
			s := newTestService(failing(err))
			setHostedActor(s, &testActor)

			got := sweepAndList(t, s)
			if len(got.GetSamples()) != 1 {
				t.Fatalf("GetActiveWorkloadStats() = %v, want one pending entry", got)
			}
			entry := got.GetSamples()[0]
			if entry.GetObservedAtUnixNano() == 0 {
				t.Error("pending entry observed_at_unix_nano = 0, want set")
			}
			want := &ateworkerpb.WorkloadStatsSample{
				Atespace:              testActor.Ref.Atespace,
				ActorName:             testActor.Ref.Name,
				ActorUid:              testActor.UID,
				ActorTemplateAtespace: testActor.TemplateAtespace,
				ActorTemplateName:     testActor.TemplateName,
				SandboxClass:          testSandboxClass,
			}
			if diff := cmp.Diff(want, entry, protocmp.Transform(), protocmp.IgnoreFields(want, "observed_at_unix_nano")); diff != "" {
				t.Errorf("pending entry mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The reading holds no lock and the runtime finds the sandbox by UID, so the
// actor can be re-hosted underneath it. Numbers are only published under the
// activation they were read for.
func TestSweepTransition(t *testing.T) {
	otherActor := testActor
	otherActor.UID = "uid-b"
	otherTemplate := testActor
	otherTemplate.TemplateName = "template-b"

	for _, tc := range []struct {
		name string
		to   *resources.ActorAttribution
	}{
		{name: "re-hosted on another template", to: &otherTemplate},
		{name: "to another actor", to: &otherActor},
		{name: "to available", to: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s *service
			s = newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
				setHostedActor(s, tc.to)
				return healthyReading(a), nil
			}})
			rec := withUsageRecorder(s)
			setHostedActor(s, &testActor)

			s.sweepUsage(context.Background())
			if got := rec.Kinds(); len(got) != 0 {
				t.Errorf("records during transition = %v, want none", got)
			}
			if tc.to != nil {
				if latest := s.lookupActor(tc.to.UID).usage.Latest(); latest != nil {
					t.Errorf("new activation serves %v, want nothing until its own sample", latest)
				}
			}
		})
	}
}

// The keyed read's side of the same window: the caller asserted an actor that
// is gone by the time the sample exists, so its mapping wants re-resolving.
func TestGetWorkloadStatsTransition(t *testing.T) {
	var s *service
	s = newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		setHostedActor(s, nil)
		return healthyReading(a), nil
	}})
	setHostedActor(s, &testActor)

	_, err := s.GetWorkloadStats(context.Background(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: testActor.UID})
	if got := apierror.Code(err); got != codes.NotFound {
		t.Errorf("GetWorkloadStats() during transition: code = %v, want %v (err: %v)", got, codes.NotFound, err)
	}
}

// One actor that cannot be measured does not stop the rest: it answers as a
// pending entry alongside their samples.
func TestGetActiveWorkloadStatsOnePending(t *testing.T) {
	gone := testActor
	gone.UID = "uid-gone"
	s := newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		if a.UID == gone.UID {
			return Reading{}, errNotReady
		}
		return healthyReading(a), nil
	}})
	s.actorsMu.Lock()
	s.actors = map[string]*hostedActor{
		testActor.UID: {attribution: testActor, usage: testActivation()},
		gone.UID:      {attribution: gone, usage: testActivation()},
	}
	s.actorsMu.Unlock()

	got := sweepAndList(t, s)
	if len(got.GetSamples()) != 2 {
		t.Fatalf("GetActiveWorkloadStats() returned %d samples, want 2: %v", len(got.GetSamples()), got)
	}
	for _, sample := range got.GetSamples() {
		measured := sample.GetSource() != ateworkerpb.StatsSource_STATS_SOURCE_UNSPECIFIED
		if want := sample.GetActorUid() == testActor.UID; measured != want {
			t.Errorf("actor %q measured = %v, want %v", sample.GetActorUid(), measured, want)
		}
	}
}

// A sweep reads at most statsFanOut actors at once.
func TestSweepBoundsConcurrentReadings(t *testing.T) {
	var inFlight, peak atomic.Int32
	s := newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		return healthyReading(a), nil
	}})
	s.actorsMu.Lock()
	for i := range statsFanOut * 3 {
		a := testActor
		a.UID = fmt.Sprintf("uid-%d", i)
		s.actors[a.UID] = &hostedActor{attribution: a, usage: testActivation()}
	}
	s.actorsMu.Unlock()

	s.sweepUsage(context.Background())
	if got := peak.Load(); got > statsFanOut {
		t.Errorf("peak concurrent readings = %d, want at most %d", got, statsFanOut)
	}
}

// captureWarnings routes the default logger to a buffer for the test.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

const deadSandboxMsg = "Sandbox has no processes left while the actor is hosted"

// deadRuntime reports every sandbox dead.
func deadRuntime() *fakeRuntime {
	return &fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		r := healthyReading(a)
		r.Dead = true
		r.DeadAttrs = []slog.Attr{slog.Uint64(string(ateattr.SandboxOOMKillsKey), 3)}
		return r, nil
	}}
}

// A dead sandbox is warned about once per activation, and not at all while a
// lifecycle RPC is tearing it down.
func TestSweepUsageDeadSandbox(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rt        *fakeRuntime
		busy      bool
		wantWarns int
	}{
		{name: "dead sandbox warns once", rt: deadRuntime(), wantWarns: 1},
		{name: "teardown in progress is skipped", rt: deadRuntime(), busy: true},
		{name: "live sandbox does not warn", rt: &fakeRuntime{}},
		{name: "booting sandbox does not warn", rt: failing(errNotReady)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWarnings(t)
			s := newTestService(tc.rt)
			setHostedActor(s, &testActor)
			if tc.busy {
				if !s.locks.Lock(context.Background(), testActor.UID) {
					t.Fatal("could not take the actor lock")
				}
				defer s.locks.Unlock(testActor.UID)
			}

			// Two sweeps: the second must not repeat the warning.
			for range 2 {
				if got := sweepAndList(t, s); len(got.GetSamples()) != 1 {
					t.Fatalf("sweepAndList() = %v, want one sample", got)
				}
			}

			if n := strings.Count(logs.String(), deadSandboxMsg); n != tc.wantWarns {
				t.Fatalf("got %d dead-sandbox warnings, want %d; logs:\n%s", n, tc.wantWarns, logs)
			}
			if tc.wantWarns > 0 {
				for _, want := range []string{`"ate.actor.uid":"uid-a"`, `"ate.actor.name":"actor-a"`, `"ate.atespace":"space-a"`, `"ate.template.name":"template-a"`, `"ate.sandbox.oom_kills":3`} {
					if !strings.Contains(logs.String(), want) {
						t.Errorf("warning missing %s; logs:\n%s", want, logs)
					}
				}
			}
		})
	}
}

// A record that is no longer the hosted one is never reported.
func TestWarnDeadSandboxStaleRecord(t *testing.T) {
	logs := captureWarnings(t)
	s := newTestService(deadRuntime())
	setHostedActor(s, &testActor)
	stale := &hostedActor{attribution: testActor}

	s.warnDeadSandbox(context.Background(), stale, nil)

	if strings.Contains(logs.String(), deadSandboxMsg) {
		t.Fatalf("warned about a stale record; logs:\n%s", logs)
	}
	if stale.deadReported.Load() {
		t.Error("stale record marked as reported")
	}
}

// A new activation of the same actor that dies again is reported again.
func TestSweepUsageDeadSandboxNewActivation(t *testing.T) {
	logs := captureWarnings(t)
	s := newTestService(deadRuntime())
	for range 2 {
		setHostedActor(s, &testActor)
		s.sweepUsage(context.Background())
	}
	if n := strings.Count(logs.String(), deadSandboxMsg); n != 2 {
		t.Fatalf("got %d dead-sandbox warnings over two activations, want 2; logs:\n%s", n, logs)
	}
}

// Concurrent sweeps of one dead sandbox still warn once.
func TestSweepUsageDeadSandboxConcurrentSweeps(t *testing.T) {
	logs := captureWarnings(t)
	s := newTestService(deadRuntime())
	setHostedActor(s, &testActor)

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			s.sweepUsage(context.Background())
		})
	}
	wg.Wait()
	if n := strings.Count(logs.String(), deadSandboxMsg); n != 1 {
		t.Fatalf("got %d dead-sandbox warnings from concurrent sweeps, want 1; logs:\n%s", n, logs)
	}
}

// The drain takes no actor locks, so a sandbox that dies during it is still
// reported.
func TestDeadSandboxCheckDuringDrain(t *testing.T) {
	logs := captureWarnings(t)
	s := newTestService(deadRuntime())
	setHostedActor(s, &testActor)
	s.shuttingDown.Store(true)

	s.sweepUsage(context.Background())
	if n := strings.Count(logs.String(), deadSandboxMsg); n != 1 {
		t.Fatalf("got %d dead-sandbox warnings, want 1; logs:\n%s", n, logs)
	}
}

// A sweep that panics in the runtime recovers, so it cannot take the worker
// down.
func TestSweepRecoversFromPanics(t *testing.T) {
	s := newTestService(&fakeRuntime{sample: func(context.Context, resources.ActorAttribution, Sandbox) (Reading, error) {
		panic("boom")
	}})
	setHostedActor(s, &testActor)
	s.sweepUsage(context.Background())
}
