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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateworker"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/workerstats"
)

// TestActorBootParamsAttribution pins the mapping from actorBootParams onto the
// attribution the service retains. The three loose fields are the ones most
// likely to get crossed, since actorBootParams names them differently than
// ActorAttribution does; the distinct placeholders below make a swap visible.
func TestActorBootParamsAttribution(t *testing.T) {
	tests := []struct {
		name string
		p    actorBootParams
		want resources.ActorAttribution
	}{
		{
			name: "fully populated",
			p: actorBootParams{
				actorRef:         resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
				actorUID:         "uid-c",
				templateAtespace: "template-ns-d",
				templateName:     "template-name-e",
			},
			want: resources.ActorAttribution{
				Ref:              resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
				UID:              "uid-c",
				TemplateAtespace: "template-ns-d",
				TemplateName:     "template-name-e",
			},
		},
		{
			name: "zero params",
			p:    actorBootParams{},
			want: resources.ActorAttribution{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, tc.p.attribution()); diff != "" {
				t.Errorf("actorBootParams.attribution() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestActorBootParamsAttributionMatchesRequest checks the two hops the
// attribution makes — request to actorBootParams (in Start) and actorBootParams to ActorAttribution — compose back into
// what the caller sent. The two hops are written in different files, so this is
// the assertion that catches them drifting apart.
func TestActorBootParamsAttributionMatchesRequest(t *testing.T) {
	req := &ateworkerpb.RunWorkloadRequest{
		Atespace:              "atespace-a",
		ActorName:             "actor-b",
		ActorUid:              "uid-c",
		ActorTemplateAtespace: "template-ns-d",
		ActorTemplateName:     "template-name-e",
	}

	// Mirrors the actorBootParams literal in Start.
	p := actorBootParams{
		actorRef:         resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		actorUID:         req.GetActorUid(),
		templateAtespace: req.GetActorTemplateAtespace(),
		templateName:     req.GetActorTemplateName(),
	}

	if diff := cmp.Diff(workerstats.ActorAttributionFromRequest(req), p.attribution()); diff != "" {
		t.Errorf("attribution via actorBootParams differs from attribution via request (-request +params):\n%s", diff)
	}
}

var testActor = resources.ActorAttribution{
	Ref:              resources.ActorRef{Atespace: "space-a", Name: "actor-a"},
	UID:              "uid-a",
	TemplateAtespace: "ns-a",
	TemplateName:     "template-a",
}

// fakeAgent stands in for the kata-agent client: a canned reply per container
// id, or a canned error for the ones that are meant to fail.
type fakeAgent struct {
	mu    sync.Mutex
	stats map[string]*agentpb.CgroupStats
	errs  map[string]error

	// onCall, when set, runs at the top of every StatsContainer, so a test can
	// change the hosted set inside the handler's lock-free read.
	onCall func()

	// calls records the container ids asked for, in order, so a test can tell
	// "summed two containers" from "read one twice".
	calls []string
	// deadlines records whether each call's context carried one, which is how
	// the per-call timeout is pinned.
	deadlines []bool
}

func (f *fakeAgent) StatsContainer(ctx context.Context, containerID string) (*agentpb.CgroupStats, error) {
	if f.onCall != nil {
		f.onCall()
	}
	_, ok := ctx.Deadline()
	f.mu.Lock()
	f.calls = append(f.calls, containerID)
	f.deadlines = append(f.deadlines, ok)
	f.mu.Unlock()
	if err, ok := f.errs[containerID]; ok {
		return nil, err
	}
	return f.stats[containerID], nil
}

// blockingAgent answers every container but block, which it holds until ctx
// ends.
type blockingAgent struct {
	fakeAgent
	block string
}

func (b *blockingAgent) StatsContainer(ctx context.Context, containerID string) (*agentpb.CgroupStats, error) {
	if containerID == b.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return b.fakeAgent.StatsContainer(ctx, containerID)
}

// TestSumContainerStatsDeadlineMidRead pins that a deadline hitting during the
// last container's call fails the read rather than returning a partial sum.
func TestSumContainerStatsDeadlineMidRead(t *testing.T) {
	agent := &blockingAgent{fakeAgent: fakeAgent{stats: map[string]*agentpb.CgroupStats{"a": containerStats(1000, 2000, 100, 5000)}}, block: "b"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := sumContainerStats(ctx, &guestStatsTarget{agent: agent, workloadIDs: []string{"a", "b"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("sumContainerStats() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

// containerStats is one container's guest reading: usage bytes, peak bytes,
// reclaimable page cache, and cumulative CPU nanoseconds.
func containerStats(usage, peak, inactiveFile, cpuNanos uint64) *agentpb.CgroupStats {
	return &agentpb.CgroupStats{
		MemoryStats: &agentpb.MemoryStats{
			Usage: &agentpb.MemoryData{Usage: usage, MaxUsage: peak},
			Stats: map[string]uint64{"inactive_file": inactiveFile},
		},
		CpuStats: &agentpb.CpuStats{CpuUsage: &agentpb.CpuUsage{TotalUsage: cpuNanos}},
	}
}

// guestOf is a running VM for testActor whose stats come from agent.
func guestOf(agent containerStatsReader, workloadIDs ...string) *runningActor {
	ra := &runningActor{actorUID: testActor.UID, workloadIDs: workloadIDs}
	ra.guest = &guestStatsTarget{actorUID: testActor.UID, agent: agent, workloadIDs: workloadIDs}
	return ra
}

func sample(t *testing.T, ra *runningActor) *ateworkerpb.WorkloadStatsSample {
	t.Helper()
	got, err := (&microvmRuntime{}).Sample(context.Background(), testActor, ra)
	if err != nil {
		t.Fatalf("Sample() error = %v, want nil", err)
	}
	return got.Sample
}

func TestSample(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl": containerStats(157286400, 209715200, 20971520, 1234567000),
	}}

	before := time.Now().UnixNano()
	got := sample(t, guestOf(agent, "app_ovl"))
	after := time.Now().UnixNano()
	if ts := got.GetObservedAtUnixNano(); ts < before || ts > after {
		t.Errorf("Sample() observed_at_unix_nano = %d, want within [%d, %d]", ts, before, after)
	}
	want := &ateworkerpb.WorkloadStatsSample{
		Atespace:              "space-a",
		ActorName:             "actor-a",
		ActorUid:              "uid-a",
		ActorTemplateAtespace: "ns-a",
		ActorTemplateName:     "template-a",
		SandboxClass:          "microvm",
		Source:                ateworkerpb.StatsSource_STATS_SOURCE_GUEST_AGENT,
		MemoryCurrentBytes:    157286400,
		MemoryPeakBytes:       209715200,
		MemoryWorkingSetBytes: 136314880,
		CpuUsageUsec:          1234567,
	}
	if diff := cmp.Diff(want, got, protocmp.Transform(), protocmp.IgnoreFields(want, "observed_at_unix_nano")); diff != "" {
		t.Errorf("Sample() mismatch (-want +got):\n%s", diff)
	}

	// A poll must not be able to hang on a guest that has stopped answering,
	// which is only true if each call is bounded rather than passed the
	// caller's context straight through.
	if want := []bool{true}; !cmp.Equal(want, agent.deadlines) {
		t.Errorf("StatsContainer call contexts had deadlines %v, want %v", agent.deadlines, want)
	}
}

// The guest gives one cgroup per container, and the proto reports one figure
// for the actor.
func TestSampleSumsContainers(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{
		"app_ovl":     containerStats(1000, 4000, 100, 7000),
		"sidecar_ovl": containerStats(500, 800, 200, 3000),
	}}
	got := sample(t, guestOf(agent, "app_ovl", "sidecar_ovl"))

	if want := uint64(1500); got.GetMemoryCurrentBytes() != want {
		t.Errorf("memory_current_bytes = %d, want %d", got.GetMemoryCurrentBytes(), want)
	}
	// The sum of the peaks, which is an upper bound on the peak of the sum: the
	// two containers need not have peaked at the same moment.
	if want := uint64(4800); got.GetMemoryPeakBytes() != want {
		t.Errorf("memory_peak_bytes = %d, want %d", got.GetMemoryPeakBytes(), want)
	}
	if want := uint64(1200); got.GetMemoryWorkingSetBytes() != want {
		t.Errorf("memory_working_set_bytes = %d, want %d", got.GetMemoryWorkingSetBytes(), want)
	}
	if want := uint64(10); got.GetCpuUsageUsec() != want {
		t.Errorf("cpu_usage_usec = %d, want %d", got.GetCpuUsageUsec(), want)
	}
	if want := []string{"app_ovl", "sidecar_ovl"}; !cmp.Equal(want, agent.calls) {
		t.Errorf("StatsContainer called for %v, want %v", agent.calls, want)
	}
}

// One container the agent cannot report must not silence the actor's
// telemetry. The usual way in is a container that has exited, which consumes
// nothing from here on, so contributing zero is the correct answer for it.
func TestSampleSkipsUnreadableContainer(t *testing.T) {
	agent := &fakeAgent{
		stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5000)},
		errs:  map[string]error{"exited_ovl": errors.New("no such container")},
	}
	got, err := (&microvmRuntime{}).Sample(context.Background(), testActor, guestOf(agent, "app_ovl", "exited_ovl"))
	if err != nil {
		t.Fatalf("Sample() error = %v, want nil", err)
	}
	if want := uint64(1000); got.Sample.GetMemoryCurrentBytes() != want {
		t.Errorf("memory_current_bytes = %d, want %d", got.Sample.GetMemoryCurrentBytes(), want)
	}
	if want := uint64(5); got.Sample.GetCpuUsageUsec() != want {
		t.Errorf("cpu_usage_usec = %d, want %d", got.Sample.GetCpuUsageUsec(), want)
	}
	// The unreadable container is missing from the per-container CPU, so the
	// activation keeps its last value instead of reading it as a drop.
	if _, ok := got.CPUByContainer["exited_ovl"]; ok || got.CPUByContainer["app_ovl"] != 5 {
		t.Errorf("CPUByContainer = %v, want only app_ovl", got.CPUByContainer)
	}
}

// A container the agent answers for without cgroup stats is a reading of zero,
// not a failure, so the sample stands even when it is the only container.
func TestSampleCountsAnsweredContainer(t *testing.T) {
	agent := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": nil}}
	got := sample(t, guestOf(agent, "app_ovl"))
	if got.GetMemoryCurrentBytes() != 0 || got.GetCpuUsageUsec() != 0 {
		t.Errorf("Sample() = %v, want an all-zero measurement", got)
	}
}

func TestSampleErrors(t *testing.T) {
	healthy := &fakeAgent{stats: map[string]*agentpb.CgroupStats{"app_ovl": containerStats(1000, 2000, 100, 5000)}}
	stale := guestOf(healthy, "app_ovl")
	stale.guest.actorUID = "uid-b"
	tornDown := guestOf(healthy, "app_ovl")
	tornDown.release()

	for _, tc := range []struct {
		name string
		sb   ateworker.Sandbox
		// routine failures are "no numbers right now"; the rest are bugs.
		wantRoutine bool
	}{
		// A poll landing in the boot or the restore, before Start returned.
		{name: "booting", sb: nil, wantRoutine: true},
		// Teardown clears the target before it closes the connection.
		{name: "torn down", sb: tornDown, wantRoutine: true},
		// Should be unreachable, so not routine: Internal, unlike every other
		// way Sample declines.
		{name: "target belongs to another actor", sb: stale, wantRoutine: false},
		// The guest as a whole not answering, which is routine here.
		{name: "no container answers", sb: guestOf(&fakeAgent{errs: map[string]error{
			"app_ovl":     errors.New("ttrpc: closed"),
			"sidecar_ovl": errors.New("ttrpc: closed"),
		}}, "app_ovl", "sidecar_ovl"), wantRoutine: true},
		// The boot path does not produce an actor with no containers, so a
		// confident zero would be reporting a state we do not understand.
		{name: "no containers to measure", sb: guestOf(healthy), wantRoutine: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&microvmRuntime{}).Sample(context.Background(), testActor, tc.sb)
			if err == nil {
				t.Fatal("Sample() error = nil, want an error")
			}
			if got := errors.Is(err, ateworker.ErrNoSample); got != tc.wantRoutine {
				t.Errorf("Sample() error %v routine = %v, want %v", err, got, tc.wantRoutine)
			}
		})
	}
}
