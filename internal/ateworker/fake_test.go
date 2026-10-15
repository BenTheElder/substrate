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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/workerstats"
	"github.com/agent-substrate/substrate/internal/workerstats/workerstatstest"
)

// The lifecycle RPCs that host actors need netlink and network namespaces, so
// they are covered end to end. These tests drive the rest against a hosted set
// built directly and a fake runtime.

const testSandboxClass = "test"

var testActor = resources.ActorAttribution{
	Ref:              resources.ActorRef{Atespace: "space-a", Name: "actor-a"},
	UID:              "uid-a",
	TemplateAtespace: "ns-a",
	TemplateName:     "template-a",
}

// testCPUUsec is the CPU in every reading fakeRuntime makes.
const testCPUUsec = 1234567

// fakeRuntime measures every actor as healthy unless sample is set.
type fakeRuntime struct {
	validateErr error
	sample      func(ctx context.Context, actor resources.ActorAttribution, sb Sandbox) (Reading, error)

	mu      sync.Mutex
	stopped []string
	drained []string
}

var _ Runtime = (*fakeRuntime)(nil)

func (f *fakeRuntime) Validate(*Actor) error { return f.validateErr }

func (f *fakeRuntime) Start(context.Context, *Activation) (Sandbox, error) {
	return nil, errors.New("fakeRuntime cannot start actors")
}

func (f *fakeRuntime) Checkpoint(context.Context, *Checkpoint) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	return nil, errors.New("fakeRuntime cannot checkpoint actors")
}

func (f *fakeRuntime) Stop(_ context.Context, a *Actor, _ Sandbox) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, a.Attribution.UID)
	return nil
}

func (f *fakeRuntime) Sample(ctx context.Context, actor resources.ActorAttribution, sb Sandbox) (Reading, error) {
	if f.sample != nil {
		return f.sample(ctx, actor, sb)
	}
	return healthyReading(actor), nil
}

func (f *fakeRuntime) Drain(_ context.Context, actor resources.ActorAttribution, _ Sandbox, _ time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drained = append(f.drained, actor.UID)
}

func healthyReading(actor resources.ActorAttribution) Reading {
	return Reading{Sample: &ateworkerpb.WorkloadStatsSample{
		Atespace:              actor.Ref.Atespace,
		ActorName:             actor.Ref.Name,
		ActorUid:              actor.UID,
		ActorTemplateAtespace: actor.TemplateAtespace,
		ActorTemplateName:     actor.TemplateName,
		SandboxClass:          testSandboxClass,
		Source:                ateworkerpb.StatsSource_STATS_SOURCE_CGROUP,
		MemoryCurrentBytes:    157286400,
		CpuUsageUsec:          testCPUUsec,
		ObservedAtUnixNano:    time.Now().UnixNano(),
	}}
}

func newTestService(rt Runtime) *service {
	return newService(Config{
		ServiceName:  "test",
		SandboxClass: testSandboxClass,
		Fidelities: []ateworkerpb.SnapshotFidelity{
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
	}, rt, nil, nil, 1000)
}

func withUsageRecorder(s *service) *workerstatstest.Recorder {
	e, rec := workerstatstest.NewEmitter()
	s.usage = e
	return rec
}

// setHostedActor makes the worker host exactly this actor, the way a start
// would, or nothing when attribution is nil.
func setHostedActor(s *service, attribution *resources.ActorAttribution) *hostedActor {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	s.actors = map[string]*hostedActor{}
	if attribution == nil {
		return nil
	}
	h := &hostedActor{attribution: *attribution, usage: testActivation()}
	s.actors[attribution.UID] = h
	return h
}

// hostWithEpoch hosts testActor; running means its initial reading is done.
func hostWithEpoch(s *service, epoch time.Time, running bool) *hostedActor {
	h := &hostedActor{attribution: testActor, usage: workerstats.NewActivation(epoch, false)}
	if running {
		h.usage.Initial(nil, nil)
	}
	s.actorsMu.Lock()
	s.actors = map[string]*hostedActor{testActor.UID: h}
	s.actorsMu.Unlock()
	return h
}

// testActivation is a running actor's activation: its initial reading is
// done, so the sweep samples it. It starts at the unix epoch, so a sample's
// epoch is zero.
func testActivation() *workerstats.Activation {
	a := workerstats.NewActivation(time.Unix(0, 0), false)
	a.Initial(nil, nil)
	return a
}

// sweepAndList runs one sampler sweep, then the discovery read that serves it.
func sweepAndList(t *testing.T, s *service) *ateworkerpb.GetActiveWorkloadStatsResponse {
	t.Helper()
	s.sweepUsage(context.Background())
	got, err := s.GetActiveWorkloadStats(context.Background(), &ateworkerpb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() error = %v, want nil", err)
	}
	return got
}
