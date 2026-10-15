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
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/workerstats"
)

func TestSweepStampsEpochAndDiscoveryServesIt(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	epoch := time.Unix(1700, 0)
	h := hostWithEpoch(s, epoch, true)

	before, err := s.GetActiveWorkloadStats(context.Background(), &ateworkerpb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := before.GetSamples()[0]; got.GetSource() != ateworkerpb.StatsSource_STATS_SOURCE_UNSPECIFIED || got.GetEpochUnixNano() != epoch.UnixNano() {
		t.Errorf("before a sweep: source %v epoch %d, want pending with epoch %d", got.GetSource(), got.GetEpochUnixNano(), epoch.UnixNano())
	}

	s.sweepUsage(context.Background())
	after, err := s.GetActiveWorkloadStats(context.Background(), &ateworkerpb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got := after.GetSamples()[0]
	if got != h.usage.Latest() {
		t.Error("discovery read did not serve the swept sample")
	}
	if got.GetEpochUnixNano() != epoch.UnixNano() || got.GetCpuUsageUsec() != testCPUUsec {
		t.Errorf("served epoch %d cpu %d, want epoch %d cpu %d", got.GetEpochUnixNano(), got.GetCpuUsageUsec(), epoch.UnixNano(), testCPUUsec)
	}
}

func TestGetWorkloadStatsStampsEpoch(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	epoch := time.Unix(1700, 0)
	hostWithEpoch(s, epoch, true)
	got, err := s.GetWorkloadStats(context.Background(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: testActor.UID})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSample().GetEpochUnixNano() != epoch.UnixNano() {
		t.Errorf("epoch = %d, want %d", got.GetSample().GetEpochUnixNano(), epoch.UnixNano())
	}
}

func TestRecordInitialAndFinal(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)

	s.recordInitial(context.Background(), h)
	if h.usage.Latest() == nil {
		t.Error("initial sample was not stored for the discovery read")
	}
	s.recordFinal(context.Background(), h)

	want := []string{ateattr.StatsKindInitial, ateattr.StatsKindFinal}
	if got := rec.Kinds(); !slices.Equal(got, want) {
		t.Errorf("record kinds = %v, want %v", got, want)
	}
	for _, src := range rec.Sources() {
		if src != ateattr.StatsSourceCgroup {
			t.Errorf("record source = %q, want measured", src)
		}
	}
}

// A failed initial reading still opens the activation's sampling window, with
// no initial record.
func TestRecordInitialWithoutAReading(t *testing.T) {
	s := newTestService(failing(errNotReady))
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)

	s.recordInitial(context.Background(), h)
	if !h.usage.Sampling() {
		t.Error("the sweep skips an activation whose initial reading failed")
	}
	if got := rec.Kinds(); len(got) != 0 {
		t.Errorf("records = %v, want none", got)
	}
}

// The final record waits for the actor to be unhosted, as a teardown does even
// when a later step fails.
func TestRecordFinalIfEnded(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)

	s.recordFinalIfEnded(context.Background(), h)
	if got := rec.Kinds(); len(got) != 0 {
		t.Fatalf("records while hosted = %v, want none", got)
	}
	setHostedActor(s, nil)
	s.recordFinalIfEnded(context.Background(), h)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Errorf("records after unhosting = %v, want one final", got)
	}
}

// A final reading that finds the sandbox gone falls back to the latest sample,
// and to a pending record when there is none.
func TestRecordFinalAfterSandboxGone(t *testing.T) {
	var gone atomic.Bool
	s := newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		if gone.Load() {
			return Reading{}, errNotReady
		}
		return healthyReading(a), nil
	}})
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)
	s.sweepUsage(context.Background())
	gone.Store(true)
	s.readFinal(context.Background(), h)
	s.recordFinal(context.Background(), h)

	fresh := hostWithEpoch(s, time.Now(), true)
	s.readFinal(context.Background(), fresh)
	s.recordFinal(context.Background(), fresh)

	var finals []string
	for i, kind := range rec.Kinds() {
		if kind == ateattr.StatsKindFinal {
			finals = append(finals, rec.Sources()[i])
		}
	}
	if want := []string{ateattr.StatsSourceCgroup, ateattr.StatsSourceUnspecified}; !slices.Equal(finals, want) {
		t.Errorf("final record sources = %v, want %v", finals, want)
	}
}

// The reading taken before a snapshot writes no record on its own, and the
// final record, written once the checkpoint succeeds, carries that reading.
func TestReadFinalWritesNothingUntilRecorded(t *testing.T) {
	var gone atomic.Bool
	s := newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		if gone.Load() {
			return Reading{}, errNotReady
		}
		return healthyReading(a), nil
	}})
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)

	s.readFinal(context.Background(), h)
	if got := rec.Kinds(); len(got) != 0 {
		t.Fatalf("records after the pre-snapshot read = %v, want none", got)
	}
	read := h.usage.Latest()
	if read == nil || read.GetSource() != ateworkerpb.StatsSource_STATS_SOURCE_CGROUP {
		t.Fatalf("pre-snapshot read stored %v, want a measured sample", read)
	}
	gone.Store(true)

	s.recordFinal(context.Background(), h)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Fatalf("records after the checkpoint = %v, want one final", got)
	}
	if got := rec.Sources(); got[0] != ateattr.StatsSourceCgroup {
		t.Errorf("final record source = %q, want the pre-snapshot reading", got[0])
	}
}

// FinalReadTimeout bounds the final reading, which sits on the checkpoint
// path. A reading that misses it leaves the newest sample to stand in.
func TestReadFinalTimeout(t *testing.T) {
	s := newTestService(&fakeRuntime{sample: func(ctx context.Context, _ resources.ActorAttribution, _ Sandbox) (Reading, error) {
		<-ctx.Done()
		return Reading{}, ctx.Err()
	}})
	s.cfg.FinalReadTimeout = time.Millisecond
	h := hostWithEpoch(s, time.Now(), true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.readFinal(context.Background(), h)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("readFinal did not give up at FinalReadTimeout")
	}
}

// The sweep reads no actor before its initial reading or after its final
// record.
func TestSweepWaitsForInitialAndStopsAtFinal(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)

	s.sweepUsage(context.Background())
	s.recordInitial(context.Background(), h)
	s.sweepUsage(context.Background())
	s.recordFinal(context.Background(), h)
	s.sweepUsage(context.Background())
	want := []string{ateattr.StatsKindInitial, ateattr.StatsKindPeriodic, ateattr.StatsKindFinal}
	if got := rec.Kinds(); !slices.Equal(got, want) {
		t.Errorf("records = %v, want %v", got, want)
	}
}

// The keyed read, which advances the activation's CPU, also updates the sample
// the cache serves.
func TestGetWorkloadStatsStoresItsSample(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	h := hostWithEpoch(s, time.Now(), true)
	got, err := s.GetWorkloadStats(context.Background(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: testActor.UID})
	if err != nil {
		t.Fatal(err)
	}
	if h.usage.Latest() != got.GetSample() {
		t.Error("the keyed read's sample is not the one the cache serves")
	}
}

// Replacing an activation stops its sandbox, read first for the final record.
func TestEndPreviousActivation(t *testing.T) {
	rt := &fakeRuntime{}
	s := newTestService(rt)
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), true)
	s.setSandbox(h, "sandbox")

	if err := s.endPreviousActivation(context.Background(), &Actor{Attribution: testActor}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rt.stopped, []string{testActor.UID}) {
		t.Errorf("stopped %v, want %v", rt.stopped, []string{testActor.UID})
	}
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Fatalf("records = %v, want one final", got)
	}
	if got := rec.Sources(); got[0] != ateattr.StatsSourceCgroup {
		t.Errorf("final record source = %q, want measured", got[0])
	}
}

// counting is a runtime that counts its readings, each answered by read.
func counting(read func(resources.ActorAttribution, Sandbox) (Reading, error)) (*fakeRuntime, *atomic.Int32) {
	var calls atomic.Int32
	return &fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, sb Sandbox) (Reading, error) {
		calls.Add(1)
		return read(a, sb)
	}}, &calls
}

// The discovery read serves the cache and never reads a sandbox itself.
func TestDiscoveryDoesNotSample(t *testing.T) {
	rt, calls := counting(func(a resources.ActorAttribution, _ Sandbox) (Reading, error) { return healthyReading(a), nil })
	s := newTestService(rt)
	hostWithEpoch(s, time.Now(), true)
	if _, err := s.GetActiveWorkloadStats(context.Background(), &ateworkerpb.GetActiveWorkloadStatsRequest{}); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("discovery read sampled %d times, want 0", n)
	}
}

// The final record reuses the newest sample and reads nothing itself.
func TestRecordFinalDoesNotSample(t *testing.T) {
	rt, calls := counting(func(a resources.ActorAttribution, _ Sandbox) (Reading, error) { return healthyReading(a), nil })
	s := newTestService(rt)
	withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)
	s.recordInitial(context.Background(), h)
	before := calls.Load()
	s.recordFinal(context.Background(), h)
	if calls.Load() != before {
		t.Error("final record read the sandbox; it must reuse the latest sample")
	}
}

func TestRecordFinalWithNoSampleIsPending(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)
	s.recordFinal(context.Background(), h)
	if got := rec.Sources(); len(got) != 1 || got[0] != ateattr.StatsSourceUnspecified {
		t.Errorf("final record sources = %v, want one pending", got)
	}
}

// An initial reading still in flight when a checkpoint or terminate writes the
// final record writes nothing.
func TestRecordInitialAfterFinal(t *testing.T) {
	var s *service
	var h *hostedActor
	s = newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		s.recordFinal(context.Background(), h)
		return healthyReading(a), nil
	}})
	rec := withUsageRecorder(s)
	h = hostWithEpoch(s, time.Now(), false)

	s.recordInitial(context.Background(), h)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Errorf("records = %v, want only the final one", got)
	}
}

// A panic in the initial reading is recovered and still opens the sampling
// window.
func TestRecordInitialRecoversFromPanic(t *testing.T) {
	s := newTestService(&fakeRuntime{sample: func(context.Context, resources.ActorAttribution, Sandbox) (Reading, error) {
		panic("boom")
	}})
	rec := withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)

	s.recordInitial(context.Background(), h)
	if got := rec.Kinds(); len(got) != 0 {
		t.Errorf("records = %v, want none", got)
	}
	if !h.usage.Sampling() {
		t.Error("activation is not sampling after the panic")
	}
}

// An initial reading waits for a slot, so it adds no readings beyond
// statsFanOut.
func TestRecordInitialTakesASlot(t *testing.T) {
	rt, calls := counting(func(a resources.ActorAttribution, _ Sandbox) (Reading, error) { return healthyReading(a), nil })
	s := newTestService(rt)
	withUsageRecorder(s)
	h := hostWithEpoch(s, time.Now(), false)
	for range statsFanOut {
		s.sampleSlots <- struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	s.recordInitial(ctx, h)
	if n := calls.Load(); n != 0 {
		t.Errorf("initial reading sampled %d times with every slot taken, want 0", n)
	}
}

// Each activation reads its own sandbox: a re-host during the old activation's
// initial reading leaves the new activation untouched.
func TestRecordInitialReadsItsOwnSandbox(t *testing.T) {
	var s *service
	var fresh *hostedActor
	s = newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, sb Sandbox) (Reading, error) {
		if fresh == nil {
			fresh = &hostedActor{attribution: testActor, usage: workerstats.NewActivation(time.Now(), false), sandbox: "new"}
			s.actorsMu.Lock()
			s.actors[testActor.UID] = fresh
			s.actorsMu.Unlock()
		}
		r := healthyReading(a)
		if sb != "old" {
			r.Sample.CpuUsageUsec = 1
		}
		return r, nil
	}})
	withUsageRecorder(s)
	old := hostWithEpoch(s, time.Now(), false)
	s.setSandbox(old, "old")

	s.recordInitial(context.Background(), old)
	if got := old.usage.Latest(); got == nil || got.GetCpuUsageUsec() != testCPUUsec || got.GetEpochUnixNano() != old.usage.Epoch() {
		t.Errorf("old activation holds %v, want its own initial reading", got)
	}
	if fresh.usage.Sampling() || fresh.usage.Latest() != nil {
		t.Error("the old activation's initial reading touched the new activation")
	}
}

// An initial reading that starts only after its activation's final record and
// a re-host writes nothing, and the new activation still waits for its own.
func TestLateInitialLeavesNewActivationAlone(t *testing.T) {
	s := newTestService(&fakeRuntime{})
	rec := withUsageRecorder(s)
	old := hostWithEpoch(s, time.Now(), false)
	s.recordFinal(context.Background(), old)
	fresh := hostWithEpoch(s, time.Now(), false)

	s.recordInitial(context.Background(), old)
	if got := rec.Kinds(); len(got) != 1 || got[0] != ateattr.StatsKindFinal {
		t.Errorf("records = %v, want only the old activation's final", got)
	}
	if fresh.usage.Sampling() {
		t.Error("the late reading opened the new activation's sampling window")
	}
}

// A sandbox whose CPU counters carry over from a snapshot counts from its first
// reading.
func TestKeptCountersAreActivationRelative(t *testing.T) {
	var cpu atomic.Uint64
	cpu.Store(9_000_000)
	s := newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		r := healthyReading(a)
		r.Sample.CpuUsageUsec = cpu.Load()
		return r, nil
	}})
	h := hostWithEpoch(s, time.Now(), false)
	h.usage = workerstats.NewActivation(time.Now(), true)
	h.usage.Initial(nil, nil)

	s.sweepUsage(context.Background())
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 0 {
		t.Errorf("first reading = %d, want 0", got)
	}
	cpu.Store(9_000_400)
	s.sweepUsage(context.Background())
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 400 {
		t.Errorf("second reading = %d, want 400", got)
	}
}

// A container missing from one reading neither drops out of the total nor is
// charged again when it is read once more.
func TestPartialReadingDoesNotDoubleCharge(t *testing.T) {
	var mu sync.Mutex
	parts := map[string]uint64{"a": 600_000, "b": 400_000}
	s := newTestService(&fakeRuntime{sample: func(_ context.Context, a resources.ActorAttribution, _ Sandbox) (Reading, error) {
		mu.Lock()
		defer mu.Unlock()
		r := healthyReading(a)
		r.CPUByContainer = maps.Clone(parts)
		return r, nil
	}})
	h := hostWithEpoch(s, time.Now(), false)
	h.usage = workerstats.NewActivation(time.Now(), true)
	h.usage.Initial(nil, nil)

	s.sweepUsage(context.Background())
	mu.Lock()
	parts = map[string]uint64{"a": 605_000}
	mu.Unlock()
	s.sweepUsage(context.Background())
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 5000 {
		t.Fatalf("cpu with b unreadable = %d, want 5000", got)
	}

	mu.Lock()
	parts = map[string]uint64{"a": 606_000, "b": 404_000}
	mu.Unlock()
	s.sweepUsage(context.Background())
	if got := h.usage.Latest().GetCpuUsageUsec(); got != 10000 {
		t.Errorf("cpu once b reads again = %d, want 10000 (b charged once)", got)
	}
}
