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
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateworker"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

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

// deadCgroup is a sandbox leaf after the kernel OOM-killed the sentry: the leaf
// and its counters remain, the processes do not.
var deadCgroup = map[string]string{
	"memory.current": "16384\n",
	"memory.events":  "oom 5334\noom_kill 3\n",
	"cgroup.events":  "populated 0\nfrozen 0\n",
}

// drainedCgroup is the sandbox leaf after a drain has killed the app
// containers: the sentry and gofers stay in the pause leaf, so it is still
// populated.
var drainedCgroup = map[string]string{
	"memory.current": "31457280\n",
	"memory.events":  "oom 0\noom_kill 0\n",
	"cgroup.events":  "populated 1\nfrozen 0\n",
}

// newStatsRuntime builds a runtime whose cgroup root is a fixture tree. A nil
// files map leaves the sandbox cgroup absent, which is what a booting or
// torn-down sandbox looks like.
func newStatsRuntime(t *testing.T, files map[string]string) *gvisorRuntime {
	t.Helper()
	root := t.TempDir()
	if files != nil {
		dir := filepath.Join(root, ocispec.GVisorCgroupLeaf(testActor.UID, sandboxCgroupContainer))
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatalf("creating fixture cgroup dir: %v", err)
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
				t.Fatalf("writing fixture %q: %v", name, err)
			}
		}
	}
	return &gvisorRuntime{cgroupRoot: root}
}

func TestSample(t *testing.T) {
	r := newStatsRuntime(t, healthyCgroup)

	before := time.Now().UnixNano()
	got, err := r.Sample(context.Background(), testActor, nil)
	after := time.Now().UnixNano()
	if err != nil {
		t.Fatalf("Sample() error = %v, want nil", err)
	}
	if ts := got.Sample.GetObservedAtUnixNano(); ts < before || ts > after {
		t.Errorf("Sample() observed_at_unix_nano = %d, want within [%d, %d]", ts, before, after)
	}
	want := &ateworkerpb.WorkloadStatsSample{
		Atespace:              "space-a",
		ActorName:             "actor-a",
		ActorUid:              "uid-a",
		ActorTemplateAtespace: "ns-a",
		ActorTemplateName:     "template-a",
		SandboxClass:          "gvisor",
		Source:                ateworkerpb.StatsSource_STATS_SOURCE_CGROUP,
		MemoryCurrentBytes:    157286400,
		MemoryPeakBytes:       209715200,
		MemoryWorkingSetBytes: 136314880,
		CpuUsageUsec:          1234567,
	}
	if diff := cmp.Diff(want, got.Sample, protocmp.Transform(), protocmp.IgnoreFields(want, "observed_at_unix_nano")); diff != "" {
		t.Errorf("Sample() mismatch (-want +got):\n%s", diff)
	}
	if got.Dead {
		t.Error("a live sandbox reads as dead")
	}
}

// A missing cgroup is routine, most often a poll landing in the boot. One that
// does not parse is not, and must not be reported as routine.
func TestSampleErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		files       map[string]string
		wantRoutine bool
	}{
		{name: "no sandbox cgroup", files: nil, wantRoutine: true},
		{name: "malformed cgroup", files: map[string]string{"memory.current": "max\n"}, wantRoutine: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newStatsRuntime(t, tc.files).Sample(context.Background(), testActor, nil)
			if err == nil {
				t.Fatal("Sample() error = nil, want an error")
			}
			if got := errors.Is(err, ateworker.ErrNoSample); got != tc.wantRoutine {
				t.Errorf("Sample() error %v routine = %v, want %v", err, got, tc.wantRoutine)
			}
		})
	}
}

// An empty sandbox cgroup is a dead sandbox, reported with its OOM kills. A
// drain kills the app containers but leaves the sentry, so it is not.
func TestSampleDeadSandbox(t *testing.T) {
	for _, tc := range []struct {
		name     string
		files    map[string]string
		wantDead bool
	}{
		{name: "OOM-killed sentry", files: deadCgroup, wantDead: true},
		{name: "drained app containers", files: drainedCgroup},
		{name: "live", files: healthyCgroup},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newStatsRuntime(t, tc.files).Sample(context.Background(), testActor, nil)
			if err != nil {
				t.Fatalf("Sample() error = %v, want nil", err)
			}
			if got.Dead != tc.wantDead {
				t.Errorf("Dead = %v, want %v", got.Dead, tc.wantDead)
			}
			if tc.wantDead {
				want := []slog.Attr{slog.Uint64(string(ateattr.SandboxOOMKillsKey), 3)}
				if len(got.DeadAttrs) != 1 || !got.DeadAttrs[0].Equal(want[0]) {
					t.Errorf("DeadAttrs = %v, want %v", got.DeadAttrs, want)
				}
			}
		})
	}
}

// TestDrainNeverKillsPauseContainer: the drain kills the names containerNames
// takes from the spec. atelet requires container names to be DNS-1123 labels
// and the pause container's name is not one, so no spec can name it and the
// drain cannot stop the sandbox itself.
func TestDrainNeverKillsPauseContainer(t *testing.T) {
	if resources.IsValidResourceName(ocispec.PauseContainer) {
		t.Fatalf("%q is a valid container name; a spec could then put it on the drain's kill list", ocispec.PauseContainer)
	}
	spec := []*ateworkerpb.Container{{Name: "app"}, {Name: "sidecar"}}
	for _, name := range containerNames(spec) {
		if name == ocispec.PauseContainer {
			t.Fatalf("containerNames(%v) includes %q", spec, ocispec.PauseContainer)
		}
	}
}
