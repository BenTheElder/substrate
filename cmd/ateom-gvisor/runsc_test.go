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
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

var testActorDirs = &ateompb.ActorDirs{RootDir: "/node/actors/test-actor-123"}

func TestKillArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}

	got := r.killArgs("my-container", "SIGTERM")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"kill",
		"my-container",
		"SIGTERM",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("killArgs() = %v, want %v", got, want)
	}
}

func TestWaitArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}

	got := r.waitArgs("my-container")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"wait",
		"my-container",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("waitArgs() = %v, want %v", got, want)
	}
}

func TestPauseArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}

	got := r.pauseArgs(ocispec.PauseContainer)
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"pause",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("pauseArgs() = %v, want %v", got, want)
	}
}

func TestResumeArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}

	got := r.resumeArgs(ocispec.PauseContainer)
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"resume",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("resumeArgs() = %v, want %v", got, want)
	}
}

func TestWriteSpec(t *testing.T) {
	dirs := &ateompb.ActorDirs{RootDir: t.TempDir(), OciBundleDir: t.TempDir()}
	r := &runsc{
		actorUID:  "test-actor-123",
		actorDirs: dirs,
		spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{
			Name:                   "app",
			Process:                &ateompb.Process{Args: []string{"/app"}, Capabilities: []string{"CAP_KILL"}},
			DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{{VolumeName: "data", MountPath: "/data"}},
		}}},
	}
	for _, tc := range []struct {
		name     string
		wantArgs []string
		wantCaps []string
		wantType string
	}{
		{"app", []string{"/app"}, []string{"CAP_KILL"}, "container"},
		{ocispec.PauseContainer, []string{"/pause"}, nil, "sandbox"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.MkdirAll(ociBundlePath(dirs, tc.name), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := r.writeSpec(tc.name); err != nil {
				t.Fatalf("writeSpec: %v", err)
			}
			spec, err := ocispec.Load(ociBundlePath(dirs, tc.name))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(spec.Process.Args, tc.wantArgs) {
				t.Errorf("args = %v, want %v", spec.Process.Args, tc.wantArgs)
			}
			if !slices.Equal(spec.Process.Capabilities.Bounding, tc.wantCaps) {
				t.Errorf("bounding caps = %v, want %v", spec.Process.Capabilities.Bounding, tc.wantCaps)
			}
			if got := spec.Annotations["io.kubernetes.cri.container-type"]; got != tc.wantType {
				t.Errorf("container-type = %q, want %q", got, tc.wantType)
			}
			if got, want := spec.Linux.Namespaces[1].Path, nodepath.ActorNetNSPath("test-actor-123"); got != want {
				t.Errorf("netns = %q, want %q", got, want)
			}
		})
	}
}

func TestWriteSpecUnknownContainer(t *testing.T) {
	r := &runsc{actorDirs: &ateompb.ActorDirs{OciBundleDir: t.TempDir()}, spec: &ateompb.WorkloadSpec{}}
	if err := r.writeSpec("missing"); err == nil {
		t.Fatal("writeSpec succeeded for a container not in the workload spec")
	}
}
