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

package ocispec

import (
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// mountFor returns the spec's mount at destination dest, or fails.
func mountFor(t *testing.T, spec *specs.Spec, dest string) specs.Mount {
	t.Helper()
	for _, m := range spec.Mounts {
		if m.Destination == dest {
			return m
		}
	}
	t.Fatalf("no mount at %q; mounts=%v", dest, spec.Mounts)
	return specs.Mount{}
}

// Each volume mount becomes a bind of its host directory, rw or ro by kind.
func TestBuild_VolumeMounts(t *testing.T) {
	dirs := &ateompb.ActorDirs{
		OciBundleDir:              "/node/actors/a/bundles",
		DurableDirVolumeMountsDir: "/node/actors/a/durable-dir",
		VolumesDir:                "/node/actors/a/volumes",
		SystemInfoVolumeRootsDir:  "/node/actors/a/system-info",
	}
	spec := Build(&ateompb.Container{
		Name:    "app",
		Process: &ateompb.Process{Args: []string{"/app"}},
		DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
			{VolumeName: "data", MountPath: "/var/data"},
			{VolumeName: "data", MountPath: "/home/counter"},
		},
		SystemInfoVolumeMounts: []*ateompb.SystemInfoVolumeMount{{VolumeName: "sysinfo", MountPath: "/run/ate"}},
		CsiVolumeMounts:        []*ateompb.VolumeMount{{VolumeName: "csi", MountPath: "/mnt/csi"}},
		ImageVolumeMounts:      []*ateompb.ImageVolumeMount{{VolumeName: "agent", MountPath: "/ate"}},
	}, dirs, "")

	for _, tc := range []struct {
		dest       string
		wantSource string
		wantOpts   []string
	}{
		{"/var/data", dirs.DurableDirVolumeMountsDir + "/data", []string{"bind", "rw"}},
		{"/home/counter", dirs.DurableDirVolumeMountsDir + "/data", []string{"bind", "rw"}},
		{"/run/ate", dirs.SystemInfoVolumeRootsDir + "/sysinfo", []string{"bind", "ro"}},
		{"/mnt/csi", dirs.VolumesDir + "/csi", []string{"bind", "rw"}},
		{"/ate", imagecache.ImageVolumeMountPath(dirs.OciBundleDir+"/app", "agent"), []string{"bind", "ro"}},
	} {
		m := mountFor(t, spec, tc.dest)
		if m.Type != "bind" {
			t.Errorf("%s type = %q, want bind", tc.dest, m.Type)
		}
		if m.Source != tc.wantSource {
			t.Errorf("%s source = %q, want %q", tc.dest, m.Source, tc.wantSource)
		}
		if !slices.Equal(m.Options, tc.wantOpts) {
			t.Errorf("%s options = %v, want %v", tc.dest, m.Options, tc.wantOpts)
		}
	}
}

// A volume nested in another volume of a different kind mounts after it.
func TestBuild_ParentMountsFirst(t *testing.T) {
	spec := Build(&ateompb.Container{
		Name:                   "app",
		DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{{VolumeName: "data", MountPath: "/data/nested/deeper"}},
		ImageVolumeMounts:      []*ateompb.ImageVolumeMount{{VolumeName: "tools", MountPath: "/data/"}},
		CsiVolumeMounts:        []*ateompb.VolumeMount{{VolumeName: "csi", MountPath: "/data/nested"}},
	}, &ateompb.ActorDirs{}, "")
	var got []string
	for _, m := range spec.Mounts {
		if m.Type == "bind" {
			got = append(got, m.Destination)
		}
	}
	if want := []string{"/data/", "/data/nested", "/data/nested/deeper"}; !slices.Equal(got, want) {
		t.Errorf("bind mount order = %v, want %v", got, want)
	}
}

// The resolved set lands in bounding, effective and permitted only.
func TestBuild_Capabilities(t *testing.T) {
	want := []string{"CAP_CHOWN", "CAP_KILL"}
	spec := Build(&ateompb.Container{Process: &ateompb.Process{Args: []string{"/app"}, Capabilities: want}}, nil, "")

	caps := spec.Process.Capabilities
	if caps == nil {
		t.Fatal("spec.Process.Capabilities is nil")
	}
	for _, set := range []struct {
		name string
		got  []string
	}{
		{"Bounding", caps.Bounding},
		{"Effective", caps.Effective},
		{"Permitted", caps.Permitted},
	} {
		if !slices.Equal(set.got, want) {
			t.Errorf("%s = %v, want %v", set.name, set.got, want)
		}
	}
	for _, set := range []struct {
		name string
		got  []string
	}{
		{"Inheritable", caps.Inheritable},
		{"Ambient", caps.Ambient},
	} {
		if len(set.got) != 0 {
			t.Errorf("%s = %v, want empty", set.name, set.got)
		}
	}
}

// The pause container gets no capabilities.
func TestBuild_NoCapabilitiesForPause(t *testing.T) {
	spec := Build(&ateompb.Container{Process: &ateompb.Process{Args: []string{"/pause"}}}, nil, "")

	caps := spec.Process.Capabilities
	if caps == nil {
		t.Fatal("spec.Process.Capabilities is nil")
	}
	for _, set := range []struct {
		name string
		got  []string
	}{
		{"Bounding", caps.Bounding},
		{"Effective", caps.Effective},
		{"Inheritable", caps.Inheritable},
		{"Permitted", caps.Permitted},
		{"Ambient", caps.Ambient},
	} {
		if len(set.got) != 0 {
			t.Errorf("%s = %v, want empty", set.name, set.got)
		}
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	bundle := t.TempDir()
	want := Build(&ateompb.Container{Process: &ateompb.Process{Args: []string{"/app"}}}, nil, "/run/netns/x")
	if err := Save(bundle, want); err != nil {
		t.Fatalf("Save() = %v", err)
	}
	got, err := Load(bundle)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if got.Hostname != want.Hostname || !slices.Equal(got.Process.Args, want.Process.Args) || len(got.Mounts) != len(want.Mounts) {
		t.Errorf("round trip changed the spec:\n got %+v\nwant %+v", got, want)
	}
}

func TestOCIResources(t *testing.T) {
	if got := ociResources(nil); got != nil {
		t.Errorf("ociResources(nil) = %v, want nil", got)
	}
	if got := ociResources(&ateompb.ResourceLimits{}); got != nil {
		t.Errorf("ociResources(zero) = %v, want nil so the spec is unchanged", got)
	}

	got := ociResources(&ateompb.ResourceLimits{MemoryBytes: 268435456, CpuMillis: 200})
	if got == nil {
		t.Fatal("ociResources() = nil, want limits")
	}
	if got.Memory == nil || got.Memory.Limit == nil || *got.Memory.Limit != 268435456 {
		t.Errorf("Memory.Limit = %v, want 268435456", got.Memory)
	}
	// Assert the ratio rather than the literal quota: retuning the period must
	// keep quota/period equal to the declared milli-cores, and a test pinned to
	// a literal would pass while every container silently got the wrong share.
	if got.CPU == nil || got.CPU.Quota == nil || got.CPU.Period == nil {
		t.Fatalf("CPU = %v, want a quota and period", got.CPU)
	}
	if millis := *got.CPU.Quota * 1000 / int64(*got.CPU.Period); millis != 200 {
		t.Errorf("quota/period = %dm, want the declared 200m (quota=%d period=%d)",
			millis, *got.CPU.Quota, *got.CPU.Period)
	}
	if *got.CPU.Period != cpuQuotaPeriodUS {
		t.Errorf("CPU.Period = %d, want cpuQuotaPeriodUS (%d)", *got.CPU.Period, cpuQuotaPeriodUS)
	}
}

// The kernel rejects a CFS quota below 1ms, so a cpu limit under 10m must be
// raised to the floor rather than producing a spec the guest refuses with
// EINVAL at container create.
func TestOCIResources_ClampsQuotaToKernelMinimum(t *testing.T) {
	for _, millis := range []int64{1, 5, 9} {
		got := ociResources(&ateompb.ResourceLimits{CpuMillis: millis})
		if got == nil || got.CPU == nil || got.CPU.Quota == nil {
			t.Fatalf("cpu=%dm: ociResources() = %v, want a quota", millis, got)
		}
		if *got.CPU.Quota < cpuQuotaMinUS {
			t.Errorf("cpu=%dm: quota = %d, want at least the kernel minimum %d",
				millis, *got.CPU.Quota, cpuQuotaMinUS)
		}
	}
	// At the floor the quota is exact, not clamped.
	got := ociResources(&ateompb.ResourceLimits{CpuMillis: 10})
	if *got.CPU.Quota != cpuQuotaMinUS {
		t.Errorf("cpu=10m: quota = %d, want exactly %d", *got.CPU.Quota, cpuQuotaMinUS)
	}
}

// A negative limit must not produce a non-nil but empty LinuxResources, which
// would put a bare "resources": {} into the spec.
func TestOCIResources_NegativeIsUnset(t *testing.T) {
	if got := ociResources(&ateompb.ResourceLimits{MemoryBytes: -1, CpuMillis: -1}); got != nil {
		t.Errorf("ociResources(negative) = %+v, want nil", got)
	}
}

// A container without limits carries no linux.resources at all.
func TestBuild_NoResourcesLeavesLinuxUntouched(t *testing.T) {
	spec := Build(&ateompb.Container{Process: &ateompb.Process{Args: []string{"/pause"}}}, nil, "")
	if spec.Linux.Resources != nil {
		t.Errorf("Linux.Resources = %v, want nil when no limits are declared", spec.Linux.Resources)
	}
}

func TestBuild_ResourcesApplied(t *testing.T) {
	spec := Build(&ateompb.Container{
		Process:   &ateompb.Process{Args: []string{"/app"}},
		Resources: &ateompb.ResourceLimits{MemoryBytes: 67108864},
	}, nil, "")
	if spec.Linux.Resources == nil || spec.Linux.Resources.Memory == nil {
		t.Fatalf("Linux.Resources = %v, want a memory limit", spec.Linux.Resources)
	}
	if *spec.Linux.Resources.Memory.Limit != 67108864 {
		t.Errorf("Memory.Limit = %d, want 67108864", *spec.Linux.Resources.Memory.Limit)
	}
}
