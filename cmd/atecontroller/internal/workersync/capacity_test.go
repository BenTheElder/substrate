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

package workersync

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestWorkerCapacity covers the worker-side extraction: capacity comes from the
// ateom container's limits, not the pod total, and other containers are ignored.
func TestWorkerCapacity(t *testing.T) {
	pod := func(ctrs ...corev1.Container) *corev1.Pod {
		return &corev1.Pod{Spec: corev1.PodSpec{Containers: ctrs}}
	}
	limited := func(name, cpu, mem string) corev1.Container {
		lim := corev1.ResourceList{}
		if cpu != "" {
			lim[corev1.ResourceCPU] = resource.MustParse(cpu)
		}
		if mem != "" {
			lim[corev1.ResourceMemory] = resource.MustParse(mem)
		}
		return corev1.Container{Name: name, Resources: corev1.ResourceRequirements{Limits: lim}}
	}

	tests := []struct {
		name       string
		pod        *corev1.Pod
		wantCPU    int64
		wantMemory int64
	}{
		{
			name:       "no ateom container yields zero",
			pod:        pod(limited("sidecar", "1", "1Gi")),
			wantCPU:    0,
			wantMemory: 0,
		},
		{
			name:       "ateom container limits become capacity",
			pod:        pod(limited(ateomContainerName, "4", "8Gi")),
			wantCPU:    4000,
			wantMemory: 8 << 30,
		},
		{
			name:       "only the ateom container counts, not the pod total",
			pod:        pod(limited("sidecar", "16", "64Gi"), limited(ateomContainerName, "2", "2Gi")),
			wantCPU:    2000,
			wantMemory: 2 << 30,
		},
		{
			name:       "unset dimension reports zero",
			pod:        pod(limited(ateomContainerName, "2", "")),
			wantCPU:    2000,
			wantMemory: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := workerCapacity(tc.pod)
			if got.GetCpuMilli() != tc.wantCPU || got.GetMemoryBytes() != tc.wantMemory {
				t.Fatalf("workerCapacity() = (%d, %d), want (%d, %d)",
					got.GetCpuMilli(), got.GetMemoryBytes(), tc.wantCPU, tc.wantMemory)
			}
			// The actors ceiling belongs to the ateom and arrives by report, so
			// the syncer must not invent one here.
			if got.GetActors() != 0 {
				t.Errorf("capacity.actors = %d, want unset: the syncer does not own it", got.GetActors())
			}
		})
	}

	// A pod that limits nothing reports nothing rather than a zeroed message, so
	// placement treats it as unknown instead of full.
	t.Run("no compute limits reports nothing", func(t *testing.T) {
		if got := workerCapacity(pod(limited("sidecar", "1", "1Gi"))); got != nil {
			t.Fatalf("workerCapacity() = %v, want nil", got)
		}
	})
}

// The two halves of capacity have different owners: the pod states cpu and
// memory, the ateom states how many Actors it can host. The syncer recomputes
// its half on every pod event, so it has to carry the other half across --
// otherwise it clears the report each time, the worker re-reports it, and the
// two fight forever.
func TestWithReportedActors(t *testing.T) {
	tests := []struct {
		name       string
		computed   *ateapipb.WorkerCapacity
		stored     *ateapipb.WorkerCapacity
		wantActors int32
		wantCPU    int64
	}{
		{
			name:       "a reported ceiling survives a recompute",
			computed:   &ateapipb.WorkerCapacity{CpuMilli: 4000},
			stored:     &ateapipb.WorkerCapacity{CpuMilli: 4000, Actors: 4094},
			wantActors: 4094,
			wantCPU:    4000,
		},
		{
			name:       "a resized pod keeps the reported ceiling",
			computed:   &ateapipb.WorkerCapacity{CpuMilli: 8000},
			stored:     &ateapipb.WorkerCapacity{CpuMilli: 4000, Actors: 4094},
			wantActors: 4094,
			wantCPU:    8000,
		},
		{
			name:       "nothing reported yet leaves actors unset",
			computed:   &ateapipb.WorkerCapacity{CpuMilli: 4000},
			stored:     &ateapipb.WorkerCapacity{CpuMilli: 4000},
			wantActors: 0,
			wantCPU:    4000,
		},
		{
			name:       "a report survives a pod that limits nothing",
			computed:   nil,
			stored:     &ateapipb.WorkerCapacity{Actors: 96},
			wantActors: 96,
			wantCPU:    0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := withReportedActors(tc.computed, tc.stored)
			if got.GetActors() != tc.wantActors {
				t.Errorf("actors = %d, want %d", got.GetActors(), tc.wantActors)
			}
			if got.GetCpuMilli() != tc.wantCPU {
				t.Errorf("cpu_milli = %d, want %d", got.GetCpuMilli(), tc.wantCPU)
			}
		})
	}
}
