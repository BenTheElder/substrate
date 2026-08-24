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
		maxActors  int32
		wantCPU    int64
		wantMemory int64
		wantActors int32
	}{
		{
			name:       "no ateom container yields zero",
			pod:        pod(limited("sidecar", "1", "1Gi")),
			maxActors:  1,
			wantCPU:    0,
			wantMemory: 0,
			wantActors: 1,
		},
		{
			name:       "ateom container limits become capacity",
			pod:        pod(limited(ateomContainerName, "4", "8Gi")),
			maxActors:  1,
			wantCPU:    4000,
			wantMemory: 8 << 30,
			wantActors: 1,
		},
		{
			name:       "only the ateom container counts, not the pod total",
			pod:        pod(limited("sidecar", "16", "64Gi"), limited(ateomContainerName, "2", "2Gi")),
			maxActors:  1,
			wantCPU:    2000,
			wantMemory: 2 << 30,
			wantActors: 1,
		},
		{
			name:       "unset dimension reports zero",
			pod:        pod(limited(ateomContainerName, "2", "")),
			maxActors:  1,
			wantCPU:    2000,
			wantMemory: 0,
			wantActors: 1,
		},
		{
			// The actors dimension comes from the pool, not the pod, so it is the
			// one that moves without the pod being resized.
			name:       "the pool's actor ceiling is a capacity dimension",
			pod:        pod(limited(ateomContainerName, "4", "8Gi")),
			maxActors:  4094,
			wantCPU:    4000,
			wantMemory: 8 << 30,
			wantActors: 4094,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := workerCapacity(tc.pod, tc.maxActors)
			if got.GetCpuMilli() != tc.wantCPU || got.GetMemoryBytes() != tc.wantMemory || got.GetActors() != tc.wantActors {
				t.Fatalf("workerCapacity() = (%d, %d, %d), want (%d, %d, %d)",
					got.GetCpuMilli(), got.GetMemoryBytes(), got.GetActors(),
					tc.wantCPU, tc.wantMemory, tc.wantActors)
			}
		})
	}

	// No dimension set at all is the one case that reports nothing rather than a
	// zeroed message, so placement treats it as unknown instead of full.
	t.Run("no dimension at all reports nothing", func(t *testing.T) {
		if got := workerCapacity(pod(limited("sidecar", "1", "1Gi")), 0); got != nil {
			t.Fatalf("workerCapacity() = %v, want nil", got)
		}
	})
}
