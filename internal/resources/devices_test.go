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

package resources

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestExclusiveDevices(t *testing.T) {
	tests := []struct {
		name string
		list corev1.ResourceList
		want map[string]int64
	}{
		{
			name: "cpu and memory are not devices",
			list: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("8"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
			want: nil,
		},
		{
			name: "a vendor device is counted",
			list: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("8"),
				"nvidia.com/gpu":   resource.MustParse("2"),
			},
			want: map[string]int64{"nvidia.com/gpu": 2},
		},
		{
			// The worker pod holds ate.dev/kvm so the runtime can open /dev/kvm,
			// but every micro-VM on the node opens it. Counting it would cap the
			// worker at one actor.
			name: "substrate's own shareable pseudo-devices are not counted",
			list: corev1.ResourceList{
				"ate.dev/kvm":    resource.MustParse("1"),
				"nvidia.com/gpu": resource.MustParse("1"),
			},
			want: map[string]int64{"nvidia.com/gpu": 1},
		},
		{
			name: "hugepages are memory, not a device",
			list: corev1.ResourceList{"hugepages-2Mi": resource.MustParse("128Mi")},
			want: nil,
		},
		{
			name: "a zero limit asks for nothing",
			list: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("0")},
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExclusiveDevices(tc.list)
			if len(got) != len(tc.want) {
				t.Fatalf("ExclusiveDevices() = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("ExclusiveDevices()[%q] = %d, want %d", k, got[k], v)
				}
			}
		})
	}
}

func TestValidateDeviceSubdivision(t *testing.T) {
	tests := []struct {
		name       string
		actor      map[string]int64
		containers map[string]map[string]int64
		wantErr    string
	}{
		{
			name: "no devices anywhere",
		},
		{
			name:  "the actor holds devices no container names",
			actor: map[string]int64{"nvidia.com/gpu": 2},
		},
		{
			name:       "one container takes all of them",
			actor:      map[string]int64{"nvidia.com/gpu": 2},
			containers: map[string]map[string]int64{"trainer": {"nvidia.com/gpu": 2}},
		},
		{
			name:  "containers split them",
			actor: map[string]int64{"nvidia.com/gpu": 2},
			containers: map[string]map[string]int64{
				"shard-a": {"nvidia.com/gpu": 1},
				"shard-b": {"nvidia.com/gpu": 1},
			},
		},
		{
			name:       "a container may take fewer than the actor holds",
			actor:      map[string]int64{"nvidia.com/gpu": 4},
			containers: map[string]map[string]int64{"trainer": {"nvidia.com/gpu": 1}},
		},
		{
			name:       "a container cannot exceed the actor",
			actor:      map[string]int64{"nvidia.com/gpu": 1},
			containers: map[string]map[string]int64{"trainer": {"nvidia.com/gpu": 2}},
			wantErr:    "containers ask for 2 of device nvidia.com/gpu, more than the actor's 1",
		},
		{
			name:  "containers do not share a device",
			actor: map[string]int64{"nvidia.com/gpu": 1},
			containers: map[string]map[string]int64{
				"trainer": {"nvidia.com/gpu": 1},
				"sidecar": {"nvidia.com/gpu": 1},
			},
			wantErr: "containers ask for 2 of device nvidia.com/gpu, more than the actor's 1",
		},
		{
			name:       "a container cannot name a device the actor never asked for",
			actor:      map[string]int64{"nvidia.com/gpu": 1},
			containers: map[string]map[string]int64{"trainer": {"example.com/fpga": 1}},
			wantErr:    `container "trainer" asks for device example.com/fpga, which the actor does not request`,
		},
		{
			name:       "a container cannot claim a device when the actor asked for none",
			containers: map[string]map[string]int64{"trainer": {"nvidia.com/gpu": 1}},
			wantErr:    `container "trainer" asks for device nvidia.com/gpu, which the actor does not request`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDeviceSubdivision(tc.actor, tc.containers)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("ValidateDeviceSubdivision() = %v, want nil", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("ValidateDeviceSubdivision() = nil, want %q", tc.wantErr)
			case tc.wantErr != "" && err.Error() != tc.wantErr:
				t.Errorf("ValidateDeviceSubdivision() = %q, want %q", err, tc.wantErr)
			}
		})
	}
}
