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

package manifest

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"
)

const templateYAML = `metadata:
  atespace: demo
  name: counter
containers:
- name: counter
  image: example.com/counter@sha256:abc
  resources:
    limits:
      cpuMillis: 250
      memoryBytes: 128Mi
resources:
  limits:
    cpuMillis: 1000
    memoryBytes: 1073741824
volumes:
- name: data
  externalVolumeTemplate:
    capacity_bytes: 10Gi
    storageClassName: fast
`

func TestUnmarshal(t *testing.T) {
	got := &ateapipb.ActorTemplate{}
	if err := Unmarshal([]byte(templateYAML), got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "demo", Name: "counter"},
		Containers: []*ateapipb.Container{{
			Name:      "counter",
			Image:     "example.com/counter@sha256:abc",
			Resources: &ateapipb.Resources{Limits: &ateapipb.ResourceList{CpuMillis: 250, MemoryBytes: 128 << 20}},
		}},
		Resources: &ateapipb.Resources{Limits: &ateapipb.ResourceList{CpuMillis: 1000, MemoryBytes: 1 << 30}},
		Volumes: []*ateapipb.Volume{{
			Name:                   "data",
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{CapacityBytes: 10 << 30, StorageClassName: "fast"},
		}},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("Unmarshal mismatch (-want +got):\n%s", diff)
	}
}

func TestUnmarshalErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"empty", "# just a comment\n", "manifest is empty"},
		{"bad quantity", "resources:\n  limits:\n    memoryBytes: lots\n", "resources.limits.memoryBytes"},
		{"negative", "resources:\n  limits:\n    memoryBytes: -1Gi\n", "must not be negative"},
		{"too large", "resources:\n  limits:\n    memoryBytes: 10E\n", "must be at most"},
		{"cpu has no quantity form", "resources:\n  limits:\n    cpuMillis: 500m\n", "cpuMillis"},
		{"unknown field", "resources:\n  limits:\n    memory: 1Gi\n", `unknown field "memory"`},
		{"nested", "containers:\n- name: c\n  resources:\n    limits:\n      memoryBytes: x\n", "containers[0].resources.limits.memoryBytes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Unmarshal([]byte(tc.yaml), &ateapipb.ActorTemplate{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Unmarshal() = %v, want an error containing %q", err, tc.want)
			}
		})
	}
	if err := Unmarshal(nil, &ateapipb.ActorTemplate{}); !errors.Is(err, ErrEmpty) {
		t.Errorf("Unmarshal(nil) = %v, want ErrEmpty", err)
	}
}

func TestMarshalYAMLRoundTrips(t *testing.T) {
	in := &ateapipb.ActorTemplate{}
	if err := Unmarshal([]byte(templateYAML), in); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	b, err := MarshalYAML(in)
	if err != nil {
		t.Fatalf("MarshalYAML: %v", err)
	}
	for _, want := range []string{`cpuMillis: "250"`, "memoryBytes: 128Mi", "memoryBytes: 1Gi", "capacityBytes: 10Gi"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("MarshalYAML output missing %q:\n%s", want, b)
		}
	}
	out := &ateapipb.ActorTemplate{}
	if err := Unmarshal(b, out); err != nil {
		t.Fatalf("Unmarshal of MarshalYAML output: %v", err)
	}
	if diff := cmp.Diff(in, out, protocmp.Transform()); diff != "" {
		t.Errorf("round trip mismatch (-want +got):\n%s", diff)
	}
}

// typehint fields nested in lists and other messages are found too, so a
// Worker list renders the same way a template does.
func TestMarshalJSONNested(t *testing.T) {
	resp := &ateapipb.ListWorkersResponse{Workers: []*ateapipb.Worker{{
		Status: &ateapipb.WorkerStatus{
			Capacity: &ateapipb.WorkerResources{Actors: 4, Resources: &ateapipb.Resources{Limits: &ateapipb.ResourceList{CpuMillis: 2000, MemoryBytes: 4 << 30}}},
		},
	}}}
	b, err := MarshalJSON(resp)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if !strings.Contains(string(b), `"cpuMillis": "2000"`) || !strings.Contains(string(b), `"memoryBytes": "4Gi"`) {
		t.Errorf("MarshalJSON output is wrong:\n%s", b)
	}
}

func TestFormatQuantity(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{FormatQuantity(512 << 20), "512Mi"},
		{FormatQuantity(10 << 30), "10Gi"},
		{FormatQuantity(1000), "1k"},
	} {
		if tc.got != tc.want {
			t.Errorf("got %q, want %q", tc.got, tc.want)
		}
	}
}
