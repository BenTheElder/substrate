//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package controlapi

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func limits(kv ...string) *ateapipb.Resources {
	out := &ateapipb.Resources{}
	for i := 0; i < len(kv); i += 2 {
		out.Limits = append(out.Limits, &ateapipb.Limits{Name: kv[i], Quantity: kv[i+1]})
	}
	return out
}

func TestValidateTemplateDevices(t *testing.T) {
	tests := []struct {
		name    string
		tmpl    *ateapipb.ActorTemplate
		wantErr string
	}{{
		name: "cpu and memory only",
		tmpl: &ateapipb.ActorTemplate{
			Resources:  limits("cpu", "2", "memory", "1Gi"),
			Containers: []*ateapipb.Container{{Name: "main", Resources: limits("memory", "256Mi")}},
		},
	}, {
		name: "containers divide the actor's devices",
		tmpl: &ateapipb.ActorTemplate{
			Resources: limits("nvidia.com/gpu", "2"),
			Containers: []*ateapipb.Container{
				{Name: "shard-a", Resources: limits("nvidia.com/gpu", "1")},
				{Name: "shard-b", Resources: limits("nvidia.com/gpu", "1")},
			},
		},
	}, {
		name: "a sidecar gets none",
		tmpl: &ateapipb.ActorTemplate{
			Resources: limits("nvidia.com/gpu", "1"),
			Containers: []*ateapipb.Container{
				{Name: "trainer", Resources: limits("nvidia.com/gpu", "1")},
				{Name: "logger", Resources: limits("memory", "64Mi")},
			},
		},
	}, {
		name: "containers overcommit the actor",
		tmpl: &ateapipb.ActorTemplate{
			Resources: limits("nvidia.com/gpu", "1"),
			Containers: []*ateapipb.Container{
				{Name: "trainer", Resources: limits("nvidia.com/gpu", "1")},
				{Name: "sidecar", Resources: limits("nvidia.com/gpu", "1")},
			},
		},
		wantErr: "more than the actor's 1",
	}, {
		name: "a container names a device the actor never asked for",
		tmpl: &ateapipb.ActorTemplate{
			Containers: []*ateapipb.Container{{Name: "trainer", Resources: limits("nvidia.com/gpu", "1")}},
		},
		wantErr: "which the actor does not request",
	}, {
		// The CRD's CEL budget has no room to check this, so it has to hold here.
		name: "a fractional device is not a smaller request",
		tmpl: &ateapipb.ActorTemplate{
			Resources: limits("nvidia.com/gpu", "500m"),
		},
		wantErr: "must be a whole number greater than zero",
	}, {
		name: "a zero device limit is rejected rather than ignored",
		tmpl: &ateapipb.ActorTemplate{
			Resources: limits("nvidia.com/gpu", "0"),
		},
		wantErr: "must be a whole number greater than zero",
	}, {
		// The worker pod holds ate.dev/kvm for every actor on it, so it is not a
		// device an actor can claim.
		name: "shareable grants are not devices",
		tmpl: &ateapipb.ActorTemplate{
			Resources:  limits("ate.dev/kvm", "1"),
			Containers: []*ateapipb.Container{{Name: "main", Resources: limits("ate.dev/kvm", "1")}},
		},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTemplateDevices(tc.tmpl)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("validateTemplateDevices() = %v, want nil", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("validateTemplateDevices() = nil, want error containing %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("validateTemplateDevices() = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
