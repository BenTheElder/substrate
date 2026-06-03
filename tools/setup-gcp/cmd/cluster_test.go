// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import "testing"

func TestGKEVersionLess(t *testing.T) {
	cases := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{"older minor", "1.35.0-gke.2398000", "1.36.0-gke.2459000", true},
		{"equal", "1.36.0-gke.2459000", "1.36.0-gke.2459000", false},
		{"newer gke build is not less", "1.36.0-gke.2460000", "1.36.0-gke.2459000", false},
		{"older gke build", "1.36.0-gke.2459000", "1.36.0-gke.2460000", true},
		{"newer patch beats large build", "1.36.1-gke.100", "1.36.0-gke.9999999", false},
		{"older patch", "1.36.0-gke.9999999", "1.36.1-gke.100", true},
		{"newer major", "2.0.0-gke.1", "1.36.0-gke.2459000", false},
		{"empty current treated as oldest", "", "1.36.0-gke.2459000", true},
		{"v prefix tolerated", "v1.36.0-gke.2459000", "1.36.0-gke.2459000", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := gkeVersionLess(tc.a, tc.b); got != tc.want {
				t.Errorf("gkeVersionLess(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestDesiredNodePoolVersion(t *testing.T) {
	if got := desiredNodePoolVersion(&Environment{NodePoolVersion: "1.36.0-gke.2459000"}); got != "1.36.0-gke.2459000" {
		t.Errorf("expected NODE_POOL_VERSION to win, got %q", got)
	}
	if got := desiredNodePoolVersion(&Environment{ClusterVersion: "1.36.0-gke.2459000"}); got != "1.36.0-gke.2459000" {
		t.Errorf("expected fallback to CLUSTER_VERSION, got %q", got)
	}
	if got := desiredNodePoolVersion(&Environment{}); got != "" {
		t.Errorf("expected empty when neither set, got %q", got)
	}
}
