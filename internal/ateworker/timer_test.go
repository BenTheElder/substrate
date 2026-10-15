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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

func TestTimer(t *testing.T) {
	start := time.Now().Add(-time.Hour)
	timer := NewTimer(start)
	timer.Lap("a")
	timer.Add("b", time.Second)
	timer.Skip()
	timer.Lap("a")
	timer.Add("b", time.Second)

	phases := timer.Phases()
	var names []string
	for _, p := range phases {
		names = append(names, p.Name)
	}
	if want := []string{"a", "b", ateattr.SnapshotPhaseTotal}; len(names) != len(want) || names[0] != want[0] || names[1] != want[1] || names[2] != want[2] {
		t.Fatalf("phases = %v, want %v in order of first appearance", names, want)
	}
	// The first lap covers the hour since start; Skip drops nothing from it.
	if d := phases[0].D; d < time.Hour {
		t.Errorf("a = %v, want at least the hour since start", d)
	}
	if d := phases[1].D; d != 2*time.Second {
		t.Errorf("b = %v, want the sum of its additions", d)
	}
	if total := phases[2].D; total < phases[0].D {
		t.Errorf("total %v is less than phase a %v", total, phases[0].D)
	}
}
