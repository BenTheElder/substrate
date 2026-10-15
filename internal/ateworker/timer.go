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
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateworker/phaselog"
)

// Timer splits an operation into named phases for its timing record. Phases
// are logged in the order they first appear.
type Timer struct {
	mu     sync.Mutex
	start  time.Time
	last   time.Time
	phases []phaselog.Phase
}

// NewTimer starts timing at start.
func NewTimer(start time.Time) *Timer {
	return &Timer{start: start, last: start}
}

// Lap adds the time since the previous lap to phase name.
func (t *Timer) Lap(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.add(name, now.Sub(t.last))
	t.last = now
}

// Add adds d to phase name without moving the lap, for phases timed on their
// own, like ones that run concurrently.
func (t *Timer) Add(name string, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.add(name, d)
}

// Skip drops the time since the previous lap.
func (t *Timer) Skip() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.last = time.Now()
}

func (t *Timer) add(name string, d time.Duration) {
	for i := range t.phases {
		if t.phases[i].Name == name {
			t.phases[i].D += d
			return
		}
	}
	t.phases = append(t.phases, phaselog.Phase{Name: name, D: d})
}

// Phases returns every phase so far, then the total.
func (t *Timer) Phases() []phaselog.Phase {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := append([]phaselog.Phase(nil), t.phases...)
	return append(out, phaselog.Phase{Name: ateattr.SnapshotPhaseTotal, D: time.Since(t.start)})
}
