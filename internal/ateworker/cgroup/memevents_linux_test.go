//go:build linux

// Copyright 2026 The Agent Substrate Authors
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

package cgroup

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/roottest"
)

type fakeTarget struct {
	guard      MemoryGuard
	root, name string
}

func (t *fakeTarget) MemoryGuard() *MemoryGuard         { return &t.guard }
func (t *fakeTarget) MemoryCgroup() (root, name string) { return t.root, t.name }
func (t *fakeTarget) MemoryKilled(Kill)                 {}

func waitHot(t *testing.T, w *memoryEvents, g *MemoryGuard, want bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if w.hot(g) == want {
			return
		}
	}
	t.Fatalf("hot() never became %t", want)
}

// A registered cgroup is woken once per event, and again after it cools down.
// cgroup.events stands in for memory.events.local: the kernel notifies both
// the same way, and it needs no memory controller.
func TestMemoryEventsWakeAndRearm(t *testing.T) {
	roottest.Require(t, "creates cgroups")
	root := ownCgroup(t)
	leaf, err := openActorLeaf(root, "memevents-test", 0, 0)
	if err != nil {
		t.Skipf("cannot create a cgroup here: %v", err)
	}
	t.Cleanup(func() { _ = removeActorLeaf(root, "memevents-test") })

	w, err := newMemoryEvents("cgroup.events")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go w.wait(ctx)
	t.Cleanup(func() { cancel(); w.close() })

	target := &fakeTarget{root: root, name: "memevents-test"}
	g := target.MemoryGuard()
	w.sync(ctx, []MemoryTarget{target})
	// a new registration is checked once, then cools with no events
	if !w.hot(g) {
		t.Fatal("a new registration isn't hot")
	}
	w.clear(g)
	w.cool(g)
	if w.hot(g) {
		t.Fatal("a cgroup with no events is hot")
	}

	// populated 0 -> 1 notifies
	cmd := exec.Command("sleep", "60")
	cmd.SysProcAttr = leaf.SysProcAttr()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	leaf.Close()
	waitHot(t, w, g, true)
	if !w.clear(g) {
		t.Error("clear() after an event didn't report the wake")
	}
	w.cool(g)
	if w.hot(g) {
		t.Fatal("hot() after cool")
	}

	// and populated 1 -> 0 notifies again
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	waitHot(t, w, g, true)

	w.sync(ctx, nil)
	if len(w.byGuard) != 0 || len(w.byToken) != 0 {
		t.Errorf("a target that's gone is still registered: %d, %d", len(w.byGuard), len(w.byToken))
	}
}

// A target that can't be registered is checked on every tick.
func TestMemoryEventsUnregisteredIsHot(t *testing.T) {
	w, err := newMemoryEvents("memory.events.local")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go w.wait(ctx)
	t.Cleanup(func() { cancel(); w.close() })

	target := &fakeTarget{root: t.TempDir(), name: "missing"}
	w.sync(ctx, []MemoryTarget{target})
	if !w.hot(target.MemoryGuard()) {
		t.Error("an unregistered target isn't hot")
	}
}
