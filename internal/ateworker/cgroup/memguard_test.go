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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// overLimitLeaf is a leaf past the overshoot cap of limit, which a guard kills
// without waiting for stalls.
func overLimitLeaf(t *testing.T, limit int) (root, name string) {
	t.Helper()
	root, name = t.TempDir(), "uid-a"
	dir := filepath.Join(root, name)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for f, v := range map[string]string{
		"memory.current": strconv.Itoa(limit+32<<20) + "\n",
		"cgroup.kill":    "",
		"cgroup.events":  "populated 0\nfrozen 0\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(v), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root, name
}

func killWritten(t *testing.T, root, name string) bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, name, "cgroup.kill"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b) == "1"
}

func TestMemoryGuardKillsOnce(t *testing.T) {
	const limit = 256 << 20
	root, name := overLimitLeaf(t, limit)
	var g MemoryGuard
	g.Resume(limit)

	kill, ok := g.Check(context.Background(), root, name)
	if !ok {
		t.Fatal("Check() did not kill a cgroup over its limit")
	}
	if err := <-kill.Done; err != nil {
		t.Fatalf("kill failed: %v", err)
	}
	if kill.Limit != limit || kill.Current != limit+32<<20 {
		t.Errorf("Kill = %d of %d, want %d of %d", kill.Current, kill.Limit, limit+32<<20, limit)
	}
	if !g.Killed() || !killWritten(t, root, name) {
		t.Fatal("cgroup was not killed")
	}

	if err := os.WriteFile(filepath.Join(root, name, "cgroup.kill"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.Check(context.Background(), root, name); ok || killWritten(t, root, name) {
		t.Error("Check() killed the cgroup twice")
	}
}

func TestMemoryGuardPaused(t *testing.T) {
	const limit = 256 << 20
	root, name := overLimitLeaf(t, limit)
	var g MemoryGuard
	if _, ok := g.Check(context.Background(), root, name); ok {
		t.Error("Check() killed before the guard was armed")
	}
	g.Resume(limit)
	if got := g.Pause(); got != limit {
		t.Errorf("Pause() = %d, want %d", got, limit)
	}
	if _, ok := g.Check(context.Background(), root, name); ok || killWritten(t, root, name) {
		t.Error("Check() killed while paused")
	}
	g.Resume(0)
	if _, ok := g.Check(context.Background(), root, name); ok {
		t.Error("Check() killed after Resume(0)")
	}
}

// A pause waits out a kill in progress, so a checkpoint never moves a sandbox
// out from under its kill.
func TestMemoryGuardPauseWaitsForKill(t *testing.T) {
	const limit = 256 << 20
	root, name := overLimitLeaf(t, limit)
	events := filepath.Join(root, name, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var g MemoryGuard
	g.Resume(limit)
	if _, ok := g.Check(context.Background(), root, name); !ok {
		t.Fatal("Check() did not kill a cgroup over its limit")
	}
	paused := make(chan struct{})
	go func() {
		g.Pause()
		close(paused)
	}()
	select {
	case <-paused:
		t.Fatal("Pause() returned while the kill was still waiting for the cgroup to empty")
	case <-time.After(100 * time.Millisecond):
	}
	if err := os.WriteFile(events, []byte("populated 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	<-paused
}

// Page cache doesn't make a cgroup near its limit.
func TestMemoryGuardNearIgnoresPageCache(t *testing.T) {
	const limit = 256 << 20
	root, name := t.TempDir(), "uid-a"
	dir := filepath.Join(root, name)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		cache uint64
		want  bool
	}{
		{cache: 64 << 20, want: false},
		{cache: 0, want: true},
	} {
		for f, v := range map[string]string{
			"memory.current":      strconv.Itoa(limit) + "\n",
			"memory.stat":         fmt.Sprintf("anon 1\ninactive_file %d\nactive_file 0\n", tc.cache),
			"memory.pressure":     "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
			"memory.events.local": "high 0\n",
		} {
			if err := os.WriteFile(filepath.Join(dir, f), []byte(v), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		var g MemoryGuard
		g.Resume(limit)
		g.Check(context.Background(), root, name)
		if got := g.Near(); got != tc.want {
			t.Errorf("with %d of page cache: Near() = %t, want %t", tc.cache, got, tc.want)
		}
	}
}
