//go:build linux

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

package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/resources"
)

func writeCgroupFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func readCgroupFile(t *testing.T, dir, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func TestSwapMemoryMaxForHigh(t *testing.T) {
	dir := t.TempDir()
	writeCgroupFiles(t, dir, map[string]string{"memory.max": "268435456\n", "memory.high": "max\n"})
	limit, err := swapMemoryMaxForHigh(dir)
	if err != nil || limit != 268435456 {
		t.Fatalf("swapMemoryMaxForHigh() = %d, %v, want 268435456", limit, err)
	}
	if got := readCgroupFile(t, dir, "memory.high"); got != "268435456" {
		t.Errorf("memory.high = %q, want 268435456", got)
	}
	if got := readCgroupFile(t, dir, "memory.max"); got != "max" {
		t.Errorf("memory.max = %q, want max", got)
	}
}

func TestSwapMemoryMaxForHighNoLimit(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"unlimited":            {"memory.max": "max\n", "memory.high": "max\n"},
		"no memory controller": {},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeCgroupFiles(t, dir, files)
			if limit, err := swapMemoryMaxForHigh(dir); err != nil || limit != 0 {
				t.Errorf("swapMemoryMaxForHigh() = %d, %v, want 0", limit, err)
			}
			if _, err := os.Stat(filepath.Join(dir, "memory.high")); len(files) == 0 && err == nil {
				t.Error("memory.high was created")
			}
		})
	}
}

func TestRemoveStaleSandboxLeaf(t *testing.T) {
	root := t.TempDir()
	s := &AteWorkerService{cgroupRoot: root}
	leaf := s.sandboxLeaf("uid-a")
	if err := os.Mkdir(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.removeStaleSandboxLeaf(context.Background(), "uid-a"); err != nil {
		t.Fatalf("removeStaleSandboxLeaf() = %v", err)
	}
	if _, err := os.Stat(leaf); !os.IsNotExist(err) {
		t.Errorf("leftover leaf still there (stat err: %v)", err)
	}
	if err := s.removeStaleSandboxLeaf(context.Background(), "uid-a"); err != nil {
		t.Errorf("removeStaleSandboxLeaf() without a leaf = %v", err)
	}
}

// The watch guards each hosted sandbox's cgroup, except ones being stopped.
func TestMemoryTargets(t *testing.T) {
	root := t.TempDir()
	running := &hostedActor{attribution: resources.ActorAttribution{UID: "uid-a"}}
	stopping := &hostedActor{attribution: resources.ActorAttribution{UID: "uid-b"}}
	stopping.stopping.Store(true)
	s := &AteWorkerService{cgroupRoot: root, actors: map[string]*hostedActor{"uid-a": running, "uid-b": stopping}}

	targets := s.MemoryTargets()
	if len(targets) != 1 || targets[0].MemoryGuard() != &running.memory {
		t.Fatalf("MemoryTargets() = %v, want only the running sandbox", targets)
	}
	leaf := s.sandboxLeaf("uid-a")
	if r, name := targets[0].MemoryCgroup(); filepath.Join(r, name) != leaf {
		t.Errorf("MemoryCgroup() = %s/%s, want %s", r, name, leaf)
	}

	// a sandbox past the overshoot cap is killed through that cgroup
	if err := os.Mkdir(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	const limit = 256 << 20
	writeCgroupFiles(t, leaf, map[string]string{
		"memory.current": strconv.Itoa(limit+32<<20) + "\n",
		"cgroup.kill":    "",
		"cgroup.events":  "populated 0\nfrozen 0\n",
	})
	running.memory.Resume(limit)
	r, name := targets[0].MemoryCgroup()
	kill, ok := running.memory.Check(context.Background(), r, name)
	if !ok {
		t.Fatal("sandbox over its limit was not killed")
	}
	if err := <-kill.Done; err != nil {
		t.Fatal(err)
	}
	if got := readCgroupFile(t, leaf, "cgroup.kill"); got != "1" {
		t.Errorf("cgroup.kill = %q, want 1", got)
	}
}
