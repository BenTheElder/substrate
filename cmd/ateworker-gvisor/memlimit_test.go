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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestMemoryOverage(t *testing.T) {
	const limit = 256 << 20
	stat := func(file uint64) string {
		return fmt.Sprintf("anon 1\ninactive_file %d\nactive_file 0\n", file)
	}
	type step struct {
		after   time.Duration
		current uint64
		cache   uint64
		want    bool
	}
	for _, tc := range []struct {
		name  string
		steps []step
	}{
		{
			name: "stuck over the limit",
			steps: []step{
				{0, limit + 4<<20, 1 << 20, false},
				{500 * time.Millisecond, limit + 4<<20, 1 << 20, false},
				{time.Second, limit + 4<<20, 1 << 20, true},
			},
		},
		{
			name: "back under the limit restarts the clock",
			steps: []step{
				{0, limit + 4<<20, 0, false},
				{500 * time.Millisecond, limit, 0, false},
				{750 * time.Millisecond, limit + 4<<20, 0, false},
				{1500 * time.Millisecond, limit + 4<<20, 0, false},
				{1750 * time.Millisecond, limit + 4<<20, 0, true},
			},
		},
		{
			name: "over on page cache",
			steps: []step{
				{0, limit + 4<<20, 64 << 20, false},
				{time.Second, limit + 4<<20, 64 << 20, false},
				{2 * time.Second, limit + 4<<20, 64 << 20, false},
			},
		},
		{
			name: "at the limit",
			steps: []step{
				{0, limit, 0, false},
				{2 * time.Second, limit, 0, false},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			start := time.Now()
			var o memoryOverage
			for i, s := range tc.steps {
				writeCgroupFiles(t, dir, map[string]string{
					"memory.current": fmt.Sprint(s.current),
					"memory.stat":    stat(s.cache),
				})
				got, current, err := o.check(dir, limit, start.Add(s.after))
				if err != nil {
					t.Fatal(err)
				}
				if got != s.want || current != s.current {
					t.Errorf("step %d: check() = %v, %d, want %v, %d", i, got, current, s.want, s.current)
				}
			}
		})
	}
}

func TestMemoryOverageMissingLeaf(t *testing.T) {
	var o memoryOverage
	if _, _, err := o.check(filepath.Join(t.TempDir(), "gone"), 1, time.Now()); err == nil {
		t.Error("check() on a missing leaf succeeded")
	}
}
