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
	"os"
	"path/filepath"
	"strings"
	"testing"
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
