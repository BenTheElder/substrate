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
	"strconv"
	"strings"
	"syscall"
)

// parseSize parses sizes like "64MiB", "1GiB", "512KiB", "4096", "2MB".
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	mult := int64(1)
	suffixes := []struct {
		s string
		m int64
	}{
		{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3},
		{"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}, {"B", 1},
	}
	num := s
	for _, suf := range suffixes {
		if strings.HasSuffix(s, suf.s) {
			mult = suf.m
			num = strings.TrimSpace(strings.TrimSuffix(s, suf.s))
			break
		}
	}
	v, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing size %q: %w", s, err)
	}
	return int64(v * float64(mult)), nil
}

// humanSize renders a byte count compactly (for log lines only).
func humanSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.0fGiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0fMiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0fKiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// setZswap toggles host zswap (Stage-2 diagnostic to attribute a swap win to
// compression vs the suspend mechanism). Needs root.
func setZswap(on bool) error {
	val := "0"
	if on {
		val = "1"
	}
	return os.WriteFile("/sys/module/zswap/parameters/enabled", []byte(val), 0o644)
}

// dropCaches flushes the page cache so cold-sensitive measurements start clean.
func dropCaches() {
	// sync first so dirty pages are written and can be dropped.
	syscall.Sync()
	_ = os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0o644)
}
