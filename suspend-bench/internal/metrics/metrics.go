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

// Package metrics provides monotonic timing helpers and /proc + filesystem
// accounting readers used by the harness.
package metrics

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Ms returns the elapsed milliseconds since start, using the monotonic clock that
// time.Now() carries.
func Ms(start time.Time) float64 {
	return float64(time.Since(start).Nanoseconds()) / 1e6
}

// ProcStatusBytes reads a "VmRSS"-style field (reported in kB) from
// /proc/<pid>/status and returns bytes, or -1 if unavailable.
func ProcStatusBytes(pid int, field string) int64 {
	f, err := os.Open("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return -1
	}
	defer f.Close()
	prefix := field + ":"
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, prefix) {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return kb * 1024
				}
			}
		}
	}
	return -1
}

// RSSBytes is the resident set size of a process in bytes (-1 if gone).
func RSSBytes(pid int) int64 { return ProcStatusBytes(pid, "VmRSS") }

// SwapBytes is the swapped-out anonymous memory of a process in bytes.
func SwapBytes(pid int) int64 { return ProcStatusBytes(pid, "VmSwap") }

// DirApparentBytes sums the logical (apparent) sizes of all files under dir —
// the on-paper snapshot size before sparseness/compression.
func DirApparentBytes(dir string) int64 {
	var total int64
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, e := d.Info(); e == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// DirActualBytes sums the real on-disk block usage under dir (st_blocks*512),
// capturing sparse holes — the size that actually matters for SSD/transfer cost.
func DirActualBytes(dir string) int64 {
	var total int64
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		total += statBlocksBytes(p)
		return nil
	})
	return total
}
