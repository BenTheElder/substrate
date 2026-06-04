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

import "testing"

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"4096":   4096,
		"64MiB":  64 << 20,
		"1GiB":   1 << 30,
		"512KiB": 512 << 10,
		"2 MiB":  2 << 20,
		"1MB":    1_000_000,
		"1.5GiB": 1<<30 + 1<<29,
	}
	for in, want := range cases {
		got, err := parseSize(in)
		if err != nil {
			t.Errorf("parseSize(%q) error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseSize(%q) = %d, want %d", in, got, want)
		}
	}
	if _, err := parseSize("notasize"); err == nil {
		t.Error("parseSize(notasize) should error")
	}
}

func TestBuildMatrixGating(t *testing.T) {
	// swap should ignore compression/restore-mode; checkpoint_local on gvisor
	// should ignore restore-mode (gVisor has one restore path).
	cfg := config{
		runtimes:    []string{"ch", "gvisor"},
		mechanisms:  []string{"checkpoint_local", "swap"},
		workloads:   []string{"c"},
		workingSets: []int64{1 << 20},
		reps:        1,
		compression: []string{"none", "zstd"},
		restoreMode: []string{"copy", "ondemand"},
		zswap:       []string{"on", "off"},
	}
	cells := buildMatrix(cfg)
	for _, c := range cells {
		if c.mech == "swap" {
			if c.compression != "" || c.restoreMode != "" {
				t.Errorf("swap cell has diagnostic compression/restore set: %+v", c)
			}
			if c.zswap == nil {
				t.Errorf("swap cell missing zswap value: %+v", c)
			}
		}
		if c.mech == "checkpoint_local" && c.runtime == "gvisor" && c.restoreMode != "" {
			t.Errorf("gvisor checkpoint cell has restore mode set: %+v", c)
		}
		if c.mech == "checkpoint_local" && c.zswap != nil {
			t.Errorf("checkpoint cell should not carry zswap: %+v", c)
		}
	}
	// ch checkpoint: 2 comps * 2 restore modes = 4; gvisor checkpoint: 2 comps * 1 = 2;
	// ch swap: 2 zswap; gvisor swap: 2 zswap. Total = 4+2+2+2 = 10.
	if len(cells) != 10 {
		t.Errorf("expected 10 cells, got %d", len(cells))
	}
}
