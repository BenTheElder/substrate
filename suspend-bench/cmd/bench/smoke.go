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
	"fmt"
	"os"

	"github.com/agent-substrate/substrate/suspend-bench/internal/results"
)

// runSmoke exercises each mechanism once on the requested runtimes with the C
// workload and asserts the invariants that prove the pipeline works end-to-end:
// state survives suspend/resume (checkpoint, swap) and swap actually frees host
// RAM. Exits non-zero on any failure. This is the gate before a full matrix run.
func runSmoke(cfg config) error {
	ctx := context.Background()
	host, _ := results.LoadHostMeta(cfg.hostMeta)
	const ws = 64 << 20

	type check struct {
		runtime, mech string
	}
	var plan []check
	for _, rt := range cfg.runtimes {
		plan = append(plan,
			check{rt, "checkpoint_local"},
			check{rt, "swap"},
			check{rt, "coldstart"},
		)
	}

	failed := false
	for _, ck := range plan {
		cl := cell{
			runtime: ck.runtime, mech: ck.mech, workload: "c",
			wsBytes: ws, rep: 0, compression: "none",
		}
		if ck.mech == "checkpoint_local" && ck.runtime == "ch" {
			cl.restoreMode = "copy"
		}
		cctx, ccancel := context.WithTimeout(ctx, cfg.cellTimeout)
		res := runCell(cctx, cfg, host, cl)
		ccancel()

		ok, reason := smokeVerdict(ck.mech, res)
		status := "PASS"
		if !ok {
			status, failed = "FAIL", true
		}
		fmt.Printf("[smoke] %-7s %-16s resume=%.1fms freed=%s swap=%s img=%s -> %s%s\n",
			ck.runtime, ck.mech, res.ResumeToFirstPingMs,
			humanSize(max0(res.HostRSSFreedBytes)), humanSize(max0(res.SwapCurrentBytes)),
			humanSize(max0(res.ImageActualBytes)), status, reason)
	}
	if failed {
		os.Exit(1)
	}
	fmt.Println("[smoke] all checks passed")
	return nil
}

func smokeVerdict(mech string, res results.Result) (bool, string) {
	if res.Error != "" {
		return false, " (" + res.Error + ")"
	}
	switch mech {
	case "coldstart":
		return res.ResumeToFirstPingMs > 0, ""
	case "swap":
		if !res.CorrectnessOK {
			return false, " (state corrupted)"
		}
		if res.HostRSSFreedBytes <= 0 {
			return false, " (no host RAM freed — sentry/VMM not in leaf cgroup?)"
		}
		return true, ""
	default: // checkpoint_local
		if !res.CorrectnessOK {
			return false, " (state corrupted)"
		}
		if res.ImageActualBytes <= 0 {
			return false, " (empty snapshot)"
		}
		return true, ""
	}
}

func max0(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}
