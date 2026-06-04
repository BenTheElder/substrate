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

package mechanism

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/suspend-bench/internal/metrics"
	"github.com/agent-substrate/substrate/suspend-bench/internal/runtime"
)

// ColdStart is the baseline: tear the sandbox down entirely and boot a fresh one.
// State is NOT preserved — it measures the floor that suspend/resume must beat to
// be worth any complexity (resume-to-first-ping of a brand new sandbox).
type ColdStart struct{}

func (ColdStart) Name() string         { return "coldstart" }
func (ColdStart) PreservesState() bool { return false }
func (ColdStart) NeedsImageDir() bool  { return false }

func (ColdStart) Suspend(ctx context.Context, rt runtime.Runtime, h *runtime.Handle, _ SuspendInput) (SuspendResult, error) {
	var res SuspendResult
	preRSS := metrics.RSSBytes(h.VMMPid)
	start := time.Now()
	if err := rt.Teardown(ctx, h); err != nil {
		return res, fmt.Errorf("teardown: %w", err)
	}
	res.SuspendMs = metrics.Ms(start)
	res.HostRSSFreedBytes = preRSS
	res.ImageApparentBytes = -1
	res.ImageActualBytes = -1
	res.SwapCurrentBytes = -1
	res.ZswapCurrentBytes = -1
	return res, nil
}

func (ColdStart) Resume(ctx context.Context, rt runtime.Runtime, h *runtime.Handle, in ResumeInput) (ResumeResult, error) {
	var res ResumeResult
	start := time.Now()
	fresh, err := rt.Boot(ctx, h.Spec)
	if err != nil {
		return res, fmt.Errorf("cold boot: %w", err)
	}
	*h = *fresh // replace the torn-down handle with the fresh sandbox
	res.ResumeCallMs = metrics.Ms(start)

	client, tok, err := runtime.FirstPing(h, in.ResumeDeadline)
	if err != nil {
		return res, fmt.Errorf("first ping after cold boot: %w", err)
	}
	res.ResumeToFirstPingMs = metrics.Ms(start)
	res.Client = client
	res.Token = tok
	return res, nil
}
