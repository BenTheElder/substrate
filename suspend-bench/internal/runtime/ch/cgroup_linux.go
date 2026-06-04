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

//go:build linux

package ch

import (
	"os"
	"syscall"

	"github.com/agent-substrate/substrate/suspend-bench/internal/metrics"
)

// cgroupProcAttr returns a SysProcAttr that launches the child directly into the
// leaf cgroup via the clone3 CLONE_INTO_CGROUP path (Go's UseCgroupFD), plus the
// open cgroup dir file the caller must keep alive until Start and then close.
// Returns (nil, nil, nil) when no cgroup is requested.
func cgroupProcAttr(cg *metrics.Cgroup) (*syscall.SysProcAttr, *os.File, error) {
	if cg == nil {
		return nil, nil, nil
	}
	f, err := os.Open(cg.Path)
	if err != nil {
		return nil, nil, err
	}
	return &syscall.SysProcAttr{
		UseCgroupFD: true,
		CgroupFD:    int(f.Fd()),
	}, f, nil
}
