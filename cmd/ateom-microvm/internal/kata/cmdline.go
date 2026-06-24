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

package kata

import "runtime"

// GuestVsockCID is the guest hybrid-vsock context id ateom assigns on cold boot.
// Hybrid vsock bridges through the clh.sock unix socket (the host connects via the
// socket, not a real CID), so any valid guest CID (>=3) works; 3 matches kata's
// single-guest default.
const GuestVsockCID = 3

// KataKernelCmdline returns the guest kernel command line for an ateom-owned cold
// boot, reconstructed from how kata's clh.go assembles it (GetKernelRootParams +
// clhKernelParams + the debug console param + the stock clh config kernel_params),
// plus the agent.debug_console params ateom needs for reset-to-golden and bind ops.
//
// It is STATIC apart from the console device (arch-dependent) and the debug flag —
// there are no per-actor variables. Captured/validated against kata 3.31; if kata
// is upgraded, re-capture from a real snapshot config.json (see
// ~/agent-shared/kata-ch-vmconfig-reference-arm64.json). Deriving it fully from the
// rendered config.toml is a follow-up.
func KataKernelCmdline(debug bool) string {
	// kata clh debug console device: ttyAMA0 on arm64, ttyS0 on x86_64.
	console := "console=ttyS0,115200n8"
	if runtime.GOARCH == "arm64" {
		console = "console=ttyAMA0,115200n8"
	}
	cmdline := "root=/dev/vda1 rootflags=data=ordered,errors=remount-ro ro rootfstype=ext4 " +
		"panic=1 no_timer_check noreplace-smp " + console + " systemd.log_target=console " +
		"systemd.unit=kata-containers.target " +
		"systemd.mask=systemd-networkd.service systemd.mask=systemd-networkd.socket " +
		"agent.cdh_api_timeout=50 cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1 " +
		"agent.debug_console agent.debug_console_vport=1026"
	if debug {
		cmdline += " agent.log=debug systemd.journald.forward_to_console=1"
	}
	return cmdline
}
