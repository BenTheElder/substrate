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

// Package ch drives cloud-hypervisor directly over its REST API (the primary
// runtime under test). The guest boots a Kata vmlinux + an ext4 virtio-blk rootfs
// built from a container image, and the control channel is virtio-vsock (never
// virtio-fs, which breaks CH restore). Each VMM is launched directly into its
// per-instance leaf cgroup so the swap mechanism's accounting is clean.
package ch

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/suspend-bench/internal/runtime"
	"github.com/agent-substrate/substrate/suspend-bench/internal/vsock"
)

// workloadVsockPort is the guest port the workloads listen on (see workloads/).
const workloadVsockPort = 1234

// Runtime is the cloud-hypervisor Runtime implementation.
type Runtime struct {
	bin string // cloud-hypervisor binary
}

// New returns a CH runtime using `cloud-hypervisor` from PATH (override via bin).
func New(bin string) *Runtime {
	if bin == "" {
		bin = "cloud-hypervisor"
	}
	return &Runtime{bin: bin}
}

func (*Runtime) Name() string { return "ch" }

func (*Runtime) SupportsRestoreMode(mode string) bool {
	return mode == "copy" || mode == "ondemand"
}

// RestoreLeavesPaused: cloud-hypervisor restores into a paused VM.
func (*Runtime) RestoreLeavesPaused() bool { return true }

// chState is the per-handle private state.
type chState struct {
	cmd        *exec.Cmd
	api        *apiClient
	apiSock    string
	vsockSock  string
	consoleLog string
	vmmLog     string
}

func (r *Runtime) paths(workDir string) (apiSock, vsockSock, consoleLog, vmmLog string) {
	return filepath.Join(workDir, "ch.sock"),
		filepath.Join(workDir, "vsock.sock"),
		filepath.Join(workDir, "console.log"),
		filepath.Join(workDir, "vmm.log")
}

// launchVMM starts a cloud-hypervisor process in the instance's leaf cgroup,
// with stdout/stderr to vmmLog. extraArgs lets Restore add --restore.
func (r *Runtime) launchVMM(ctx context.Context, spec runtime.BootSpec, apiSock, vmmLog string, extraArgs ...string) (*exec.Cmd, error) {
	_ = os.Remove(apiSock)
	args := append([]string{"--api-socket", apiSock}, extraArgs...)
	cmd := exec.CommandContext(ctx, r.bin, args...)

	logf, err := os.Create(vmmLog)
	if err != nil {
		return nil, err
	}
	cmd.Stdout = logf
	cmd.Stderr = logf

	// Launch directly into the leaf cgroup (Linux: SysProcAttr.UseCgroupFD).
	attr, cgFile, err := cgroupProcAttr(spec.Cgroup)
	if err != nil {
		return nil, err
	}
	if cgFile != nil {
		defer cgFile.Close()
	}
	cmd.SysProcAttr = attr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting cloud-hypervisor: %w", err)
	}
	return cmd, nil
}

func (r *Runtime) dialFunc(vsockSock string) vsock.DialFunc {
	return func(timeout time.Duration) (*vsock.Client, error) {
		return vsock.DialCH(vsockSock, workloadVsockPort, timeout)
	}
}

func (r *Runtime) Boot(ctx context.Context, spec runtime.BootSpec) (*runtime.Handle, error) {
	if err := os.MkdirAll(spec.WorkDir, 0o755); err != nil {
		return nil, err
	}
	apiSock, vsockSock, consoleLog, vmmLog := r.paths(spec.WorkDir)
	_ = os.Remove(vsockSock)

	cmd, err := r.launchVMM(ctx, spec, apiSock, vmmLog)
	if err != nil {
		return nil, err
	}
	api := newAPIClient(apiSock)
	if err := api.waitReady(ctx, 10*time.Second); err != nil {
		cmd.Process.Kill()
		return nil, err
	}

	cfg := vmConfig{
		Cpus:   cpusConfig{BootVcpus: spec.VCPUs, MaxVcpus: spec.VCPUs},
		Memory: memoryConfig{Size: spec.MemoryBytes},
		Payload: payload{
			Kernel:  spec.KernelPath,
			Cmdline: "console=ttyS0 root=/dev/vda rw init=/sbin/init",
		},
		Disks:   []diskConfig{{Path: spec.RootfsPath, ImageType: "Raw"}},
		Vsock:   vsockConfig{Cid: spec.VsockCID, Socket: vsockSock},
		Serial:  consoleCfg{Mode: "File", File: consoleLog},
		Console: consoleCfg{Mode: "Off"},
	}
	if spec.SharedMem {
		// memfd-backed guest RAM; lets CH v52 snapshot sparsely (PR #8113) by
		// walking SEEK_DATA/SEEK_HOLE and skipping never-faulted pages.
		cfg.Memory.Shared = true
	}
	if spec.BackingFile {
		cfg.Memory.File = filepath.Join(spec.WorkDir, "guest-mem")
	}
	bctx, bcancel := context.WithTimeout(ctx, 60*time.Second)
	defer bcancel()
	if err := api.put(bctx, "/api/v1/vm.create", cfg); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("vm.create: %w", err)
	}
	if err := api.put(bctx, "/api/v1/vm.boot", nil); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("vm.boot: %w", err)
	}

	h := &runtime.Handle{
		ID:     spec.ID,
		VMMPid: cmd.Process.Pid,
		Cgroup: spec.Cgroup,
		Dial:   r.dialFunc(vsockSock),
		Spec:   spec,
	}
	h.SetPriv(&chState{cmd: cmd, api: api, apiSock: apiSock, vsockSock: vsockSock, consoleLog: consoleLog, vmmLog: vmmLog})
	return h, nil
}

func (r *Runtime) Pause(ctx context.Context, h *runtime.Handle) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	return h.Priv().(*chState).api.put(ctx, "/api/v1/vm.pause", nil)
}

func (r *Runtime) Resume(ctx context.Context, h *runtime.Handle) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	return h.Priv().(*chState).api.put(ctx, "/api/v1/vm.resume", nil)
}

func (r *Runtime) Snapshot(ctx context.Context, h *runtime.Handle, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	// Snapshot writes the full guest RAM to disk; allow generous time.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	body := snapshotConfig{DestinationURL: "file://" + destDir}
	return h.Priv().(*chState).api.put(ctx, "/api/v1/vm.snapshot", body)
}

// Restore spawns a fresh VMM with --restore (the CLI form reliably accepts
// memory_restore_mode), rebinding the same vsock socket from the snapshot's
// config.json. The restored VM is left paused.
func (r *Runtime) Restore(ctx context.Context, h *runtime.Handle, srcDir, mode string) error {
	st := h.Priv().(*chState)
	_ = os.Remove(st.vsockSock)

	restoreArg := "source_url=file://" + srcDir
	switch mode {
	case "ondemand":
		// ondemand uses userfaultfd; prefault must be off.
		restoreArg += ",memory_restore_mode=ondemand,prefault=off"
	case "copy", "":
		// Eager copy (CH's default), so leave memory_restore_mode unset.
	default:
		return fmt.Errorf("unknown restore mode %q", mode)
	}

	cmd, err := r.launchVMM(ctx, h.Spec, st.apiSock, st.vmmLog, "--restore", restoreArg)
	if err != nil {
		return err
	}
	api := newAPIClient(st.apiSock)
	if err := api.waitReady(ctx, 15*time.Second); err != nil {
		cmd.Process.Kill()
		return err
	}
	// New VMM is up and the VM is restored (paused). Update handle state.
	st.cmd = cmd
	st.api = api
	h.VMMPid = cmd.Process.Pid
	return nil
}

func (r *Runtime) Teardown(ctx context.Context, h *runtime.Handle) error {
	st, ok := h.Priv().(*chState)
	if !ok || st == nil {
		return nil
	}
	// Best-effort graceful shutdown, then make sure the process is gone.
	shutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	_ = st.api.put(shutCtx, "/api/v1/vm.shutdown", nil)
	_ = st.api.put(shutCtx, "/api/v1/vmm.shutdown", nil)
	cancel()
	if st.cmd != nil && st.cmd.Process != nil {
		_ = st.cmd.Process.Kill()
		_, _ = st.cmd.Process.Wait()
	}
	_ = os.Remove(st.apiSock)
	_ = os.Remove(st.vsockSock)
	return nil
}
