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

// Package gvisor drives gVisor/runsc as a secondary runtime, reusing the exact
// runsc flags the production code uses (see cmd/ateom-gvisor/runsc.go in the
// parent repo): checkpoint `-image-path`, restore `-bundle -image-path -pid-file
// -background -direct -detach`, plus pause/resume for the swap mechanism.
//
// gVisor has no vsock, so the workload binds an AF_UNIX socket inside a
// bind-mounted dir the harness dials directly. The sandbox is placed in the
// instance's leaf cgroup (OCI cgroupsPath + an explicit PID move) so the swap
// mechanism's memory.reclaim accounting is clean — the poc2 finding is that
// charges don't migrate on move, so we put the sandbox there before it faults its
// working set.
package gvisor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/suspend-bench/internal/runtime"
	"github.com/agent-substrate/substrate/suspend-bench/internal/vsock"
)

// containerName is the single container per sandbox (mirrors the repo's "pause"
// root container; here there is just one).
const containerName = "workload"

// workloadTCPPort is the guest TCP port the workload listens on (netstack),
// reachable from the host via the per-instance veth.
const workloadTCPPort = 1234

// Runtime is the gVisor/runsc Runtime implementation.
type Runtime struct {
	bin string // runsc binary
}

// New returns a gVisor runtime using `runsc` from PATH (override via bin).
func New(bin string) *Runtime {
	if bin == "" {
		bin = "runsc"
	}
	return &Runtime{bin: bin}
}

func (*Runtime) Name() string { return "gvisor" }

// SupportsRestoreMode: runsc has a single restore path, so only the default
// (empty/"copy") is meaningful; "ondemand" is CH-specific.
func (*Runtime) SupportsRestoreMode(mode string) bool { return mode == "" || mode == "copy" }

// RestoreLeavesPaused: runsc restore brings the sandbox back running.
func (*Runtime) RestoreLeavesPaused() bool { return false }

type gvState struct {
	stateRoot string // runsc -root dir
	bundle    string // per-instance OCI bundle dir (holds config.json only)
	rootfsDir string // shared extracted image rootfs (referenced by config.json)
	pidFile   string
	net       *netConf // per-instance veth/netns; net.guestAddr is the TCP control addr
	logFile   string
}

// runsc runs a runsc subcommand with the given args, appending stdout/stderr to
// the instance log. global flags (-root, -log-format) precede the subcommand.
func (r *Runtime) runsc(ctx context.Context, st *gvState, args ...string) error {
	full := append([]string{
		"-log-format", "json",
		"--alsologtostderr",
		// In-memory overlay: writes (Vite's cache, /tmp, the agent's edits) go to a
		// memory-backed upper layer, so the shared read-only rootfs stays clean
		// (no .gvisor.filestore written into it) and writable-rootfs workloads like
		// Node/Vite work. The C workload writes nothing, so its overlay stays empty.
		// Also: do NOT use -host-uds — a bound host socket cannot be checkpointed;
		// the control channel is netstack TCP over a per-instance veth/netns.
		"-overlay2=all:memory",
		"-root", st.stateRoot,
	}, args...)
	cmd := exec.CommandContext(ctx, r.bin, full...)
	logf, err := os.OpenFile(st.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer logf.Close()
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("runsc %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func (r *Runtime) dialFunc(tcpAddr string) vsock.DialFunc {
	return func(timeout time.Duration) (*vsock.Client, error) {
		return vsock.DialTCP(tcpAddr, timeout)
	}
}

func (r *Runtime) Boot(ctx context.Context, spec runtime.BootSpec) (*runtime.Handle, error) {
	if spec.BundlePath == "" {
		return nil, fmt.Errorf("gvisor Boot requires BundlePath (a dir containing rootfs/)")
	}
	st := &gvState{
		stateRoot: filepath.Join(spec.WorkDir, "runsc-root"),
		// Per-instance bundle (just config.json) so `runsc spec` never collides
		// with an existing file; rootfs is the shared extracted image, referenced
		// by an absolute root.path (no per-instance copy).
		bundle:    filepath.Join(spec.WorkDir, "bundle"),
		rootfsDir: filepath.Join(spec.BundlePath, "rootfs"),
		pidFile:   filepath.Join(spec.WorkDir, "sentry.pid"),
		logFile:   filepath.Join(spec.WorkDir, "runsc.log"),
	}
	for _, d := range []string{st.stateRoot, st.bundle} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	// Per-instance veth/netns so the host can reach the sandbox's netstack TCP
	// listener (checkpointable, unlike a host-uds socket).
	nc, err := setupNet(ctx, spec.ID, workloadTCPPort)
	if err != nil {
		return nil, fmt.Errorf("network setup: %w", err)
	}
	st.net = nc
	ok := false
	defer func() {
		if !ok {
			teardownNet(st.net)
		}
	}()

	if err := r.writeOCIConfig(ctx, spec, st); err != nil {
		return nil, err
	}
	if err := r.runsc(ctx, st, "create", "-bundle", st.bundle, "-pid-file", st.pidFile, containerName); err != nil {
		return nil, err
	}
	if err := r.runsc(ctx, st, "start", containerName); err != nil {
		return nil, err
	}

	pid, err := readPIDFile(st.pidFile)
	if err != nil {
		return nil, err
	}
	// Belt-and-suspenders: ensure the sentry is in our leaf cgroup before the
	// workload faults its working set (so swap accounting lands here).
	if spec.Cgroup != nil {
		_ = spec.Cgroup.AddPID(pid)
	}

	h := &runtime.Handle{
		ID:     spec.ID,
		VMMPid: pid,
		Cgroup: spec.Cgroup,
		Dial:   r.dialFunc(st.net.guestAddr),
		Spec:   spec,
	}
	h.SetPriv(st)
	ok = true
	return h, nil
}

func (r *Runtime) Pause(ctx context.Context, h *runtime.Handle) error {
	return r.runsc(ctx, h.Priv().(*gvState), "pause", containerName)
}

func (r *Runtime) Resume(ctx context.Context, h *runtime.Handle) error {
	return r.runsc(ctx, h.Priv().(*gvState), "resume", containerName)
}

func (r *Runtime) Snapshot(ctx context.Context, h *runtime.Handle, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	return r.runsc(ctx, h.Priv().(*gvState), "checkpoint", "-image-path", destDir, containerName)
}

// Restore re-creates the sandbox from the image. Flags mirror the repo's
// production restore (-background -direct -detach). gVisor restores running, so
// (unlike CH) there is no separate resume after restore — Resume is a no-op.
func (r *Runtime) Restore(ctx context.Context, h *runtime.Handle, srcDir, _ string) error {
	st := h.Priv().(*gvState)
	// The post-checkpoint Teardown deletes the netns; re-create it identically
	// (deterministic in the instance ID) so config.json's netns path resolves.
	nc, err := setupNet(ctx, h.Spec.ID, workloadTCPPort)
	if err != nil {
		return fmt.Errorf("network re-setup for restore: %w", err)
	}
	st.net = nc
	if err := r.runsc(ctx, st,
		"restore",
		"-bundle", st.bundle,
		"-image-path", srcDir,
		"-pid-file", st.pidFile,
		"-background", "-direct", "-detach",
		containerName,
	); err != nil {
		return err
	}
	pid, err := readPIDFile(st.pidFile)
	if err != nil {
		return err
	}
	h.VMMPid = pid
	if h.Cgroup != nil {
		_ = h.Cgroup.AddPID(pid)
	}
	return nil
}

func (r *Runtime) Teardown(ctx context.Context, h *runtime.Handle) error {
	st, ok := h.Priv().(*gvState)
	if !ok || st == nil {
		return nil
	}
	err := r.runsc(ctx, st, "delete", "-force", containerName)
	teardownNet(st.net)
	return err
}

func readPIDFile(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}

// writeOCIConfig generates a default config.json via `runsc spec` and patches it:
// process args -> the workload listening on TCP, the rootfs path, the per-instance
// network namespace, and the leaf cgroupsPath.
func (r *Runtime) writeOCIConfig(ctx context.Context, spec runtime.BootSpec, st *gvState) error {
	// `runsc spec -bundle DIR` writes a template config.json into DIR, but refuses
	// to overwrite an existing one (exit 128). Boot can run twice for one instance
	// (coldstart: initial boot + resume), so remove any prior config first.
	cfgPath := filepath.Join(st.bundle, "config.json")
	_ = os.Remove(cfgPath)
	if err := r.runsc(ctx, st, "spec", "-bundle", st.bundle); err != nil {
		return fmt.Errorf("runsc spec: %w", err)
	}
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return err
	}

	// process.args -> ["/workload", "tcp:<port>"], no terminal.
	if proc, ok := cfg["process"].(map[string]any); ok {
		proc["args"] = []any{"/workload", fmt.Sprintf("tcp:%d", workloadTCPPort)}
		proc["terminal"] = false
	}
	// root.path -> absolute path to the shared extracted image rootfs. Writable=
	// true so Node/Vite can write; the in-memory overlay (-overlay2=all:memory)
	// keeps those writes off the shared rootfs.
	cfg["root"] = map[string]any{"path": st.rootfsDir, "readonly": false}

	// Point the network namespace at our per-instance veth netns so gVisor's
	// netstack adopts the veth + IP and the host can reach the workload's TCP port.
	if lx, ok := cfg["linux"].(map[string]any); ok {
		nsList, _ := lx["namespaces"].([]any)
		for _, e := range nsList {
			if m, ok := e.(map[string]any); ok && m["type"] == "network" {
				m["path"] = st.net.nsPath
			}
		}
		lx["namespaces"] = nsList
		cfg["linux"] = lx
	}

	// Place the sandbox in our leaf cgroup (absolute path under the cgroup root).
	if lx, ok := cfg["linux"].(map[string]any); ok && spec.Cgroup != nil {
		lx["cgroupsPath"] = strings.TrimPrefix(spec.Cgroup.Path, "/sys/fs/cgroup")
		cfg["linux"] = lx
	}

	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cfgPath, out, 0o644)
}
