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

// Command ateworker-microvm runs actors as kata + cloud-hypervisor micro-VMs,
// with full suspend/resume through CH's native snapshot/restore (see
// internal/ch).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateworker"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/spf13/pflag"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	chBinary      = pflag.String("cloud-hypervisor-binary", "cloud-hypervisor", "Path to the cloud-hypervisor binary (used to relaunch on restore).")
	guestDebug    = pflag.Bool("guest-debug", false, "enable guest debugging (agent debug logs, debug console, and early serial console)")
	vmmMemReserve = pflag.Int("vmm-mem-reserve-mib", vmmMemReserveMiB, "Guest RAM (MiB) held back from the pod's memory limit for the cloud-hypervisor VMM + virtiofsd, which run as host processes in the pod cgroup alongside the guest RAM. Prevents the pod OOMing when the VM is sized to the pod's memory limit.")
)

var config = ateworker.Config{
	ServiceName:  "ateworker-microvm",
	SandboxClass: string(atev1alpha1.SandboxClassMicroVM),
	// The rootfs upper is host-backed, so it ships with or without guest
	// memory.
	Fidelities: []ateworkerpb.SnapshotFidelity{
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
	},
	// A healthy guest reading takes about 2ms; a slower one falls back to the
	// newest cached sample instead of holding up the checkpoint.
	FinalReadTimeout: 10 * time.Millisecond,
	// The guest's CPU counters come back with its memory.
	MemoryRestoreKeepsCounters: true,
}

func main() {
	ateworker.Main(config, newRuntime)
}

// microvmRuntime runs each actor as a cloud-hypervisor micro-VM.
type microvmRuntime struct {
	podUID     string
	chBinary   string
	guestDebug bool
	// memReserveMiB is guest RAM held back from the pod's memory limit for the
	// VMM and virtiofsd, host processes sharing the pod cgroup with the guest
	// RAM.
	memReserveMiB int
	// actorLogger forwards the actor containers' output to the pod's stdout.
	actorLogger *actorlog.ActorLogger
	// actorCgroups is set when the worker's cgroup is delegated, so each actor's
	// VMM and virtiofsd run in a leaf of their own.
	actorCgroups bool
}

var _ ateworker.Runtime = (*microvmRuntime)(nil)

func newRuntime(ctx context.Context, env ateworker.Env) (ateworker.Runtime, error) {
	// kata's virtio-fs sharing depends on mount propagation: it slave-binds
	// .../shared (served by virtiofsd) from .../mounts and expects the later
	// per-container rootfs bind under mounts/ to propagate across. That only
	// works if the underlying mount is SHARED. On a host systemd makes /
	// rshared, but a container rootfs is rprivate (runc default), so the
	// propagation silently never happens: the guest sees an empty rootfs and
	// createContainer fails ENOENT. Self-bind /run/kata-containers and mark it
	// rshared so kata's propagation chain works inside the pod.
	if err := ensureSharedPropagation(ctx, "/run/kata-containers"); err != nil {
		return nil, fmt.Errorf("while making /run/kata-containers a shared mount: %w", err)
	}
	return &microvmRuntime{
		podUID:        env.PodUID,
		chBinary:      *chBinary,
		guestDebug:    *guestDebug,
		memReserveMiB: *vmmMemReserve,
		actorLogger:   env.ActorLogger,
		actorCgroups:  env.ActorCgroups,
	}, nil
}

// Validate ensures we only run assets from the static files dir.
func (s *microvmRuntime) Validate(a *ateworker.Actor) error {
	return validateRuntimeAssetPaths(a.AssetPaths)
}

// ensureSharedPropagation makes path a mount point with rshared propagation
// (self-bind + MS_SHARED|MS_REC), so mounts created beneath it propagate to
// slave binds (kata's mounts/ -> shared/ chain). Idempotent: skips if path is
// already a shared mount point.
func ensureSharedPropagation(ctx context.Context, path string) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return fmt.Errorf("creating %q: %w", path, err)
	}
	if b, err := os.ReadFile("/proc/self/mountinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			// mountinfo: ID parentID major:minor root mountpoint opts optional... - fstype ...
			fields := strings.Fields(line)
			if len(fields) >= 7 && fields[4] == path && strings.Contains(line, "shared:") {
				slog.InfoContext(ctx, "Mount already shared", slog.String("path", path))
				return nil
			}
		}
	}
	if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("self-binding %q: %w", path, err)
	}
	if err := unix.Mount("", path, "", unix.MS_SHARED|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("marking %q rshared: %w", path, err)
	}
	slog.InfoContext(ctx, "Made mount rshared for kata virtio-fs propagation", slog.String("path", path))
	return nil
}

// validateRuntimeAssetPaths ensures we only run assets from the static files dir
func validateRuntimeAssetPaths(paths map[string]string) error {
	var errs field.ErrorList
	for name, p := range paths {
		errs = append(errs, resources.ValidateRuntimeAssetPath(nodepath.StaticFilesDir, p, field.NewPath("runtime_asset_paths").Key(name))...)
	}
	if len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}
