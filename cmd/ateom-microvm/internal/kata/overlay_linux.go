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

// Overlay rootfs path (large-image variant): instead of building the whole actor
// rootfs onto /dev/vdb, serve the OCI image read-only over virtio-fs (the lower)
// and use /dev/vdb as a small writable ext4 overlay upper. Setup is O(1) per actor
// (bind the image into the find-paths dir, format an empty upper) rather than
// O(image). Writes still land off guest RAM (on /dev/vdb), so the snapshot stays
// memory-only. This file holds the overlay-specific helpers; the boot/network/agent
// machinery is shared with the full-disk path.

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// FsTag is the virtio-fs tag kata uses for the shared filesystem. The CH fs
	// device Tag and the agent mount Source must both be this value.
	FsTag = "kataShared"
	// typeVirtioFS / virtioFSDriver are the agent fstype + driver for it.
	typeVirtioFS   = "virtiofs"
	virtioFSDriver = "virtio-fs"
	// guestSharedDir is where the agent mounts the kataShared tag in the guest;
	// per-container rootfs then lives at <guestSharedDir>/<cid>/rootfs.
	guestSharedDir = "/run/kata-containers/shared/containers/"
)

// SharedDir is the host directory virtiofsd serves into the guest as the RO base.
// Its layout (<sourceID>/rootfs) is what find-paths re-opens by path on restore.
func SharedDir(id string) string {
	return filepath.Join("/run/kata-containers/shared/sandboxes", id, "shared")
}

// VirtiofsdSocketPath is the vhost-user-fs socket CH connects to for the fs device.
func VirtiofsdSocketPath(id string) string { return filepath.Join(VMDir(id), "virtiofsd.sock") }

// OverlayUpperBase is the in-guest mount point for the boot-time scratch disk
// (/dev/vdb) holding the overlay upper/work. Keyed on the FROZEN base id (the
// golden cold-run id), NOT the per-actor id, so the mount + overlay are stable
// across the lineage and the reset-to-golden wipe targets the same path.
func OverlayUpperBase(baseID string) string { return "/run/ateom-upper/" + baseID }

// GuestSharedRootfs is the in-guest path the kataShared mount exposes a container's
// rootfs at. A carrier container with this as Root.Path makes the agent eagerly bind
// it to /run/kata-containers/<cid>/rootfs (stable on arm64), the overlay lowerdir.
func GuestSharedRootfs(containerID string) string { return guestSharedDir + containerID + "/rootfs" }

// VirtiofsdOptions configures StartVirtiofsd.
type VirtiofsdOptions struct {
	Binary     string // virtiofsd executable; defaults to "virtiofsd"
	SocketPath string // vhost-user socket CH connects to (VirtiofsdSocketPath)
	SharedDir  string // directory to serve (SharedDir(id))
	Log        io.Writer
}

// StartVirtiofsd launches virtiofsd in find-paths migration mode serving o.SharedDir
// on o.SocketPath, and waits for the socket to appear. The returned cmd outlives the
// caller's ctx (CH demand-pages from it under the running VM); the caller owns it.
func StartVirtiofsd(ctx context.Context, o VirtiofsdOptions) (*exec.Cmd, error) {
	bin := o.Binary
	if bin == "" {
		bin = "virtiofsd"
	}
	_ = os.Remove(o.SocketPath)
	cmd := exec.Command(bin,
		"--socket-path="+o.SocketPath,
		"--shared-dir="+o.SharedDir,
		"--cache=auto",
		"--thread-pool-size=1",
		"--announce-submounts",
		"--migration-mode", "find-paths",
	)
	cmd.Stdout = o.Log
	cmd.Stderr = o.Log
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting virtiofsd: %w", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(o.SocketPath); err == nil {
			return cmd, nil
		}
		select {
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	_ = cmd.Process.Kill()
	return nil, fmt.Errorf("virtiofsd socket %q did not appear", o.SocketPath)
}

// ReconstructSharedDirFromImage makes the RO base available to virtiofsd at the
// find-paths location (<sourceID>/rootfs under SharedDir(restoreID)) by BIND-MOUNTING
// the OCI image rootfs there — O(1) vs cp -a, the large-image win. The guest-visible
// path is unchanged, so the carrier bind + overlay lowerdir + find-paths are
// untouched; cross-node consistency relies on the bundle being a deterministic unpack
// of the same image. sourceID is the FROZEN base id (== restoreID on cold boot; the
// snapshot's base-id on restore).
func ReconstructSharedDirFromImage(ctx context.Context, bundleRootfs, restoreID, sourceID string) error {
	if sourceID == "" {
		return fmt.Errorf("ReconstructSharedDirFromImage: empty sourceID")
	}
	dst := filepath.Join(SharedDir(restoreID), sourceID, "rootfs")
	// Drop any stale bind FIRST (lazy if busy) so we never recurse into — and delete —
	// the bind source, then ensure a clean mountpoint. Deliberately NOT RemoveAll(root)
	// (it would chase a live bind into bundleRootfs).
	if err := exec.Command("umount", dst).Run(); err != nil {
		_ = exec.Command("umount", "-l", dst).Run()
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("creating shared dir %q: %w", dst, err)
	}
	cmd := exec.CommandContext(ctx, "mount", "--bind", bundleRootfs, dst)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bind-mounting image rootfs %q -> %q: %w (%s)", bundleRootfs, dst, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// CreateSandboxForActor creates the guest sandbox with the kataShared virtio-fs
// mount (the RO base backing the container rootfs). Mirrors kata startSandbox.
func (a *AgentClient) CreateSandboxForActor(ctx context.Context, sandboxID, hostname string) error {
	return a.CreateSandbox(ctx, &agentpb.CreateSandboxRequest{
		Hostname:  hostname,
		SandboxId: sandboxID,
		Storages: []*agentpb.Storage{{
			Driver:     virtioFSDriver,
			Source:     FsTag,
			Fstype:     typeVirtioFS,
			MountPoint: guestSharedDir,
		}},
	})
}

// SetupScratchUpper formats /dev/vdb (the boot-time scratch disk) and mounts it at
// upperBase via the debug console (vsock 1026). The kata guest has mkfs.ext4 + mount,
// so no host-side e2fsprogs is needed. Run after boot, before StartOverlayWorkload.
func SetupScratchUpper(ctx context.Context, vsockPath, upperBase string) error {
	cmd := "mkfs.ext4 -F -q /dev/vdb && mkdir -p " + upperBase +
		" && mount /dev/vdb " + upperBase + " && echo ATE_SETUP_OK"
	out := DebugConsoleDump(ctx, vsockPath, cmd)
	if !strings.Contains(out, "ATE_SETUP_OK") {
		return fmt.Errorf("scratch-disk setup did not confirm (console: %s)", strings.TrimSpace(out))
	}
	return nil
}

// CreateCarrier creates the "carrier" container (id == sandboxID): rootfs = the
// kataShared virtio-fs base, created but NOT started, so the agent's setup_bundle
// eagerly binds the base to /run/kata-containers/<id>/rootfs (the overlay lowerdir).
func (a *AgentClient) CreateCarrier(ctx context.Context, sandboxID string, spec *specs.Spec) error {
	pbSpec := SpecToAgentPB(spec)
	pbSpec.Root = &agentpb.Root{Path: GuestSharedRootfs(sandboxID), Readonly: false}
	if pbSpec.Linux != nil {
		pbSpec.Linux.CgroupsPath = "/ateomchv/" + sandboxID + "-carrier"
	}
	if err := a.CreateContainer(ctx, &agentpb.CreateContainerRequest{
		ContainerId: sandboxID,
		ExecId:      sandboxID,
		OCI:         pbSpec,
	}); err != nil {
		return fmt.Errorf("creating carrier container %q: %w", sandboxID, err)
	}
	return nil
}

// StartOverlayWorkload creates + starts the actor container with an overlayfs rootfs:
// lower = the carrier's resolved bind (/run/kata-containers/<sandboxID>/rootfs from
// the RO virtio-fs base), upper/work = <upperBase>/{fs,work} on the /dev/vdb disk so
// rootfs writes stay off snapshot-captured RAM.
func (a *AgentClient) StartOverlayWorkload(ctx context.Context, sandboxID, containerID, upperBase string, spec *specs.Spec) error {
	const createDir = "io.katacontainers.volume.overlayfs.create_directory"
	sharedBase := "/run/kata-containers/" + sandboxID + "/rootfs"
	base := "/run/kata-containers/" + containerID
	lower := base + "/lower"
	ovlRoot := base + "/rootfs"
	upper := upperBase + "/fs"
	work := upperBase + "/work"

	storages := []*agentpb.Storage{
		{
			Driver:     virtioFSDriver,
			Source:     sharedBase,
			MountPoint: lower,
			Fstype:     "bind",
			Options:    []string{"bind"},
		},
		{
			Driver:        "overlayfs",
			Source:        "overlay",
			Fstype:        "overlay",
			MountPoint:    ovlRoot,
			DriverOptions: []string{createDir + "=" + upper, createDir + "=" + work},
			Options:       []string{"lowerdir=" + lower, "upperdir=" + upper, "workdir=" + work},
		},
	}
	pbSpec := SpecToAgentPB(spec)
	pbSpec.Root = &agentpb.Root{Path: ovlRoot, Readonly: false}

	if err := a.CreateContainer(ctx, &agentpb.CreateContainerRequest{
		ContainerId: containerID,
		ExecId:      containerID,
		Storages:    storages,
		OCI:         pbSpec,
	}); err != nil {
		return fmt.Errorf("creating overlay workload %q: %w", containerID, err)
	}
	if err := a.StartContainer(ctx, containerID); err != nil {
		return fmt.Errorf("starting overlay workload %q: %w", containerID, err)
	}
	return nil
}

// CreateSparseFile (re)creates a sparse file of the given size — the empty /dev/vdb
// scratch upper. Sparse, so it costs nothing until the guest writes.
func CreateSparseFile(path string, size int64) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing stale scratch %q: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("creating scratch %q: %w", path, err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("sizing scratch %q: %w", path, err)
	}
	return nil
}
