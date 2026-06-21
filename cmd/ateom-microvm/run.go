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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	ctrtypes "github.com/containerd/containerd/api/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runningActor holds the live state for one actor's micro-VM.
//
// Two ownership modes:
//   - kata-shim-owned (from RunWorkload): shim != nil; kata owns the CH process.
//   - ateom-owned (from RestoreWorkload): shim == nil; ateom relaunched CH +
//     virtiofsd directly and owns those processes.
type runningActor struct {
	containerName string

	// baseID is the FROZEN base sandbox id the guest's virtio-fs find-paths are
	// pinned to (<baseID>/rootfs) — the id the RO base was first shared under,
	// invariant across restores. For a cold (shim-owned) actor this is the actor's
	// own id; for a restored actor it is the id read from the snapshot's base-id
	// file (the golden id, propagated). CheckpointWorkload writes it back into the
	// next snapshot's base-id file so the chain survives suspend->resume->suspend.
	baseID string

	// ovlContainerID is the kata-agent container id of the ateom-created overlay
	// workload (RunWorkload's "be your own hook scheduler" path); empty otherwise.
	// The workload lives in the VM RAM, so checkpoint/restore (CH-level) need not
	// reference it — kept for clarity/forward-compat (e.g. an explicit stop).
	ovlContainerID string

	// kata-shim-owned
	shim *kata.Shim

	// ateom-owned (post-restore)
	chCmd   *exec.Cmd
	vfsdCmd *exec.Cmd
	// apiSocket is the CH api-socket for an ateom-owned (restored) VMM; empty
	// for kata-shim-owned actors (use kata.CLHSocketPath then).
	apiSocket string

	// restoreSourceDir is the snapshot dir this actor was OnDemand-restored from
	// (the base CH is demand-paging from). Set only on the owned-boot virtio-blk
	// path when restored via OnDemand. CheckpointWorkload overlays CH's new (sparse,
	// faulted-only) snapshot onto this base to produce a COMPLETE snapshot (CH's
	// OnDemand snapshot alone drops the un-faulted pages). Empty for cold-run actors
	// (their snapshot is already complete) and for the eager/shim paths.
	restoreSourceDir string
}

// baseIDFile is a tiny snapshot file (under the checkpoint/restore dir) holding
// the FROZEN base sandbox id — the id the guest's virtio-fs find-paths are pinned
// to (<baseID>/rootfs). It is the id the RO base was FIRST shared under (the golden
// actor's cold-run id) and is INVARIANT across every restore of that actor's
// lineage: the guest memory keeps referencing <baseID>/rootfs, while the snapshot
// config.json's socket paths get rewritten to the current actor id on each restore.
// RestoreWorkload reads this to lay the reconstructed-from-image base at the path
// the guest expects. (The config.json socket id is the WRONG source — it equals the
// current id, not the frozen golden id, for any restored-then-checkpointed actor.)
const baseIDFile = "base-id"

// Asset names in RunWorkloadRequest.runtime_asset_paths (set by atelet's
// fetchRuntimeAssets, keyed by the ActorTemplate runtime asset names).
const (
	assetShim      = "kata-shim"
	assetCH        = "cloud-hypervisor"
	assetVirtiofsd = "virtiofsd"
	assetKernel    = "kata-kernel"
	assetImage     = "kata-image"
	assetConfig    = "kata-config"
)

// actorRootfsSizeMiB is the size of the actor's writable virtio-blk rootfs disk
// (/dev/vdb) on the owned-boot path. The image is sparse + ext4 metadata is the
// only eagerly-written part, so an oversized value is cheap; a busybox/counter
// rootfs is a few MiB. TODO: size dynamically from the bundle rootfs + slack.
const actorRootfsSizeMiB = 512

// actorRootfsDiskName is the actor's writable rootfs disk file under the actor
// dir; it is the /dev/vdb backing path recorded in the snapshot config.json and
// reopened verbatim on restore.
const actorRootfsDiskName = "actor-rootfs.ext4"

// goldenRootfsDiskName is the verbatim copy of the actor's /dev/vdb disk AS-OF the
// golden snapshot, kept under the actor dir. reset-to-golden recreates /dev/vdb
// from it on restore (byte-identical to what the snapshot's guest RAM/ext4 cache
// expects), discarding the actor's later rootfs writes — gVisor semantics.
const goldenRootfsDiskName = "golden-rootfs.ext4"

// fileMissing reports whether path does not exist.
func fileMissing(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}

// copyDiskFile copies a (sparse) disk image verbatim, preserving holes so the
// 512MiB ext4 image doesn't materialize its empty blocks. Used to save/restore the
// golden rootfs disk template.
func copyDiskFile(ctx context.Context, src, dst string) error {
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)
	if out, err := exec.CommandContext(ctx, "cp", "--sparse=always", src, tmp).CombinedOutput(); err != nil {
		return fmt.Errorf("cp %s -> %s: %w: %s", src, tmp, err, out)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, dst, err)
	}
	return nil
}

// blkRootfsMode reports whether the worker runs the ateom-owned-boot virtio-blk
// rootfs path (ateom boots CH itself; actor rootfs on a writable /dev/vdb). It
// gates Run/Checkpoint/Restore so the proven kata-shim path stays the default.
func blkRootfsMode() bool { return os.Getenv("ATEOM_VIRTIO_BLK_ROOTFS") != "" }

// resolvedRuntime holds the concrete binary/config paths for a request, taken
// from fetched runtime assets when present, else the process flags.
type resolvedRuntime struct {
	shim       string
	ch         string
	virtiofsd  string
	configFile string
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// resolveRuntime determines the kata shim / cloud-hypervisor / virtiofsd binaries
// and the kata config for a request. atelet fetches these content-addressed and
// passes their local paths; when a base config + kernel + image are present we
// render a configuration.toml pointing at the fetched paths (fetch-not-bake).
// Falls back to the process flags for anything not supplied.
func (s *AteomService) resolveRuntime(actorDir string, paths map[string]string) (resolvedRuntime, error) {
	r := resolvedRuntime{
		shim:       firstNonEmpty(paths[assetShim], s.shimBinary),
		ch:         firstNonEmpty(paths[assetCH], s.chBinary),
		virtiofsd:  firstNonEmpty(paths[assetVirtiofsd], s.virtiofsdBinary),
		configFile: s.kataConfig,
	}
	base, kernel, image := paths[assetConfig], paths[assetKernel], paths[assetImage]
	if base != "" && kernel != "" && image != "" {
		baseBytes, err := os.ReadFile(base)
		if err != nil {
			return r, fmt.Errorf("reading base kata config %q: %w", base, err)
		}
		// TODO: make guest VM memory size a first-class ActorTemplate field and
		// rewrite default_memory here (like the asset paths) instead of requiring
		// per-size configuration.toml asset variants. Suspend/resume latency
		// scales directly with guest memory (CH's snapshot scans/writes the whole
		// memfd): on GKE pd-balanced disks, a 512MiB guest measured ~3.4s suspend
		// / ~1.6s resume vs ~18s / ~4.4s at kata's stock default_memory=2048.
		rendered, err := kata.RenderConfig(baseBytes, kata.ConfigAssets{
			Kernel: kernel, Image: image, Hypervisor: r.ch, Virtiofsd: r.virtiofsd,
		})
		if err != nil {
			return r, fmt.Errorf("rendering kata config: %w", err)
		}
		// Add the CH virtio-balloon + free-page-reporting so CheckpointWorkload can
		// free guest pages before snapshot (gVisor-parity snapshot shrink).
		rendered, err = kata.EnableReclaimGuestFreedMemory(rendered)
		if err != nil {
			return r, fmt.Errorf("enabling reclaim_guest_freed_memory: %w", err)
		}
		// Enable the guest debug console (vsock 1026) so CheckpointWorkload can run
		// the in-guest reset-to-golden step (wipe the overlay tmpfs upper).
		rendered = kata.EnableDebugConsole(rendered)
		if s.kataDebug {
			// Verbose kata: hypervisor/agent/runtime debug on, guest console (with
			// the kata-agent's logs) forwarded into the shim log -> pod logs.
			rendered = kata.EnableDebug(rendered)
		}
		cfgPath := filepath.Join(actorDir, "configuration.toml")
		if err := os.MkdirAll(actorDir, 0o700); err != nil {
			return r, fmt.Errorf("creating actor dir: %w", err)
		}
		if err := os.WriteFile(cfgPath, rendered, 0o600); err != nil {
			return r, fmt.Errorf("writing rendered kata config: %w", err)
		}
		r.configFile = cfgPath
	}
	return r, nil
}

// RunWorkload boots the actor as a kata + cloud-hypervisor micro-VM.
//
// Contract with atelet (mirrors ateom-gvisor):
//   - The runtime assets (kata shim, guest kernel, rootfs, cloud-hypervisor,
//     virtiofsd) are on disk and configured.
//   - The OCI bundle (config.json + populated rootfs/) is prepared per container.
//
// ateom drives the kata shim v2 ttrpc Task API directly (no containerd daemon):
// Bootstrap (start action) -> Create (boots the CH VM) -> Start. The CH api
// socket then lives at kata.CLHSocketPath(id), which CheckpointWorkload drives.
//
// Proven end-to-end by TestKataLifecycle: boot + run + pause + resume of a
// busybox container in a CH micro-VM, no containerd.
func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	// virtio-blk-rootfs experiment: ateom owns the CH boot itself (no kata shim)
	// and gives the actor a writable boot-time virtio-blk disk (/dev/vdb) as its
	// rootfs, so rootfs writes land off guest RAM -> memory-only snapshot, no
	// balloon. Gated by env so the proven shim path stays the default until this
	// path is validated end-to-end on a cluster.
	if blkRootfsMode() {
		return s.runWorkloadBlkRootfs(ctx, req)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		// POC: one container per sandbox. Multi-container pods are future work.
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}
	containerName := containers[0].GetName()

	// Networking (mirrors ateom-gvisor's veth model): build the per-activation
	// veth into the interior netns and point kata at it; kata wires the guest
	// to the stable actor address via its tap + TC mirror.
	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Run failure", slog.Any("err", cleanupErr))
			}
		}
	}()

	bundle := ateompath.OCIBundlePath(ns, name, id, containerName)
	spec, err := ensureKataCompatibleSpec(bundle, id, ateompath.AteomNetNSPath(s.podUID))
	if err != nil {
		return nil, fmt.Errorf("while preparing kata OCI spec: %w", err)
	}

	actorDir := ateompath.ActorPath(ns, name, id)
	rr, err := s.resolveRuntime(actorDir, req.GetRuntimeAssetPaths())
	if err != nil {
		return nil, fmt.Errorf("while resolving runtime assets: %w", err)
	}
	shim := &kata.Shim{
		Binary:    rr.shim,
		ID:        id,
		Bundle:    bundle,
		Namespace: s.namespace,
		// No containerd: these only need to be stable, unique paths. The shim
		// derives its ttrpc socket from (GRPCAddress, Namespace, ID) and would
		// publish events to TTRPCAddress (best-effort, logged if unreachable).
		GRPCAddress:  filepath.Join(actorDir, "shim-containerd.sock"),
		TTRPCAddress: filepath.Join(actorDir, "shim-containerd.sock.ttrpc"),
		ConfigFile:   rr.configFile,
		Diagnostics:  slogWriter{ctx},
		Debug:        s.kataDebug,
	}

	// Clear any leftover kata host-side state for this (deterministic) sandbox id
	// from a prior failed/torn-down attempt, so the shim's share + virtiofsd
	// socket setup doesn't collide ("address already in use" / "directory not
	// empty"). Since we drive the shim directly, failed Creates don't self-clean.
	kata.CleanupSandboxState(id)

	slog.InfoContext(ctx, "Bootstrapping kata shim", slog.String("id", id), slog.String("bundle", bundle))
	if err := shim.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("while bootstrapping kata shim: %w", err)
	}
	// On any failure after Bootstrap, tear the shim down so the pod isn't wedged.
	// Kill the shim FIRST (don't call its graceful Shutdown/CleanupAction, which
	// drive kata's container-stop → can nil-deref on the dying VM), then let
	// CleanupSandboxState sweep the orphaned CH/virtiofsd + host state. Same
	// ordering as teardownActor.
	defer func() {
		if retErr != nil {
			_ = shim.Close()
			kata.CleanupSandboxState(id)
		}
	}()

	// Pass the rootfs as a bind mount, mirroring what containerd's snapshotter
	// provides: the mount SOURCE must differ from the target (<bundle>/rootfs),
	// so kata's shim bind-mounts source->target and virtio-fs-shares the target.
	// atelet populates <bundle>/rootfs directly; a self-bind (source==target) is
	// not shared, leaving the guest rootfs empty -> agent createContainer ENOENT.
	// Relocate the populated rootfs to a sibling source dir (once per bundle) and
	// point kata at it, with an empty <bundle>/rootfs as the mount target.
	rootfsDir := filepath.Join(bundle, "rootfs")
	rootfsSrc := filepath.Join(bundle, "rootfs-src")
	if fi, statErr := os.Stat(rootfsSrc); statErr != nil || !fi.IsDir() {
		if err := os.Rename(rootfsDir, rootfsSrc); err != nil {
			return nil, fmt.Errorf("relocating populated rootfs to source dir: %w", err)
		}
	}
	if err := os.MkdirAll(rootfsDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating rootfs mount target: %w", err)
	}
	// Create the "carrier": the shim boots the VM (+agent+virtiofsd) and shares
	// the RO base over virtio-fs at /run/kata-containers/shared/containers/<id>/
	// rootfs. It is created but NOT started — the carrier exists only to boot the
	// sandbox + serve the base; the actual workload is created by ateom below.
	if _, err := shim.Create(ctx, kata.CreateOptions{
		Rootfs: []*ctrtypes.Mount{{
			Type:    "bind",
			Source:  rootfsSrc,
			Options: []string{"bind", "rw"},
		}},
	}); err != nil {
		return nil, fmt.Errorf("while creating carrier/VM: %w", err)
	}
	slog.InfoContext(ctx, "Micro-VM created", slog.String("clh_socket", kata.CLHSocketPath(id)))

	// "Be your own hook scheduler": drive the STOCK kata-agent over ttrpc to
	// assemble the overlay rootfs (RO virtio-fs base + guest-tmpfs upper) and
	// start the actual workload — no patched shim. The workload joins the
	// carrier's sandbox (same guest eth0 = actor IP) and lives in the VM RAM, so
	// CH snapshot/restore captures it like any shim-created container.
	ovlID := id + "-ovl"
	vsockPath := kata.VsockSocketPath(id)
	if !waitForFile(vsockPath, 10*time.Second) {
		return nil, fmt.Errorf("kata-agent vsock socket %q did not appear", vsockPath)
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, 20*time.Second)
	ac, err := kata.DialAgent(dialCtx, vsockPath)
	dialCancel()
	if err != nil {
		return nil, fmt.Errorf("while dialing kata-agent ttrpc: %w", err)
	}
	defer ac.Close()
	wlCtx, wlCancel := context.WithTimeout(ctx, 30*time.Second)
	err = ac.StartOverlayWorkload(wlCtx, id, ovlID, spec)
	wlCancel()
	if err != nil {
		// Diagnostic: dump the guest layout via the kata debug console so we can
		// see why the overlay storages didn't resolve. Best-effort.
		dump := kata.DebugConsoleDump(ctx, vsockPath,
			"echo '== /run/kata-containers =='; ls -la /run/kata-containers/ 2>&1; "+
				"echo '== shared/containers =='; ls -la /run/kata-containers/shared/containers/ 2>&1; "+
				"echo '== carrier "+id+" =='; ls -la /run/kata-containers/"+id+"/ 2>&1; "+
				"echo '== carrier rootfs =='; ls /run/kata-containers/"+id+"/rootfs/ 2>&1 | head; "+
				"echo '== mounts(kata) =='; grep kata-containers /proc/mounts 2>&1")
		slog.ErrorContext(ctx, "overlay workload failed; guest debug-console dump", slog.String("dump", dump))
		return nil, fmt.Errorf("while starting overlay workload via agent: %w", err)
	}

	s.running[id] = &runningActor{shim: shim, containerName: containerName, ovlContainerID: ovlID, baseID: id}
	slog.InfoContext(ctx, "Actor started", slog.String("id", id), slog.String("workload", ovlID))
	return &ateompb.RunWorkloadResponse{}, nil
}

// runWorkloadBlkRootfs is the ateom-owned-boot path (gated by ATEOM_VIRTIO_BLK_ROOTFS):
// ateom boots cloud-hypervisor itself — no kata shim — and gives the actor a
// writable boot-time virtio-blk disk (/dev/vdb, built from the OCI bundle rootfs)
// as its container rootfs. Rootfs writes land on that host-backed disk (off guest
// RAM), so the CH snapshot is memory-only with no balloon and no virtiofsd
// find-paths. It replicates the kata clh boot (vm.create kernel+image, add-net,
// vm.boot) and the shim's post-boot work (agent CreateSandbox + guest network
// config) before driving the agent to start the blk-rootfs container.
func (s *AteomService) runWorkloadBlkRootfs(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}
	containerName := containers[0].GetName()

	// Owned-boot builds the CH vm.create itself, so it needs the guest kernel +
	// image paths directly. resolveRuntime still renders the config (for the agent
	// kernel_params + mem/vcpu sizing) and resolves the CH binary.
	paths := req.GetRuntimeAssetPaths()
	kernel, image := paths[assetKernel], paths[assetImage]
	if kernel == "" || image == "" {
		return nil, fmt.Errorf("owned-boot requires %q and %q asset paths", assetKernel, assetImage)
	}
	actorDir := ateompath.ActorPath(ns, name, id)
	rr, err := s.resolveRuntime(actorDir, paths)
	if err != nil {
		return nil, fmt.Errorf("while resolving runtime assets: %w", err)
	}

	// Networking (host side): per-activation veth into the interior netns. The
	// tap + TC mirror is built below (after the VM exists) so its FDs are fresh.
	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Run failure", slog.Any("err", cleanupErr))
			}
		}
	}()

	bundle := ateompath.OCIBundlePath(ns, name, id, containerName)
	spec, err := ensureKataCompatibleSpec(bundle, id, ateompath.AteomNetNSPath(s.podUID))
	if err != nil {
		return nil, fmt.Errorf("while preparing kata OCI spec: %w", err)
	}

	// Build the actor's writable rootfs as a raw ext4 virtio-blk disk from the
	// atelet-populated OCI bundle rootfs. This becomes /dev/vdb.
	diskPath := filepath.Join(actorDir, actorRootfsDiskName)
	if err := kata.BuildExt4Image(ctx, filepath.Join(bundle, "rootfs"), diskPath, actorRootfsSizeMiB); err != nil {
		return nil, fmt.Errorf("while building actor rootfs disk: %w", err)
	}

	// Sizing + agent params from the rendered kata config.
	var cfgBytes []byte
	if rr.configFile != "" {
		cfgBytes, _ = os.ReadFile(rr.configFile)
	}
	memMiB := kata.ConfigMemoryMiB(cfgBytes, 2048)
	vcpus := kata.ConfigVCPUs(cfgBytes, 1)
	kparams := kata.ConfigKernelParams(cfgBytes)

	// Clean stale per-sandbox state + create the runtime dir for the sockets.
	kata.CleanupSandboxState(id)
	if err := os.MkdirAll(kata.VMDir(id), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// Launch a bare VMM (CH + api-socket); ateom owns this process for teardown.
	apiSocket := filepath.Join(kata.VMDir(id), "clh-api.sock")
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary:    rr.ch,
		APISocket: apiSocket,
		Stdout:    slogWriter{ctx},
		Stderr:    slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
			_, _ = chCmd.Process.Wait()
		}
	}()

	// Kernel cmdline: replicate kata's clh boot cmdline (verified against a live
	// kata snapshot's payload.cmdline). Beyond the root/clh base params it MUST
	// include systemd.unit=kata-containers.target (else systemd boots the default
	// target and powers off — the guest exits ~6s in) and mask systemd-networkd
	// (the agent owns eth0). The console is ARCH-SPECIFIC: ttyAMA0 (PL011) on
	// arm64, ttyS0 (8250) on amd64 — wrong console => "unable to open an initial
	// console". The config's kernel_params (agent.* etc.) are appended. Serial is
	// captured to a file for boot debugging.
	serialLog := filepath.Join(kata.VMDir(id), "serial.log")
	console := "ttyS0"
	if runtime.GOARCH == "arm64" {
		console = "ttyAMA0"
	}
	cmdline := "root=/dev/vda1 rootflags=data=ordered,errors=remount-ro ro rootfstype=ext4 " +
		"panic=1 no_timer_check noreplace-smp console=" + console + ",115200n8 " +
		"systemd.unit=kata-containers.target systemd.mask=systemd-networkd.service systemd.mask=systemd-networkd.socket"
	if kparams != "" {
		cmdline += " " + kparams
	}
	cfg := ch.VmConfig{
		Cpus:    ch.CpusConfig{BootVcpus: int32(vcpus), MaxVcpus: int32(vcpus)},
		Memory:  ch.MemoryConfig{Size: int64(memMiB) * 1024 * 1024, Shared: true},
		Payload: ch.PayloadConfig{Kernel: kernel, Cmdline: cmdline},
		Disks: []ch.DiskConfig{
			{Path: image, Readonly: true, ImageType: "Raw", NumQueues: int32(vcpus), QueueSize: 1024},
			{Path: diskPath, Readonly: false, ImageType: "Raw", NumQueues: int32(vcpus), QueueSize: 1024},
		},
		Rng:    &ch.RngConfig{Src: "/dev/urandom"},
		Serial: &ch.ConsoleConfig{Mode: "File", File: serialLog},
		Vsock:  &ch.VsockConfig{Cid: 3, Socket: kata.VsockSocketPath(id)},
	}
	if err := client.CreateVM(ctx, cfg); err != nil {
		return nil, fmt.Errorf("while creating VM: %w", err)
	}

	// Network device: build the tap + TC mirror against the actor veth and add a
	// virtio-net to the created (pre-boot) VM with the tap FDs (SCM_RIGHTS).
	tapFiles, err := s.setupRestoreTap(ctx, "tap0_kata", 1)
	if err != nil {
		return nil, fmt.Errorf("while building tap: %w", err)
	}
	defer func() {
		for _, f := range tapFiles {
			_ = f.Close() // CH dups adopted FDs; ours always close.
		}
	}()
	var fds []int
	for _, f := range tapFiles {
		fds = append(fds, int(f.Fd()))
	}
	if err := client.AddNetWithFDs(ctx, actorGuestMAC, 2*len(tapFiles), fds); err != nil {
		return nil, fmt.Errorf("while adding net device: %w", err)
	}

	// Boot.
	if err := client.BootVM(ctx); err != nil {
		return nil, fmt.Errorf("while booting VM: %w", err)
	}
	slog.InfoContext(ctx, "Micro-VM booted (owned-boot)", slog.String("id", id), slog.String("api", apiSocket))

	// Dial the kata-agent over hybrid-vsock. The agent only starts listening once
	// the guest's init reaches kata-containers.target — well after CH creates the
	// vsock socket file — so poll the CONNECT until it answers (as the kata shim
	// does), rather than dialing once.
	vsockPath := kata.VsockSocketPath(id)
	if !waitForFile(vsockPath, 15*time.Second) {
		return nil, fmt.Errorf("kata-agent vsock socket %q did not appear", vsockPath)
	}
	ac, err := dialAgentRetry(ctx, vsockPath, 60*time.Second)
	if err != nil {
		if b, rerr := os.ReadFile(serialLog); rerr == nil {
			slog.ErrorContext(ctx, "agent dial failed; guest serial tail", slog.String("serial", tailString(string(b), 3000)))
		}
		return nil, fmt.Errorf("while dialing kata-agent: %w", err)
	}
	defer ac.Close()

	// Establish the agent sandbox (the shim normally does this at boot).
	sbCtx, sbCancel := context.WithTimeout(ctx, 20*time.Second)
	err = ac.CreateSandbox(sbCtx, &agentpb.CreateSandboxRequest{Hostname: spec.Hostname, SandboxId: id})
	sbCancel()
	if err != nil {
		return nil, fmt.Errorf("while creating agent sandbox: %w", err)
	}

	// Configure guest networking (the shim's job): eth0 IP/MAC/MTU, routes, ARP.
	mtu := uint64(s.actorVethMTU(ctx))
	netCtx, netCancel := context.WithTimeout(ctx, 20*time.Second)
	err = s.configureGuestNetwork(netCtx, ac, mtu)
	netCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath, "ip addr 2>&1; echo '== route =='; ip route 2>&1; echo '== neigh =='; ip neigh 2>&1")
		slog.ErrorContext(ctx, "guest network config failed; dump", slog.String("dump", dump))
		return nil, fmt.Errorf("while configuring guest network: %w", err)
	}

	// Start the actor with its rootfs on /dev/vdb (single blk storage).
	wlCtx, wlCancel := context.WithTimeout(ctx, 30*time.Second)
	err = ac.StartBlkWorkload(wlCtx, id, "/dev/vdb", spec)
	wlCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath,
			"echo '== /dev/vdb =='; ls -l /dev/vdb 2>&1; blkid /dev/vdb 2>&1; "+
				"echo '== mounts =='; grep kata /proc/mounts 2>&1")
		slog.ErrorContext(ctx, "blk workload failed; dump", slog.String("dump", dump))
		return nil, fmt.Errorf("while starting blk workload: %w", err)
	}

	s.running[id] = &runningActor{chCmd: chCmd, apiSocket: apiSocket, containerName: containerName, ovlContainerID: id, baseID: id}
	slog.InfoContext(ctx, "Actor started (owned-boot, virtio-blk rootfs)", slog.String("id", id))
	return &ateompb.RunWorkloadResponse{}, nil
}

// dialAgentRetry polls DialAgent until the kata-agent answers the hybrid-vsock
// CONNECT (the socket file exists at boot, but the agent only listens once the
// guest reaches kata-containers.target) or timeout elapses.
func dialAgentRetry(ctx context.Context, vsockPath string, timeout time.Duration) (*kata.AgentClient, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ac, err := kata.DialAgent(dctx, vsockPath)
		cancel()
		if err == nil {
			return ac, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// tailString returns the last n bytes of s (for logging a serial-console tail).
func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// configureGuestNetwork replicates the kata shim's guest network setup over the
// agent: configure eth0 (IP/MAC/MTU), install the connected + default routes, and
// pin the gateway's ARP entry to its fixed MAC (so a restored guest's frozen
// neighbor entry stays valid).
func (s *AteomService) configureGuestNetwork(ctx context.Context, ac *kata.AgentClient, mtu uint64) error {
	if err := ac.UpdateInterface(ctx, &agentpb.Interface{
		Device: actorVethName,
		Name:   actorVethName,
		HwAddr: actorGuestMAC,
		Mtu:    mtu,
		IPAddresses: []*agentpb.IPAddress{
			{Family: agentpb.IPFamily_v4, Address: actorVethIP, Mask: "30"},
		},
	}); err != nil {
		return err
	}
	if err := ac.UpdateRoutes(ctx, []*agentpb.Route{
		{Dest: actorVethSubnet, Device: actorVethName, Scope: uint32(unix.RT_SCOPE_LINK), Family: agentpb.IPFamily_v4},
		{Dest: "", Gateway: actorVethGateway, Device: actorVethName, Family: agentpb.IPFamily_v4},
	}); err != nil {
		return err
	}
	return ac.AddARPNeighbors(ctx, []*agentpb.ARPNeighbor{{
		ToIPAddress: &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: actorVethGateway},
		Device:      actorVethName,
		Lladdr:      hostVethMAC,
		State:       0x80, // NUD_PERMANENT
	}})
}

// waitForFile polls for path to exist, up to d. Used to wait for the kata-agent
// hybrid-vsock socket the shim creates during VM boot before dialing it.
func waitForFile(path string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ensureKataCompatibleSpec augments the bundle's config.json with the fields
// kata's OCI conversion requires but atelet's (gVisor-oriented) spec omits.
// Without linux.resources, kata's ContainerConfig nil-derefs and the shim
// crashes. This shaper is a bridge; a future atelet change should emit
// runtime-appropriate specs so it can retire.
func ensureKataCompatibleSpec(bundle, id, netnsPath string) (*specs.Spec, error) {
	specPath := filepath.Join(bundle, "config.json")
	b, err := os.ReadFile(specPath)
	if err != nil {
		return nil, fmt.Errorf("reading %q: %w", specPath, err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, fmt.Errorf("parsing %q: %w", specPath, err)
	}

	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.Resources == nil {
		spec.Linux.Resources = defaultKataResources()
	}
	if spec.Linux.CgroupsPath == "" {
		spec.Linux.CgroupsPath = "/ateomchv/" + id
	}

	// atelet's spec carries gVisor pause-model CRI annotations
	// (container-type=container, sandbox-id=pause). kata reads those and waits
	// for a separate "pause" sandbox that we never create, failing with "the
	// sandbox hasn't been created". Strip them so kata treats this single
	// container as its own sandbox (creates the VM), as in the integration tests.
	for k := range spec.Annotations {
		if strings.HasPrefix(k, "io.kubernetes.cri.") {
			delete(spec.Annotations, k)
		}
	}

	// NB: no virtio-fs-overlay annotation here. With the STOCK shim, this spec is
	// for the "carrier" container that only boots the VM + shares the RO base over
	// virtio-fs. ateom assembles the actual overlay rootfs itself by driving the
	// kata-agent CreateContainer over ttrpc (see RunWorkload) — no patched shim.

	// Point the network namespace at our interior netns (which holds the pod's
	// eth0); kata finds eth0 there and wires it to the VM's virtio-net.
	netnsSet := false
	for i := range spec.Linux.Namespaces {
		if spec.Linux.Namespaces[i].Type == specs.NetworkNamespace {
			spec.Linux.Namespaces[i].Path = netnsPath
			netnsSet = true
		}
	}
	if !netnsSet {
		spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
			Type: specs.NetworkNamespace, Path: netnsPath,
		})
	}

	// Replace atelet's gVisor-oriented mounts (minimal /dev tmpfs, a
	// /etc/resolv.conf host bind that ENOENTs against the distroless rootfs) with
	// the exact set `ctr run --runtime io.containerd.kata.v2` emits, which kata's
	// agent accepts. (POC shaper; pod DNS integration is future work.)
	spec.Mounts = defaultKataMounts()

	out, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshaling spec: %w", err)
	}
	if err := os.WriteFile(specPath, out, 0o600); err != nil {
		return nil, fmt.Errorf("writing %q: %w", specPath, err)
	}
	return &spec, nil
}

// defaultKataMounts mirrors the mount set `ctr run --runtime io.containerd.kata.v2`
// produces (the proven-good shape for the kata agent).
func defaultKataMounts() []specs.Mount {
	return []specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		{Destination: "/run", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
	}
}

// defaultKataResources mirrors the device allowlist + cpu shares that
// `ctr run --runtime io.containerd.kata.v2` emits (the proven-good shape).
func defaultKataResources() *specs.LinuxResources {
	dev := func(t string, major, minor int64, access string) specs.LinuxDeviceCgroup {
		d := specs.LinuxDeviceCgroup{Allow: true, Type: t, Access: access}
		if major != 0 {
			d.Major = &major
		}
		if minor >= 0 {
			d.Minor = &minor
		}
		return d
	}
	shares := uint64(1024)
	return &specs.LinuxResources{
		Devices: []specs.LinuxDeviceCgroup{
			{Allow: false, Access: "rwm"},
			dev("c", 1, 3, "rwm"),    // /dev/null
			dev("c", 1, 8, "rwm"),    // /dev/random
			dev("c", 1, 7, "rwm"),    // /dev/full
			dev("c", 5, 0, "rwm"),    // /dev/tty
			dev("c", 1, 5, "rwm"),    // /dev/zero
			dev("c", 1, 9, "rwm"),    // /dev/urandom
			dev("c", 5, 1, "rwm"),    // /dev/console
			dev("c", 136, -1, "rwm"), // pts
			dev("c", 5, 2, "rwm"),    // /dev/ptmx
		},
		CPU: &specs.LinuxCPU{Shares: &shares},
	}
}

// reclaimMargin is the resume headroom (bytes) the pre-snapshot balloon reclaim
// leaves below the guest's resident floor. Reclaiming all the way to the floor
// leaves the restored guest too tight to vm.resume; a generous margin is ~free
// (the snapshot size plateaus at the floor regardless). Bench-validated at 256MiB.
const reclaimMargin = 256 << 20

// CheckpointWorkload suspends the actor and writes a portable CH snapshot.
//
// Contract with atelet (mirrors ateom-gvisor): after we return, atelet uploads
// the checkpoint dir to object storage, then tears down bundles and resets the
// actor dir.
//
// ateom drives CH's REST api-socket (the one kata created at CLHSocketPath(id)):
// pause -> snapshot file://<CheckpointStateDir> (config.json + state.json +
// sparse memory-ranges) -> tear the sandbox down (shim + VMM). Driving CH's
// REST API under a kata-owned VMM does not corrupt shim state.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	// kata-shim-owned actors expose CH at kata's socket; restored (ateom-owned)
	// actors at the socket we launched the VMM on.
	ra := s.running[id]
	chSocket := kata.CLHSocketPath(id)
	if ra != nil && ra.apiSocket != "" {
		chSocket = ra.apiSocket
	}
	client := ch.NewClient(chSocket)
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		return nil, fmt.Errorf("while waiting for CH api-socket: %w", err)
	}

	// The owned-boot / virtio-blk-rootfs path is the whole reason this approach
	// exists: the actor's rootfs writes are on the host-backed /dev/vdb (NOT guest
	// RAM), so there is no overlay tmpfs upper to wipe and no balloon to inflate —
	// the snapshot is naturally memory-only and small. Skip both steps; just
	// pause+snapshot below. (Reset-to-golden happens at restore by recreating the
	// disk — Phase 3 — not by an in-guest wipe.)
	if !blkRootfsMode() {
		// Reset-to-golden (gVisor semantics, the default): wipe the overlay tmpfs upper
		// in the guest so the actor's rootfs writes do NOT persist across
		// checkpoint/restore — on resume the rootfs is the golden image again. The
		// upper is at /run/kata-containers/<id>-ovl/fs in the guest PID-1 mount ns
		// (deterministic from id; not in the container's ns), reached via the kata
		// debug console (vsock 1026). This frees tmpfs pages (then claimed by the
		// reclaim below) and is restore-safe: deleted upper files are not re-faulted
		// from virtio-fs (unlike dropping the virtio-fs read cache, which is). Must run
		// while the guest is running (before Pause). Best-effort.
		ovlUpper := "/run/kata-containers/" + id + "-ovl/fs"
		// Count entries before/after so the log proves the wipe actually cleared the
		// upper (upper_before=N>0, upper_after=0). The $-vars are evaluated by the guest
		// shell; DebugConsoleDump ignores the PTY's echo of this command line.
		wipeCmd := "b=$(ls -A " + ovlUpper + " 2>/dev/null | wc -l); " +
			"rm -rf " + ovlUpper + "/* " + ovlUpper + "/.[!.]* 2>/dev/null; sync; " +
			"a=$(ls -A " + ovlUpper + " 2>/dev/null | wc -l); echo upper_before=$b upper_after=$a"
		wipeOut := kata.DebugConsoleDump(ctx, kata.VsockSocketPath(id), wipeCmd)
		if strings.Contains(wipeOut, "upper_after=0") {
			slog.InfoContext(ctx, "Reset overlay upper to golden", slog.String("id", id),
				slog.String("console", strings.TrimSpace(wipeOut)))
		} else {
			// No "upper_after=0" in the output => the wipe didn't confirm empty (debug
			// console hiccup, or writes remain). Surface it; the rootfs writes may
			// persist this checkpoint. Best-effort: continue to snapshot regardless.
			slog.WarnContext(ctx, "Reset-to-golden did NOT confirm empty upper; continuing",
				slog.String("id", id), slog.String("console", strings.TrimSpace(wipeOut)))
		}

		// gVisor-parity snapshot shrink: free guest memory BEFORE pausing (the balloon
		// only hands pages over while the guest runs) so the sparse snapshot drops them.
		// Host-side balloon inflate->deflate-to-margin via the CH api; needs the VM to
		// have been created with reclaim_guest_freed_memory=true. Best-effort — an actor
		// without a balloon (or a transient hiccup) must NOT fail the checkpoint.
		tReclaim := time.Now()
		if usable, rerr := client.ReclaimBeforeSnapshot(ctx, reclaimMargin); rerr != nil {
			slog.WarnContext(ctx, "Pre-snapshot memory reclaim skipped/failed; snapshotting anyway",
				slog.String("id", id), slog.Any("err", rerr))
		} else {
			slog.InfoContext(ctx, "Pre-snapshot memory reclaim done",
				slog.String("id", id), slog.Int64("usable_bytes", usable),
				slog.Duration("reclaim", time.Since(tReclaim)))
		}
	}

	tPause := time.Now()
	if err := client.Pause(ctx); err != nil {
		return nil, fmt.Errorf("while pausing guest: %w", err)
	}
	dPause := time.Since(tPause)

	checkpointDir := ateompath.CheckpointStateDir(ns, name, id)
	// Start from a clean dir so CH's snapshot files are the only contents.
	if err := os.RemoveAll(checkpointDir); err != nil {
		return nil, fmt.Errorf("while clearing checkpoint dir: %w", err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint dir: %w", err)
	}

	// Record the FROZEN base id (the id the guest's virtio-fs find-paths are pinned
	// to, <baseID>/rootfs). For a cold (shim-owned) actor this is its own id; for a
	// restored actor it is the golden id propagated via ra.baseID (set from the
	// snapshot we restored from). RestoreWorkload reads this to lay the
	// reconstructed-from-image base at the path the guest expects. We can NOT derive
	// it from config.json (its socket paths get rewritten to the current id on every
	// restore, losing the invariant golden id).
	baseID := id
	if ra != nil && ra.baseID != "" {
		baseID = ra.baseID
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, baseIDFile), []byte(baseID), 0o600); err != nil {
		return nil, fmt.Errorf("while writing %s: %w", baseIDFile, err)
	}

	// NB: we deliberately do NOT capture the virtio-fs shared dir (the RO base) —
	// it is the golden OCI image, identical every checkpoint (reset-to-golden wipes
	// the upper), and atelet re-unpacks the image to the bundle at restore. So the
	// snapshot is MEMORY-ONLY (config/state/memory-ranges); RestoreWorkload
	// reconstructs the base from the local image (ReconstructSharedDirFromImage),
	// mirroring gVisor ateom. This drops the per-checkpoint shared-dir.tar from the
	// captured/uploaded/downloaded set + the nsenter+tar capture.

	slog.InfoContext(ctx, "Snapshotting guest", slog.String("id", id), slog.String("dir", checkpointDir))
	tSnapshot := time.Now()
	if err := client.Snapshot(ctx, checkpointDir); err != nil {
		return nil, fmt.Errorf("while snapshotting guest: %w", err)
	}
	dSnapshot := time.Since(tSnapshot)

	// Diff-snapshot completion for an OnDemand-restored actor: CH's snapshot here is
	// sparse — only the pages faulted in since the OnDemand restore — so on its own
	// it's INCOMPLETE (the un-faulted pages were being demand-paged from the restore
	// source). Overlay it onto that source to rebuild a COMPLETE memory-ranges, so the
	// snapshot is self-contained and re-restorable. (A cold-run actor has no restore
	// source and its snapshot is already complete — no merge.)
	if ra != nil && ra.restoreSourceDir != "" {
		base := filepath.Join(ra.restoreSourceDir, "memory-ranges")
		delta := filepath.Join(checkpointDir, "memory-ranges")
		tMerge := time.Now()
		if err := ch.MergeSparseOverlay(ctx, base, delta, delta); err != nil {
			return nil, fmt.Errorf("while merging OnDemand delta onto restore source: %w", err)
		}
		slog.InfoContext(ctx, "Merged OnDemand delta onto restore source (complete snapshot)",
			slog.String("id", id), slog.Duration("merge", time.Since(tMerge)))
	}

	// reset-to-golden support (owned-boot): save the actor's /dev/vdb AS-OF this
	// (paused, consistent) snapshot as a verbatim golden template, so future
	// restores can recreate the disk byte-identical to what the snapshot's guest RAM
	// expects while discarding the actor's later rootfs writes. Saved once (the
	// first/golden checkpoint) and kept; best-effort (without it, restore reopens the
	// live disk = Phase 2 continuity). TODO: ship the template with the snapshot for
	// cross-node restore (it's golden, shipped once per template, like the OCI base).
	if blkRootfsMode() {
		actorDir := ateompath.ActorPath(ns, name, id)
		if tmpl := filepath.Join(actorDir, goldenRootfsDiskName); fileMissing(tmpl) {
			if cerr := copyDiskFile(ctx, filepath.Join(actorDir, actorRootfsDiskName), tmpl); cerr != nil {
				slog.WarnContext(ctx, "Failed to save golden rootfs template; restore will reopen live disk", slog.Any("err", cerr))
			} else {
				slog.InfoContext(ctx, "Saved golden rootfs disk template", slog.String("id", id))
			}
		}
	}

	// Report exactly the files we wrote so atelet ships precisely the CH snapshot
	// (config.json + state.json + memory-ranges + base-id), not gVisor's fixed set.
	// Memory-only: the RO base is reconstructed from the OCI image at restore.
	snapshotFiles, err := listFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("while listing snapshot files: %w", err)
	}

	// Tear down: the actor returns to "available". Best-effort; the snapshot is
	// already on disk for atelet to ship.
	tTeardown := time.Now()
	s.teardownActor(ctx, id, ra, client)
	dTeardown := time.Since(tTeardown)
	delete(s.running, id)

	// Tear down the per-activation actor network (mirrors gVisor).
	if err := s.cleanupActorNetwork(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to clean up actor network after checkpoint", slog.Any("err", err))
	}

	slog.InfoContext(ctx, "Actor checkpointed", slog.String("id", id), slog.Any("snapshot_files", snapshotFiles),
		slog.Duration("pause", dPause),
		slog.Duration("snapshot", dSnapshot), slog.Duration("teardown", dTeardown))
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles}, nil
}

// listFiles returns the (relative) names of regular files directly under dir.
func listFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			files = append(files, e.Name())
		}
	}
	return files, nil
}

// teardownActor stops the CH VMM and the kata shim for an actor. Best-effort:
// the snapshot is already on disk, so this only needs to release resources. ra
// may be nil (e.g. ateom restarted and lost in-memory state).
//
// Order matters for kata-shim-owned actors: kill the shim process BEFORE
// destroying the VM. If the CH VM is shut down first, the shim's wait goroutine
// observes the task vanish and runs kata's container-stop path, which signals the
// (now-gone) guest agent over vsock and panics on a nil agent connection
// (kata_agent.go signalProcess). Killing the shim first avoids that path
// entirely; CleanupSandboxState then sweeps the orphaned VMM/virtiofsd + host
// state without needing kata's (buggy) graceful teardown.
func (s *AteomService) teardownActor(ctx context.Context, id string, ra *runningActor, client *ch.Client) {
	if ra != nil && ra.shim != nil {
		// SIGKILL the foreground shim server + close ttrpc, before the VM goes.
		_ = ra.shim.Close()
	}

	if client != nil {
		tShutdown := time.Now()
		shutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := client.Shutdown(shutCtx); err != nil {
			slog.WarnContext(ctx, "CH shutdown failed (continuing teardown)", slog.Any("err", err))
		}
		cancel()
		slog.InfoContext(ctx, "CH API shutdown done", slog.Duration("took", time.Since(tShutdown)))
	}

	if ra != nil {
		// ateom-owned (post-restore): kill the CH + virtiofsd we launched.
		if ra.chCmd != nil && ra.chCmd.Process != nil {
			_ = ra.chCmd.Process.Kill()
			_, _ = ra.chCmd.Process.Wait()
		}
		if ra.vfsdCmd != nil && ra.vfsdCmd.Process != nil {
			_ = ra.vfsdCmd.Process.Kill()
			_, _ = ra.vfsdCmd.Process.Wait()
		}
	}

	// Sweep kata's host-side state + any orphaned per-sandbox processes (e.g. a
	// shim-owned actor's now-parentless virtiofsd). This is ateom's own cleanup
	// (process kill + unmount + rm); it never calls into the kata agent, so it
	// can't hit the teardown panic above.
	kata.CleanupSandboxState(id)
}

// RestoreWorkload restores the actor on a (possibly different) pod by relaunching
// cloud-hypervisor directly from the downloaded snapshot — bypassing the kata
// shim — and resuming.
//
// Contract with atelet: the memory-only snapshot dir (config.json + state.json +
// memory-ranges + base-id) has been downloaded to RestoreStateDir.
//
// Steps:
//  1. Reconstruct the virtio-fs shared dir (the RO base) from the LOCAL OCI image
//     (atelet re-unpacks it to the bundle at restore) at the frozen <baseID>/rootfs
//     layout the snapshot references, so find-paths can re-open the inodes.
//  2. Start virtiofsd on the vhost-user socket the snapshot expects.
//  3. Relaunch CH with --restore source_url=file://<dir> (ch.Restore), which
//     reconnects to virtiofsd, recreates vsock, and reloads guest RAM (paused).
//  4. Resume.
//
// The snapshot's config.json references kernel + guest-OS image at static node
// paths (present on any kata node) and per-sandbox sockets under VMDir(id); this
// POC restores under the same id, so those paths line up. ateom then owns the CH.
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	if blkRootfsMode() {
		return s.restoreWorkloadBlkRootfs(ctx, req)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	restoreDir := ateompath.RestoreStateDir(ns, name, id)

	// Per-step timing so we can see where the ateom-side of resume goes (the rustfs
	// download/decompress is timed separately by atelet). Logged in one line at the end.
	tStart := time.Now()
	var dRecon, dNet, dVfsd, dLaunch, dRestoreCH, dResume time.Duration

	// Resolve the cloud-hypervisor + virtiofsd binaries (fetched assets or flags).
	rr, err := s.resolveRuntime(ateompath.ActorPath(ns, name, id), req.GetRuntimeAssetPaths())
	if err != nil {
		return nil, fmt.Errorf("while resolving runtime assets: %w", err)
	}

	// Clear leftover per-sandbox state/processes from prior failed attempts
	// (stale virtiofsd holding the vhost-user socket, etc.).
	kata.CleanupSandboxState(id)

	// The snapshot's config.json references the source actor's socket paths.
	// Rewrite them to this actor's VMDir so the sockets we create are the ones CH
	// reopens. (The frozen base id for the shared-dir layout comes from the base-id
	// file below, NOT these socket paths — they were rewritten to the current id on
	// the prior restore and so don't carry the invariant golden id.)
	if err := rewriteSnapshotSocketPaths(restoreDir, id); err != nil {
		return nil, fmt.Errorf("while rewriting snapshot socket paths: %w", err)
	}

	// The guest's virtio-fs find-paths are frozen to <baseID>/rootfs, where baseID
	// is the id the RO base was FIRST shared under (the golden cold-run id) — it is
	// invariant across this actor's whole restore lineage. CheckpointWorkload records
	// it in the base-id snapshot file; read it back to lay the base where the guest
	// expects it.
	srcBytes, err := os.ReadFile(filepath.Join(restoreDir, baseIDFile))
	if err != nil {
		return nil, fmt.Errorf("while reading snapshot %s: %w", baseIDFile, err)
	}
	srcID := strings.TrimSpace(string(srcBytes))
	if srcID == "" {
		return nil, fmt.Errorf("empty %s in snapshot", baseIDFile)
	}

	// 1. Reconstruct the virtio-fs shared dir (the RO base) from the LOCAL OCI
	// image — atelet unpacks the image to the bundle rootfs at restore (same as at
	// Run), and reset-to-golden means the base is always the golden image. This
	// makes the snapshot memory-only (no per-checkpoint shared-dir.tar), mirroring
	// gVisor ateom (rootfs from the image at restore).
	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}
	bundleRootfs := filepath.Join(ateompath.OCIBundlePath(ns, name, id, containers[0].GetName()), "rootfs")
	tRecon := time.Now()
	if err := kata.ReconstructSharedDirFromImage(ctx, bundleRootfs, id, srcID); err != nil {
		return nil, fmt.Errorf("while reconstructing shared dir from image: %w", err)
	}
	dRecon = time.Since(tRecon)

	// kata's per-sandbox runtime dir holds the sockets the snapshot references.
	if err := os.MkdirAll(kata.VMDir(id), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// 2. Networking: rebuild the per-activation actor veth, then recreate
	// kata's tap + TC mirror against it. The snapshot's virtio-net device is
	// fd-backed, so CH requires fresh tap FDs on restore (net_fds). The guest's
	// frozen network config (stable actor address, gateway with a fixed MAC)
	// remains valid as-is — no in-guest reconfiguration.
	tNet := time.Now()
	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Restore failure", slog.Any("err", cleanupErr))
			}
		}
	}()
	netDevs, err := ch.SnapshotNetDevices(restoreDir)
	if err != nil {
		return nil, fmt.Errorf("while reading snapshot net devices: %w", err)
	}
	var restoredNets []ch.RestoredNet
	var tapFiles []*os.File
	defer func() {
		// CH dups the FDs it adopts (SCM_RIGHTS), so ours close unconditionally.
		for _, f := range tapFiles {
			_ = f.Close()
		}
	}()
	for i, nd := range netDevs {
		files, terr := s.setupRestoreTap(ctx, fmt.Sprintf("tap%d_kata", i), nd.QueuePairs)
		if terr != nil {
			return nil, fmt.Errorf("while building restore tap for %s: %w", nd.ID, terr)
		}
		tapFiles = append(tapFiles, files...)
		rn := ch.RestoredNet{ID: nd.ID}
		for _, f := range files {
			rn.FDs = append(rn.FDs, int(f.Fd()))
		}
		restoredNets = append(restoredNets, rn)
	}
	dNet = time.Since(tNet)

	// 3. Start virtiofsd on the vhost-user socket from config.json.
	tVfsd := time.Now()
	vfsdCmd, err := kata.StartVirtiofsd(ctx, kata.VirtiofsdOptions{
		Binary:     rr.virtiofsd,
		SocketPath: kata.VirtiofsdSocketPath(id),
		SharedDir:  kata.SharedDir(id),
		Log:        slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while starting virtiofsd: %w", err)
	}
	dVfsd = time.Since(tVfsd)
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
		}
	}()

	// 4. Launch a bare VMM and restore with the tap FDs attached (SCM_RIGHTS).
	apiSocket := filepath.Join(kata.VMDir(id), "clh-api-restore.sock")
	slog.InfoContext(ctx, "Restoring CH from snapshot", slog.String("id", id), slog.String("dir", restoreDir))
	tLaunch := time.Now()
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary:    rr.ch,
		APISocket: apiSocket,
		Stdout:    slogWriter{ctx},
		Stderr:    slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM for restore: %w", err)
	}
	dLaunch = time.Since(tLaunch)
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
		}
	}()
	// Eager (copy) memory restore for the legacy virtio-fs path (unchanged).
	tRestoreCH := time.Now()
	if err := client.RestoreWithNetFDs(ctx, restoreDir, restoredNets, ""); err != nil {
		return nil, fmt.Errorf("while restoring VM with net FDs: %w", err)
	}
	dRestoreCH = time.Since(tRestoreCH)

	// 5. Resume (restore comes back paused).
	tResume := time.Now()
	if err := client.Resume(ctx); err != nil {
		return nil, fmt.Errorf("while resuming restored guest: %w", err)
	}
	dResume = time.Since(tResume)

	// The snapshot carries the balloon inflated (deflate-to-margin from the
	// pre-snapshot reclaim). Deflate it fully so the restored actor regains all its
	// RAM. Best-effort — a balloon-less snapshot just no-ops here.
	if _, err := client.MemoryActualSize(ctx); err == nil {
		if derr := client.BalloonResize(ctx, 0); derr != nil {
			slog.WarnContext(ctx, "Post-restore balloon deflate failed", slog.String("id", id), slog.Any("err", derr))
		}
	}

	s.running[id] = &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket, baseID: srcID}
	slog.InfoContext(ctx, "Actor restored", slog.String("id", id),
		slog.Duration("recon_base", dRecon), // cp -a of the RO base image into the find-paths layout
		slog.Duration("net", dNet),          // veth + tap + TC rebuild
		slog.Duration("virtiofsd", dVfsd),   // spawn + wait for socket
		slog.Duration("launch_vmm", dLaunch),
		slog.Duration("restore_ch", dRestoreCH), // CH memory reload (RestoreWithNetFDs)
		slog.Duration("resume", dResume),
		slog.Duration("total", time.Since(tStart)))
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// restoreWorkloadBlkRootfs restores an ateom-owned-boot actor. It is much simpler
// than the shim/virtio-fs restore: there is NO virtiofsd and NO shared-dir to
// reconstruct — the rootfs is the writable /dev/vdb disk, which CH reopens from
// the path recorded in the snapshot config.json. Steps: rewrite the vsock socket
// path to this actor's VMDir, ensure the /dev/vdb backing file is present, rebuild
// the tap (the snapshot's virtio-net is fd-backed → fresh net_fds), relaunch CH
// with --restore, and resume. Guest RAM (incl. the actor's in-memory state and
// frozen network config) comes back from the memory-only snapshot.
//
// Phase 2 (continuity) reopens the SAME disk file left by the original run on this
// node; Phase 3 (reset-to-golden) will recreate it from the golden image first.
func (s *AteomService) restoreWorkloadBlkRootfs(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()
	restoreDir := ateompath.RestoreStateDir(ns, name, id)
	tStart := time.Now()

	rr, err := s.resolveRuntime(ateompath.ActorPath(ns, name, id), req.GetRuntimeAssetPaths())
	if err != nil {
		return nil, fmt.Errorf("while resolving runtime assets: %w", err)
	}
	kata.CleanupSandboxState(id)

	// Repoint the snapshot's vsock socket to this actor's VMDir (the disk + kernel
	// paths are content-addressed/per-actor and already line up on the same node).
	if err := rewriteSnapshotSocketPaths(restoreDir, id); err != nil {
		return nil, fmt.Errorf("while rewriting snapshot socket paths: %w", err)
	}
	srcID := id
	if b, rerr := os.ReadFile(filepath.Join(restoreDir, baseIDFile)); rerr == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			srcID = v
		}
	}
	if err := os.MkdirAll(kata.VMDir(id), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// Recreate the /dev/vdb backing file the snapshot references (the actor dir),
	// reset-to-golden. Two ways, both byte-consistent with the golden snapshot's
	// guest ext4 cache:
	//   - same-node: a verbatim golden template (copyDiskFile) — guaranteed identical.
	//   - cross-node: rebuild from the OCI image atelet unpacked to the bundle at
	//     restore (mkfs.ext4 -d is LAYOUT-deterministic for identical inputs, so the
	//     data blocks land at the same offsets the guest cache expects; only the
	//     superblock UUID/timestamps differ, which are cached in RAM and not re-read).
	// Either way the actor's prior rootfs writes are discarded (gVisor semantics).
	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}
	actorDir := ateompath.ActorPath(ns, name, id)
	diskPath := filepath.Join(actorDir, actorRootfsDiskName)
	if tmpl := filepath.Join(actorDir, goldenRootfsDiskName); !fileMissing(tmpl) {
		if err := copyDiskFile(ctx, tmpl, diskPath); err != nil {
			return nil, fmt.Errorf("while resetting rootfs disk to golden (template): %w", err)
		}
		slog.InfoContext(ctx, "Reset actor rootfs disk to golden (template)", slog.String("id", id))
	} else {
		bundleRootfs := filepath.Join(ateompath.OCIBundlePath(ns, name, id, containers[0].GetName()), "rootfs")
		if err := kata.BuildExt4Image(ctx, bundleRootfs, diskPath, actorRootfsSizeMiB); err != nil {
			return nil, fmt.Errorf("while reconstructing rootfs disk from image: %w", err)
		}
		slog.InfoContext(ctx, "Reconstructed actor rootfs disk from image", slog.String("id", id))
	}

	// Networking: rebuild the per-activation veth + tap; the snapshot's virtio-net
	// is fd-backed, so CH needs fresh tap FDs (net_fds) on restore.
	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Restore failure", slog.Any("err", cleanupErr))
			}
		}
	}()
	netDevs, err := ch.SnapshotNetDevices(restoreDir)
	if err != nil {
		return nil, fmt.Errorf("while reading snapshot net devices: %w", err)
	}
	var restoredNets []ch.RestoredNet
	var tapFiles []*os.File
	defer func() {
		for _, f := range tapFiles {
			_ = f.Close()
		}
	}()
	for i, nd := range netDevs {
		files, terr := s.setupRestoreTap(ctx, fmt.Sprintf("tap%d_kata", i), nd.QueuePairs)
		if terr != nil {
			return nil, fmt.Errorf("while building restore tap for %s: %w", nd.ID, terr)
		}
		tapFiles = append(tapFiles, files...)
		rn := ch.RestoredNet{ID: nd.ID}
		for _, f := range files {
			rn.FDs = append(rn.FDs, int(f.Fd()))
		}
		restoredNets = append(restoredNets, rn)
	}

	// Relaunch CH and restore with the tap FDs attached (SCM_RIGHTS). CH reopens
	// /dev/vda (image) + /dev/vdb (actor rootfs) from the snapshot config paths.
	apiSocket := filepath.Join(kata.VMDir(id), "clh-api-restore.sock")
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary: rr.ch, APISocket: apiSocket, Stdout: slogWriter{ctx}, Stderr: slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM for restore: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
		}
	}()
	// OnDemand (userfaultfd) memory restore: ~75ms vs ~1.8s eager, and it keeps the
	// memfd SPARSE so the next suspend isn't the eager-copy-densified full-RAM scan.
	// CH's OnDemand snapshot alone would be INCOMPLETE (it writes only faulted pages,
	// dropping the un-faulted ones it demand-pages from this source) — so
	// CheckpointWorkload overlays CH's delta onto this source (restoreSourceDir) to
	// rebuild a complete snapshot. CH demand-pages from restoreDir for the VM's whole
	// lifetime, so it must persist until teardown (atelet keeps it until reset).
	if err := client.RestoreWithNetFDs(ctx, restoreDir, restoredNets, "OnDemand"); err != nil {
		return nil, fmt.Errorf("while restoring VM with net FDs: %w", err)
	}
	if err := client.Resume(ctx); err != nil {
		return nil, fmt.Errorf("while resuming restored guest: %w", err)
	}

	s.running[id] = &runningActor{chCmd: chCmd, apiSocket: apiSocket, baseID: srcID, restoreSourceDir: restoreDir}
	slog.InfoContext(ctx, "Actor restored (owned-boot, virtio-blk rootfs)",
		slog.String("id", id), slog.Duration("total", time.Since(tStart)))
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// rewriteSnapshotSocketPaths repoints the snapshot config.json's per-sandbox
// socket paths (virtio-fs vhost-user socket, hybrid-vsock socket) from the source
// actor's VMDir to the restoring actor's VMDir, so the sockets we create are the
// ones CH reopens. Disk/kernel paths are content-addressed static files and
// identical on every node. (The frozen base id for the shared-dir layout comes
// from the base-id snapshot file, NOT from these socket paths — they are rewritten
// per-restore and so do not carry the invariant golden id.)
func rewriteSnapshotSocketPaths(snapshotDir, id string) error {
	cfgPath := filepath.Join(snapshotDir, "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("parsing %q: %w", cfgPath, err)
	}
	if fsList, ok := cfg["fs"].([]any); ok {
		for _, f := range fsList {
			if fm, ok := f.(map[string]any); ok {
				fm["socket"] = kata.VirtiofsdSocketPath(id)
			}
		}
	}
	if vsock, ok := cfg["vsock"].(map[string]any); ok {
		vsock["socket"] = kata.VsockSocketPath(id)
	}
	// The owned-boot path captures the guest serial console to a file under the
	// source actor's VMDir (Serial{Mode:"File"}). On restore that path is stale
	// (points at the golden/source pod's VMDir), so CH's CreateConsoleDevice fails
	// (No such file or directory). Repoint it at this actor's VMDir.
	if serial, ok := cfg["serial"].(map[string]any); ok {
		if mode, _ := serial["mode"].(string); mode == "File" {
			serial["file"] = filepath.Join(kata.VMDir(id), "serial.log")
		}
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfgPath, out, 0o600); err != nil {
		return err
	}
	return nil
}

// slogWriter adapts an io.Writer to slog at info level, for the kata shim's
// start/delete diagnostics.
type slogWriter struct{ ctx context.Context }

func (w slogWriter) Write(p []byte) (int, error) {
	slog.InfoContext(w.ctx, "kata shim", slog.String("out", string(p)))
	return len(p), nil
}
