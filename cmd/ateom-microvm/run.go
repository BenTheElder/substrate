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
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runningActor holds the live state for one actor's micro-VM. ateom owns the
// cloud-hypervisor process directly (booted by RunWorkload or relaunched by
// RestoreWorkload), so it tracks that process and its api-socket for teardown.
type runningActor struct {
	containerName string

	// ovlContainerID is the kata-agent container id of the overlay WORKLOAD (id-ovl),
	// distinct from the carrier container (id). The agent keys the actor's stdout/
	// stderr streams by this id; checkpoint/reset target the upper, not this id.
	ovlContainerID string

	// baseID is the FROZEN base sandbox id propagated across this actor's restore
	// lineage. For a cold-run actor this is the actor's own id; for a restored
	// actor it is the id read from the snapshot's base-id file (the golden id,
	// propagated). CheckpointWorkload writes it back into the next snapshot's
	// base-id file so the chain survives suspend->resume->suspend.
	baseID string

	// ateom owns this CH process (booted at Run or relaunched at Restore).
	chCmd *exec.Cmd
	// vfsdCmd is the virtiofsd serving the overlay RO lower (the CH fs device
	// demand-pages from it for the actor's lifetime). ateom owns it; teardownActor
	// kills it after the CH process. nil on any non-overlay path.
	vfsdCmd *exec.Cmd
	// apiSocket is the CH api-socket for this ateom-owned VMM.
	apiSocket string

	// restoreSourceDir is the snapshot dir this actor was OnDemand-restored from
	// (the base CH is demand-paging from). Set only on the owned-boot virtio-blk
	// path when restored via OnDemand. CheckpointWorkload overlays CH's new (sparse,
	// faulted-only) snapshot onto this base to produce a COMPLETE snapshot (CH's
	// OnDemand snapshot alone drops the un-faulted pages). Empty for cold-run actors
	// (their snapshot is already complete) and for the eager/shim paths.
	restoreSourceDir string

	// logAgent is the kata-agent ttrpc client kept open for the lifetime of the
	// stdout/stderr forwarding goroutines (they pump the container's output via
	// ReadStdout/ReadStderr on this connection). It is NOT closed when RunWorkload /
	// RestoreWorkload return — teardownActor closes it, which makes the in-flight
	// ReadStdout/ReadStderr calls fail and the forwarding goroutines exit (io.EOF).
	// nil if forwarding was not started (e.g. a best-effort post-restore dial failed).
	logAgent *kata.AgentClient
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
	assetCH        = "cloud-hypervisor"
	assetKernel    = "kata-kernel"
	assetImage     = "kata-image"
	assetConfig    = "kata-config"
	assetVirtiofsd = "virtiofsd"
)

// scratchDiskFile / scratchDiskBytes describe the empty writable /dev/vdb disk
// backing the overlay upper. It is sparse (costs nothing until written), formatted
// in-guest (mkfs.ext4), and mounted as the overlayfs upper/work so the actor's
// rootfs writes stay off the snapshot-captured guest RAM.
const (
	scratchDiskFile  = "scratch.img"
	scratchDiskBytes = 1 << 30 // 1 GiB sparse
)

// resolvedRuntime holds the concrete binary/config paths for a request, taken
// from fetched runtime assets when present, else the process flags.
type resolvedRuntime struct {
	ch         string
	configFile string
	// virtiofsd is the vhost-user-fs daemon serving the overlay RO lower. Empty
	// falls back to "virtiofsd" on PATH (StartVirtiofsd's default).
	virtiofsd string
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// resolveRuntime resolves the cloud-hypervisor binary + the kata config path from
// fetched assets, falling back to flags.
func (s *AteomService) resolveRuntime(paths map[string]string) resolvedRuntime {
	return resolvedRuntime{
		ch:         firstNonEmpty(paths[assetCH], s.chBinary),
		configFile: firstNonEmpty(paths[assetConfig], s.kataConfig),
		virtiofsd:  paths[assetVirtiofsd],
	}
}

// RunWorkload boots the actor as a cloud-hypervisor micro-VM that ateom owns.
//
// ateom boots cloud-hypervisor itself — no kata shim — and gives the actor an
// OVERLAY rootfs: the OCI image is served read-only over virtio-fs (the lower, by
// virtiofsd find-paths) and a small empty boot-time virtio-blk disk (/dev/vdb) is
// the writable ext4 upper. Per-actor setup is O(1) (bind the image, format an empty
// upper) instead of O(image), and rootfs writes land on the host-backed upper (off
// guest RAM), so the CH snapshot stays small. It replicates the kata clh boot
// (vm.create kernel+image+fs, add-net, vm.boot) and the shim's post-boot work (agent
// CreateSandbox + guest network config) before driving the kata-agent to create the
// carrier + start the overlay container.
//
// Contract with atelet (mirrors ateom-gvisor):
//   - The runtime assets (guest kernel, guest OS image, cloud-hypervisor, base
//     kata config) are on disk and passed as runtime asset paths.
//   - The OCI bundle (config.json + populated rootfs/) is prepared per container.
func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	s.actorLogger.EmitLifecycleLog("Actor starting", id, name, ns)

	// KNOWN GAP vs the gVisor runtime: it runs multiple containers per actor; this
	// runtime is single-container for now. Multi-container is a mechanical extension
	// (one boot-time virtio-blk rootfs disk + agent CreateContainer per container,
	// sharing the one guest/sandbox) and is tracked as follow-up work.
	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}
	containerName := containers[0].GetName()

	// Owned-boot builds the CH vm.create itself, so it needs the guest kernel +
	// image paths directly.
	paths := req.GetRuntimeAssetPaths()
	kernel, image := paths[assetKernel], paths[assetImage]
	if kernel == "" || image == "" {
		return nil, fmt.Errorf("owned-boot requires %q and %q asset paths", assetKernel, assetImage)
	}
	rr := s.resolveRuntime(paths)

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

	// Overlay rootfs (large-image path): the actor rootfs is overlay(virtio-fs RO
	// lower + virtio-blk ext4 upper) rather than a per-actor full ext4 disk. The
	// lower is the OCI image bundle served by virtiofsd (bound in, O(1)/actor); the
	// upper is a small empty /dev/vdb formatted in-guest. Rootfs writes land on the
	// upper, off the snapshot-captured guest RAM. Assembled below, after the VM dir
	// + sockets exist.
	bundleRootfs := filepath.Join(bundle, "rootfs")
	scratchPath := filepath.Join(kata.VMDir(id), scratchDiskFile)

	// Sizing + agent params from the kata config.
	var cfgBytes []byte
	if rr.configFile != "" {
		cfgBytes, _ = os.ReadFile(rr.configFile)
	}
	cfg, err := kata.ParseConfig(cfgBytes, 2048, 1)
	if err != nil {
		return nil, fmt.Errorf("while parsing kata config: %w", err)
	}
	memMiB, vcpus := cfg.MemoryMiB, cfg.VCPUs
	// Enable the guest debug console (vsock 1026) for in-guest diagnostics on
	// failure; with kata-debug also raise the agent log level.
	kparams := kata.WithDebugConsole(cfg.KernelParams)
	if s.kataDebug {
		kparams = kata.WithAgentDebug(kparams)
	}

	// Clean stale per-sandbox state + create the runtime dir for the sockets.
	kata.CleanupSandboxState(id)
	if err := os.MkdirAll(kata.VMDir(id), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// Stage the overlay lower: bind the image bundle into virtiofsd's find-paths
	// location (O(1)), create the empty sparse scratch upper disk, and start
	// virtiofsd serving the lower. CH demand-pages from virtiofsd for the actor's
	// lifetime, so ateom owns the process (killed in teardownActor). sourceID == id
	// on a cold run (the frozen base id this lineage carries).
	if err := kata.ReconstructSharedDirFromImage(ctx, bundleRootfs, id, id); err != nil {
		return nil, fmt.Errorf("while staging overlay lower: %w", err)
	}
	if err := kata.CreateSparseFile(scratchPath, scratchDiskBytes); err != nil {
		return nil, fmt.Errorf("while creating scratch upper: %w", err)
	}
	vfsdLogW, _ := os.OpenFile(filepath.Join(kata.VMDir(id), "virtiofsd.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	vfsdCmd, err := kata.StartVirtiofsd(ctx, kata.VirtiofsdOptions{
		Binary:     rr.virtiofsd,
		SocketPath: kata.VirtiofsdSocketPath(id),
		SharedDir:  kata.SharedDir(id),
		Log:        vfsdLogW,
	})
	if err != nil {
		return nil, fmt.Errorf("while starting virtiofsd: %w", err)
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

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
	vmCfg := ch.VmConfig{
		Cpus:    ch.CpusConfig{BootVcpus: int32(vcpus), MaxVcpus: int32(vcpus)},
		Memory:  ch.MemoryConfig{Size: int64(memMiB) * 1024 * 1024, Shared: true},
		Payload: ch.PayloadConfig{Kernel: kernel, Cmdline: cmdline},
		Disks: []ch.DiskConfig{
			// /dev/vda: the kata guest image (RO). /dev/vdb: the empty overlay upper.
			{Path: image, Readonly: true, ImageType: "Raw", NumQueues: int32(vcpus), QueueSize: 1024},
			{Path: scratchPath, Readonly: false, ImageType: "Raw", NumQueues: int32(vcpus), QueueSize: 1024},
		},
		// virtio-fs RO lower, on PCI segment 1 (kata's convention) — needs 2 segments.
		Fs: []ch.FsConfig{{
			Tag: kata.FsTag, Socket: kata.VirtiofsdSocketPath(id),
			NumQueues: 1, QueueSize: 1024, PciSegment: 1,
		}},
		Platform: &ch.PlatformConfig{NumPciSegments: 2},
		Rng:      &ch.RngConfig{Src: "/dev/urandom"},
		Serial:   &ch.ConsoleConfig{Mode: "File", File: serialLog},
		Vsock:    &ch.VsockConfig{Cid: 3, Socket: kata.VsockSocketPath(id)},
	}
	if err := client.CreateVM(ctx, vmCfg); err != nil {
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
	// The agent client must stay open past this RPC: the stdout/stderr forwarding
	// goroutines (started below) read over it for the actor's lifetime. It is stored
	// on the runningActor and closed by teardownActor. Close it here only if Run
	// fails after this point (no runningActor recorded).
	defer func() {
		if retErr != nil {
			_ = ac.Close()
		}
	}()

	// Establish the agent sandbox (the shim normally does this at boot).
	sbCtx, sbCancel := context.WithTimeout(ctx, 20*time.Second)
	err = ac.CreateSandboxForActor(sbCtx, id, spec.Hostname)
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

	// Format + mount the empty /dev/vdb scratch as the overlay upper (in-guest
	// mkfs.ext4 over the debug console — no host e2fsprogs needed). upperBase is
	// keyed on the FROZEN base id (== id on a cold run) so the lineage's reset-to-
	// golden wipe targets a stable path across suspend/resume.
	upperBase := kata.OverlayUpperBase(id)
	if err = kata.SetupScratchUpper(ctx, vsockPath, upperBase); err != nil {
		return nil, fmt.Errorf("while setting up overlay upper: %w", err)
	}

	// Carrier container (id == sandbox): rootfs = the virtio-fs base, created but NOT
	// started, so the agent's setup_bundle eagerly binds the base to
	// /run/kata-containers/<id>/rootfs (the overlay lowerdir — stable on arm64 where
	// bare virtio-fs submounts ENOENT).
	carrierCtx, carrierCancel := context.WithTimeout(ctx, 30*time.Second)
	err = ac.CreateCarrier(carrierCtx, id, spec)
	carrierCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath,
			"echo '== shared/containers =='; ls -la /run/kata-containers/shared/containers/ 2>&1 | head -40")
		slog.ErrorContext(ctx, "carrier create failed; dump", slog.String("dump", dump))
		return nil, fmt.Errorf("while creating carrier: %w", err)
	}

	// Start the actor with an overlayfs rootfs: lower = the carrier's bind of the RO
	// base, upper/work = <upperBase>/{fs,work} on /dev/vdb (writes off snapshot RAM).
	// The workload is a SEPARATE container id (id-ovl) from the carrier (id).
	ovlID := id + "-ovl"
	wlCtx, wlCancel := context.WithTimeout(ctx, 30*time.Second)
	err = ac.StartOverlayWorkload(wlCtx, id, ovlID, upperBase, spec)
	wlCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, vsockPath,
			"echo '== /dev/vdb =='; blkid /dev/vdb 2>&1; echo '== upper =='; ls -la "+upperBase+" 2>&1; "+
				"echo '== carrier rootfs =='; ls /run/kata-containers/"+id+"/rootfs/ 2>&1 | head; "+
				"echo '== mounts =='; grep -E 'kata|overlay|vdb' /proc/mounts 2>&1")
		slog.ErrorContext(ctx, "overlay workload failed; dump", slog.String("dump", dump))
		return nil, fmt.Errorf("while starting overlay workload: %w", err)
	}

	ra := &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket, containerName: containerName, ovlContainerID: ovlID, baseID: id, logAgent: ac}
	s.running[id] = ra

	// Forward the actor container's stdout/stderr into the pod logs (parity with
	// ateom-gvisor). The overlay workload's container/exec id is ovlID, so the agent
	// keys the streams by ovlID. The goroutines read over ac for the actor's lifetime
	// and exit (io.EOF) when teardownActor closes ac.
	s.startActorLogForwarding(ac, ovlID, name, ns, containerName)

	s.actorLogger.EmitLifecycleLog("Actor started", id, name, ns)
	slog.InfoContext(ctx, "Actor started (owned-boot, overlay rootfs)", slog.String("id", id), slog.String("workload", ovlID))
	return &ateompb.RunWorkloadResponse{}, nil
}

// startActorLogForwarding spawns two goroutines that pump the actor container's
// stdout and stderr (read over the kata-agent ttrpc client ac via repeated
// ReadStdout/ReadStderr) through the shared actorlog forwarder, which annotates
// each line with the actor's ate.dev/* labels and writes it to the pod's stdout.
//
// The streams are keyed by containerID==execID==id (the value StartBlkWorkload
// passed); lines are tagged with the container name (ate.dev/container_name). The
// reader contexts are context.Background() — the goroutines are NOT bound to the RPC
// that started them; they terminate when ac is closed (by teardownActor), which
// makes the in-flight ReadStdout/ReadStderr fail and the StreamReader return
// io.EOF, ending WrapContainerLogs. This keeps the agent connection (which ttrpc
// allows concurrent Calls on) alive for forwarding while guaranteeing no goroutine
// outlives the connection.
func (s *AteomService) startActorLogForwarding(ac *kata.AgentClient, id, name, ns, containerName string) {
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, id, id, false), id, name, ns, containerName)
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, id, id, true), id, name, ns, containerName)
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
	// agent accepts. (Static shaper; pod DNS integration is future work.)
	//
	// KNOWN GAP vs the gVisor runtime: this also drops atelet's read-only actor
	// identity bind mount (/run/ate/actor-id). The micro-VM guest can't see host
	// paths (the rootfs is a virtio-blk disk, not a shared filesystem), and
	// reset-to-golden restores guest RAM + rootfs from the golden snapshot, so a
	// per-actor file written into the rootfs would be shadowed/incorrect on restore.
	// Exposing the identity needs a per-actor volume injected from OUTSIDE the golden
	// state; not yet implemented. No micro-VM workload depends on it today.
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

// CheckpointWorkload suspends the actor and writes a portable CH snapshot.
//
// Contract with atelet (mirrors ateom-gvisor): after we return, atelet uploads
// the checkpoint dir to object storage, then tears down bundles and resets the
// actor dir.
//
// ateom drives the ateom-owned CH's REST api-socket: pause -> snapshot
// file://<CheckpointStateDir> (config.json + state.json + sparse memory-ranges) ->
// tear the VMM down. The actor's rootfs writes are on the host-backed /dev/vdb
// (NOT guest RAM), so the snapshot is naturally memory-only and small — no overlay
// tmpfs upper to wipe and no balloon to inflate before snapshot.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()

	s.actorLogger.EmitLifecycleLog("Actor checkpointing", id, name, ns)

	// The actor's CH was booted by RunWorkload or relaunched by RestoreWorkload;
	// either way ateom owns it and tracks its api-socket.
	ra := s.running[id]
	chSocket := kata.CLHSocketPath(id)
	if ra != nil && ra.apiSocket != "" {
		chSocket = ra.apiSocket
	}
	client := ch.NewClient(chSocket)
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		return nil, fmt.Errorf("while waiting for CH api-socket: %w", err)
	}

	// The FROZEN base id (the golden cold-run id) keys both the find-paths base layout
	// AND the overlay upper mount path, invariant across the actor's whole restore
	// lineage. For a cold-run actor it is its own id; for a restored actor it is the
	// golden id propagated via ra.baseID (set from the snapshot we restored from). We
	// can NOT derive it from config.json (its socket paths get rewritten to the current
	// id on every restore, losing the invariant golden id).
	baseID := id
	if ra != nil && ra.baseID != "" {
		baseID = ra.baseID
	}

	// Reset-to-golden (gVisor semantics): wipe the overlay upper (on the host-backed
	// /dev/vdb scratch, mounted at <upperBase>/fs in the guest) so the actor's rootfs
	// writes are discarded — on resume the rootfs is the golden image again. fstrim
	// frees the blocks back to holes so the shipped scratch.img stays tiny. Done via
	// the guest debug console (vsock 1026) BEFORE pause (it needs live vCPUs). Keyed on
	// baseID so a restored actor wipes the path the lineage froze. Best-effort.
	upperBase := kata.OverlayUpperBase(baseID)
	ovlUpper := upperBase + "/fs"
	wipeCmd := "b=$(ls -A " + ovlUpper + " 2>/dev/null | wc -l); " +
		"rm -rf " + ovlUpper + "/* " + ovlUpper + "/.[!.]* 2>/dev/null; " +
		"fstrim " + upperBase + " 2>/dev/null; " +
		"a=$(ls -A " + ovlUpper + " 2>/dev/null | wc -l); echo upper_before=$b upper_after=$a"
	wipeOut := kata.DebugConsoleDump(ctx, kata.VsockSocketPath(id), wipeCmd)
	if strings.Contains(wipeOut, "upper_after=0") {
		slog.InfoContext(ctx, "Reset overlay upper to golden", slog.String("id", id), slog.String("console", strings.TrimSpace(wipeOut)))
	} else {
		slog.WarnContext(ctx, "Reset-to-golden did NOT confirm empty upper; continuing", slog.String("id", id), slog.String("console", strings.TrimSpace(wipeOut)))
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

	// Record the FROZEN base id (computed above, the id the guest's virtio-fs
	// find-paths are pinned to at <baseID>/rootfs). RestoreWorkload reads this to lay
	// the reconstructed-from-image lower at the path the guest expects.
	if err := os.WriteFile(filepath.Join(checkpointDir, baseIDFile), []byte(baseID), 0o600); err != nil {
		return nil, fmt.Errorf("while writing %s: %w", baseIDFile, err)
	}

	// NB: the snapshot ships the memory image (config/state/memory-ranges + base-id)
	// PLUS the tiny reset-to-golden overlay upper (scratch.img, written below). The RO
	// lower (the OCI image, served via virtio-fs) is reconstructed from the local
	// bundle at restore, so it never ships — keeping the snapshot small and the rootfs
	// gVisor-ephemeral (rootfs writes reset to the golden image at restore).

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
		// Reuse base's on-disk working set (rename + overlay) instead of copying it —
		// CH is paused and about to be torn down, and base is discarded after. See
		// MergeDeltaIntoBase. (Falls back to the copying merge across filesystems.)
		if err := ch.MergeDeltaIntoBase(ctx, base, delta); err != nil {
			return nil, fmt.Errorf("while merging OnDemand delta into restore source: %w", err)
		}
		slog.InfoContext(ctx, "Merged OnDemand delta into base (complete snapshot)",
			slog.String("id", id), slog.Duration("merge", time.Since(tMerge)))
	}

	// Ship the overlay upper (the /dev/vdb scratch) byte-exact alongside the memory
	// snapshot. The guest froze with this ext4 mounted, so its on-disk state must match
	// the snapshot's guest page cache on resume — it is COPIED, not reconstructed (only
	// the empty/golden lower can be rebuilt deterministically; a live ext4 cannot).
	// After reset-to-golden + fstrim above it is empty + sparse (tiny). RestoreWorkload
	// places it into the restoring actor's VMDir and repoints the config at it.
	scratchSrc := filepath.Join(kata.VMDir(id), scratchDiskFile)
	scratchDst := filepath.Join(checkpointDir, scratchDiskFile)
	if out, cerr := exec.CommandContext(ctx, "cp", "--sparse=always", scratchSrc, scratchDst).CombinedOutput(); cerr != nil {
		return nil, fmt.Errorf("while shipping overlay upper scratch: %w (%s)", cerr, strings.TrimSpace(string(out)))
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

	s.actorLogger.EmitLifecycleLog("Actor checkpointed", id, name, ns)
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

// teardownActor stops the ateom-owned CH VMM for an actor. Best-effort: the
// snapshot is already on disk, so this only needs to release resources. ra may be
// nil (e.g. ateom restarted and lost in-memory state).
func (s *AteomService) teardownActor(ctx context.Context, id string, ra *runningActor, client *ch.Client) {
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
		// Close the kata-agent client kept open for stdout/stderr forwarding. This
		// fails the forwarding goroutines' in-flight ReadStdout/ReadStderr calls, so
		// they return io.EOF and exit (no goroutine leak). Guarded so a second
		// teardown / a never-forwarded actor is a no-op.
		if ra.logAgent != nil {
			_ = ra.logAgent.Close()
			ra.logAgent = nil
		}

		// Kill the CH process ateom launched.
		if ra.chCmd != nil && ra.chCmd.Process != nil {
			_ = ra.chCmd.Process.Kill()
			_, _ = ra.chCmd.Process.Wait()
		}
		// Kill the virtiofsd serving the overlay RO lower (after CH, which was the
		// only client). nil on the non-overlay path.
		if ra.vfsdCmd != nil && ra.vfsdCmd.Process != nil {
			_ = ra.vfsdCmd.Process.Kill()
			_, _ = ra.vfsdCmd.Process.Wait()
		}
	}

	// Sweep any leftover per-sandbox host-side state + orphaned per-sandbox
	// processes. This is ateom's own cleanup (process kill + unmount + rm).
	kata.CleanupSandboxState(id)
}

// RestoreWorkload restores the actor on a (possibly different) pod by relaunching
// cloud-hypervisor directly from the downloaded snapshot and resuming.
//
// Contract with atelet: the snapshot dir (config.json + state.json + memory-ranges
// + base-id + scratch.img overlay upper) has been downloaded to RestoreStateDir.
//
// The overlay rootfs is reconstructed: rebuild the virtio-fs RO lower from the local
// OCI bundle (atelet re-unpacked the golden image) at the frozen find-paths path,
// place the shipped /dev/vdb upper (scratch.img) in this actor's VMDir, and start
// virtiofsd. Then rewrite the snapshot config's per-VMDir paths (vsock + serial + fs
// socket + the upper disk) to this actor's, rebuild the tap (the snapshot's
// virtio-net is fd-backed → fresh net_fds), relaunch CH with --restore (eager Copy),
// and resume. Guest RAM (incl. the actor's in-memory state and frozen network config)
// comes back from the memory image.
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()
	restoreDir := ateompath.RestoreStateDir(ns, name, id)
	tStart := time.Now()

	s.actorLogger.EmitLifecycleLog("Actor restoring", id, name, ns)

	rr := s.resolveRuntime(req.GetRuntimeAssetPaths())
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

	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}

	// Reconstruct the overlay RO lower from the LOCAL OCI bundle (atelet re-unpacked
	// the golden image; reset-to-golden means the base is always the golden image) at
	// the frozen find-paths location <SharedDir(id)>/<srcID>/rootfs. The guest's
	// virtio-fs is pinned to srcID, so the bind must land at srcID's path even though
	// this actor's id differs. Cross-node consistency relies on a deterministic unpack.
	bundleRootfs := filepath.Join(ateompath.OCIBundlePath(ns, name, id, containers[0].GetName()), "rootfs")
	if err := kata.ReconstructSharedDirFromImage(ctx, bundleRootfs, id, srcID); err != nil {
		return nil, fmt.Errorf("while reconstructing overlay lower: %w", err)
	}

	// Place the shipped overlay upper (scratch.img) at this actor's VMDir and repoint
	// the snapshot config's /dev/vdb disk at it (the snapshot recorded the golden
	// actor's per-actor path). The guest resumes with this ext4 mounted, so it must be
	// the byte-exact disk the snapshot's page cache expects — copied, not rebuilt.
	scratchSrc := filepath.Join(restoreDir, scratchDiskFile)
	scratchDst := filepath.Join(kata.VMDir(id), scratchDiskFile)
	if out, cerr := exec.CommandContext(ctx, "cp", "--sparse=always", scratchSrc, scratchDst).CombinedOutput(); cerr != nil {
		return nil, fmt.Errorf("while placing overlay upper for restore: %w (%s)", cerr, strings.TrimSpace(string(out)))
	}
	if err := repointDisk(restoreDir, scratchDiskFile, scratchDst); err != nil {
		return nil, fmt.Errorf("while repointing overlay upper in snapshot config: %w", err)
	}

	// Start virtiofsd serving the reconstructed lower; CH connects to it at restore
	// (the fs socket was repointed to this VMDir above) and demand-pages for the VM's
	// lifetime, so ateom owns it (killed in teardownActor).
	vfsdLogW, _ := os.OpenFile(filepath.Join(kata.VMDir(id), "virtiofsd.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	vfsdCmd, err := kata.StartVirtiofsd(ctx, kata.VirtiofsdOptions{
		Binary:     rr.virtiofsd,
		SocketPath: kata.VirtiofsdSocketPath(id),
		SharedDir:  kata.SharedDir(id),
		Log:        vfsdLogW,
	})
	if err != nil {
		return nil, fmt.Errorf("while starting virtiofsd for restore: %w", err)
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

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
	// Eager (Copy) memory restore. The overlay path uses it (not OnDemand) because the
	// snapshot is self-contained (memory image + shipped scratch upper) — eager restore
	// reads it fully and produces a COMPLETE next snapshot, so there is no restore
	// source to demand-page from or to overlay deltas onto at checkpoint.
	if err := client.RestoreWithNetFDs(ctx, restoreDir, restoredNets, "Copy"); err != nil {
		return nil, fmt.Errorf("while restoring VM with net FDs: %w", err)
	}
	if err := client.Resume(ctx); err != nil {
		return nil, fmt.Errorf("while resuming restored guest: %w", err)
	}

	ra := &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket, baseID: srcID, ovlContainerID: id + "-ovl"}

	// Re-attach stdout/stderr forwarding: the restored guest's container + kata-agent
	// are alive, so a fresh dial over this actor's vsock resumes ReadStdout/ReadStderr.
	// The overlay workload's container/exec id is ovlContainerID (id-ovl), so the agent
	// keys the streams by it. Best-effort — a failed dial must not fail the restore (the
	// actor is already running); forwarding is just skipped.
	vsockPath := kata.VsockSocketPath(id)
	logAC, dialErr := dialAgentRetry(ctx, vsockPath, 15*time.Second)
	if dialErr != nil {
		slog.WarnContext(ctx, "post-restore agent dial failed; actor log forwarding disabled for this restore",
			slog.String("id", id), slog.Any("err", dialErr))
	} else {
		ra.logAgent = logAC
		s.startActorLogForwarding(logAC, ra.ovlContainerID, name, ns, containers[0].GetName())
	}

	s.running[id] = ra
	s.actorLogger.EmitLifecycleLog("Actor restored", id, name, ns)
	slog.InfoContext(ctx, "Actor restored (owned-boot, overlay rootfs)",
		slog.String("id", id), slog.Duration("total", time.Since(tStart)))
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// rewriteSnapshotSocketPaths repoints the snapshot config.json's per-sandbox
// hybrid-vsock socket from the source actor's VMDir to the restoring actor's
// VMDir, so the socket we create is the one CH reopens. The kernel and /dev/vda
// kata image are content-addressed static files with identical paths on every
// node, so they need no rewrite; the writable /dev/vdb actor rootfs disk is
// per-actor and is repointed separately (see repointActorRootfsDisk).
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
	// The overlay RO lower is served by a per-VMDir virtiofsd socket; the snapshot
	// recorded the golden actor's, so repoint each fs device at this actor's VMDir.
	if fss, ok := cfg["fs"].([]any); ok {
		for _, f := range fss {
			if fm, ok := f.(map[string]any); ok {
				fm["socket"] = kata.VirtiofsdSocketPath(id)
			}
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

// repointDisk rewrites the snapshot config.json so the writable disk whose backing
// file has the given basename points at newPath. The overlay upper (scratch.img)
// lives under the actor's per-actor VMDir (keyed by actor id), so the golden
// snapshot's recorded path is the GOLDEN actor's — stale on any pod restoring a
// different actor, and absent on any node that never ran the golden. (This is the
// disk analogue of the serial.file/fs.socket repoints in rewriteSnapshotSocketPaths.)
// The disk is identified by basename so the read-only /dev/vda kata image (a
// content-addressed static file) is left untouched; it is an error if no disk with
// that basename is present to repoint.
func repointDisk(snapshotDir, basename, newPath string) error {
	cfgPath := filepath.Join(snapshotDir, "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("parsing %q: %w", cfgPath, err)
	}
	rewrote := false
	if disks, ok := cfg["disks"].([]any); ok {
		for _, d := range disks {
			dm, ok := d.(map[string]any)
			if !ok {
				continue
			}
			if p, _ := dm["path"].(string); filepath.Base(p) == basename {
				dm["path"] = newPath
				rewrote = true
			}
		}
	}
	if !rewrote {
		return fmt.Errorf("no %q disk found in %q to repoint", basename, cfgPath)
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(cfgPath, out, 0o600)
}

// slogWriter adapts an io.Writer to slog at info level, capturing the
// cloud-hypervisor process's stdout/stderr into the worker logs.
type slogWriter struct{ ctx context.Context }

func (w slogWriter) Write(p []byte) (int, error) {
	slog.InfoContext(w.ctx, "cloud-hypervisor", slog.String("out", string(p)))
	return len(p), nil
}
