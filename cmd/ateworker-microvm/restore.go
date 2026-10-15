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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateworker"

	"github.com/agent-substrate/substrate/cmd/ateworker-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateworker-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
)

// restoreMemMode picks how cloud-hypervisor should load guest RAM, from what the VMM
// just told us about itself over vmm.ping.
//
// OnDemand is what we want: it faults pages in as the guest touches them, so an idle
// restored actor holds its working set rather than its whole snapshot — on the counter
// demo, 16MiB against 158MiB. Eager gives that up, reading every populated extent up
// front.
//
// It is still the right choice on a VMM that prefaults, where OnDemand is not merely
// wasteful but unusable: the prefault storm starves the guest and its wakeup probe
// never passes.
func restoreMemMode(ctx context.Context, info ch.VMMInfo) string {
	if !info.PrefaultsUnconditionally() {
		return ch.MemRestoreOnDemand
	}
	if info.Version == "" && info.BuildVersion == "" {
		// Unknown version: eager works everywhere, so prefer a bigger idle footprint
		// over an actor that cannot start. Say so, because that cost is invisible.
		slog.WarnContext(ctx, "cloud-hypervisor did not report a version; restoring eagerly",
			slog.String("mode", ch.MemRestoreEager))
	}
	return ch.MemRestoreEager
}

// reseedGuestCRNG mixes fresh, per-restore entropy into the restored guest's kernel
// CRNG through the kata-agent (see the call site in restoreMemoryFidelity for why a restore
// needs this). The nonce is a throwaway; its only job is to differ between restores so
// that clones of one snapshot diverge instead of producing identical randomness. The
// caller owns ac: it stays open for log forwarding and guest stats.
func reseedGuestCRNG(ctx context.Context, ac *kata.AgentClient) error {
	nonce, err := newReseedNonce()
	if err != nil {
		return err
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return ac.ReseedRandomDev(rctx, nonce)
}

// newReseedNonce returns 32 bytes of fresh entropy to mix into the guest CRNG.
func newReseedNonce() ([]byte, error) {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generating reseed nonce: %w", err)
	}
	return nonce, nil
}

// restore brings the actor back from a snapshot, on a possibly different
// pod. What that means depends on the fidelity the snapshot was taken with:
//
//   - MEMORY: relaunch cloud-hypervisor from the snapshot and resume the guest
//     (restoreMemoryFidelity).
//   - ROOTFS: as VOLUMES, but each container's rootfs upper is re-materialized
//     from its tar first, so the cold boot sees the rootfs writes the actor had
//     made.
//   - VOLUMES: there is no guest to resume — re-materialize the durable-dir
//     volumes and cold-boot the actor, which starts its containers afresh from
//     the OCI image.
//
// Contract with atelet: the snapshot's files are in ActorDirs.restore_dir,
// and the durable-dir volume directories re-created (empty).
func (s *microvmRuntime) restore(ctx context.Context, act *ateworker.Activation, p actorBootParams) (ateworker.Sandbox, error) {
	restoreDir := p.actorDirs.GetRestoreDir()
	// Restore the durable-dir volumes before anything can observe them: for MEMORY
	// that means before the share's virtiofsd starts, for the lower fidelities
	// before the workload cold-starts. A retried start finds them in place.
	if hasDurableVolumes(p.containers) && act.Attempt == 1 {
		if err := untarDurableVolumes(p.actorDirs.GetDurableDirVolumeMountsDir(), restoreDir, durableVolumeNames(p.containers)); err != nil {
			return nil, err
		}
	}

	var ra *runningActor
	var err error
	switch act.Fidelity {
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		ra, err = s.restoreMemoryFidelity(ctx, act, p)
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		// A ROOTFS snapshot holds no guest state either, but it carries each
		// container's rootfs upper. The cold boot re-materializes the uppers
		// from the snapshot instead of starting from a pristine dir, overlapped
		// with its bundle preparation like the MEMORY path does.
		p.rootfsUpperSnapshotDir = restoreDir
		ra, err = s.coldBootActor(ctx, p)
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		// A VOLUMES snapshot holds no guest state, so this is a cold boot that
		// happens to start with the volumes already populated.
		ra, err = s.coldBootActor(ctx, p)
	default:
		return nil, apierror.InvalidArgument("unsupported snapshot fidelity: %v", act.Fidelity)
	}
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Actor restored", slog.String("id", p.actorUID), slog.String("fidelity", act.Fidelity.String()))
	return ra, nil
}

// restoreMemoryFidelity restores a whole-guest snapshot: relaunch cloud-hypervisor
// directly from it and resume.
//
// Each container's rootfs is a host-merged overlay (image lower + host upper). Steps:
// rewrite the snapshot config's per-VMDir paths (vsock + serial + fs sockets) to this
// actor's; re-materialize the uppers from the per-container tars (in the background,
// overlapped with bundle preparation) and re-mount the merged trees at the frozen
// find-paths paths; start the virtiofsd serving them; rebuild the tap (the snapshot's
// virtio-net is fd-backed → fresh net_fds); relaunch CH with --restore (OnDemand),
// and resume. Guest RAM — the actor's in-memory state and the frozen network config —
// comes back from the memory snapshot; the durable-dir volumes were restored by the
// caller from their tar.
func (s *microvmRuntime) restoreMemoryFidelity(ctx context.Context, act *ateworker.Activation, p actorBootParams) (_ *runningActor, retErr error) {
	actorUID := p.actorUID
	restoreDir := p.actorDirs.GetRestoreDir()
	preserveRestoreDir := act.PreserveRestoreDir
	timer := act.Timer

	rr := s.resolveRuntime(p.assetPaths)
	s.cleanupSandboxState(ctx, actorUID)

	// Repoint the snapshot's vsock socket to this actor's VMDir (the disk + kernel
	// paths are content-addressed/per-actor and already line up on the same node).
	if err := rewriteSnapshotSocketPaths(restoreDir, actorUID); err != nil {
		return nil, fmt.Errorf("while rewriting snapshot socket paths: %w", err)
	}
	srcID := actorUID
	if b, rerr := os.ReadFile(filepath.Join(restoreDir, baseIDFile)); rerr == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			srcID = v
		}
	}
	if err := os.MkdirAll(kata.VMDir(actorUID), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}
	timer.Lap(ateworker.PhasePrep)

	// MEMORY snapshots carry each container's upper as a tar (rootfsUpperTarFile).
	// Start re-materializing the upper contents NOW, in the background: the
	// untar scales with the actor's data and is joined right before the host
	// overlay mounts need it, so it hides behind the bundle preparation below.
	//
	// An error return between here and the join MUST drain the goroutine (the
	// deferred receive below): returning with the untar still writing would let
	// a retried restore's own untar race it inside the same directory.
	untarDone := make(chan error, 1)
	untarJoined := false
	go func() {
		untarDone <- untarRootfsUpper(rootfsUpperDir(p.actorDirs), restoreDir, containerNames(p.containers))
	}()
	defer func() {
		if !untarJoined {
			<-untarDone
		}
	}()

	// Reconstruct each container's rootfs at the frozen find-paths location
	// SharedDir(id)/<cid>/rootfs from the LOCAL OCI bundle (atelet re-unpacked
	// the golden image) and start the one virtiofsd serving the tree. The fs
	// sockets in the snapshot config are repointed to this VMDir by
	// rewriteSnapshotSocketPaths above. Cross-node consistency relies on a
	// deterministic unpack of the same image at the same <cid>/rootfs path
	// (plus, for merged rootfs, the upper re-materialized from the tar).
	containers := p.containers
	if len(containers) == 0 {
		return nil, apierror.InvalidArgument("actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return nil, apierror.Unimplemented("ateworker-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}
	ctrs, err := s.buildActorContainers(p.actorUID, p.actorDirs, containers)
	if err != nil {
		return nil, err
	}
	timer.Lap(phaseBundles)
	// The overlay mounts need the upper on disk: join the background untar (still
	// overlapped with the bundle preparation above), then assemble the merged trees
	// and serve them.
	untarErr := <-untarDone
	untarJoined = true
	if untarErr != nil {
		return nil, untarErr
	}
	timer.Lap(phaseUpperJoin)
	leaf, err := s.actorLeaf(actorUID, p.size)
	if err != nil {
		return nil, err
	}
	defer leaf.Close()
	vfsdCmd, err := s.stageMergedRootfs(ctx, rr, actorUID, p.actorDirs, ctrs, containers, leaf.SysProcAttr())
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

	timer.Lap(phaseLowers)

	// The snapshot's virtio-net is fd-backed, so CH needs fresh tap FDs
	// (net_fds) on restore.
	defer func() {
		if retErr != nil {
			// Detach any bundle rootfs overlays mounted by buildActorContainers
			// before the failure, mirroring teardownActor's cleanup.
			if err := imagecache.UnmountAllUnder(p.actorDirs.GetOciBundleDir()); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Restore failure", slog.Any("err", err))
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
		files, terr := setupActorTap(ctx, p.netNS, fmt.Sprintf("tap%d_kata", i), nd.QueuePairs)
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

	// Relaunch CH and restore with the tap FDs attached (SCM_RIGHTS).
	apiSocket := filepath.Join(kata.VMDir(actorUID), "clh-api-restore.sock")
	timer.Lap(phaseTap)
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary: rr.chBinary, APISocket: apiSocket, Stdout: slogWriter{ctx}, Stderr: slogWriter{ctx},
		SysProcAttr: leaf.SysProcAttr(),
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM for restore: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
			_, _ = chCmd.Process.Wait()
		}
	}()
	// How guest RAM comes back depends on the VMM (see restoreMemMode), and the rest
	// of the actor's lifecycle follows from that choice:
	//
	//   - OnDemand: cloud-hypervisor demand-pages from restoreDir for the VM's whole
	//     lifetime, so it must stay put, and the snapshot it writes later holds only
	//     the pages faulted in meanwhile — CheckpointWorkload overlays that delta onto
	//     this source to rebuild a complete one.
	//   - Eager: every populated extent is read here and now. Nothing pages from the
	//     source afterwards and nothing merges against it, so it is dropped below and
	//     the next snapshot stands on its own.
	timer.Lap(phaseVMMLaunch)
	memMode := restoreMemMode(ctx, client.Info())
	slog.InfoContext(ctx, "restoring guest memory",
		slog.String("mode", memMode), slog.String("vmm_version", client.Info().Version))
	if err := client.RestoreWithNetFDs(ctx, restoreDir, restoredNets, memMode); err != nil {
		return nil, fmt.Errorf("while restoring VM with net FDs: %w", err)
	}
	timer.Lap(phaseVMRestore)
	// An eager restore has read the whole snapshot into guest memory, and nothing
	// merges against it afterwards, so drop the staged memory image and evict its
	// page cache before resuming the VM so guest RAM and snapshot page cache do
	// not overlap during Resume and wakeup probe gating.
	//
	// Dropping ahead of Resume means a Resume, agent dial, or wakeup failure leaves
	// restore-state empty; atelet re-downloads with O_TRUNC and resetActorDirs
	// wipes the directory on retry, so this costs a re-download on retry rather
	// than correctness.
	maybeDropStagedMemoryImage(ctx, restoreDir, memMode, preserveRestoreDir)
	dropActorSnapshotCacheAsync(restoreDir, memMode == ch.MemRestoreEager)
	if err := client.Resume(ctx); err != nil {
		return nil, fmt.Errorf("while resuming restored guest: %w", err)
	}
	timer.Lap(phaseResume)

	// One kata-agent connection serves this whole activation: the CRNG reseed below,
	// then log forwarding and guest stats. As on cold boot, not reaching the agent
	// fails the restore. A failing restore closes the connection on its way out.
	guestAC, err := dialAgentRetry(ctx, kata.VsockSocketPath(actorUID), 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("while dialing kata-agent after resume: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = guestAC.Close()
		}
	}()

	// Reseed the guest CRNG before the workload gets far. A restored guest resumes with
	// the entropy pool frozen in the snapshot, so every actor restored from one snapshot
	// (golden cold-start, tag clones) would share an identical CRNG state and emit the
	// same "random" values. Cloud Hypervisor has no VmGenID device to signal the guest,
	// so we feed fresh per-restore entropy through the kata-agent's ReseedRandomDev, which
	// mixes it into /dev/random and reseeds. A failed reseed fails the restore, since the
	// actor would otherwise run with randomness it shares with its clones.
	//
	// The reseed runs just after Resume, so a workload that reads randomness in its first
	// instants after resume can still see the frozen state. Fully closing that needs the
	// workload frozen across the reseed, or a VMM VmGenID that acts before the vCPUs resume.
	//
	// TODO: switch to a Cloud Hypervisor VmGenID device once it exists. clh has no such
	// device today; Firecracker and QEMU do. With one, the VMM changes the generation id
	// and notifies the guest before unpausing the vCPUs, so a >=5.18 kernel reseeds its
	// CRNG on its own with no host round-trip, no per-restore RPC, and no post-Resume
	// race. At that point this agent-driven reseed can be dropped.
	if err := reseedGuestCRNG(ctx, guestAC); err != nil {
		return nil, fmt.Errorf("while reseeding guest CRNG: %w", err)
	}

	ra := newRunningActor(actorUID, guestAC, workloadIDs(ctrs))
	ra.chCmd, ra.vfsdCmd, ra.apiSocket, ra.baseID = chCmd, vfsdCmd, apiSocket, srcID
	ra.restoreSourceDir = restoreDir
	ra.preserveRestoreSource = preserveRestoreDir
	ra.snapshotIsSelfContained = memMode == ch.MemRestoreEager

	// Re-attach stdout/stderr forwarding for each container over the agent
	// connection dialed after resume: the restored guest's containers are alive, so
	// ReadStdout/ReadStderr pick up where they left off.
	attribution := p.attribution()
	for _, c := range containers {
		s.startActorLogForwarding(guestAC, attribution, c.GetName(), c.GetName())
	}
	return ra, nil
}

// maybeDropStagedMemoryImage deletes the staged memory-ranges file and
// already-extracted tar archives after an eager restore (it is fully in guest
// memory and nothing merges against it), unless restoreDir is a preserved
// snapshot.
func maybeDropStagedMemoryImage(ctx context.Context, restoreDir, memMode string, preserveRestoreDir bool) {
	if memMode != ch.MemRestoreEager || preserveRestoreDir {
		return
	}
	dropStagedMemoryImage(ctx, restoreDir)
}

// rewriteSnapshotSocketPaths repoints the snapshot config.json's per-VMDir paths from
// the source actor's VMDir to the restoring actor's: the hybrid-vsock socket, the
// File serial console, and each virtio-fs socket, so the sockets/files we create are
// the ones CH reopens. The kernel and the kata image are content-addressed static
// files with identical paths on every node, so they need no rewrite, and the overlay
// has no per-actor disk to repoint.
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
	changed := false
	if vsock, ok := cfg["vsock"].(map[string]any); ok {
		want := kata.VsockSocketPath(id)
		if got, _ := vsock["socket"].(string); got != want {
			vsock["socket"] = want
			changed = true
		}
	}
	// ateworker captures the guest console to a file under the source actor's VMDir
	// (virtio-console normally, plus the UART in debug mode). On restore those paths
	// are stale (they point at the golden/source pod's VMDir), so CH's
	// CreateConsoleDevice fails (No such file or directory). Repoint them at this
	// actor's VMDir.
	for key, path := range map[string]string{
		"console": kata.ConsoleLogPath(id),
		"serial":  kata.SerialLogPath(id),
	} {
		dev, ok := cfg[key].(map[string]any)
		if !ok {
			continue
		}
		if mode, _ := dev["mode"].(string); mode == "File" {
			if got, _ := dev["file"].(string); got != path {
				dev["file"] = path
				changed = true
			}
		}
	}
	// The virtio-fs share is served by its per-VMDir virtiofsd socket; the
	// snapshot recorded the golden actor's, so repoint it at this actor's VMDir.
	if fss, ok := cfg["fs"].([]any); ok {
		for _, f := range fss {
			fm, ok := f.(map[string]any)
			if !ok {
				return fmt.Errorf("snapshot config %q has a malformed fs device", cfgPath)
			}
			switch tag, _ := fm["tag"].(string); tag {
			case kata.FsTag:
				want := kata.VirtiofsdSocketPath(id)
				if got, _ := fm["socket"].(string); got != want {
					fm["socket"] = want
					changed = true
				}
			default:
				return fmt.Errorf("snapshot config %q has fs device with unknown tag %q", cfgPath, tag)
			}
		}
	}
	if !changed {
		// Same-actor resume: nothing to rewrite, so leave a preserved snapshot untouched.
		return nil
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	// Temp file + rename, not O_TRUNC, which would write through a shared inode.
	tmp, err := os.CreateTemp(snapshotDir, ".config.json.tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(out); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), cfgPath)
}
