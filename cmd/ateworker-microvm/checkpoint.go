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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateworker"

	"github.com/agent-substrate/substrate/cmd/ateworker-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateworker-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"golang.org/x/sync/errgroup"
)

// Checkpoint suspends the actor and writes a portable snapshot.
//
// Contract with atelet: after we return, atelet uploads the checkpoint dir to object
// storage, then tears down bundles and resets the actor dir.
//
// What the snapshot holds depends on the requested fidelity; each level
// includes the ones below it:
//
//   - MEMORY: the whole guest. ateworker drives the CH REST api-socket: pause ->
//     snapshot file://<checkpoint_dir> (config.json + state.json + sparse
//     memory-ranges) -> tear the VMM down. Process memory persists via the
//     memory snapshot; the volume and upper tars ship alongside as for ROOTFS.
//   - ROOTFS: the volumes plus each container's rootfs upper. Each container's
//     rootfs is overlay(virtio-fs RO lower + disk-backed upper): the upper is
//     host-backed like the durable-dir volumes and ships as its own tar (see
//     rootfsupper.go). The RO lower is reconstructed from the OCI image at
//     restore, so it never ships. The guest is discarded; the actor cold-starts
//     on restore with its rootfs writes back in place.
//   - VOLUMES: the durable-dir volumes, as per-volume tars. The guest is
//     discarded, so the actor cold-starts on restore with its volumes
//     re-materialized.
//
// Whatever the fidelity the guest is paused first, which is what makes the tar coherent: the
// durable share is served write-through, so every completed guest write is already on
// the host and no further ones can arrive.
//
// Allow checkpointing even if the pod is shutting down. This will allow actors
// (or the harness) to suspend on shutdown.
func (s *microvmRuntime) Checkpoint(ctx context.Context, c *ateworker.Checkpoint) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	actorUID := c.Attribution.UID
	actorDirs := c.Dirs
	scope := c.Fidelity
	containers := c.Containers()

	// Check what the request asks for BEFORE touching the guest: these are
	// properties of the request, and pausing first would leave the actor
	// suspended mid-flight for a call that could never have succeeded.
	//
	// Durable-dir volumes are host-backed, so they are captured the same way
	// under every fidelity — and are the ONLY thing a VOLUMES snapshot
	// captures. ROOTFS and MEMORY always have something to capture: every
	// container has a rootfs upper.
	durable := hasDurableVolumes(containers)
	csi := hasCsiVolumes(containers)
	switch scope {
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		// TODO: Revisit handling for CSI volumes since snapshots are currently quietly ignored.
		if !durable && !csi {
			return nil, apierror.FailedPrecondition(
				"no durable-dir or CSI volumes found for a VOLUMES snapshot")
		}
	default:
		return nil, apierror.InvalidArgument("unsupported snapshot fidelity: %v", scope)
	}

	// The actor's CH was booted or relaunched by Start; either way ateworker owns
	// it and tracks its api-socket.
	ra, _ := c.Sandbox.(*runningActor)
	client := ch.NewClient(chSocketOf(actorUID, ra))
	if _, err := client.WaitReady(ctx, 10*time.Second); err != nil {
		return nil, fmt.Errorf("while waiting for CH api-socket: %w", err)
	}
	c.Timer.Lap(ateworker.PhasePrep)

	pauseErr := client.Pause(ctx)
	c.Timer.Lap(phasePause)
	if pauseErr != nil {
		return nil, fmt.Errorf("while pausing guest: %w", pauseErr)
	}

	checkpointDir := actorDirs.GetCheckpointDir()
	// Start from a clean dir so CH's snapshot files are the only contents.
	if err := os.RemoveAll(checkpointDir); err != nil {
		return nil, fmt.Errorf("while clearing checkpoint dir %q: %w", checkpointDir, err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint dir %q: %w", checkpointDir, err)
	}

	// Capture the snapshot's pieces CONCURRENTLY: the CH snapshot, the
	// durable-dir tars, and the rootfs upper tars read independent data from a
	// quiesced guest and write distinct files into checkpointDir, so the paused
	// window costs the slowest of them rather than their sum (the tars scale
	// with the actor's data; suspend latency is the metric that matters). Their
	// phases are timed on their own for the same reason.
	//
	//   - CH snapshot (MEMORY only): the guest memory + VM state. The lower
	//     fidelities deliberately capture no VM state — no memory image, and no
	//     base-id, since nothing will reattach to the frozen virtio-fs lower: at
	//     restore the actor cold-boots from the OCI image.
	//   - Durable-dir tars (any fidelity, when declared): host-backed, so
	//     pausing the write-through share makes the tars coherent.
	//   - Rootfs upper tars (ROOTFS and MEMORY): host-backed like the durable
	//     volumes — the memory snapshot does not carry rootfs writes. Under
	//     VOLUMES the workload cold-starts on restore, discarding rootfs state.
	var durableFiles []string
	g, gctx := errgroup.WithContext(ctx)
	if scope == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		g.Go(func() error {
			t := time.Now()
			d, err := s.snapshotVMState(gctx, client, ra, actorUID, checkpointDir)
			if err != nil {
				d = time.Since(t)
			}
			c.Timer.Add(phaseSnapshot, d)
			return err
		})
	}
	if durable {
		g.Go(func() error {
			t := time.Now()
			var err error
			durableFiles, err = tarDurableVolumes(gctx, actorDirs.GetDurableDirVolumeMountsDir(), checkpointDir, durableVolumeNames(containers))
			c.Timer.Add(phaseDurableDir, time.Since(t))
			return err
		})
	}
	if includesRootfs(scope) {
		g.Go(func() error {
			t := time.Now()
			err := tarRootfsUpper(gctx, rootfsUpperDir(actorDirs), checkpointDir, containerNames(containers))
			c.Timer.Add(phaseRootfsUpper, time.Since(t))
			return err
		})
	}
	err := g.Wait()
	c.Timer.Skip()
	if err != nil {
		return nil, err
	}

	// Report exactly the files we wrote so atelet ships precisely this snapshot:
	// for MEMORY, the CH snapshot (config.json + state.json + memory-ranges +
	// base-id) plus the upper and durable-dir tars; for ROOTFS, the upper and
	// durable-dir tars; for VOLUMES, the durable-dir tars alone.
	snapshotFiles, err := listFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("while listing snapshot files: %w", err)
	}
	slog.InfoContext(ctx, "Actor checkpointed", slog.String("id", actorUID), slog.Any("snapshot_files", snapshotFiles),
		slog.String("scope", scope.String()))
	return &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles, DataSnapshotFiles: durableFiles}, nil
}

// chSocketOf is the api-socket of the actor's VMM. ra is nil when the worker
// has no record of the VM.
func chSocketOf(actorUID string, ra *runningActor) string {
	if ra != nil && ra.apiSocket != "" {
		return ra.apiSocket
	}
	return kata.CLHSocketPath(actorUID)
}

// snapshotVMState captures the paused guest into checkpointDir: the CH snapshot
// (config.json + state.json + memory-ranges) plus the base-id the restore side
// needs, and returns how long the snapshot itself took.
func (s *microvmRuntime) snapshotVMState(ctx context.Context, client *ch.Client, ra *runningActor, actorUID, checkpointDir string) (time.Duration, error) {
	// Record the FROZEN base id (the id the guest's virtio-fs find-paths are pinned
	// to, <baseID>/rootfs). For a cold-run actor this is its own id; for a restored
	// actor it is the golden id propagated via ra.baseID (set from the snapshot we
	// restored from). RestoreWorkload reads this to lay the
	// reconstructed-from-image base at the path the guest expects. We can NOT derive
	// it from config.json (its socket paths get rewritten to the current id on every
	// restore, losing the invariant golden id).
	baseID := actorUID
	if ra != nil && ra.baseID != "" {
		baseID = ra.baseID
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, baseIDFile), []byte(baseID), 0o600); err != nil {
		return 0, fmt.Errorf("while writing %s: %w", baseIDFile, err)
	}

	slog.InfoContext(ctx, "Snapshotting guest", slog.String("id", actorUID), slog.String("dir", checkpointDir))
	tSnapshot := time.Now()
	if err := client.Snapshot(ctx, checkpointDir); err != nil {
		return 0, fmt.Errorf("while snapshotting guest: %w", err)
	}
	dSnapshot := time.Since(tSnapshot)

	// Diff-snapshot completion for an OnDemand-restored actor: CH's snapshot here is
	// sparse — only the pages faulted in since the OnDemand restore — so on its own
	// it's INCOMPLETE (the un-faulted pages were being demand-paged from the restore
	// source). Overlay it onto that source to rebuild a COMPLETE memory-ranges, so the
	// snapshot is self-contained and re-restorable. (A cold-run actor has no restore
	// source and its snapshot is already complete — no merge.)
	if ra != nil && ra.snapshotIsSelfContained {
		// Eager restore already pulled every populated extent into guest memory, so
		// what cloud-hypervisor just wrote is the whole guest, not a delta. Merging
		// would copy the entire resident set onto the restore source for nothing.
		slog.InfoContext(ctx, "Snapshot is self-contained (eager restore); skipping merge",
			slog.String("id", actorUID))
	} else if ra != nil && ra.restoreSourceDir != "" {
		tMerge := time.Now()
		if err := mergeOnDemandDelta(ctx, ra.restoreSourceDir, checkpointDir, ra.preserveRestoreSource); err != nil {
			return 0, fmt.Errorf("while merging OnDemand delta into restore source: %w", err)
		}
		slog.InfoContext(ctx, "Merged OnDemand delta into base (complete snapshot)",
			slog.String("id", actorUID), slog.Duration("merge", time.Since(tMerge)))
	}

	// The RO lower never ships (reconstructed from the OCI image at restore).
	// The disk-backed upper ships as its own tar from CheckpointWorkload; a
	return dSnapshot, nil
}

// mergeOnDemandDelta merges the OnDemand delta in checkpointDir with the base in
// restoreSourceDir. A preserved base is copied, never modified in place.
func mergeOnDemandDelta(ctx context.Context, restoreSourceDir, checkpointDir string, preserveRestoreSource bool) error {
	base := filepath.Join(restoreSourceDir, "memory-ranges")
	delta := filepath.Join(checkpointDir, "memory-ranges")
	if preserveRestoreSource {
		return ch.MergeSparseOverlay(ctx, base, delta, delta)
	}
	return ch.MergeDeltaIntoBase(ctx, base, delta)
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

// teardownActor stops the ateworker-owned CH VMM for an actor. ra may be
// nil (e.g. ateworker restarted and lost in-memory state).
func (s *microvmRuntime) teardownActor(ctx context.Context, id string, actorDirs *ateworkerpb.ActorDirs, ra *runningActor, client *ch.Client) error {
	var errs []error
	if client != nil {
		tShutdown := time.Now()
		shutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := client.Shutdown(shutCtx); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				slog.WarnContext(ctx, "CH shutdown returned error during teardown", slog.Any("err", err))
			}
		}
		cancel()
		slog.InfoContext(ctx, "CH API shutdown done", slog.Duration("took", time.Since(tShutdown)))
	}

	if ra != nil {
		// Close the kata-agent client kept open for stdout/stderr forwarding. This
		// fails the forwarding goroutines' in-flight ReadStdout/ReadStderr calls, so
		// they return io.EOF and exit (no goroutine leak). A second teardown, or a
		// never-forwarded actor, finds nothing to close.
		if agent := ra.release(); agent != nil {
			_ = agent.Close()
		}

		// Kill the CH process ateworker launched.
		if ra.chCmd != nil && ra.chCmd.Process != nil {
			_ = ra.chCmd.Process.Kill()
			_, _ = ra.chCmd.Process.Wait()
		}
		// Kill the virtiofsd (after CH, its only client).
		if ra.vfsdCmd != nil && ra.vfsdCmd.Process != nil {
			_ = ra.vfsdCmd.Process.Kill()
			_, _ = ra.vfsdCmd.Process.Wait()
		}
	}

	// Sweep any leftover per-sandbox host-side state + orphaned per-sandbox
	// processes. This is ateworker's own cleanup (process kill + unmount + rm) —
	// it also drops the merged rootfs overlay mounts, which MUST come before
	// the upper-dir removal below (removing a live overlay's upperdir would
	// corrupt the mount rather than delete the files).
	s.cleanupSandboxState(ctx, id)

	// Remove the rootfs upper dir: ateworker owns it — atelet's actor-dir reset
	// doesn't know it — and its absence is what marks a worker as holding no
	// disk-backed upper. Runs after the checkpoint tar, which is already on disk.
	if err := os.RemoveAll(rootfsUpperDir(actorDirs)); err != nil {
		slog.WarnContext(ctx, "Failed to remove rootfs upper dir", slog.String("actorUID", id), slog.Any("err", err))
	}

	// Detach the bundle rootfs overlays composed in buildActorContainers, so
	// atelet's bundle wipe doesn't strand live mounts in this namespace.
	if err := imagecache.UnmountAllUnder(actorDirs.GetOciBundleDir()); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("while unmounting bundle rootfs overlays: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Stop tears down the actor's VMM and cleans up its host state. sb is nil when
// the worker has no record of the VM, which teardownActor handles.
func (s *microvmRuntime) Stop(ctx context.Context, a *ateworker.Actor, sb ateworker.Sandbox) error {
	actorUID := a.Attribution.UID
	ra, _ := sb.(*runningActor)
	err := s.teardownActor(ctx, actorUID, a.Dirs, ra, ch.NewClient(chSocketOf(actorUID, ra)))
	s.removeActorLeaf(ctx, actorUID)
	return err
}
