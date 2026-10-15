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
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateworker"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// session is the runsc state of a running actor, for the drain.
type session struct {
	rcmd       *runsc
	containers []string
}

// Start boots or restores the actor's sandbox.
//
// Contract with atelet:
//
//   - Correct runsc version is downloaded and placed on disk.
//   - All OCI bundles are set up, including for the pause container.
//   - On restore, the checkpoint is downloaded and placed on disk.
func (r *gvisorRuntime) Start(ctx context.Context, act *ateworker.Activation) (_ ateworker.Sandbox, retErr error) {
	if err := resetRunscStateAndPidFileDirs(act.Dirs); err != nil {
		return nil, err
	}
	// Use the actor's gateway as its DNS resolver.
	if err := writeActorResolvConf(act.Dirs); err != nil {
		return nil, err
	}
	act.Timer.Lap(ateworker.PhasePrep)

	rcmd := r.runsc(&act.Actor)
	var created []string
	defer func() {
		if retErr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		deleteContainers(cleanupCtx, rcmd, created)
		// Detach any bundle rootfs overlays a partial setup mounted, or they
		// linger in this namespace until atelet wipes the bundle dirs.
		if err := imagecache.UnmountAllUnder(act.Dirs.GetOciBundleDir()); err != nil {
			slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after start failure",
				"actorUID", act.Attribution.UID, "err", err)
		}
		removeActorResolvConf(cleanupCtx, act.Dirs)
	}()

	var err error
	if act.Fidelity == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		err = r.boot(ctx, act, rcmd, &created)
	} else {
		err = r.restore(ctx, act, rcmd, &created)
	}
	if err != nil {
		return nil, err
	}
	return &session{rcmd: rcmd, containers: containerNames(act.Containers())}, nil
}

// boot creates and starts the pause container, then each application
// container. created collects the containers to delete on failure.
func (r *gvisorRuntime) boot(ctx context.Context, act *ateworker.Activation, rcmd *runsc, created *[]string) error {
	// The bundle rootfs is composed here, an overlay of the node's cached image
	// layers plus the bundle's private upper, because mounting is the worker's
	// job (atelet runs with no capabilities). runsc's gofer resolves the mount
	// in this pod's mount namespace.
	if err := imagecache.SetupBundleRootfs(ociBundlePath(act.Dirs, ocispec.PauseContainer)); err != nil {
		return fmt.Errorf("while composing pause rootfs: %w", err)
	}
	*created = append(*created, ocispec.PauseContainer)
	// Attribute runsc CLI output and the pause container's stdio to the actor.
	pw, err := r.actorLogger.StartJSONLogPipe(act.Attribution, ocispec.PauseContainer)
	if err != nil {
		return fmt.Errorf("while starting pause log pipe: %w", err)
	}
	defer pw.Close()
	if err := rcmd.cmdCreate(ctx, pw, ocispec.PauseContainer, nil); err != nil {
		return fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdStart(ctx, pw, ocispec.PauseContainer); err != nil {
		return fmt.Errorf("while starting pause container: %w", err)
	}

	// Each application container gets its own log pipe, so every line is
	// tagged with the container it came from.
	for _, ac := range act.Containers() {
		pw, err := r.actorLogger.StartJSONLogPipe(act.Attribution, ac.GetName())
		if err != nil {
			return fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		if err := imagecache.SetupBundleRootfs(ociBundlePath(act.Dirs, ac.GetName())); err != nil {
			return fmt.Errorf("while composing %q rootfs: %w", ac.GetName(), err)
		}
		*created = append(*created, ac.GetName())
		if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
			return fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
			return fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
		}
	}
	return nil
}

// restore creates the pause container and each application container and
// restores them from the checkpoint, or cold starts them with the durable-dir
// volumes restored for a VOLUMES snapshot.
func (r *gvisorRuntime) restore(ctx context.Context, act *ateworker.Activation, rcmd *runsc, created *[]string) error {
	checkpointDir := act.Dirs.GetRestoreDir()
	restoreArgs, err := fsRestoreArgs(checkpointDir)
	if err != nil {
		return err
	}
	memory := act.Fidelity == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY

	// runsc restore only needs the rootfs to hold the right content; whether it
	// came from an untar or an overlay of cached layers is transparent to it.
	err = imagecache.SetupBundleRootfs(ociBundlePath(act.Dirs, ocispec.PauseContainer))
	act.Timer.Lap(phasePauseRootfs)
	if err != nil {
		return fmt.Errorf("while composing pause rootfs: %w", err)
	}

	// Attribute runsc CLI output and the pause container's stdio to the actor.
	pw, err := r.actorLogger.StartJSONLogPipe(act.Attribution, ocispec.PauseContainer)
	if err != nil {
		return fmt.Errorf("while starting pause log pipe: %w", err)
	}
	defer pw.Close()
	*created = append(*created, ocispec.PauseContainer)
	err = rcmd.cmdCreate(ctx, pw, ocispec.PauseContainer, restoreArgs)
	act.Timer.Lap(phasePauseCreate)
	if err != nil {
		return fmt.Errorf("while creating pause container: %w", err)
	}
	verb := "starting"
	if memory {
		verb = "restoring"
		err = rcmd.cmdRestore(ctx, pw, ocispec.PauseContainer, checkpointDir)
	} else {
		err = rcmd.cmdStart(ctx, pw, ocispec.PauseContainer)
	}
	act.Timer.Lap(phasePauseRestore)
	if err != nil {
		return fmt.Errorf("while %s pause container: %w", verb, err)
	}

	// app_create includes starting each container's log pipe.
	for _, ac := range act.Containers() {
		pw, err := r.actorLogger.StartJSONLogPipe(act.Attribution, ac.GetName())
		act.Timer.Lap(phaseAppCreate)
		if err != nil {
			return fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		err = imagecache.SetupBundleRootfs(ociBundlePath(act.Dirs, ac.GetName()))
		act.Timer.Lap(phaseAppRootfs)
		if err != nil {
			return fmt.Errorf("while composing %q rootfs: %w", ac.GetName(), err)
		}
		*created = append(*created, ac.GetName())
		err = rcmd.cmdCreate(ctx, pw, ac.GetName(), nil)
		act.Timer.Lap(phaseAppCreate)
		if err != nil {
			return fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if memory {
			err = rcmd.cmdRestore(ctx, pw, ac.GetName(), checkpointDir)
		} else {
			err = rcmd.cmdStart(ctx, pw, ac.GetName())
		}
		act.Timer.Lap(phaseAppRestore)
		if err != nil {
			return fmt.Errorf("while %s %q application container: %w", verb, ac.GetName(), err)
		}
	}
	return nil
}

// Checkpoint saves the actor's sandbox state. MEMORY checkpoints the whole
// sandbox and stops it; VOLUMES saves only the durable-dir volumes.
//
// Contract with atelet: afterwards atelet uploads the checkpoint, then tears
// down the OCI bundles and resets the actor directory.
func (r *gvisorRuntime) Checkpoint(ctx context.Context, c *ateworker.Checkpoint) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	rcmd := r.runsc(&c.Actor)
	checkpointPath := c.Dirs.GetCheckpointDir()
	err := os.MkdirAll(checkpointPath, 0o700)
	c.Timer.Lap(ateworker.PhasePrep)
	if err != nil {
		return nil, fmt.Errorf("while creating checkpoint directory: %w", err)
	}

	durablePaths := durableFSCheckpointPaths(c.Containers())
	switch c.Fidelity {
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		if len(durablePaths) == 0 {
			return nil, fmt.Errorf("no durable-dir volumes found for VOLUMES snapshot")
		}
		err := rcmd.cmdFsCheckpoint(ctx, ocispec.PauseContainer, checkpointPath, durablePaths)
		c.Timer.Lap(phaseDurableDir)
		if err != nil {
			return nil, fmt.Errorf("while checkpointing durable-dir volumes: %w", err)
		}
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		// Checkpoint the pause container, the root of the sandbox, splitting
		// durable-dir volumes into <checkpointPath>/fs when present.
		sandbox, sbErr := openSandboxProcess(c.Dirs)
		if sbErr != nil {
			slog.WarnContext(ctx, "Cannot watch the sandbox exit after checkpoint", slog.Any("err", sbErr))
		} else {
			defer sandbox.Close()
		}
		err := rcmd.cmdCheckpoint(ctx, ocispec.PauseContainer, checkpointPath, durablePaths)
		if err != nil {
			c.Timer.Lap(phaseCheckpoint)
			return nil, fmt.Errorf("while checkpointing pause: %w", err)
		}
		// the checkpoint stops the sandbox, runsc delete fails until it is reaped
		if sandbox != nil {
			if err := sandbox.waitGone(sandboxExitTimeout); err != nil {
				slog.WarnContext(ctx, "Sandbox did not go away after checkpoint", slog.Any("err", err))
			}
		}
		c.Timer.Lap(phaseCheckpoint)
	default:
		return nil, fmt.Errorf("unsupported snapshot fidelity: %v", c.Fidelity)
	}

	// Report exactly the files runsc wrote, so atelet ships precisely this set.
	snapshotFiles, err := listSnapshotFiles(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("while listing checkpoint files: %w", err)
	}
	return &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles, DataSnapshotFiles: durableSnapshotFiles(snapshotFiles)}, nil
}

// Stop deletes the actor's containers and the state runsc and the worker left
// on the node. It works from the request alone, so sb is unused.
func (r *gvisorRuntime) Stop(ctx context.Context, a *ateworker.Actor, _ ateworker.Sandbox) error {
	var errs []error
	rcmd := r.runsc(a)

	// Detached from the caller: a deadline mid-`runsc delete` would leave the
	// container without its record and the actor unrecoverable.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	// Stop the containers before deleting them, to avoid leaving a live
	// container with no bundle on disk. Best-effort: if they are already
	// stopped, the delete succeeds anyway.
	stopContainers(cleanupCtx, rcmd, a.Containers())
	if err := cleanupContainers(cleanupCtx, rcmd, a.Containers()); err != nil {
		errs = append(errs, fmt.Errorf("while cleaning up runsc containers: %w", err))
	}

	// The actor may resume on another worker, so this one may never see another
	// Run or Restore for it. Reset files here to close the loop.
	if err := resetRunscStateAndPidFileDirs(a.Dirs); err != nil {
		errs = append(errs, fmt.Errorf("while resetting runsc state and pid file dirs: %w", err))
	}

	// Detach the overlay rootfs mounts before atelet wipes the bundle dirs, or
	// they stay orphaned in this namespace until the pod restarts.
	if err := imagecache.UnmountAllUnder(a.Dirs.GetOciBundleDir()); err != nil {
		errs = append(errs, fmt.Errorf("while unmounting bundle rootfs overlays: %w", err))
	}
	removeActorResolvConf(ctx, a.Dirs)
	return errors.Join(errs...)
}

// Drain propagates SIGTERM to every application container and waits until
// deadline, escalating to SIGKILL.
func (r *gvisorRuntime) Drain(ctx context.Context, _ resources.ActorAttribution, sb ateworker.Sandbox, deadline time.Time) {
	s := sb.(*session)
	var wg sync.WaitGroup
	for _, name := range s.containers {
		wg.Go(func() {
			if err := killContainer(ctx, s.rcmd, name, deadline); err != nil {
				slog.WarnContext(ctx, "Failed to kill container during shutdown", slog.String("container", name), slog.Any("err", err))
			}
		})
	}
	wg.Wait()
}

// containerKillTimeout bounds the post-SIGKILL wait, so a completely broken
// gVisor cannot hold shutdown open indefinitely. It is not drawn from the
// grace period: by this point the container has had its allowance. A var so
// tests can shorten it.
var containerKillTimeout = 5 * time.Second

// containerRuntime is the slice of *runsc that stopping and tearing down
// containers needs, so that code can be exercised without executing runsc.
type containerRuntime interface {
	cmdKill(ctx context.Context, containerName, signal string) error
	cmdWait(ctx context.Context, containerName string) error
	cmdState(ctx context.Context, containerName string) error
	cmdDelete(ctx context.Context, containerName string) error
	cmdList(ctx context.Context) ([]string, error)
}

// killContainer stops a container by sending SIGTERM, waiting until deadline,
// and escalating to SIGKILL if necessary. deadline is the shared drain
// deadline, so a caller that has spent most of the grace period elsewhere
// leaves the container only what is left of it.
func killContainer(ctx context.Context, rcmd containerRuntime, name string, deadline time.Time) error {
	// An actor that installed no SIGTERM handler terminates immediately.
	slog.InfoContext(ctx, "Sending SIGTERM to container", slog.String("container", name))
	if err := rcmd.cmdKill(ctx, name, "SIGTERM"); err != nil {
		slog.ErrorContext(ctx, "Failed to propagate SIGTERM to container", slog.String("container", name), slog.Any("err", err))
		return fmt.Errorf("failed to propagate SIGTERM to container %q: %w", name, err)
	}

	done := make(chan error, 1)
	go func() {
		done <- rcmd.cmdWait(ctx, name)
	}()

	sigTermCtx, sigTermCtxCancel := context.WithDeadline(ctx, deadline)
	defer sigTermCtxCancel()

	err := waitContainerStop(sigTermCtx, done)
	if err == nil {
		slog.InfoContext(ctx, "Container exited successfully", slog.String("container", name))
		return nil
	}

	// The wait itself failed, not timed out, so don't escalate: the worker
	// shutting down will kill the containers.
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("wait failed: %w", err)
	}

	// If the parent context was cancelled or exceeded return immediately
	if ctx.Err() != nil {
		return ctx.Err()
	}

	slog.WarnContext(ctx, "Grace period expired; killing container", slog.String("container", name))
	if err := rcmd.cmdKill(ctx, name, "SIGKILL"); err != nil {
		slog.WarnContext(ctx, "Failed to send SIGKILL to container (it might have already exited)", slog.String("container", name), slog.Any("err", err))
	}

	killCtx, cancel := context.WithTimeout(ctx, containerKillTimeout)
	defer cancel()

	err = waitContainerStop(killCtx, done)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("container %q failed to exit even after SIGKILL: %w", name, err)
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
	}

	slog.InfoContext(ctx, "Container exited after SIGKILL", slog.String("container", name))
	return nil
}

// waitContainerStop waits for container exit or context termination.
func waitContainerStop(ctx context.Context, done <-chan error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func containerNames(containers []*ateworkerpb.Container) []string {
	names := make([]string, 0, len(containers))
	for _, c := range containers {
		names = append(names, c.GetName())
	}
	return names
}

// listSnapshotFiles returns the (relative) names of regular files directly under
// dir and under dir/fs, which atelet ships to object storage as the snapshot.
func listSnapshotFiles(dir string) ([]string, error) {
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
	fsEntries, err := os.ReadDir(filepath.Join(dir, resources.FSCheckpointSubdir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, e := range fsEntries {
		if e.Type().IsRegular() {
			files = append(files, filepath.Join(resources.FSCheckpointSubdir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// stopContainers stops the actor's application containers. The pause container
// is left running: it is the sandbox, and cleanupContainers deletes the others
// through it before deleting it last. Killing it first leaves the sentry a
// zombie until reaped, which runsc delete mistakes for a live sandbox.
func stopContainers(ctx context.Context, rcmd containerRuntime, containers []*ateworkerpb.Container) {
	for _, ctr := range containers {
		_ = rcmd.cmdKill(ctx, ctr.GetName(), "SIGKILL")
		_ = rcmd.cmdWait(ctx, ctr.GetName())
	}
}

func cleanupContainers(ctx context.Context, rcmd containerRuntime, containers []*ateworkerpb.Container) error {
	// Application containers first, the pause (root) container last.
	names := make([]string, 0, len(containers)+1)
	for _, ctr := range containers {
		names = append(names, ctr.GetName())
	}
	names = append(names, ocispec.PauseContainer)

	// Check state of all containers to mimic containerd.
	// Without this, `runsc delete` occasionally throws an error.
	present := make([]string, 0, len(names))
	for _, name := range names {
		if err := rcmd.cmdState(ctx, name); err != nil {
			err = fmt.Errorf("while checking state of %q container: %w", name, err)
			gone, listErr := isContainerAlreadyGone(ctx, rcmd, name)
			if listErr != nil {
				return errors.Join(err, listErr)
			}
			if gone {
				slog.InfoContext(ctx, "runsc container already destroyed, skipping its cleanup", slog.String("container", name))
				continue
			}
			return err
		}
		present = append(present, name)
	}

	for _, name := range present {
		if err := rcmd.cmdDelete(ctx, name); err != nil {
			return fmt.Errorf("while deleting %q container: %w", name, err)
		}
	}

	return nil
}

// isContainerAlreadyGone reports whether runsc no longer has a record of the
// container.
func isContainerAlreadyGone(ctx context.Context, rcmd containerRuntime, name string) (bool, error) {
	ids, err := rcmd.cmdList(ctx)
	if err != nil {
		return false, err
	}
	return !slices.Contains(ids, name), nil
}

func deleteContainers(ctx context.Context, rcmd *runsc, containers []string) {
	for _, container := range slices.Backward(containers) {
		if err := rcmd.cmdDelete(ctx, container); err != nil {
			slog.WarnContext(ctx, "Failed to delete runsc container after failure",
				"container", container, "err", err)
		}
	}
}
