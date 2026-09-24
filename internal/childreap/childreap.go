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

//go:build unix

// Package childreap reaps children that nothing else waits for, such as the
// orphans reparented to a container's PID 1.
//
// Subprocesses started through a Reaper are tracked by PID and left for
// exec.Cmd.Wait, and every other child is reaped by PID as it exits. Reaping
// never takes an exit status a tracked subprocess is waiting for, so it never
// has to wait for subprocesses to finish.
package childreap

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"sync"

	"golang.org/x/sys/unix"
)

// Reaper collects orphaned children. The zero value is not usable; call New.
type Reaper struct {
	mu sync.Mutex
	// untracked is broadcast whenever a PID leaves tracked.
	untracked *sync.Cond
	// tracked holds the PIDs of subprocesses an exec.Cmd.Wait will collect.
	tracked map[int]bool
	// wake requests a reap round.
	wake chan struct{}
}

// New returns a Reaper. Call Run to start reaping.
func New() *Reaper {
	r := &Reaper{tracked: map[int]bool{}, wake: make(chan struct{}, 1)}
	r.untracked = sync.NewCond(&r.mu)
	return r
}

// RunCommand runs cmd and leaves its exit status to cmd.Wait.
func (r *Reaper) RunCommand(cmd *exec.Cmd) error {
	pid, err := r.start(cmd)
	if err != nil {
		return err
	}
	defer r.untrack(pid)
	return cmd.Wait()
}

// CombinedOutput is RunCommand for callers that need cmd.CombinedOutput.
func (r *Reaper) CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
	if cmd.Stdout != nil || cmd.Stderr != nil {
		return nil, errors.New("childreap: Stdout or Stderr already set")
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := r.RunCommand(cmd)
	return out.Bytes(), err
}

// start starts cmd and tracks it before any reap round can see it exit: a
// round holds mu, and so cannot run between the fork and the tracking.
func (r *Reaper) start(cmd *exec.Cmd) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	r.tracked[cmd.Process.Pid] = true
	return cmd.Process.Pid, nil
}

func (r *Reaper) untrack(pid int) {
	r.mu.Lock()
	delete(r.tracked, pid)
	r.untracked.Broadcast()
	r.mu.Unlock()
	// A round that ran while pid was tracked skipped it; another child may
	// since have been given the PID and exited.
	r.request()
}

// request asks Run for a reap round without blocking.
func (r *Reaper) request() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run reaps until ctx ends. Call it once in its own goroutine.
func (r *Reaper) Run(ctx context.Context) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGCHLD)
	defer signal.Stop(sigs)
	for {
		r.reap(ctx)
		select {
		case <-ctx.Done():
			return
		case <-sigs:
		case <-r.wake:
		}
	}
}

// reap collects every exited child that no exec.Cmd.Wait is waiting for.
func (r *Reaper) reap(ctx context.Context) {
	for {
		pid, err := peekExited()
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case errors.Is(err, unix.ECHILD):
			return
		case err != nil:
			slog.WarnContext(ctx, "Finding exited children failed", slog.Any("err", err))
			return
		case pid == 0:
			return
		}
		r.mu.Lock()
		// A tracked child's Wait is already collecting it, and will untrack it
		// right after.
		for r.tracked[pid] {
			r.untracked.Wait()
		}
		// Still under mu, so no new subprocess can be given pid meanwhile. If
		// its Wait took it, this finds nothing.
		var status unix.WaitStatus
		_, err = unix.Wait4(pid, &status, unix.WNOHANG, nil)
		r.mu.Unlock()
		if err != nil && !errors.Is(err, unix.ECHILD) && !errors.Is(err, unix.EINTR) {
			slog.WarnContext(ctx, "Reaping a child failed", slog.Int("pid", pid), slog.Any("err", err))
			return
		}
	}
}
