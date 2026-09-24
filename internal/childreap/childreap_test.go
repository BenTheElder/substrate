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

//go:build linux

package childreap

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func startReaper(t *testing.T) *Reaper {
	t.Helper()
	r := New()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go r.Run(ctx)
	return r
}

// startOrphan starts a child that nothing waits for, as an orphan reparented
// to PID 1 would be.
func startOrphan(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	return cmd
}

// exists reports whether pid names a process, counting an unreaped zombie.
// Process.Signal cannot tell: it signals through a pidfd, which refuses once
// the process has exited, reaped or not.
func exists(pid int) bool {
	return unix.Kill(pid, 0) == nil
}

// waitGone waits for the child to have been reaped.
func waitGone(t *testing.T, cmd *exec.Cmd, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for exists(cmd.Process.Pid) {
		if time.Now().After(deadline) {
			t.Fatalf("child %d was not reaped within %v", cmd.Process.Pid, within)
		}
		time.Sleep(time.Millisecond)
	}
}

// signalWriter closes ch on the first write, which tells a test its subprocess
// is running.
type signalWriter chan struct{}

func (w signalWriter) Write(p []byte) (int, error) {
	select {
	case <-w:
	default:
		close(w)
	}
	return len(p), nil
}

func TestReapCollectsOrphans(t *testing.T) {
	startReaper(t)
	waitGone(t, startOrphan(t, "true"), 5*time.Second)
}

// A subprocess in flight does not hold off reaping: a dead sandbox must be
// gone before the next runsc command looks for it.
func TestOrphansAreReapedWhileSubprocessesRun(t *testing.T) {
	r := startReaper(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	started := make(chan struct{})
	cmd := exec.CommandContext(ctx, "sh", "-c", "echo; exec sleep 30")
	cmd.Stdout = signalWriter(started)
	go func() { done <- r.RunCommand(cmd) }()
	t.Cleanup(func() { cancel(); <-done })
	<-started

	orphan := startOrphan(t, "true")
	waitGone(t, orphan, time.Second)
}

// Reaping never takes an exit status a subprocess's Wait expects, however the
// two interleave.
func TestSubprocessExitStatusesAreNeverTaken(t *testing.T) {
	r := startReaper(t)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := range 200 {
		wg.Go(func() {
			// Orphans exiting throughout keep the reaper busy.
			if i%4 == 0 {
				_ = startOrphan(t, "true")
			}
			code := i % 7
			err := r.RunCommand(exec.Command("sh", "-c", fmt.Sprintf("exit %d", code)))
			var exitErr *exec.ExitError
			switch {
			case code == 0 && err != nil:
				errs <- fmt.Errorf("exit 0: got %v", err)
			case code != 0 && (!errors.As(err, &exitErr) || exitErr.ExitCode() != code):
				errs <- fmt.Errorf("exit %d: got %v", code, err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// WaitGone reaps a child of its own and returns once the PID is free.
func TestWatchWaitGone(t *testing.T) {
	orphan := startOrphan(t, "sleep", "0.2")
	w, err := WatchPID(orphan.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := w.WaitGone(ctx); err != nil {
		t.Fatalf("WaitGone: %v", err)
	}
	if exists(orphan.Process.Pid) {
		t.Error("WaitGone returned while the process still existed")
	}
}

// A Reaper collecting the child first does not strand WaitGone.
func TestWatchWaitGoneAfterAReaper(t *testing.T) {
	startReaper(t)
	orphan := startOrphan(t, "sleep", "0.2")
	w, err := WatchPID(orphan.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	waitGone(t, orphan, 5*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := w.WaitGone(ctx); err != nil {
		t.Fatalf("WaitGone after the Reaper reaped: %v", err)
	}
}

func TestWatchWaitGoneHonorsCancellation(t *testing.T) {
	orphan := startOrphan(t, "sleep", "30")
	t.Cleanup(func() { _ = orphan.Process.Kill(); _ = orphan.Wait() })
	w, err := WatchPID(orphan.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := w.WaitGone(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("WaitGone on a live process: got %v, want DeadlineExceeded", err)
	}
}

func TestCombinedOutput(t *testing.T) {
	r := startReaper(t)
	out, err := r.CombinedOutput(exec.Command("sh", "-c", "echo out; echo err >&2"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(string(out)); len(got) != 2 {
		t.Errorf("CombinedOutput = %q, want both streams", out)
	}
	cmd := exec.Command("true")
	cmd.Stdout = &strings.Builder{}
	if _, err := r.CombinedOutput(cmd); err == nil {
		t.Error("CombinedOutput with Stdout already set succeeded")
	}
}
