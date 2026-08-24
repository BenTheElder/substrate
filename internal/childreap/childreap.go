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

// Package childreap reaps orphaned child processes without serializing the
// subprocesses the program runs itself.
//
// An ateom is PID 1 of its pod's PID namespace, so orphans land on it and
// something must wait() for them. The difficulty is that wait4(-1) takes ANY
// child, including one os/exec is about to collect: lose that race and exec's
// Wait returns ECHILD, so a healthy subprocess reads as failed.
//
// Guarding that with a RWMutex -- read-held per subprocess, write-held to reap
// -- is correct but collapses under concurrency. With thousands of children
// SIGCHLD is continuous, so a writer is permanently queued, and Go's RWMutex
// blocks new readers the moment one is. Subprocesses then advance in lock-step,
// each waiting for the slowest one running.
//
// So reaping waits for a moment when the program is running no subprocesses of
// its own, which costs them nothing. It is only forced -- excluding new
// subprocesses and draining the running ones -- if no such moment arrives for
// MaxDefer.
package childreap

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// MaxDefer bounds how long reaping waits for a quiet moment before forcing its
// way in. Long deliberately: a late-reaped child costs only a PID and a
// task_struct, and the orphans this exists for appear at teardown, when the
// program is not saturated with subprocesses of its own.
const MaxDefer = 60 * time.Second

// Reaper collects orphaned children. The zero value is not usable; call New.
type Reaper struct {
	mu sync.Mutex
	// cond is broadcast when inFlight reaches zero or reaping ends, which are
	// the two things either side waits for.
	cond *sync.Cond
	// inFlight counts subprocesses the program is running itself, from just
	// before the fork to just after its status is collected.
	inFlight int
	// reaping excludes new subprocesses for the (microseconds) that wait4 runs.
	reaping bool
	// draining is set while a forced reap waits for inFlight to fall to zero.
	// New subprocesses wait rather than joining, so the drain can finish.
	draining bool
}

func New() *Reaper {
	r := &Reaper{}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Enter marks the start of a stretch in which the caller runs subprocesses and
// collects their exit statuses; reaping will not take a child out from under
// it. The returned function ends that stretch and must be called.
//
//	defer reaper.Enter()()
//
// It blocks only while a reap is actually running, never for the duration of
// another caller's subprocess.
func (r *Reaper) Enter() func() {
	r.enter()
	return r.leave
}

func (r *Reaper) enter() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.reaping || r.draining {
		r.cond.Wait()
	}
	r.inFlight++
}

func (r *Reaper) leave() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight--
	if r.inFlight == 0 {
		r.cond.Broadcast()
	}
}

// Run reaps until ctx ends. Intended to be called once, in its own goroutine.
func (r *Reaper) Run(ctx context.Context) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGCHLD)
	defer signal.Stop(sigs)

	for {
		select {
		case <-ctx.Done():
			return
		case <-sigs:
		}
		// One activation's subprocesses produce a burst of SIGCHLD, and they all
		// want the same single pass over wait4.
		drain(sigs)
		r.reapOnce(ctx)
	}
}

// drain empties ch without blocking.
func drain(ch <-chan os.Signal) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// reapOnce waits for a quiet moment, then collects everything waiting.
func (r *Reaper) reapOnce(ctx context.Context) {
	if !r.acquire(ctx) {
		return
	}
	defer r.release()
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
		switch err {
		case nil:
			if pid <= 0 {
				// Children exist but none have exited. Another SIGCHLD will
				// bring us back.
				return
			}
		case unix.EINTR:
			continue
		case unix.ECHILD:
			return
		default:
			slog.WarnContext(ctx, "Reaping children failed", slog.Any("err", err))
			return
		}
	}
}

// acquire blocks until no subprocess of the program's own is running, then
// excludes new ones for the duration of the reap. Reports false if ctx ended
// first. After MaxDefer it holds off new subprocesses and drains the running
// ones instead, which exists for liveness rather than the common case.
func (r *Reaper) acquire(ctx context.Context) bool {
	// Broadcast on the deadline so a waiter blocked in cond.Wait re-evaluates;
	// sync.Cond has no timed wait.
	forced := false
	timer := time.AfterFunc(MaxDefer, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		forced = true
		r.draining = true
		r.cond.Broadcast()
	})
	defer timer.Stop()

	stop := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.cond.Broadcast()
	})
	defer stop()

	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if ctx.Err() != nil {
			r.draining = false
			r.cond.Broadcast()
			return false
		}
		// Another reap is in progress (it drained on our behalf); let it.
		if r.reaping {
			r.cond.Wait()
			continue
		}
		if r.inFlight == 0 {
			if forced {
				slog.WarnContext(ctx, "Reaped children only after holding off subprocesses",
					slog.Duration("waited", MaxDefer))
			}
			r.draining = false
			r.reaping = true
			return true
		}
		r.cond.Wait()
	}
}

func (r *Reaper) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reaping = false
	r.cond.Broadcast()
}
