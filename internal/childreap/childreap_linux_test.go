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

package childreap

import (
	"context"
	"os/exec"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestShortSubprocessesAreNotPacedByLongOnes is the regression test for the
// convoy.
//
// The arrangement it guards against: a reaper that takes an exclusive lock on
// every SIGCHLD, against subprocesses that hold a shared one for their whole
// lifetime. Correct, and it collapses under concurrency, because a writer is
// then permanently queued and Go's RWMutex stops admitting readers as soon as
// one is -- so subprocesses stop overlapping and advance in rounds, each round
// as long as its slowest member.
//
// Reproducing that needs more than running N things at once: start them
// together and they all take the read lock before any writer queues, and it
// looks fine. What exposes it is CONTINUOUS arrival with MIXED durations, which
// is what a worker activating actors produces -- most invocations are quick,
// some are not, and they keep coming. Under a convoy the quick ones inherit the
// slow one's duration; without it they do not.
//
// So the assertion is on the short subprocesses alone: they must stay short
// while a long one is always in flight beside them.
func TestShortSubprocessesAreNotPacedByLongOnes(t *testing.T) {
	const (
		shortRunners = 8
		short        = 20 * time.Millisecond
		long         = 400 * time.Millisecond
		testFor      = 3 * time.Second
	)

	r := New()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.Run(ctx)

	run, stop := context.WithTimeout(ctx, testFor)
	defer stop()
	var wg sync.WaitGroup

	// SIGCHLD without pause, so the reaper always wants to run. A worker with
	// thousands of children gets this for free.
	for range 4 {
		wg.Go(func() {
			for run.Err() == nil {
				cmd := exec.Command("/bin/true")
				if cmd.Start() != nil {
					return
				}
				_ = cmd.Wait()
			}
		})
	}

	// One long subprocess always in flight. This is the round-pacer: under a
	// convoy every short subprocess waits behind it.
	wg.Go(func() {
		for run.Err() == nil {
			_ = runUnder(r, exec.Command("/bin/sleep", "0.4"))
		}
	})

	var mu sync.Mutex
	var samples []time.Duration
	for range shortRunners {
		wg.Go(func() {
			for run.Err() == nil {
				started := time.Now()
				err := runUnder(r, exec.Command("/bin/sleep", "0.02"))
				elapsed := time.Since(started)
				mu.Lock()
				if err != nil {
					t.Errorf("short subprocess: %v", err)
				}
				samples = append(samples, elapsed)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(samples) < 50 {
		t.Fatalf("only %d short subprocesses completed in %s; too few to judge", len(samples), testFor)
	}
	slices.Sort(samples)
	p90 := samples[len(samples)*90/100]

	// A 20ms sleep plus fork/exec is a few tens of ms. Paced by the 400ms
	// runner it would be hundreds. The gap between those is wide enough that
	// this needs no tuning.
	if limit := 8 * short; p90 > limit {
		t.Errorf("p90 of %d short subprocesses was %s, want under %s; they are being paced by the long one (median %s, max %s)",
			len(samples), p90.Round(time.Millisecond), limit,
			samples[len(samples)/2].Round(time.Millisecond), samples[len(samples)-1].Round(time.Millisecond))
	}
	t.Logf("%d short subprocesses: p50=%s p90=%s max=%s", len(samples),
		samples[len(samples)/2].Round(time.Millisecond), p90.Round(time.Millisecond),
		samples[len(samples)-1].Round(time.Millisecond))
}

// runUnder runs cmd inside the reaper's exec window.
func runUnder(r *Reaper, cmd *exec.Cmd) error {
	defer r.Enter()()
	return cmd.Run()
}

// TestReapCollectsOrphans pins the reason any of this exists: a child nobody
// waits for must not stay a zombie.
func TestReapCollectsOrphans(t *testing.T) {
	r := New()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.Run(ctx)

	// Started and abandoned: nothing ever calls Wait on it, so only the reaper
	// can collect it.
	cmd := exec.Command("/bin/true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	pid := cmd.Process.Pid

	deadline := time.Now().Add(10 * time.Second)
	for {
		// Signal 0 probes for existence; a reaped child is gone entirely, while
		// an unreaped zombie still answers.
		if err := cmd.Process.Signal(nil); err != nil {
			return // reaped
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %d was never reaped", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestExecIsExcludedWhileReaping pins the correctness half: Exec must not
// overlap a reap, or wait4(-1) can take a subprocess's exit status.
func TestExecIsExcludedWhileReaping(t *testing.T) {
	r := New()

	// Stand in for the reaper's critical section without racing a real one.
	if !r.acquire(context.Background()) {
		t.Fatal("acquire failed on an idle reaper")
	}

	entered := make(chan struct{})
	go func() {
		defer r.Enter()()
		close(entered)
	}()

	select {
	case <-entered:
		t.Fatal("Exec ran while a reap was in progress")
	case <-time.After(100 * time.Millisecond):
	}

	r.release()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Exec never ran after the reap finished")
	}
}
