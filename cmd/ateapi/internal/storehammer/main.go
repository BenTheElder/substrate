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

// storehammer drives the claim path against the store alone -- no atelet, no
// ateom, no gRPC -- to find what the database can take.
//
// A claim is a read of the worker and a BindActorToWorker at the version it was
// read at, which is what an activation does and the only part of it that
// touches contended rows. Two things bound it and they fail differently, so the
// harness separates them: total write rate against the instance, and the
// per-worker row lock that every claim onto one worker serializes on. Spreading
// the same rate over more workers isolates the first; concentrating it isolates
// the second.
package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/atepg"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/spf13/pflag"
)

var (
	dsn         = pflag.String("dsn", "", "PostgreSQL DSN. Required.")
	workers     = pflag.Int("workers", 128, "Worker records to seed and claim against.")
	claims      = pflag.Int("claims", 20000, "Total claims to attempt.")
	concurrency = pflag.Int("concurrency", 64, "Claims in flight at once.")
	release     = pflag.Bool("release", false, "Release each claim after binding it, doubling the writes per iteration.")
	seedOnly    = pflag.Bool("seed-only", false, "Seed the workers and exit.")
	keep        = pflag.Bool("keep", false, "Leave the seeded workers behind instead of deleting them.")
)

func main() {
	pflag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "--dsn is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := atepg.Connect(ctx, *dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connecting: %v\n", err)
		os.Exit(1)
	}

	run := uuid.NewString()[:8]
	names, err := seed(ctx, st, run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seeding: %v\n", err)
		os.Exit(1)
	}
	if !*keep {
		defer cleanup(st, names)
	}
	if *seedOnly {
		fmt.Printf("seeded %d workers with prefix %s\n", len(names), run)
		return
	}
	report(hammer(ctx, st, names))
}

// seed creates the workers to claim against. They are the harness's own, named
// with a per-run prefix, so a run neither disturbs nor is disturbed by whatever
// else the database holds.
func seed(ctx context.Context, st store.Interface, run string) ([]string, error) {
	names := make([]string, *workers)
	var (
		wg   sync.WaitGroup
		sem  = make(chan struct{}, 64)
		errs = make([]error, *workers)
	)
	for i := range *workers {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			name := uuid.NewString()
			if _, err := st.CreateWorker(ctx, &ateapipb.Worker{
				Metadata:        &ateapipb.ResourceMetadata{Name: name},
				WorkerNamespace: "storehammer",
				WorkerPool:      "storehammer-" + run,
				WorkerPod:       fmt.Sprintf("hammer-%s-%d", run, i),
				WorkerPodUid:    uuid.NewString(),
				NodeName:        fmt.Sprintf("node-%d", i),
				Ip:              "10.0.0.1",
				SandboxClass:    "gvisor",
				Capacity:        &ateapipb.WorkerCapacity{CpuMilli: 64000, MemoryBytes: 256 << 30, Actors: 4094},
				Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
			}); err != nil {
				errs[i] = fmt.Errorf("creating worker %d: %w", i, err)
				return
			}
			names[i] = name
		}()
	}
	wg.Wait()
	return names, errors.Join(errs...)
}

func cleanup(st store.Interface, names []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for _, n := range names {
		// DeleteWorker cascades the assignments the run bound to it.
		_, _ = st.DeleteWorker(ctx, n, store.DeletePreconditions{})
	}
}

type result struct {
	latencies []time.Duration
	conflicts int64
	failures  int64
	elapsed   time.Duration
}

// hammer runs the claims, each one a read of a randomly chosen worker and a bind
// at the version it was read at. A version conflict is retried rather than
// counted as a failure: that is what the resume workflow does, and counting it
// as an error would report contention as breakage.
func hammer(ctx context.Context, st store.Interface, names []string) result {
	var (
		mu        sync.Mutex
		latencies = make([]time.Duration, 0, *claims)
		conflicts atomic.Int64
		failures  atomic.Int64
		next      atomic.Int64
	)

	start := time.Now()
	var wg sync.WaitGroup
	for range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + next.Load()))
			for {
				if next.Add(1) > int64(*claims) || ctx.Err() != nil {
					return
				}
				actorUID := uuid.NewString()
				name := names[rng.Intn(len(names))]

				t0 := time.Now()
				err := claim(ctx, st, name, actorUID, &conflicts)
				took := time.Since(t0)

				if err != nil {
					failures.Add(1)
					continue
				}
				mu.Lock()
				latencies = append(latencies, took)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	return result{latencies: latencies, conflicts: conflicts.Load(), failures: failures.Load(), elapsed: time.Since(start)}
}

// claim is the activation's write path: read the worker, bind at that version,
// and optionally give it back. Retries a lost version race the way the resume
// workflow does.
func claim(ctx context.Context, st store.Interface, workerName, actorUID string, conflicts *atomic.Int64) error {
	const attempts = 50
	for range attempts {
		worker, err := st.GetWorker(ctx, workerName)
		if err != nil {
			return err
		}
		version := worker.GetMetadata().GetVersion()
		err = st.BindActorToWorker(ctx, workerName, version, &ateapipb.ActorAssignment{
			Actor:     &ateapipb.ObjectRef{Atespace: "storehammer", Name: actorUID},
			ActorUid:  actorUID,
			Resources: &ateapipb.WorkerCapacity{CpuMilli: 10, MemoryBytes: 1 << 20},
		})
		if err != nil {
			if isConflict(err) {
				conflicts.Add(1)
				continue
			}
			return err
		}
		if !*release {
			return nil
		}
		for range attempts {
			fresh, err := st.GetWorker(ctx, workerName)
			if err != nil {
				return err
			}
			if _, err := st.ReleaseActorFromWorker(ctx, workerName, fresh.GetMetadata().GetVersion(), actorUID); err != nil {
				if isConflict(err) {
					conflicts.Add(1)
					continue
				}
				return err
			}
			return nil
		}
		return fmt.Errorf("release of %s lost %d version races", actorUID, attempts)
	}
	return fmt.Errorf("claim of %s lost %d version races", actorUID, attempts)
}

func isConflict(err error) bool {
	return errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrUIDConflict)
}

func report(r result) {
	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
	n := len(r.latencies)
	at := func(q float64) time.Duration {
		if n == 0 {
			return 0
		}
		i := int(float64(n) * q)
		if i >= n {
			i = n - 1
		}
		return r.latencies[i]
	}
	rate := float64(n) / r.elapsed.Seconds()
	fmt.Printf("workers=%d concurrency=%d release=%v\n", *workers, *concurrency, *release)
	fmt.Printf("%d claims in %s = %.0f/s  (%.1f per worker)\n", n, r.elapsed.Round(time.Millisecond), rate, float64(n)/float64(*workers))
	fmt.Printf("p50=%s p90=%s p99=%s max=%s\n", at(0.5).Round(time.Microsecond), at(0.9).Round(time.Microsecond), at(0.99).Round(time.Microsecond), at(1).Round(time.Microsecond))
	fmt.Printf("version conflicts=%d (%.2f per claim)  failures=%d\n", r.conflicts, float64(r.conflicts)/float64(max(n, 1)), r.failures)
}
