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

package controlapi

import (
	"errors"
	"sync"
)

// claimLocks serializes worker claims by worker, within this process.
//
// Claiming a worker is a read-modify-write of that worker's record under a
// version precondition. Optimistic concurrency is the right shape for it --
// several ateapi replicas can be claiming at once and nothing but the store can
// arbitrate between them -- but it degrades badly when the racing writers are
// GOROUTINES rather than replicas. Every actor activating onto a given worker
// wants that same record, all but one lose each round, and the losers burn a
// full read-modify-write to discover it. Measured with 128 activations onto one
// worker, a widened retry budget and a store re-read still left several
// refused, because no amount of retrying makes 128 writers fit through a
// one-winner-per-round gate.
//
// Holding this across the claim removes the goroutine half of that contention
// outright: within a replica the claims queue instead of colliding, so each one
// reads current state and its compare-and-swap succeeds first time. What is
// left for the store to arbitrate is replica-versus-replica, which is a handful
// of writers rather than hundreds, and which the retry budget absorbs easily.
//
// A fixed set of locks rather than one per worker, keyed by hash. There is
// nothing to allocate, nothing to reference-count, and nothing to clean up when
// a worker goes away -- and the cost of a collision is only that two unrelated
// claims occasionally queue behind each other, which is exactly what the lock
// does on purpose anyway.
//
// The zero value is ready to use, which matters because tests build an
// ActorWorkflow as a struct literal and would otherwise get a nil dereference
// on a path that has nothing to do with what they are testing.
type claimLocks struct {
	locks [claimLockCount]sync.Mutex
}

// claimLockCount is comfortably above the per-node worker count and is a power
// of two so the hash reduction is a mask.
const claimLockCount = 256

// lock returns the unlock function for the named worker.
func (c *claimLocks) lock(workerName string) func() {
	m := &c.locks[bucket(workerName)]
	m.Lock()
	return m.Unlock
}

// bucket is FNV-1a, reduced to the lock count. Unseeded and not
// collision-resistant on purpose: nothing adversarial reaches this, and a
// collision only means two workers' claims queue behind each other.
func bucket(s string) uint64 {
	const (
		offset = 1469598103934665603
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := range len(s) {
		h ^= uint64(s[i])
		h *= prime
	}
	return h & (claimLockCount - 1)
}

// errWorkerFilledUp reports that the worker the scheduler picked no longer has
// room once read under the claim lock. Retryable, and not a failure: the next
// attempt re-runs scheduling and picks somewhere else.
var errWorkerFilledUp = errors.New("picked worker no longer has room")
