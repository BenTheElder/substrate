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
// Claiming a worker is a read-modify-write of its record under a version
// precondition. Optimistic concurrency suits racing ateapi replicas, but not
// racing goroutines: every actor activating onto a worker wants the same
// record, all but one lose each round, and the losers burn a full
// read-modify-write to find out. Serializing here leaves only
// replica-versus-replica for the store to arbitrate, which the retry budget
// absorbs easily.
//
// A fixed set of locks keyed by hash, so there is nothing to allocate or clean
// up when a worker goes away. A hash collision only makes two unrelated claims
// queue, which is what the lock does anyway.
//
// The zero value is ready to use: tests build an ActorWorkflow as a struct
// literal.
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

// bucket is FNV-1a, reduced to the lock count. Unseeded: nothing adversarial
// reaches this.
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

// errWorkerFilledUp reports that the picked worker no longer has room once read
// under the claim lock. Retryable: the next attempt re-runs scheduling.
var errWorkerFilledUp = errors.New("picked worker no longer has room")
