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

// claimLocks serializes claims of one worker within this process, avoiding
// optimistic-concurrency retries between local goroutines. Every worker in the
// cluster hashes into one table; nothing here is per node. The store still
// arbitrates across replicas. Collisions serialize unrelated workers, and the
// zero value is ready to use.
type claimLocks struct {
	locks [claimLockCount]sync.Mutex
}

// claimLockCount is sized against the claims in flight at once, not the fleet:
// a collision costs only while two claims overlap. A power of two for the mask.
const claimLockCount = 8192

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
