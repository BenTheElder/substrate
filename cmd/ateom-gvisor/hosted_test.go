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
	"fmt"
	"sync"
	"testing"
)

// TestFreeSlotLockedReservesBeforeTheNetworkExists is the regression test for a
// race that only concurrency can produce, and that cost a scale run to find.
//
// hostActor registers an actor and THEN builds its network, which takes long
// enough to matter. The slot has to be reserved for that whole window. It was
// not: freeSlotLocked derived the taken set from hosted.network.Slot, and
// network is nil until SetupActorNetwork returns, so an actor mid-setup marked
// nothing. Concurrent activations therefore all chose the lowest free slot,
// built veths with the same name, and every loser failed with
// "bringing up host veth: no such device".
//
// Nothing sequential catches this: with one activation at a time the network is
// always built before the next call asks. It appeared within ten actors at a
// concurrency of twelve.
func TestFreeSlotLockedReservesBeforeTheNetworkExists(t *testing.T) {
	s := &AteomService{actors: map[string]*hostedActor{}}

	// Register an actor the way hostActor does before its network exists.
	s.actorsMu.Lock()
	slot, err := s.freeSlotLocked()
	if err != nil {
		t.Fatalf("freeSlotLocked: %v", err)
	}
	s.actors["actor-a"] = &hostedActor{slot: slot}
	s.actorsMu.Unlock()

	// A second activation arriving while the first is still in SetupActorNetwork
	// must not be handed the same slot.
	s.actorsMu.Lock()
	next, err := s.freeSlotLocked()
	s.actorsMu.Unlock()
	if err != nil {
		t.Fatalf("freeSlotLocked (second): %v", err)
	}
	if next == slot {
		t.Errorf("second activation got slot %d while the first still holds it; "+
			"the slot is not reserved until its network exists", next)
	}
}

// TestFreeSlotLockedConcurrentAllocationsAreUnique is the same property under
// real contention: N goroutines allocating at once must come away with N
// distinct slots, because the slot is what names the veth and the pod-side
// address. Two actors on one slot is not a degraded state, it is two actors
// fighting over one interface.
func TestFreeSlotLockedConcurrentAllocationsAreUnique(t *testing.T) {
	s := &AteomService{actors: map[string]*hostedActor{}}

	const n = 64
	var wg sync.WaitGroup
	slots := make([]int, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Exactly hostActor's critical section: allocate and register under
			// one hold of actorsMu, before any network exists.
			s.actorsMu.Lock()
			defer s.actorsMu.Unlock()
			slot, err := s.freeSlotLocked()
			if err != nil {
				t.Errorf("freeSlotLocked: %v", err)
				return
			}
			s.actors[fmt.Sprintf("actor-%d", i)] = &hostedActor{slot: slot}
			slots[i] = slot
		}()
	}
	wg.Wait()

	seen := map[int]int{}
	for i, slot := range slots {
		if prev, dup := seen[slot]; dup {
			t.Fatalf("actors %d and %d were both given slot %d", prev, i, slot)
		}
		seen[slot] = i
	}
	if len(seen) != n {
		t.Errorf("got %d distinct slots for %d actors, want %d", len(seen), n, n)
	}
}
