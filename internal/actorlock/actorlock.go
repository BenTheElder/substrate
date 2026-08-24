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

// Package actorlock serializes an ateom's lifecycle RPCs per actor.
//
// Both ateoms need exactly this: a worker hosts several actors, activations of
// different actors share nothing worth serializing, and graceful shutdown must
// be able to stop waiting for a lock an RPC is wedged holding. It lives here
// rather than in either command because a refcounted lock map is subtle enough
// that two copies would drift.
package actorlock

import (
	"context"
	"sync"
)

// CancelableMutex is a mutex whose acquisition can be abandoned. sync.Mutex has
// no bounded Lock, and graceful shutdown must not park forever behind an RPC
// that is wedged: it needs to give up and get on with signaling the guest while
// the pod's termination grace period still has room.
type CancelableMutex struct {
	ch chan struct{}
}

// NewCancelableMutex returns an unlocked CancelableMutex.
func NewCancelableMutex() *CancelableMutex {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return &CancelableMutex{ch: ch}
}

// Lock acquires the mutex, blocking until it is free.
func (m *CancelableMutex) Lock() {
	<-m.ch
}

// Unlock releases the mutex.
func (m *CancelableMutex) Unlock() {
	m.ch <- struct{}{}
}

// LockContext acquires the mutex, reporting false if ctx terminates first. On
// false the mutex is NOT held and must not be unlocked.
func (m *CancelableMutex) LockContext(ctx context.Context) bool {
	select {
	case <-m.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// Locks serializes the lifecycle RPCs per actor rather than per process.
//
// One lock for the whole ateom made every activation wait for the one before
// it, which is most of the reason to put several actors on a worker in the first
// place: the slow parts of a boot -- unpacking the bundle, creating and
// restoring the sandbox, waiting for readyz -- are the actor's own and do not
// touch anything another actor is using. What they share is the nftables
// ruleset, which gets its own short lock rather than one held across a boot.
//
// Two RPCs naming the SAME actor still serialize. The control plane should not
// send them, but "should not" is not a guarantee, and a checkpoint racing a
// restore of one actor would interleave sandbox calls against a single sandbox.
type Locks struct {
	mu sync.Mutex
	// held is the lock per actor, with a reference count so an entry can be
	// dropped once nobody is using it. Without the count the map would grow by
	// one entry for every actor a long-lived worker ever hosted; deleting
	// without it would free a lock another caller is about to take.
	held map[string]*heldLock
}

type heldLock struct {
	mutex *CancelableMutex
	refs  int
}

// New returns an empty set of per-actor locks.
func New() *Locks {
	return &Locks{held: map[string]*heldLock{}}
}

// Lock takes the named actor's lock, waiting until ctx is done. It reports
// whether the lock was taken; a false return means ctx ended first and the
// caller holds nothing.
func (l *Locks) Lock(ctx context.Context, actorUID string) bool {
	l.mu.Lock()
	entry, ok := l.held[actorUID]
	if !ok {
		entry = &heldLock{mutex: NewCancelableMutex()}
		l.held[actorUID] = entry
	}
	entry.refs++
	l.mu.Unlock()

	// entry, not another map read: the map is only safe under l.mu, and the
	// entry cannot be freed while this reference is counted.
	if entry.mutex.LockContext(ctx) {
		return true
	}
	// Not taken, so drop the reference this attempt added.
	l.release(actorUID, false)
	return false
}

// Unlock releases the named actor's lock.
func (l *Locks) Unlock(actorUID string) {
	l.release(actorUID, true)
}

// release drops one reference, unlocking first when the caller held the lock,
// and forgets the entry once nothing references it.
func (l *Locks) release(actorUID string, wasHeld bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.held[actorUID]
	if !ok {
		return
	}
	if wasHeld {
		entry.mutex.Unlock()
	}
	entry.refs--
	if entry.refs <= 0 {
		delete(l.held, actorUID)
	}
}
