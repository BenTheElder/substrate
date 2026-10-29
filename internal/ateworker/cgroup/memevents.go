//go:build linux

// Copyright 2026 The Agent Substrate Authors
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

package cgroup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// memoryGuardSweep is how often every target is checked, events or not, in
// case one was missed or a target couldn't be registered.
const memoryGuardSweep = 5 * time.Second

// MemoryTarget is a cgroup for WatchMemory to guard.
type MemoryTarget interface {
	// MemoryGuard returns the target's guard, which also identifies it.
	MemoryGuard() *MemoryGuard
	// MemoryCgroup returns the target's cgroup as root/name.
	MemoryCgroup() (root, name string)
	// MemoryKilled is told about each kill the guard starts.
	MemoryKilled(Kill)
}

// MemoryTargets lists what WatchMemory should guard right now.
type MemoryTargets interface {
	MemoryTargets() []MemoryTarget
}

// WatchMemory checks targets with their guards until ctx is done. A target is
// checked every MemoryGuardInterval once its cgroup reports a memory event,
// until it is no longer near its limit, and every target is checked about
// every memoryGuardSweep. Without epoll it checks every target every interval.
//
// The kernel notifies memory.events.local on each memory.high event, and polls
// on cgroup files report EPOLLPRI until the file is read again.
// https://github.com/torvalds/linux/blob/v7.0/mm/memcontrol.c#L1651-L1653
// https://github.com/torvalds/linux/blob/v7.0/fs/kernfs/file.c#L855-L863
func WatchMemory(ctx context.Context, targets MemoryTargets) {
	w, err := newMemoryEvents("memory.events.local")
	if err != nil {
		slog.WarnContext(ctx, "Checking every actor's memory on every tick", slog.Any("err", err))
	} else {
		defer w.close()
		go w.wait(ctx)
	}
	ticker := time.NewTicker(MemoryGuardInterval)
	defer ticker.Stop()
	lastSweep := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep := time.Since(lastSweep) >= memoryGuardSweep
			if sweep {
				lastSweep = time.Now()
			}
			current := targets.MemoryTargets()
			if w != nil {
				w.sync(ctx, current)
			}
			for _, t := range current {
				if sweep || w.hot(t.MemoryGuard()) {
					checkTarget(ctx, w, t)
				}
			}
		}
	}
}

func checkTarget(ctx context.Context, w *memoryEvents, t MemoryTarget) {
	root, name := t.MemoryCgroup()
	g := t.MemoryGuard()
	woke := w.clear(g)
	if kill, ok := g.check(ctx, root, name, woke); ok {
		t.MemoryKilled(kill)
	}
	if g.Near() {
		w.heat(g)
	} else {
		w.cool(g)
	}
}

// memoryEvents tracks which targets' cgroups reported memory events, through
// one epoll instance. Only WatchMemory's loop calls its methods, besides wait.
type memoryEvents struct {
	// file is the cgroup file whose notifications wake a target
	file string
	epfd int
	// woken carries the tokens of watches with events from wait
	woken chan uint64
	// waited is closed once wait returns, so the epoll fd can be closed
	waited chan struct{}
	// broken is set if wait gave up, so every target gets checked
	broken  atomic.Bool
	next    uint64
	byGuard map[*MemoryGuard]*eventWatch
	byToken map[uint64]*eventWatch
}

type eventWatch struct {
	token uint64
	// fd is the cgroup's file, -1 while it isn't registered. Raw, so Go's
	// own poller doesn't wake on its events too.
	fd int
	// hot is set from an event until the cgroup is no longer near its limit
	hot bool
	// woke is set from an event until the next check
	woke bool
	seen bool
}

func newMemoryEvents(file string) (*memoryEvents, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("while creating epoll instance: %w", err)
	}
	return &memoryEvents{
		file:    file,
		epfd:    epfd,
		woken:   make(chan uint64, 64),
		waited:  make(chan struct{}),
		byGuard: map[*MemoryGuard]*eventWatch{},
		byToken: map[uint64]*eventWatch{},
	}, nil
}

// wait forwards the tokens of cgroups with events to woken until ctx is done.
// It is the only method that runs outside WatchMemory's loop.
func (w *memoryEvents) wait(ctx context.Context) {
	defer close(w.waited)
	events := make([]unix.EpollEvent, 64)
	for ctx.Err() == nil {
		// a timeout, so a done ctx is noticed
		n, err := unix.EpollWait(w.epfd, events, 1000)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			w.broken.Store(true)
			slog.ErrorContext(ctx, "Failed to wait for memory events, checking every actor's memory on every tick", slog.Any("err", err))
			return
		}
		for _, e := range events[:n] {
			token := uint64(e.Fd)<<32 | uint64(uint32(e.Pad))
			select {
			case w.woken <- token:
			case <-ctx.Done():
				return
			}
		}
	}
}

// hot reports whether g's cgroup is near its limit or had an event since. A
// target that couldn't be registered is always hot.
func (w *memoryEvents) hot(g *MemoryGuard) bool {
	if w == nil || w.broken.Load() {
		return true
	}
	w.drain()
	ew := w.byGuard[g]
	return ew == nil || ew.fd < 0 || ew.hot
}

func (w *memoryEvents) drain() {
	for {
		select {
		case token := <-w.woken:
			if ew := w.byToken[token]; ew != nil {
				ew.hot, ew.woke = true, true
			}
		default:
			return
		}
	}
}

// sync registers new targets and drops ones that are gone.
func (w *memoryEvents) sync(ctx context.Context, targets []MemoryTarget) {
	for _, ew := range w.byGuard {
		ew.seen = false
	}
	for _, t := range targets {
		g := t.MemoryGuard()
		ew := w.byGuard[g]
		if ew == nil {
			w.next++
			ew = &eventWatch{token: w.next, fd: -1}
			w.byGuard[g] = ew
			w.byToken[ew.token] = ew
		}
		ew.seen = true
		if ew.fd < 0 {
			root, name := t.MemoryCgroup()
			if err := w.register(ew, filepath.Join(root, name, w.file)); err != nil {
				slog.DebugContext(ctx, "Checking a cgroup on every tick", slog.String("cgroup", name), slog.Any("err", err))
			}
		}
	}
	for g, ew := range w.byGuard {
		if !ew.seen {
			w.unregister(ew)
			delete(w.byGuard, g)
			delete(w.byToken, ew.token)
		}
	}
}

// register starts watching ew. It comes up hot, so the cgroup gets checked
// once even if it is already past its limit and has no new events.
func (w *memoryEvents) register(ew *eventWatch, path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", path, err)
	}
	if err := unix.EpollCtl(w.epfd, unix.EPOLL_CTL_ADD, fd, w.event(ew)); err != nil {
		unix.Close(fd)
		return fmt.Errorf("while watching %q: %w", path, err)
	}
	ew.fd, ew.hot = fd, true
	return nil
}

// clear reads g's cgroup file before a check, so an event during the check
// wakes it again, and reports whether it woke since the last check.
func (w *memoryEvents) clear(g *MemoryGuard) bool {
	if w == nil {
		return false
	}
	ew := w.byGuard[g]
	if ew == nil || ew.fd < 0 || !ew.hot {
		return false
	}
	if _, err := unix.Pread(ew.fd, make([]byte, 512), 0); err != nil {
		w.unregister(ew)
		return false
	}
	woke := ew.woke
	ew.woke = false
	return woke
}

// heat keeps g checked every interval, e.g. when a sweep finds it near its
// limit without an event.
func (w *memoryEvents) heat(g *MemoryGuard) {
	if w == nil {
		return
	}
	if ew := w.byGuard[g]; ew != nil {
		ew.hot = true
	}
}

// cool re-arms g's watch once its cgroup is no longer near its limit. A watch
// that can't be re-armed is dropped, and registered again by sync.
func (w *memoryEvents) cool(g *MemoryGuard) {
	if w == nil {
		return
	}
	ew := w.byGuard[g]
	if ew == nil || ew.fd < 0 || !ew.hot {
		return
	}
	// re-arming polls the file, so an event since clear wakes it right away
	if err := unix.EpollCtl(w.epfd, unix.EPOLL_CTL_MOD, ew.fd, w.event(ew)); err != nil {
		w.unregister(ew)
		return
	}
	ew.hot = false
}

func (w *memoryEvents) unregister(ew *eventWatch) {
	if ew.fd < 0 {
		return
	}
	_ = unix.EpollCtl(w.epfd, unix.EPOLL_CTL_DEL, ew.fd, nil)
	unix.Close(ew.fd)
	ew.fd = -1
}

// event is a one-shot wake for ew, carrying its token, so a reused fd number
// can't wake the wrong target.
func (w *memoryEvents) event(ew *eventWatch) *unix.EpollEvent {
	return &unix.EpollEvent{
		Events: unix.EPOLLPRI | unix.EPOLLONESHOT,
		Fd:     int32(ew.token >> 32),
		Pad:    int32(uint32(ew.token)),
	}
}

// close waits for wait to return, so call it once ctx is done.
func (w *memoryEvents) close() {
	<-w.waited
	for _, ew := range w.byGuard {
		w.unregister(ew)
	}
	unix.Close(w.epfd)
}
