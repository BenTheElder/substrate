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
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// dropPageCache drops the page cache backing path.
//
// FADV_DONTNEED silently skips dirty pages, so the write-back has to happen first
// or this does nothing for a file we have just written — hence the Fsync. That is
// also why this is not free: it makes writeback that the kernel would have done in
// the background synchronous here instead.
func dropPageCache(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	// Fsync on a read-only descriptor is allowed and is what makes the pages clean
	// and therefore evictable.
	if err := unix.Fsync(int(f.Fd())); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
		return fmt.Errorf("fadvise: %w", err)
	}
	return nil
}

// dropSnapshotPageCache evicts the page cache of a snapshot ateom has just written.
//
// Guest RAM is charged to the worker pod's cgroup as the shared memfd, which is
// accounted for by the VMM reserve. A checkpoint adds to that a second copy of the
// same bytes as ordinary page cache, because cloud-hypervisor writes the memory
// image through the cache: measured on the counter demo, a pause/resume took the pod
// cgroup from 94MiB to 153MiB and left ~78MiB of it behind afterwards, on a snapshot
// of ~80MiB.
//
// Nothing in this pod reads that back. atelet ships the snapshot from its own pod, so
// the pages it needs are charged to atelet when it reads them, and a restore stages a
// fresh copy. Keeping them here only inflates the worker's footprint and the reserve
// that has to cover it.
//
// Best-effort by design: failing to drop cache costs memory, not correctness, and it
// must not fail a checkpoint that has already succeeded.
func dropSnapshotPageCache(ctx context.Context, dir string, files []string) {
	for _, name := range files {
		path := filepath.Join(dir, name)
		if err := dropPageCache(path); err != nil {
			slog.WarnContext(ctx, "Failed to drop snapshot page cache",
				slog.String("path", path), slog.Any("err", err))
		}
	}
}
