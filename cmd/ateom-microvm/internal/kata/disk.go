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

package kata

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
)

// BuildExt4Image creates a raw ext4 disk image at outPath of sizeMiB megabytes,
// pre-populated with the contents of srcDir, in a single mkfs pass
// (`mkfs.ext4 -d <srcDir> <out> <blocks>`). This is how the ateom-owned-boot path
// turns the actor's OCI bundle rootfs into a writable virtio-blk disk (/dev/vdb):
// the guest mounts it as the container rootfs, so rootfs writes land on this
// host-backed file (off guest RAM) -> memory-only CH snapshot, no balloon.
//
// Requires mkfs.ext4 (e2fsprogs) on PATH in the worker image. The image is
// recreated from scratch each call (reset-to-golden recreates it from the golden
// bundle), so any prior file at outPath is truncated.
//
// mkfs.ext4 -d copies srcDir's tree (perms, ownership, symlinks, xattrs) into the
// new filesystem without needing a loop mount or root's mount privileges — it
// writes the filesystem structures directly to the image file.
func BuildExt4Image(ctx context.Context, srcDir, outPath string, sizeMiB int) error {
	if sizeMiB <= 0 {
		return fmt.Errorf("BuildExt4Image: sizeMiB must be positive, got %d", sizeMiB)
	}
	if fi, err := os.Stat(srcDir); err != nil || !fi.IsDir() {
		return fmt.Errorf("BuildExt4Image: source %q is not a directory: %v", srcDir, err)
	}
	// Truncate to size first so mkfs writes into a sparse file of the right size
	// (mkfs.ext4 also accepts a size argument, but a pre-sized file is unambiguous
	// and keeps the on-disk size predictable for the snapshot config).
	if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("BuildExt4Image: removing stale image %q: %w", outPath, err)
	}
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("BuildExt4Image: creating image %q: %w", outPath, err)
	}
	if err := f.Truncate(int64(sizeMiB) * 1024 * 1024); err != nil {
		f.Close()
		return fmt.Errorf("BuildExt4Image: sizing image %q: %w", outPath, err)
	}
	f.Close()

	// -F: don't prompt (operating on a regular file, not a block device).
	// -q: quiet. -d: populate from srcDir. -E lazy_*=0: write tables eagerly so
	// the image is fully materialized (deterministic on-disk bytes, important for
	// the reset-to-golden "verbatim copy" approach later). -O ^has_journal: a
	// reset-each-restore rootfs gains nothing from a journal and it adds nondeterminism.
	args := []string{
		"-F", "-q",
		"-E", "lazy_itable_init=0,lazy_journal_init=0",
		"-O", "^has_journal",
		"-d", srcDir,
		outPath,
		strconv.Itoa(sizeMiB) + "M",
	}
	cmd := exec.CommandContext(ctx, "mkfs.ext4", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("BuildExt4Image: mkfs.ext4 %v: %w: %s", args, err, out)
	}
	return nil
}
