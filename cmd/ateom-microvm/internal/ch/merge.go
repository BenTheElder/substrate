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

package ch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

	"golang.org/x/sys/unix"
)

// MergeSparseOverlay reconstructs a COMPLETE memory snapshot from an OnDemand
// (userfaultfd) restore. CH's new snapshot (delta) contains only the pages the
// guest faulted in since the OnDemand restore; every other page is unchanged from
// the snapshot it restored FROM (base). So the complete current memory =
// base, with delta's populated pages overlaid.
//
// It writes out = a sparse copy of base, then overlays every DATA region of delta
// (located via SEEK_DATA/SEEK_HOLE, so holes — the un-faulted pages — are skipped)
// at the same byte offsets. base and delta MUST be flat images of identical size
// and layout (CH memory-ranges of the same guest + CH version), which holds across
// a restore/snapshot of one actor. This is a Firecracker-style differential
// snapshot implemented on top of CH (which has no native diff snapshot): it lets us
// keep OnDemand's fast, non-densifying restore while still producing complete,
// re-restorable snapshots for the suspend/resume chain.
func MergeSparseOverlay(ctx context.Context, base, delta, out string) error {
	bi, err := os.Stat(base)
	if err != nil {
		return fmt.Errorf("stat base %q: %w", base, err)
	}
	// out := sparse copy of base (preserves holes so the merged image stays sparse).
	tmp := out + ".merge.tmp"
	_ = os.Remove(tmp)
	if o, err := exec.CommandContext(ctx, "cp", "--sparse=always", base, tmp).CombinedOutput(); err != nil {
		return fmt.Errorf("cp base->tmp: %w: %s", err, o)
	}

	d, err := os.Open(delta)
	if err != nil {
		return fmt.Errorf("open delta %q: %w", delta, err)
	}
	defer d.Close()
	di, err := d.Stat()
	if err != nil {
		return err
	}
	if di.Size() != bi.Size() {
		// Same guest => identical memory-ranges length. A mismatch means the overlay
		// offsets wouldn't line up, so refuse rather than corrupt.
		return fmt.Errorf("MergeSparseOverlay: size mismatch base=%d delta=%d", bi.Size(), di.Size())
	}

	o, err := os.OpenFile(tmp, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer o.Close()

	dfd := int(d.Fd())
	size := di.Size()
	buf := make([]byte, 1<<20)
	off := int64(0)
	for off < size {
		// Next populated region [ds, de) in delta.
		ds, err := unix.Seek(dfd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break // no more data
			}
			return fmt.Errorf("SEEK_DATA: %w", err)
		}
		de, err := unix.Seek(dfd, ds, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("SEEK_HOLE: %w", err)
		}
		if _, err := d.Seek(ds, io.SeekStart); err != nil {
			return err
		}
		if _, err := o.Seek(ds, io.SeekStart); err != nil {
			return err
		}
		remaining := de - ds
		for remaining > 0 {
			n := int64(len(buf))
			if n > remaining {
				n = remaining
			}
			r, err := io.ReadFull(d, buf[:n])
			if r > 0 {
				if _, werr := o.Write(buf[:r]); werr != nil {
					return werr
				}
			}
			if err != nil {
				return fmt.Errorf("reading delta region: %w", err)
			}
			remaining -= int64(r)
		}
		off = de
	}
	if err := o.Sync(); err != nil {
		return err
	}
	if err := o.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, out)
}
