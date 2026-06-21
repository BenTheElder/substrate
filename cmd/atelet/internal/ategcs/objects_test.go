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

package ategcs

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// diskBlocks returns the number of 512-byte blocks the file occupies on disk (for
// the sparseness check), or 0 if unavailable on this platform/fs.
func diskBlocks(fi os.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks)
	}
	return 0
}

// TestCopyZstdSparse checks the sparse decompress is byte-exact (it's the guest
// memory image — corruption = a dead guest) and actually punches holes for the
// zero regions.
func TestCopyZstdSparse(t *testing.T) {
	// A mostly-zero image with non-zero data at the start, an interior block, the
	// tail-but-not-end, and aligned + unaligned sizes. Mirrors a guest memory-ranges:
	// scattered resident pages in a sea of zero (free) RAM.
	const size = 4 << 20 // 4 MiB
	want := make([]byte, size)
	for i := 0; i < 4096; i++ { // first page
		want[i] = byte(i%251 + 1)
	}
	for i := 1 << 20; i < (1<<20)+9000; i++ { // interior, crosses a 64KiB block boundary
		want[i] = byte(i%253 + 1)
	}
	for i := size - 5000; i < size-1000; i++ { // near the end, leaving a trailing zero hole
		want[i] = byte(i%249 + 1)
	}

	dir := t.TempDir()
	out := filepath.Join(dir, "memory-ranges")
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size64, written, err := copyZstdSparse(f, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("copyZstdSparse: %v", err)
	}
	if size64 != int64(len(want)) {
		t.Errorf("logical size = %d, want %d", size64, len(want))
	}
	if written >= int64(len(want)) {
		t.Errorf("written %d bytes; expected far less than %d for a mostly-zero image (not sparse)", written, len(want))
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-trip mismatch: len(got)=%d len(want)=%d", len(got), len(want))
	}

	// Best-effort sparseness check: the file should occupy fewer 512-byte blocks than
	// its apparent size (holes punched). fs-dependent, so only log if unavailable.
	if fi, err := os.Stat(out); err == nil {
		if st := diskBlocks(fi); st > 0 {
			actual := st * 512
			t.Logf("sparse: apparent=%d actual=%d written=%d", len(want), actual, written)
			if actual >= int64(len(want)) {
				t.Logf("note: file not sparse on this fs (actual=%d >= apparent=%d) — correctness still holds", actual, len(want))
			}
		}
	}
}
