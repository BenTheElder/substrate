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
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// residentPages reports how many of a file's pages are in the page cache, via
// mincore over a shared mapping — the same way the snapshot-size probes measure it.
func residentPages(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %q: %v", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatalf("mmap %q: %v", path, err)
	}
	defer unix.Munmap(data)

	pages := (int(st.Size()) + os.Getpagesize() - 1) / os.Getpagesize()
	vec := make([]byte, pages)
	if _, _, errno := unix.Syscall(unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&data[0])), uintptr(st.Size()), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
		t.Fatalf("mincore %q: %v", path, errno)
	}
	resident := 0
	for _, b := range vec {
		if b&1 == 1 {
			resident++
		}
	}
	return resident
}

// A snapshot cloud-hypervisor has just written is dirty page cache in the worker
// pod's cgroup, and FADV_DONTNEED skips dirty pages — so the interesting case is
// exactly this one: a file written and not synced.
func TestDropPageCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory-ranges")
	content := bytes.Repeat([]byte("ateom"), 2<<20) // 10MiB, enough to measure
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("writing %q: %v", path, err)
	}

	before := residentPages(t, path)
	if before == 0 {
		t.Skip("the file we just wrote is not in the page cache; nothing to drop")
	}

	if err := dropPageCache(path); err != nil {
		t.Fatalf("dropPageCache(%q) = %v", path, err)
	}

	// Allow a few stragglers: another process on the machine may fault a page back
	// in, and mmap itself can leave metadata pages resident.
	after := residentPages(t, path)
	if want := before / 10; after > want {
		t.Errorf("after dropPageCache, %d of %d pages still resident; want at most %d", after, before, want)
	}

	// Evicting cache must not change what the file says.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("re-reading %q: %v", path, err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("file contents changed after dropping its page cache")
	}
}
