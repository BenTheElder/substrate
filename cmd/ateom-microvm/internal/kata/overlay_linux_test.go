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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The container rootfs is untrusted (the image below, the guest's own snapshot
// upper above): a symlink planted at proc/sys/dev must not send the mountpoint
// mkdir somewhere else on the worker pod.
func TestEnsureOCIMountpoints(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(dir, "outside")
	rootfs := filepath.Join(dir, "rootfs")
	if err := os.MkdirAll(filepath.Join(rootfs, "realsys"), 0o755); err != nil {
		t.Fatal(err)
	}
	// proc escapes, sys is an in-rootfs symlink to an existing dir, dev is absent.
	if err := os.Symlink(filepath.Join(outside, "proc"), filepath.Join(rootfs, "proc")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("realsys", filepath.Join(rootfs, "sys")); err != nil {
		t.Fatal(err)
	}

	if err := ensureOCIMountpoints(rootfs); err != nil {
		t.Fatalf("ensureOCIMountpoints(%q) = %v", rootfs, err)
	}

	if _, err := os.Lstat(outside); !os.IsNotExist(err) {
		t.Errorf("Lstat(%q) = %v, want it never created: the symlink was followed out of the rootfs", outside, err)
	}
	if fi, err := os.Stat(filepath.Join(rootfs, "dev")); err != nil || !fi.IsDir() {
		t.Errorf("dev: Stat = %v, %v; want a directory", fi, err)
	}
	if got, err := os.Readlink(filepath.Join(rootfs, "sys")); err != nil || got != "realsys" {
		t.Errorf("sys: Readlink = %q, %v; want the in-rootfs symlink left alone", got, err)
	}
}

// The kernel requires overlay upperdir and workdir on the same filesystem and
// rejects a workdir nested inside (or equal to) upperdir — so they must be
// SIBLINGS under the container's subdirectory of the actor's upper base. The
// layout is also the snapshot tar's entry layout (<cid>/fs, <cid>/work), so a
// change here breaks every overlay mount AND every existing snapshot.
func TestUpperWorkDirsAreSiblings(t *testing.T) {
	const base = "/var/lib/ateom-gvisor/actors/uid/rootfs-upper"
	upper, work := UpperWorkDirs(base, "app")
	cidDir := filepath.Join(base, "app")
	if filepath.Dir(upper) != cidDir || filepath.Dir(work) != cidDir {
		t.Errorf("UpperWorkDirs = %q, %q; want both directly under %q", upper, work, cidDir)
	}
	if upper == work {
		t.Errorf("UpperWorkDirs: upper and work are the same directory %q", upper)
	}
	if strings.HasPrefix(work+"/", upper+"/") {
		t.Errorf("UpperWorkDirs: work %q is nested inside upper %q", work, upper)
	}
	// Tar-layout invariant: entries are <cid>/fs and <cid>/work.
	if upper != filepath.Join(base, "app", "fs") || work != filepath.Join(base, "app", "work") {
		t.Errorf("UpperWorkDirs = %q, %q; want the snapshot layout <base>/app/{fs,work}", upper, work)
	}
}

func TestVirtiofsdArgs(t *testing.T) {
	args := virtiofsdArgs(VirtiofsdOptions{
		SocketPath: "/run/vm/virtiofsd.sock",
		SharedDir:  "/run/kata-containers/shared/sandboxes/uid/shared",
	})
	if !slices.Contains(args, "--cache=auto") {
		t.Errorf("args %v do not contain --cache=auto", args)
	}
	// The host kernel owns the overlay; the guest needs no xattr passthrough, so
	// the flag must never be emitted.
	if slices.Contains(args, "--xattr") {
		t.Errorf("args %v contain --xattr; the guest has no overlay to feed it to", args)
	}
	// With the default (abort), a guest holding a reference to an unlinked
	// inode, such as a live-rotated trust bundle, could never be restored.
	if i := slices.Index(args, "--migration-on-error"); i < 0 || i+1 >= len(args) || args[i+1] != "guest-error" {
		t.Errorf("args %v do not set --migration-on-error guest-error", args)
	}
}

// The upper base is restored from the snapshot tar, so a symlink at <cid> or
// <cid>/fs must be refused, and one at the scratch <cid>/work replaced, rather
// than followed by the workdir wipe, the mkdirs, or the overlay mount.
func TestPrepareUpperWorkDirsRefusesSymlinks(t *testing.T) {
	for _, link := range []string{"app", "app/fs", "app/work"} {
		t.Run(link, func(t *testing.T) {
			dir := t.TempDir()
			victim := filepath.Join(dir, "victim")
			if err := os.MkdirAll(filepath.Join(victim, "work"), 0o755); err != nil {
				t.Fatal(err)
			}
			base := filepath.Join(dir, "upper")
			if err := os.MkdirAll(filepath.Join(base, filepath.Dir(link)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(victim, filepath.Join(base, link)); err != nil {
				t.Fatal(err)
			}

			if err := prepareUpperWorkDirs(base, "app"); err == nil && link != "app/work" {
				t.Errorf("prepareUpperWorkDirs with a symlink at %q = nil, want an error", link)
			}
			if _, err := os.Stat(filepath.Join(victim, "work")); err != nil {
				t.Errorf("victim/work: Stat = %v; want it left alone", err)
			}
			if _, err := os.Stat(filepath.Join(victim, "fs")); !os.IsNotExist(err) {
				t.Errorf("victim/fs: Stat = %v; want it never created", err)
			}
		})
	}
}

func TestPrepareUpperWorkDirs(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "app", "fs", "keep"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "app", "work", "stale"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := prepareUpperWorkDirs(base, "app"); err != nil {
		t.Fatalf("prepareUpperWorkDirs = %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "app", "fs", "keep")); err != nil {
		t.Errorf("upper contents: Stat = %v; want them kept", err)
	}
	entries, err := os.ReadDir(filepath.Join(base, "app", "work"))
	if err != nil || len(entries) != 0 {
		t.Errorf("workdir: ReadDir = %v, %v; want an empty directory", entries, err)
	}
	for _, cid := range []string{"", ".", "..", "a/b", "/abs"} {
		if err := prepareUpperWorkDirs(base, cid); err == nil {
			t.Errorf("prepareUpperWorkDirs(%q) = nil, want an error", cid)
		}
	}
}
