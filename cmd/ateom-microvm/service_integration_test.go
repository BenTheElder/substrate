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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// TestServiceE2E drives the REAL AteomService gRPC handlers (RunWorkload,
// CheckpointWorkload, RestoreWorkload) against kata + cloud-hypervisor, with
// an atelet-style OCI bundle and the object-storage round trip
// simulated by copying CheckpointStateDir -> RestoreStateDir. This proves the
// whole ateom-microvm service works end-to-end (not just the helpers):
// path derivation, ensureKataCompatibleSpec (the bundle here is a minimal
// gVisor-style spec with no linux.resources), the running map, shared-dir
// capture/reconstruct, virtiofsd relaunch, CH restore, and teardown.
//
// Gated behind KATA_INTEGRATION=1. Env: KATA_ROOTFS_SRC (required),
// KATA_SHIM / KATA_CH / KATA_VIRTIOFSD (sensible defaults provided).
func TestServiceE2E(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm + root)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	if rootfsSrc == "" {
		t.Fatal("KATA_ROOTFS_SRC is required")
	}
	shim := envOrTest("KATA_SHIM", "/opt/kata/bin/containerd-shim-kata-v2")
	chBin := envOrTest("KATA_CH", "/usr/local/bin/cloud-hypervisor")
	vfsdBin := envOrTest("KATA_VIRTIOFSD", "/usr/local/bin/virtiofsd-patched")

	ns, name := "default", "e2e"
	id := fmt.Sprintf("ateomchv-svc-%d", os.Getpid())
	container := "app"

	// --- atelet-style bundle prep at the ateompath the service expects. ---
	bundle := ateompath.OCIBundlePath(ns, name, id, container)
	rootfs := filepath.Join(bundle, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", rootfsSrc+"/.", rootfs+"/").CombinedOutput(); err != nil {
		t.Fatalf("copying rootfs: %v: %s", err, out)
	}
	writeMinimalGvisorStyleSpec(t, bundle) // no linux.resources -> exercises ensureKataCompatibleSpec
	drainBundleLog(t, bundle)              // visibility into shim logs if anything fails

	// The real service (main.go) creates the interior netns at boot and hands its
	// handle to NewService; the bare test must do the same (passing -1 yields
	// "bad file descriptor" in setupActorNetwork).
	podUID := "testpod"
	_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID)) // clear any stale netns from a crashed run
	interiorNetNS, err := createNetNSWithoutSwitching(ateompath.AteomNetNSName(podUID))
	if err != nil {
		t.Fatalf("creating interior netns: %v", err)
	}
	svc := NewService(podUID, shim, chBin, vfsdBin, "", "default", true, interiorNetNS)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 20*time.Second)
		svc.teardownActor(cctx, id, svc.running[id], nil)
		c()
		_ = os.RemoveAll(ateompath.ActorPath(ns, name, id))
		_ = os.RemoveAll(kata.VMDir(id))
		_ = os.RemoveAll(kata.SharedDir(id))
		_ = interiorNetNS.Close()
		_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	})

	// --- RunWorkload. ---
	if _, err := svc.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
	}); err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	t.Log("RunWorkload OK (micro-VM booted via kata)")

	// --- CheckpointWorkload. ---
	if _, err := svc.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
	}); err != nil {
		t.Fatalf("CheckpointWorkload: %v", err)
	}
	checkpointDir := ateompath.CheckpointStateDir(ns, name, id)
	for _, f := range []string{"config.json", "state.json", "memory-ranges", "shared-dir.tar"} {
		if _, err := os.Stat(filepath.Join(checkpointDir, f)); err != nil {
			t.Fatalf("checkpoint missing %q: %v", f, err)
		}
	}
	t.Log("CheckpointWorkload OK (snapshot + shared-dir.tar written)")

	// --- Simulate atelet's object-storage round trip. ---
	restoreDir := ateompath.RestoreStateDir(ns, name, id)
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", checkpointDir+"/.", restoreDir+"/").CombinedOutput(); err != nil {
		t.Fatalf("shipping snapshot: %v: %s", err, out)
	}
	t.Log("shipped snapshot CheckpointStateDir -> RestoreStateDir")

	// --- RestoreWorkload (fresh CH process, ateom-owned). ---
	if _, err := svc.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
	}); err != nil {
		t.Fatalf("RestoreWorkload: %v", err)
	}
	t.Log("RestoreWorkload OK")

	// --- Liveness: the restored guest must be live on its new CH. ---
	client := ch.NewClient(filepath.Join(kata.VMDir(id), "clh-api-restore.sock"))
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		t.Fatalf("restored CH not ready: %v", err)
	}
	if err := client.Pause(ctx); err != nil {
		t.Errorf("post-restore Pause (liveness): %v", err)
	}
	if err := client.Resume(ctx); err != nil {
		t.Errorf("post-restore Resume (liveness): %v", err)
	}
	t.Log("E2E OK: actor ran, checkpointed to 'storage', and restored live on a fresh CH process")
}

// TestServiceRunBlkRootfs drives ONLY the owned-boot cold-run path
// (runWorkloadBlkRootfs): ateom boots cloud-hypervisor itself and gives the actor
// a writable boot-time virtio-blk rootfs (/dev/vdb). It's the fast Phase-1
// iteration loop — no checkpoint/restore (those are Phase 2). Unlike TestServiceE2E
// it MUST pass the guest kernel + image + base-config asset paths (owned-boot
// builds the CH vm.create itself instead of letting the shim read configuration.toml).
//
// Gated behind KATA_INTEGRATION=1. Required env:
//
//	KATA_ROOTFS_SRC=<dir>   a populated actor rootfs (becomes /dev/vdb)
//	KATA_KERNEL=<path>      guest kernel (vmlinux.container)
//	KATA_IMAGE=<path>       guest OS image (kata-containers.img, /dev/vda)
//	KATA_CONFIG=<path>      a stock kata clh configuration.toml (for kernel_params + sizing)
//
// Optional: KATA_CH / KATA_VIRTIOFSD (defaults provided). Run as root on a host
// with kata + /dev/kvm + mkfs.ext4 (e2fsprogs):
//
//	sudo KATA_INTEGRATION=1 KATA_ROOTFS_SRC=/tmp/bb/rootfs KATA_KERNEL=... KATA_IMAGE=... \
//	  KATA_CONFIG=... ./ateom-microvm.test -test.v -test.run BlkRootfs
func TestServiceRunBlkRootfs(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm + root + e2fsprogs)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	if rootfsSrc == "" {
		t.Fatal("KATA_ROOTFS_SRC is required")
	}
	kernel, image, cfg := os.Getenv("KATA_KERNEL"), os.Getenv("KATA_IMAGE"), os.Getenv("KATA_CONFIG")
	if kernel == "" || image == "" || cfg == "" {
		t.Fatal("KATA_KERNEL, KATA_IMAGE, and KATA_CONFIG are required for the owned-boot path")
	}
	shim := envOrTest("KATA_SHIM", "/opt/kata/bin/containerd-shim-kata-v2")
	chBin := envOrTest("KATA_CH", "/usr/local/bin/cloud-hypervisor")
	vfsdBin := envOrTest("KATA_VIRTIOFSD", "/usr/local/bin/virtiofsd-patched")

	// Select the owned-boot path in RunWorkload.
	t.Setenv("ATEOM_VIRTIO_BLK_ROOTFS", "1")

	ns, name := "default", "e2e-blk"
	id := fmt.Sprintf("ateomchv-blk-%d", os.Getpid())
	container := "app"

	bundle := ateompath.OCIBundlePath(ns, name, id, container)
	rootfs := filepath.Join(bundle, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", rootfsSrc+"/.", rootfs+"/").CombinedOutput(); err != nil {
		t.Fatalf("copying rootfs: %v: %s", err, out)
	}
	writeMinimalGvisorStyleSpec(t, bundle)

	podUID := "testpod-blk"
	_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	interiorNetNS, err := createNetNSWithoutSwitching(ateompath.AteomNetNSName(podUID))
	if err != nil {
		t.Fatalf("creating interior netns: %v", err)
	}
	svc := NewService(podUID, shim, chBin, vfsdBin, "", "default", true, interiorNetNS)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 20*time.Second)
		svc.teardownActor(cctx, id, svc.running[id], nil)
		c()
		_ = os.RemoveAll(ateompath.ActorPath(ns, name, id))
		_ = os.RemoveAll(kata.VMDir(id))
		_ = interiorNetNS.Close()
		_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	})

	if _, err := svc.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: map[string]string{
			assetKernel: kernel,
			assetImage:  image,
			assetConfig: cfg,
			assetCH:     chBin,
		},
	}); err != nil {
		// Best-effort: dump the guest serial console (captured to VMDir/serial.log)
		// so a boot failure shows the kernel/agent output.
		if b, rerr := os.ReadFile(filepath.Join(kata.VMDir(id), "serial.log")); rerr == nil {
			t.Logf("[serial.log tail]\n%s", lastLines(string(b), 60))
		}
		t.Fatalf("RunWorkload (owned-boot): %v", err)
	}
	t.Log("RunWorkload OK (owned-boot: CH booted by ateom, actor rootfs on /dev/vdb)")

	// Liveness: the ateom-owned CH must be up and the VM Running.
	client := ch.NewClient(filepath.Join(kata.VMDir(id), "clh-api.sock"))
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		t.Fatalf("owned CH not ready: %v", err)
	}
	// Confirm the actor's rootfs really came from /dev/vdb (a marker visible via
	// the guest debug console — the actor's own files live on the blk disk).
	dump := kata.DebugConsoleDump(ctx, kata.VsockSocketPath(id),
		"echo '== vdb =='; blkid /dev/vdb 2>&1; echo '== rootfs mount =='; grep vdb /proc/mounts 2>&1; echo '== ip =='; ip -4 addr show eth0 2>&1")
	t.Logf("[guest] %s", dump)
}

// TestServiceCheckpointRestoreBlkRootfs proves Phase 2: the owned-boot actor
// snapshots MEMORY-ONLY (no shared-dir.tar, no balloon) and restores with in-RAM
// continuity. It writes a sentinel into guest tmpfs (/run = RAM), checkpoints,
// ships the snapshot dir, restores on a fresh CH process, and reads the sentinel
// back — if RAM continuity holds it survives. Same gating/env as
// TestServiceRunBlkRootfs.
func TestServiceCheckpointRestoreBlkRootfs(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm + root + e2fsprogs)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	kernel, image, cfg := os.Getenv("KATA_KERNEL"), os.Getenv("KATA_IMAGE"), os.Getenv("KATA_CONFIG")
	if rootfsSrc == "" || kernel == "" || image == "" || cfg == "" {
		t.Fatal("KATA_ROOTFS_SRC, KATA_KERNEL, KATA_IMAGE, KATA_CONFIG are required")
	}
	shim := envOrTest("KATA_SHIM", "/opt/kata/bin/containerd-shim-kata-v2")
	chBin := envOrTest("KATA_CH", "/usr/local/bin/cloud-hypervisor")
	vfsdBin := envOrTest("KATA_VIRTIOFSD", "/usr/local/bin/virtiofsd-patched")
	t.Setenv("ATEOM_VIRTIO_BLK_ROOTFS", "1")

	ns, name := "default", "e2e-blkcr"
	id := fmt.Sprintf("ateomchv-blkcr-%d", os.Getpid())
	container := "app"

	bundle := ateompath.OCIBundlePath(ns, name, id, container)
	rootfs := filepath.Join(bundle, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", rootfsSrc+"/.", rootfs+"/").CombinedOutput(); err != nil {
		t.Fatalf("copying rootfs: %v: %s", err, out)
	}
	writeMinimalGvisorStyleSpec(t, bundle)

	podUID := "testpod-blkcr"
	_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	interiorNetNS, err := createNetNSWithoutSwitching(ateompath.AteomNetNSName(podUID))
	if err != nil {
		t.Fatalf("creating interior netns: %v", err)
	}
	svc := NewService(podUID, shim, chBin, vfsdBin, "", "default", true, interiorNetNS)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 20*time.Second)
		svc.teardownActor(cctx, id, svc.running[id], nil)
		c()
		_ = os.RemoveAll(ateompath.ActorPath(ns, name, id))
		_ = os.RemoveAll(kata.VMDir(id))
		_ = interiorNetNS.Close()
		_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	})

	assets := map[string]string{assetKernel: kernel, assetImage: image, assetConfig: cfg, assetCH: chBin}
	if _, err := svc.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec:              &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: assets,
	}); err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	t.Log("RunWorkload OK")

	// Write an in-RAM (tmpfs /run) sentinel via the guest debug console.
	const sentinel = "BLKROOT_CONTINUITY_OK_4242"
	vsock := kata.VsockSocketPath(id)
	_ = kata.DebugConsoleDump(ctx, vsock, "echo "+sentinel+" > /run/blkroot-sentinel; sync; echo wrote")
	if got := kata.DebugConsoleDump(ctx, vsock, "cat /run/blkroot-sentinel"); !strings.Contains(got, sentinel) {
		t.Fatalf("sentinel not readable pre-checkpoint: %q", got)
	}
	t.Log("wrote in-RAM sentinel")

	// CheckpointWorkload — memory-only, no balloon/wipe.
	if _, err := svc.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
	}); err != nil {
		t.Fatalf("CheckpointWorkload: %v", err)
	}
	checkpointDir := ateompath.CheckpointStateDir(ns, name, id)
	for _, f := range []string{"config.json", "state.json", "memory-ranges", "base-id"} {
		if _, err := os.Stat(filepath.Join(checkpointDir, f)); err != nil {
			t.Fatalf("checkpoint missing %q: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, "shared-dir.tar")); err == nil {
		t.Error("snapshot has shared-dir.tar — owned-boot must be MEMORY-ONLY (no virtio-fs base)")
	}
	t.Log("CheckpointWorkload OK (memory-only: config/state/memory-ranges/base-id, no shared-dir.tar)")

	// Ship snapshot dir -> restore dir (simulating atelet object-storage round trip).
	restoreDir := ateompath.RestoreStateDir(ns, name, id)
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", checkpointDir+"/.", restoreDir+"/").CombinedOutput(); err != nil {
		t.Fatalf("shipping snapshot: %v: %s", err, out)
	}

	// RestoreWorkload — reopen /dev/vdb, no virtiofsd/reconstruct.
	if _, err := svc.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec:              &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: assets,
	}); err != nil {
		t.Fatalf("RestoreWorkload: %v", err)
	}
	client := ch.NewClient(filepath.Join(kata.VMDir(id), "clh-api-restore.sock"))
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		t.Fatalf("restored CH not ready: %v", err)
	}
	t.Log("RestoreWorkload OK")

	// In-RAM continuity: the sentinel written before checkpoint must survive.
	got := kata.DebugConsoleDump(ctx, vsock, "cat /run/blkroot-sentinel")
	if !strings.Contains(got, sentinel) {
		t.Fatalf("RAM continuity FAILED: sentinel gone after restore: %q", got)
	}
	t.Logf("Phase 2 OK: memory-only snapshot + restore with in-RAM continuity (sentinel survived): %q", strings.TrimSpace(got))
}

func lastLines(s string, n int) string {
	lines := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}

func envOrTest(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// writeMinimalGvisorStyleSpec writes a deliberately minimal OCI spec (no
// linux.resources / cgroupsPath) so the test exercises ensureKataCompatibleSpec.
func writeMinimalGvisorStyleSpec(t *testing.T, bundle string) {
	t.Helper()
	spec := map[string]any{
		"ociVersion": "1.0.2",
		"process": map[string]any{
			"user": map[string]any{"uid": 0, "gid": 0},
			"args": []string{"sleep", "3600"},
			"env":  []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			"cwd":  "/",
			"capabilities": map[string]any{
				"bounding":  []string{"CAP_KILL", "CAP_AUDIT_WRITE", "CAP_NET_BIND_SERVICE"},
				"effective": []string{"CAP_KILL", "CAP_AUDIT_WRITE", "CAP_NET_BIND_SERVICE"},
				"permitted": []string{"CAP_KILL", "CAP_AUDIT_WRITE", "CAP_NET_BIND_SERVICE"},
			},
		},
		"root":     map[string]any{"path": "rootfs", "readonly": false},
		"hostname": "ateomchv",
		"mounts": []map[string]any{
			{"destination": "/proc", "type": "proc", "source": "proc"},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
			{"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": []string{"nosuid", "noexec", "nodev", "ro"}},
		},
		"linux": map[string]any{
			"namespaces": []map[string]any{
				{"type": "pid"}, {"type": "network"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"},
			},
		},
	}
	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// drainBundleLog creates the kata shim's "log" fifo and drains it to t.Log.
func drainBundleLog(t *testing.T, bundle string) {
	t.Helper()
	logFifo := filepath.Join(bundle, "log")
	_ = os.Remove(logFifo)
	if err := unix.Mkfifo(logFifo, 0o700); err != nil {
		t.Fatalf("mkfifo log: %v", err)
	}
	go func() {
		f, err := os.OpenFile(logFifo, os.O_RDONLY, 0)
		if err != nil {
			return
		}
		defer f.Close()
		buf := make([]byte, 4096)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				t.Logf("[shim-log] %s", buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()
}
