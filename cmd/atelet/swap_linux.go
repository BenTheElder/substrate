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

// PoC (poc2-atelet) swap mechanic.
//
// Active actors keep running under ateom (inside the worker pod) exactly as on
// main: ateom runs `runsc` and owns the gVisor sandbox while it is hot. This
// file adds an *alternative* suspend/resume path, invoked manually as an atelet
// subcommand, that hands a paused actor's memory off to the node-level atelet
// and back:
//
//   swap-out: runsc pause -> process_madvise(MADV_PAGEOUT) -> move the sandbox's
//             sentry + gofer processes out of the worker pod's cgroup into a
//             dedicated node-level cgroup (/sys/fs/cgroup/ate-suspended/<actor>).
//
//   swap-in:  move those processes back into the (assigned) worker pod's cgroup
//             -> runsc resume, restoring the "active actors run under ateom"
//             invariant.
//
// This relies on both atelet and the worker pods sharing the host PID namespace
// (hostPID: true) so the sentry's pid-file PID is host-global and so PIDs written
// to cgroup.procs are interpreted in the host PID namespace.
//
// Assumptions / PoC limitations:
//   - cgroup v2, with zswap/swap enabled on the node.
//   - Resume happens on the SAME worker pod, so the interior netns and IP are
//     left untouched. Cross-worker / cross-node resume is out of scope.
//   - With worker-pod hostPID, ateom is no longer PID 1 of its own namespace, so
//     its go-reap reaper no longer adopts orphaned runsc daemons. Fine for a PoC.

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"golang.org/x/sys/unix"
)

// cgroupRoot is the host cgroup v2 mount, made available to atelet via the
// host /sys hostPath mount (see manifests/ate-install/atelet.yaml).
const cgroupRoot = "/sys/fs/cgroup"

// suspendedCgroupParent is the node-level cgroup under which swapped-out actors
// are parked, one leaf cgroup per actor.
const suspendedCgroupParent = cgroupRoot + "/ate-suspended"

// rootContainer is the sandbox root ("pause") container. Pausing/resuming it
// freezes/thaws the whole gVisor sandbox, since all containers share one sentry.
const rootContainer = "pause"

// swapParams holds the actor coordinates parsed from the subcommand flags.
type swapParams struct {
	actorTemplateNamespace string
	actorTemplateName      string
	actorID                string
	targetAteomNamespace   string
	targetAteomName        string
	runscPath              string
}

func (p *swapParams) runscRoot() string {
	return ateompath.RunSCStateDir(p.actorTemplateNamespace, p.actorTemplateName, p.actorID)
}

// actorKey is the per-actor leaf-cgroup name; it matches ateompath's actor dir
// convention (namespace:template:id).
func (p *swapParams) actorKey() string {
	return p.actorTemplateNamespace + ":" + p.actorTemplateName + ":" + p.actorID
}

func (p *swapParams) suspendedCgroup() string {
	return filepath.Join(suspendedCgroupParent, p.actorKey())
}

// runSwap parses the subcommand flags and dispatches to swap-out / swap-in.
func runSwap(ctx context.Context, mode string, args []string) error {
	fs := flag.NewFlagSet("atelet "+mode, flag.ContinueOnError)
	p := &swapParams{}
	fs.StringVar(&p.actorTemplateNamespace, "actor-template-namespace", "", "Actor template namespace")
	fs.StringVar(&p.actorTemplateName, "actor-template-name", "", "Actor template name")
	fs.StringVar(&p.actorID, "actor-id", "", "Actor ID")
	fs.StringVar(&p.targetAteomNamespace, "target-ateom-namespace", "", "Worker pod namespace to resume into (swap-in only)")
	fs.StringVar(&p.targetAteomName, "target-ateom-name", "", "Worker pod name to resume into (swap-in only)")
	fs.StringVar(&p.runscPath, "runsc-path", "", "Path to the runsc binary (default: auto-discover in the static-files dir)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if p.actorTemplateNamespace == "" || p.actorTemplateName == "" || p.actorID == "" {
		return errors.New("--actor-template-namespace, --actor-template-name and --actor-id are required")
	}

	if p.runscPath == "" {
		discovered, err := discoverRunsc()
		if err != nil {
			return err
		}
		p.runscPath = discovered
	}

	switch mode {
	case "swap-out":
		return swapOut(ctx, p)
	case "swap-in":
		if p.targetAteomNamespace == "" || p.targetAteomName == "" {
			return errors.New("swap-in requires --target-ateom-namespace and --target-ateom-name")
		}
		return swapIn(ctx, p)
	default:
		return fmt.Errorf("unknown swap mode %q", mode)
	}
}

// swapOut freezes the actor, reparents the sandbox processes from their current
// (worker-side) cgroup into a dedicated node-level cgroup, and then pages their
// memory out to swap so the swap is charged to that node-level cgroup.
//
// Order matters. cgroup v2 does NOT migrate a process's already-charged pages
// when the process is moved (move_charge_at_immigrate is gone in v2): pages stay
// charged to the memcg where they were first faulted, and a swap slot is charged
// to whichever memcg owns the page at swap-out time. So to make the actor's swap
// accounting follow it into the node-level cgroup we must, after the move,
// *recharge* the pages: evict them from the old memcg, fault them back in (now
// charged to the new memcg), then evict them again (now swapped under the new
// memcg). See rechargeAndPageOut.
func swapOut(ctx context.Context, p *swapParams) error {
	root := p.runscRoot()

	slog.InfoContext(ctx, "swap-out: pausing sandbox", slog.String("actor", p.actorKey()))
	if err := runscCmd(ctx, p.runscPath, root, "pause"); err != nil {
		return fmt.Errorf("while pausing sandbox: %w", err)
	}

	pids := findSandboxPIDs(ctx, root)
	if len(pids) == 0 {
		return fmt.Errorf("found no sandbox processes referencing %q; is the actor running?", root)
	}

	dst := p.suspendedCgroup()
	if err := ensureSuspendedCgroup(ctx, dst); err != nil {
		return fmt.Errorf("while preparing suspended cgroup: %w", err)
	}
	slog.InfoContext(ctx, "swap-out: reparenting sandbox into node-level cgroup",
		slog.String("cgroup", dst), slog.Any("pids", pids))
	if err := moveToCgroup(ctx, dst, pids); err != nil {
		return fmt.Errorf("while reparenting sandbox cgroup: %w", err)
	}

	// Recharge + page out, so the swapped memory is accounted to dst.
	for _, pid := range pids {
		slog.InfoContext(ctx, "swap-out: recharging and paging out", slog.Int("pid", pid))
		if err := rechargeAndPageOut(ctx, pid); err != nil {
			// Non-fatal: the freeze + reparent already happened; accounting is
			// best-effort and depends on swap being available on the node.
			slog.ErrorContext(ctx, "swap-out: recharge/page-out failed", slog.Int("pid", pid), slog.Any("err", err))
		}
	}

	slog.InfoContext(ctx, "swap-out: done", slog.String("actor", p.actorKey()), slog.String("cgroup", dst))
	return nil
}

// swapIn reparents the sandbox processes back into the target worker pod's
// cgroup and resumes the sandbox.
func swapIn(ctx context.Context, p *swapParams) error {
	root := p.runscRoot()

	ateomPID, err := findAteomPID(ctx, p.targetAteomNamespace, p.targetAteomName)
	if err != nil {
		return err
	}
	workerCgroup, err := cgroupOf(ateomPID)
	if err != nil {
		return fmt.Errorf("while reading target worker pod cgroup (ateom pid %d): %w", ateomPID, err)
	}

	pids := findSandboxPIDs(ctx, root)
	if len(pids) == 0 {
		return fmt.Errorf("found no sandbox processes referencing %q; was the actor swapped out?", root)
	}

	slog.InfoContext(ctx, "swap-in: reparenting sandbox back into worker pod cgroup",
		slog.String("cgroup", workerCgroup), slog.Any("pids", pids))
	if err := moveToCgroup(ctx, workerCgroup, pids); err != nil {
		return fmt.Errorf("while reparenting sandbox cgroup: %w", err)
	}

	slog.InfoContext(ctx, "swap-in: resuming sandbox", slog.String("actor", p.actorKey()))
	if err := runscCmd(ctx, p.runscPath, root, "resume"); err != nil {
		return fmt.Errorf("while resuming sandbox: %w", err)
	}

	// Best-effort cleanup of the now-empty per-actor suspended cgroup.
	if err := os.Remove(p.suspendedCgroup()); err != nil && !os.IsNotExist(err) {
		slog.WarnContext(ctx, "swap-in: failed to remove empty suspended cgroup", slog.String("cgroup", p.suspendedCgroup()), slog.Any("err", err))
	}

	slog.InfoContext(ctx, "swap-in: done", slog.String("actor", p.actorKey()))
	return nil
}

// runscCmd runs a runsc subcommand (pause/resume) against the actor's sandbox.
// Global flags (-root) must precede the subcommand.
func runscCmd(ctx context.Context, runscPath, root, subcommand string) error {
	cmd := exec.CommandContext(ctx, runscPath,
		"-root", root,
		subcommand,
		rootContainer,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc %s`: %w", subcommand, err)
	}
	return nil
}

// discoverRunsc returns the single runsc binary that atelet has previously
// fetched into the static-files dir.
func discoverRunsc() (string, error) {
	matches, err := filepath.Glob(filepath.Join(ateompath.StaticFilesDir, "runsc-*"))
	if err != nil {
		return "", fmt.Errorf("while globbing for runsc binary: %w", err)
	}
	for _, m := range matches {
		// Skip in-progress downloads (see fetchRunsc temp file naming).
		if strings.Contains(filepath.Base(m), "-download-") {
			continue
		}
		if fi, err := os.Stat(m); err == nil && !fi.IsDir() {
			return m, nil
		}
	}
	return "", fmt.Errorf("no runsc binary found in %q; pass --runsc-path", ateompath.StaticFilesDir)
}

// findSandboxPIDs returns the host PIDs of every process whose cmdline
// references this actor's runsc root state dir — i.e. the gVisor sentry
// (runsc-sandbox) and gofer (runsc-gofer) processes for this actor.
func findSandboxPIDs(ctx context.Context, runscRoot string) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		slog.ErrorContext(ctx, "failed to read /proc", slog.Any("err", err))
		return nil
	}
	self := os.Getpid()
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue // process may have exited, or is a kernel thread
		}
		// cmdline is NUL-separated; join with spaces for substring matching.
		cmdline := strings.ReplaceAll(string(raw), "\x00", " ")
		if strings.Contains(cmdline, runscRoot) {
			pids = append(pids, pid)
		}
	}
	return pids
}

// findAteomPID locates the running ateom process for the given worker pod by
// matching the -pod-namespace / -pod-name args it was launched with.
func findAteomPID(ctx context.Context, namespace, name string) (int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("while reading /proc: %w", err)
	}
	wantNS := "-pod-namespace=" + namespace
	wantName := "-pod-name=" + name
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		cmdline := strings.ReplaceAll(string(raw), "\x00", " ")
		if strings.Contains(cmdline, wantNS) && strings.Contains(cmdline, wantName) {
			return pid, nil
		}
	}
	return 0, fmt.Errorf("no running ateom process found for worker pod %s/%s", namespace, name)
}

// cgroupOf returns the absolute path (under the mounted host cgroupRoot) of the
// cgroup that pid belongs to.
//
// We deliberately do NOT parse /proc/<pid>/cgroup: that path is rendered
// relative to the *reader's* cgroup namespace, and atelet runs in a private
// cgroup namespace (e.g. inside a kind node), so the reported path may not
// resolve against the host /sys/fs/cgroup bind mount. Instead we search the
// real mounted cgroup tree for the leaf cgroup.procs that actually contains the
// (host-global, thanks to hostPID) pid. This is namespace-proof.
func cgroupOf(pid int) (string, error) {
	var found string
	want := strconv.Itoa(pid)
	err := filepath.WalkDir(cgroupRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "cgroup.procs" {
			return nil //nolint:nilerr // skip unreadable entries; keep walking
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		for _, line := range strings.Fields(string(raw)) {
			if line == want {
				found = filepath.Dir(path)
				return filepath.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("while searching cgroup tree for pid %d: %w", pid, err)
	}
	if found == "" {
		return "", fmt.Errorf("pid %d not found in any cgroup under %s", pid, cgroupRoot)
	}
	return found, nil
}

// ensureSuspendedCgroup creates the per-actor suspended cgroup (dst) and enables
// the memory controller on its parent's cgroup.subtree_control, so that dst
// exposes memory.* / memory.swap.* / memory.zswap.* accounting files. Without
// delegating +memory to the parent, a freshly created leaf cgroup has no memory
// accounting interface at all.
func ensureSuspendedCgroup(ctx context.Context, dst string) error {
	parent := filepath.Dir(dst)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("while creating %q: %w", parent, err)
	}
	// Delegate the memory controller to children of parent (idempotent). The
	// root cgroup already lists "memory" in its subtree_control, so parent has
	// it available to delegate further down.
	sc := filepath.Join(parent, "cgroup.subtree_control")
	if f, err := os.OpenFile(sc, os.O_WRONLY, 0); err != nil {
		slog.WarnContext(ctx, "could not open subtree_control to enable memory controller", slog.String("path", sc), slog.Any("err", err))
	} else {
		if _, err := f.WriteString("+memory"); err != nil {
			slog.WarnContext(ctx, "could not enable memory controller on suspended cgroup parent", slog.String("path", sc), slog.Any("err", err))
		}
		f.Close()
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("while creating %q: %w", dst, err)
	}
	return nil
}

// moveToCgroup ensures dst exists and moves each pid into it by writing to
// dst/cgroup.procs (one pid per write, as cgroup v2 requires). Failures for
// individual pids (e.g. already-exited) are logged but not fatal.
func moveToCgroup(ctx context.Context, dst string, pids []int) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("while creating cgroup %q: %w", dst, err)
	}
	procsPath := filepath.Join(dst, "cgroup.procs")
	var moved int
	for _, pid := range pids {
		if err := writeCgroupProc(procsPath, pid); err != nil {
			slog.WarnContext(ctx, "failed to move pid into cgroup", slog.Int("pid", pid), slog.String("cgroup", dst), slog.Any("err", err))
			continue
		}
		moved++
	}
	if moved == 0 {
		return fmt.Errorf("moved 0 of %d pids into %q", len(pids), dst)
	}
	return nil
}

// writeCgroupProc writes a single PID to a cgroup.procs file. cgroup v2 accepts
// only one PID per write() and rejects O_TRUNC semantics, so we open the file
// directly rather than using os.WriteFile.
func writeCgroupProc(procsPath string, pid int) error {
	f, err := os.OpenFile(procsPath, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(strconv.Itoa(pid)); err != nil {
		return err
	}
	return nil
}

// rechargeAndPageOut makes the swapped-out memory of pid be accounted to pid's
// *current* cgroup (the node-level suspended cgroup it was just moved into), then
// leaves it swapped out.
//
// Because cgroup v2 never migrates a process's already-charged pages on move,
// the pages are still charged to the old (worker-side) memcg right after the
// move. We fix that with a swap round-trip:
//
//  1. MADV_PAGEOUT  — evict the pages; swap slots are charged to the OLD memcg.
//  2. MADV_WILLNEED — fault them back in; the new pages are charged to pid's
//     current memcg (the suspended cgroup), and the old swap slots are freed.
//  3. MADV_PAGEOUT  — evict again; now the swap is charged to the suspended
//     cgroup, and physical RAM is freed.
//
// The WILLNEED step transiently pulls the whole working set back into RAM, which
// is fine for a PoC; a production version would recharge region-by-region to
// bound the spike.
func rechargeAndPageOut(ctx context.Context, pid int) error {
	if err := madvisePages(ctx, pid, unix.MADV_PAGEOUT, "page-out(evict from old memcg)"); err != nil {
		return err
	}
	if err := madvisePages(ctx, pid, unix.MADV_WILLNEED, "will-need(recharge to new memcg)"); err != nil {
		return err
	}
	return madvisePages(ctx, pid, unix.MADV_PAGEOUT, "page-out(swap under new memcg)")
}

// madvisePages applies the given madvise advice (MADV_PAGEOUT / MADV_WILLNEED)
// over every resident anonymous/file mapping of pid via process_madvise(2). The
// process should be frozen (runsc pause) first so its memory is quiescent.
//
// Derived from the poc-atelet branch (cmd/atelet/runsc_linux.go reclaimMemory).
func madvisePages(ctx context.Context, pid, advice int, label string) error {
	pidfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return fmt.Errorf("while opening pidfd for pid %d: %w", pid, err)
	}
	defer unix.Close(pidfd)

	mapsFile, err := os.Open(fmt.Sprintf("/proc/%d/maps", pid))
	if err != nil {
		return fmt.Errorf("while opening /proc/%d/maps: %w", pid, err)
	}
	defer mapsFile.Close()

	var iovecs []unix.Iovec
	flush := func() {
		if len(iovecs) == 0 {
			return
		}
		if _, err := processMadvise(pidfd, iovecs, advice, 0); err != nil {
			slog.ErrorContext(ctx, "process_madvise batch failed", slog.String("advice", label), slog.Int("pid", pid), slog.Any("err", err))
		}
		iovecs = iovecs[:0]
	}

	scanner := bufio.NewScanner(mapsFile)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		var start, end uintptr
		if _, err := fmt.Sscanf(fields[0], "%x-%x", &start, &end); err != nil || end <= start {
			continue
		}
		// Skip regions without read/write/execute perms (e.g. guard pages).
		if !strings.ContainsAny(fields[1], "rwx") {
			continue
		}
		// Skip special kernel mappings that cannot be paged out.
		if len(fields) >= 6 {
			switch fields[5] {
			case "[vvar]", "[vdso]", "[vsyscall]":
				continue
			}
		}
		iovecs = append(iovecs, unix.Iovec{
			Base: (*byte)(unsafe.Pointer(start)),
			Len:  uint64(end - start),
		})
		// UIO_MAXIOV is 1024; flush in batches to stay under the limit.
		if len(iovecs) >= 1024 {
			flush()
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("while scanning /proc/%d/maps: %w", pid, err)
	}
	flush()
	return nil
}

// processMadvise issues the process_madvise(2) syscall over the given iovecs.
func processMadvise(pidfd int, iovecs []unix.Iovec, advice int, flags uint32) (int, error) {
	if len(iovecs) == 0 {
		return 0, nil
	}
	r1, _, errno := unix.Syscall6(
		unix.SYS_PROCESS_MADVISE,
		uintptr(pidfd),
		uintptr(unsafe.Pointer(&iovecs[0])),
		uintptr(len(iovecs)),
		uintptr(advice),
		uintptr(flags),
		0,
	)
	if errno != 0 {
		return 0, errno
	}
	return int(r1), nil
}
