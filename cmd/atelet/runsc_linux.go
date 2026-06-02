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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"unsafe"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"golang.org/x/sys/unix"
)

type runsc struct {
	path                   string
	actorTemplateNamespace string
	actorTemplateName      string
	actorID                string
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string) error {
	lockReaper()
	defer unlockReaper()

	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	logDir := ateompath.RunscDebugLogDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("while creating debug log dir: %w", err)
	}

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-debug",
		"-debug-log", logDir+"/",
		"-root", ateompath.RunSCStateDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID),
		"create",
		"-bundle", ateompath.OCIBundlePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName),
		"-pid-file", ateompath.PIDFilePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName),
		containerName,
	)
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc create`: %w", err)
	}

	return nil
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	lockReaper()
	defer unlockReaper()

	slog.InfoContext(ctx, "About to run runsc start", slog.String("container", containerName))

	logDir := ateompath.RunscDebugLogDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("while creating debug log dir: %w", err)
	}

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-debug",
		"-debug-log", logDir+"/",
		"-allow-connected-on-save",
		"-root", ateompath.RunSCStateDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID),
		"start",
		containerName,
	)
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc start`: %w", err)
	}

	return nil
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string) error {
	lockReaper()
	defer unlockReaper()

	slog.InfoContext(ctx, "About to run runsc checkpoint", slog.String("container", containerName))

	logDir := ateompath.RunscDebugLogDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("while creating debug log dir: %w", err)
	}

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-debug",
		"-debug-log", logDir+"/",
		"-root", ateompath.RunSCStateDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID),
		"checkpoint",
		"-image-path", checkpointPath,
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc checkpoint`: %w", err)
	}
	return nil
}

func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	lockReaper()
	defer unlockReaper()

	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	logDir := ateompath.RunscDebugLogDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("while creating debug log dir: %w", err)
	}

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-debug",
		"-debug-log", logDir+"/",
		"-root", ateompath.RunSCStateDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID),
		"restore",
		"-bundle", ateompath.OCIBundlePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName),
		"-image-path", checkpointPath,
		"-pid-file", ateompath.PIDFilePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName),
		"-detach",
		containerName,
	)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc restore`: %w", err)
	}
	return nil
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	lockReaper()
	defer unlockReaper()

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID),
		"delete",
		"-force",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc delete`: %w", err)
	}

	return nil
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
	lockReaper()
	defer unlockReaper()

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID),
		"state",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc state`: %w", err)
	}
	return nil
}

func (r *runsc) cmdPause(ctx context.Context, containerName string) error {
	lockReaper()
	defer unlockReaper()

	slog.InfoContext(ctx, "About to run runsc pause", slog.String("container", containerName))

	logDir := ateompath.RunscDebugLogDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("while creating debug log dir: %w", err)
	}

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-debug",
		"-debug-log", logDir+"/",
		"-root", ateompath.RunSCStateDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID),
		"pause",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc pause`: %w", err)
	}

	if reclaimErr := r.reclaimMemory(ctx, containerName); reclaimErr != nil {
		slog.ErrorContext(ctx, "failed to reclaim memory after pause", slog.String("container", containerName), slog.Any("error", reclaimErr))
	}

	return nil
}

func (r *runsc) cmdResume(ctx context.Context, containerName string) error {
	lockReaper()
	defer unlockReaper()

	slog.InfoContext(ctx, "About to run runsc resume", slog.String("container", containerName))

	logDir := ateompath.RunscDebugLogDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("while creating debug log dir: %w", err)
	}

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"-log-format", "json",
		"--alsologtostderr",
		"-debug",
		"-debug-log", logDir+"/",
		"-root", ateompath.RunSCStateDir(r.actorTemplateNamespace, r.actorTemplateName, r.actorID),
		"resume",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc resume`: %w", err)
	}

	return nil
}

func (r *runsc) reclaimMemory(ctx context.Context, containerName string) error {
	pidFilePath := ateompath.PIDFilePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	pidBytes, err := os.ReadFile(pidFilePath)
	if err != nil {
		return fmt.Errorf("failed to read PID file %s: %w", pidFilePath, err)
	}
	var pid int
	if _, err := fmt.Sscanf(string(bytes.TrimSpace(pidBytes)), "%d", &pid); err != nil {
		return fmt.Errorf("failed to parse PID %q: %w", string(pidBytes), err)
	}

	slog.InfoContext(ctx, "Triggering memory reclaim for process", slog.Int("pid", pid), slog.String("container", containerName))

	// Open pidfd for the process
	pidfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return fmt.Errorf("failed to open pidfd for PID %d: %w", pid, err)
	}
	defer unix.Close(pidfd)

	mapsFile, err := os.Open(fmt.Sprintf("/proc/%d/maps", pid))
	if err != nil {
		return fmt.Errorf("failed to open proc maps: %w", err)
	}
	defer mapsFile.Close()

	var iovecs []unix.Iovec
	scanner := bufio.NewScanner(mapsFile)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var start, end uintptr
		if _, err := fmt.Sscanf(fields[0], "%x-%x", &start, &end); err != nil {
			continue
		}
		if end <= start {
			continue
		}
		perms := fields[1]
		if !strings.ContainsAny(perms, "rwx") {
			// Skip guard pages or regions without any read/write/execute permissions.
			continue
		}
		// Skip special kernel regions
		if len(fields) >= 6 {
			path := fields[5]
			if path == "[vvar]" || path == "[vdso]" || path == "[vsyscall]" {
				continue
			}
		}
		iovecs = append(iovecs, unix.Iovec{
			Base: (*byte)(unsafe.Pointer(start)),
			Len:  uint64(end - start),
		})

		// UIO_MAXIOV is typically 1024.
		if len(iovecs) >= 1024 {
			if _, err := processMadvise(pidfd, iovecs, 21 /* MADV_PAGEOUT */, 0); err != nil {
				slog.ErrorContext(ctx, "Failed to page out batch of process memory", slog.Int("pid", pid), slog.Any("error", err))
			}
			iovecs = iovecs[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading proc maps: %w", err)
	}

	if len(iovecs) > 0 {
		if _, err := processMadvise(pidfd, iovecs, 21 /* MADV_PAGEOUT */, 0); err != nil {
			slog.ErrorContext(ctx, "Failed to page out final batch of process memory", slog.Int("pid", pid), slog.Any("error", err))
		}
	}

	return nil
}

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

func setupHostSwap(ctx context.Context) error {
	const zswapEnabledPath = "/sys/module/zswap/parameters/enabled"
	if _, err := os.Stat(zswapEnabledPath); os.IsNotExist(err) {
		slog.InfoContext(ctx, "zswap is not supported or enabled in the host kernel (enabled parameter file not found)")
		return nil
	}

	slog.InfoContext(ctx, "Configuring zswap on host kernel...")

	// 1. Enable zswap
	if err := os.WriteFile(zswapEnabledPath, []byte("Y\n"), 0644); err != nil {
		return fmt.Errorf("failed to enable zswap: %w", err)
	}
	slog.InfoContext(ctx, "Enabled zswap")

	// 2. Set compressor: try zstd, fall back to lz4, then lzo
	compressors := []string{"zstd", "lz4", "lzo"}
	var compressorApplied string
	for _, comp := range compressors {
		if err := os.WriteFile("/sys/module/zswap/parameters/compressor", []byte(comp+"\n"), 0644); err == nil {
			compressorApplied = comp
			break
		}
	}
	if compressorApplied != "" {
		slog.InfoContext(ctx, "Configured zswap compressor", slog.String("compressor", compressorApplied))
	} else {
		slog.WarnContext(ctx, "Failed to configure zswap compressor (used kernel default)")
	}

	// 3. Set zpool: try zsmalloc, fall back to z3fold, then zbud
	zpools := []string{"zsmalloc", "z3fold", "zbud"}
	var zpoolApplied string
	for _, zp := range zpools {
		if err := os.WriteFile("/sys/module/zswap/parameters/zpool", []byte(zp+"\n"), 0644); err == nil {
			zpoolApplied = zp
			break
		}
	}
	if zpoolApplied != "" {
		slog.InfoContext(ctx, "Configured zswap zpool", slog.String("zpool", zpoolApplied))
	} else {
		slog.WarnContext(ctx, "Failed to configure zswap zpool (used kernel default)")
	}

	return nil
}
