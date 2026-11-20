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
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/procman"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gvisorBackend adapts runsc exec / kill / read to procman.Backend.
//
// Native gVisor primitives used:
//   - runsc exec --internal-pid-file <file> for in-container PID discovery
//   - runsc kill --pgid <pid> <sig> for process-group signaling
//   - runsc read <cid> <path> for native ReadFile (when mode == 0)
//   - runsc exec --exec-fd <fd> (when fsHelperHostPath is set) to stream
//     WriteFile / create-on-missing ReadFile without mounting anything into
//     the container rootfs or relying on tar.
type gvisorBackend struct {
	rcmd             *runsc
	defaultContainer string
	// fsHelperHostPath, when non-empty, is opened on the host and passed via
	// runsc exec --exec-fd=3 so distroless containers need no mounted helper.
	fsHelperHostPath string
	// fsHelperArgs are extra args prepended before "read"/"write" (used in tests
	// when the test binary itself acts as the helper).
	fsHelperArgs []string
}

func (b *gvisorBackend) resolveContainer(name string) (string, error) {
	if name != "" {
		return name, nil
	}
	if b.defaultContainer != "" {
		return b.defaultContainer, nil
	}
	return "", status.Error(codes.InvalidArgument, "container_name is required")
}

func (b *gvisorBackend) Exec(ctx context.Context, spec procman.ExecSpec) (procman.ExecHandle, error) {
	cid, err := b.resolveContainer(spec.ContainerName)
	if err != nil {
		return nil, err
	}
	return b.startRunscExec(ctx, cid, spec, nil)
}

func (b *gvisorBackend) startRunscExec(ctx context.Context, cid string, spec procman.ExecSpec, execBinary *os.File) (procman.ExecHandle, error) {
	pidDir, err := os.MkdirTemp("", "runsc-exec-pid-*")
	if err != nil {
		return nil, err
	}
	pidFile := filepath.Join(pidDir, "internal.pid")

	extra := []string{"--internal-pid-file", pidFile}
	if spec.Cwd != "" {
		extra = append(extra, "--cwd", spec.Cwd)
	}
	for _, k := range slices.Sorted(maps.Keys(spec.Env)) {
		extra = append(extra, "--env", k+"="+spec.Env[k])
	}
	if execBinary != nil {
		// ExtraFiles[0] becomes fd 3 in the child process.
		extra = append(extra, "--exec-fd", "3")
	}
	extra = append(extra, cid)
	extra = append(extra, spec.Command...)

	args := b.rcmd.buildArgs("exec", "", extra...)
	cmd := exec.Command(b.rcmd.path, args...)
	if execBinary != nil {
		cmd.ExtraFiles = []*os.File{execBinary}
	}

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		_ = os.RemoveAll(pidDir)
		return nil, err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		_ = os.RemoveAll(pidDir)
		return nil, err
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		_ = outR.Close()
		_ = outW.Close()
		_ = os.RemoveAll(pidDir)
		return nil, err
	}
	cmd.Stdin = stdinR
	cmd.Stdout = outW
	cmd.Stderr = errW

	if err := cmd.Start(); err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		_ = outR.Close()
		_ = outW.Close()
		_ = errR.Close()
		_ = errW.Close()
		_ = os.RemoveAll(pidDir)
		return nil, err
	}
	_ = stdinR.Close()
	_ = outW.Close()
	_ = errW.Close()

	h := &runscExecHandle{
		rcmd:   b.rcmd,
		cid:    cid,
		cmd:    cmd,
		stdin:  stdinW,
		stdout: outR,
		stderr: errR,
		done:   make(chan struct{}),
		pidDir: pidDir,
	}
	go func() {
		err := cmd.Wait()
		if err == nil {
			h.exitCode = 0
		} else if ee, ok := err.(*exec.ExitError); ok {
			ws := ee.Sys().(syscall.WaitStatus)
			if ws.Signaled() {
				h.exitCode = int32(128 + ws.Signal())
			} else {
				h.exitCode = int32(ws.ExitStatus())
			}
		} else {
			h.waitErr = err
		}
		close(h.done)
	}()

	// Best-effort wait for runsc to write --internal-pid-file so PID() and
	// Signal() (--pgid) work immediately after StartProcess returns.
	h.internalPID = waitForPIDFile(ctx, pidFile, h.done)
	_ = os.RemoveAll(pidDir)

	return h, nil
}

func waitForPIDFile(ctx context.Context, path string, done <-chan struct{}) int32 {
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if data, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 32); err == nil && pid > 0 {
				return int32(pid)
			}
		}
		select {
		case <-done:
			if data, err := os.ReadFile(path); err == nil {
				if pid, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 32); err == nil && pid > 0 {
					return int32(pid)
				}
			}
			return 0
		case <-ctx.Done():
			return 0
		case <-deadline.C:
			return 0
		case <-tick.C:
		}
	}
}

// ReadFile uses native `runsc read <cid> <path>` when mode == 0 (no helper
// binary needed, works on distroless, sees sentry overlay + /tmp). When
// mode != 0 (create-if-missing semantics from guest.proto), it falls back to
// the --exec-fd fshelper.
func (b *gvisorBackend) ReadFile(ctx context.Context, containerName, path string, mode uint32, emit func([]byte) error) error {
	cid, err := b.resolveContainer(containerName)
	if err != nil {
		return err
	}
	if mode == 0 {
		return b.readFileNative(ctx, cid, path, emit)
	}
	return b.readFileViaExecFD(ctx, cid, path, mode, emit)
}

func (b *gvisorBackend) readFileNative(ctx context.Context, cid, path string, emit func([]byte) error) error {
	args := b.rcmd.buildArgs("read", "", cid, path)
	cmd := exec.CommandContext(ctx, b.rcmd.path, args...)

	outR, outW, err := os.Pipe()
	if err != nil {
		return err
	}
	var stderrBuf bytes.Buffer
	cmd.Stdout = outW
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		_ = outR.Close()
		_ = outW.Close()
		return err
	}
	_ = outW.Close()

	streamErr := procman.StreamReaderToEmit(ctx, outR, procman.DefaultMaxChunkBytes, emit)
	_ = outR.Close()
	waitErr := cmd.Wait()
	if streamErr != nil {
		return streamErr
	}
	if waitErr != nil {
		msg := strings.TrimSpace(stderrBuf.String())
		if strings.Contains(strings.ToLower(msg), "no such file") || strings.Contains(strings.ToLower(msg), "not found") {
			return status.Errorf(codes.NotFound, "runsc read %q: %s", path, msg)
		}
		return status.Errorf(codes.Internal, "runsc read %q: %v (%s)", path, waitErr, msg)
	}
	return nil
}

func (b *gvisorBackend) readFileViaExecFD(ctx context.Context, cid, path string, mode uint32, emit func([]byte) error) error {
	if b.fsHelperHostPath == "" {
		return status.Error(codes.Unimplemented, "ReadFile with mode!=0 requires fsHelperHostPath")
	}
	f, err := os.Open(b.fsHelperHostPath)
	if err != nil {
		return fmt.Errorf("opening fshelper %q: %w", b.fsHelperHostPath, err)
	}
	defer f.Close()

	cmdArgs := append([]string{"fshelper"}, b.fsHelperArgs...)
	cmdArgs = append(cmdArgs, "read", path, strconv.FormatUint(uint64(mode), 8))
	h, err := b.startRunscExec(ctx, cid, procman.ExecSpec{
		ContainerName: cid,
		Command:       cmdArgs,
		Stdin:         false,
	}, f)
	if err != nil {
		return err
	}
	return procman.ReadFileOverExec(ctx, h, emit)
}

// WriteFile streams raw bytes via `runsc exec --exec-fd` so no tar or container
// rootfs mount is needed.
func (b *gvisorBackend) WriteFile(ctx context.Context, containerName, path string, mode uint32, seekOffset int64, r io.Reader) (int64, error) {
	cid, err := b.resolveContainer(containerName)
	if err != nil {
		return 0, err
	}
	if b.fsHelperHostPath == "" {
		return 0, status.Error(codes.Unimplemented, "runsc has no native WriteFile RPC and no fsHelperHostPath is configured")
	}
	f, err := os.Open(b.fsHelperHostPath)
	if err != nil {
		return 0, fmt.Errorf("opening fshelper %q: %w", b.fsHelperHostPath, err)
	}
	defer f.Close()

	cmdArgs := append([]string{"fshelper"}, b.fsHelperArgs...)
	cmdArgs = append(cmdArgs, "write", path, strconv.FormatUint(uint64(mode), 8), strconv.FormatInt(seekOffset, 10))
	h, err := b.startRunscExec(ctx, cid, procman.ExecSpec{
		ContainerName: cid,
		Command:       cmdArgs,
		Stdin:         true,
	}, f)
	if err != nil {
		return 0, err
	}
	return procman.WriteFileOverExec(ctx, h, r)
}

type runscExecHandle struct {
	rcmd        *runsc
	cid         string
	cmd         *exec.Cmd
	internalPID int32

	stdin  *os.File
	stdout *os.File
	stderr *os.File

	done     chan struct{}
	exitCode int32
	waitErr  error

	pidDir    string
	closeOnce sync.Once
}

func (h *runscExecHandle) PID() int32        { return h.internalPID }
func (h *runscExecHandle) Stdout() io.Reader { return h.stdout }
func (h *runscExecHandle) Stderr() io.Reader { return h.stderr }

func (h *runscExecHandle) WriteStdin(ctx context.Context, data []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return h.stdin.Write(data)
}

func (h *runscExecHandle) CloseStdin(_ context.Context) error {
	if err := h.stdin.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	return nil
}

// Signal delivers sig to the process group via `runsc kill --pgid <pid> <sig>`.
// If --internal-pid-file was unavailable, falls back to signaling the runsc exec
// host process (which runsc forwards to the sandboxed process).
func (h *runscExecHandle) Signal(ctx context.Context, sig syscall.Signal) error {
	if h.internalPID > 0 {
		args := h.rcmd.buildArgs("kill", "", "--pgid", strconv.Itoa(int(h.internalPID)), h.cid, strconv.Itoa(int(sig)))
		out, err := exec.CommandContext(ctx, h.rcmd.path, args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("runsc kill --pgid %d: %w (%s)", h.internalPID, err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if err := h.cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

func (h *runscExecHandle) Wait(ctx context.Context) (int32, error) {
	select {
	case <-h.done:
		return h.exitCode, h.waitErr
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (h *runscExecHandle) Close() error {
	h.closeOnce.Do(func() {
		_ = h.stdin.Close()
		_ = h.stdout.Close()
		_ = h.stderr.Close()
		if h.cmd.Process != nil {
			_ = h.cmd.Process.Kill()
		}
		_ = os.RemoveAll(h.pidDir)
	})
	return nil
}

func (s *AteWorkerService) setProcManager(actorUID string, mgr *procman.Manager) {
	s.actorsMu.Lock()
	var old *procman.Manager
	if h, ok := s.actors[actorUID]; ok {
		old = h.procs
		h.procs = mgr
	}
	s.actorsMu.Unlock()
	if old != nil && old != mgr {
		_ = old.Close()
	}
}

func (s *AteWorkerService) lookupProcManager(actorUID string) (*procman.Manager, error) {
	if actorUID == "" {
		return nil, apierror.InvalidArgument("actor_uid is required")
	}
	s.actorsMu.RLock()
	h := s.actors[actorUID]
	var mgr *procman.Manager
	if h != nil {
		mgr = h.procs
	}
	s.actorsMu.RUnlock()
	if h == nil {
		return nil, apierror.NotFound("worker is not executing actor %q", actorUID)
	}
	if mgr == nil {
		return nil, apierror.FailedPrecondition("actor %q sandbox is not ready for exec", actorUID)
	}
	return mgr, nil
}

func (s *AteWorkerService) StartProcess(ctx context.Context, req *ateworkerpb.StartProcessRequest) (*ateworkerpb.Process, error) {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return nil, err
	}
	return mgr.StartProcess(ctx, req)
}

func (s *AteWorkerService) GetProcess(_ context.Context, req *ateworkerpb.GetProcessRequest) (*ateworkerpb.Process, error) {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return nil, err
	}
	return mgr.GetProcess(req.GetProcessId())
}

func (s *AteWorkerService) StreamProcessOutput(req *ateworkerpb.StreamProcessOutputRequest, stream ateworkerpb.Worker_StreamProcessOutputServer) error {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return err
	}
	return mgr.StreamProcessOutput(stream.Context(), req, stream.Send)
}

func (s *AteWorkerService) WriteProcessInput(stream ateworkerpb.Worker_WriteProcessInputServer) error {
	return procman.ServeWriteProcessInput(stream, s.lookupProcManager)
}

func (s *AteWorkerService) SignalProcess(ctx context.Context, req *ateworkerpb.SignalProcessRequest) (*ateworkerpb.Process, error) {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return nil, err
	}
	return mgr.SignalProcess(ctx, req.GetProcessId(), req.GetSignal())
}

func (s *AteWorkerService) ReadFile(req *ateworkerpb.ReadFileRequest, stream ateworkerpb.Worker_ReadFileServer) error {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return err
	}
	return mgr.ReadFile(stream.Context(), req, func(chunk []byte) error {
		return stream.Send(&ateworkerpb.ReadFileResponse{Chunk: chunk})
	})
}

func (s *AteWorkerService) WriteFile(stream ateworkerpb.Worker_WriteFileServer) error {
	return procman.ServeWriteFile(stream, s.lookupProcManager)
}
