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

// Package procman implements the shared worker-side ProcessService and
// FileSystemService logic (spooling, offset replay, stdin serialization,
// timeout enforcement, and lifecycle cleanup) on top of a runtime Backend.
package procman

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// DefaultMaxSpoolBytes caps per-stream (stdout or stderr) buffered output
	// so a chatty process cannot OOM the worker.
	DefaultMaxSpoolBytes = 4 << 20 // 4 MiB
	// DefaultMaxChunkBytes bounds each gRPC output chunk and stdin write call.
	DefaultMaxChunkBytes = 64 << 10 // 64 KiB
	// DefaultMaxExitedProcs bounds how many finished process records are kept.
	DefaultMaxExitedProcs = 128

	signalTimeout = 5 * time.Second
)

// ExecSpec describes one process to spawn inside a container.
type ExecSpec struct {
	ContainerName string
	Command       []string
	Cwd           string
	Env           map[string]string
	Stdin         bool
}

// ExecHandle is one running exec'd process in a sandbox container.
type ExecHandle interface {
	// PID returns the in-container PID, or 0 if the runtime does not report it.
	PID() int32
	Stdout() io.Reader
	Stderr() io.Reader
	WriteStdin(ctx context.Context, data []byte) (int, error)
	CloseStdin(ctx context.Context) error
	Signal(ctx context.Context, sig syscall.Signal) error
	Wait(ctx context.Context) (int32, error)
	Close() error
}

// Backend adapts a sandbox runtime (microVM kata-agent or gVisor runsc) to Manager.
type Backend interface {
	Exec(ctx context.Context, spec ExecSpec) (ExecHandle, error)
	ReadFile(ctx context.Context, containerName, path string, mode uint32, emit func([]byte) error) error
	WriteFile(ctx context.Context, containerName, path string, mode uint32, seekOffset int64, r io.Reader) (int64, error)
}

// Options configures a Manager. Zero values use defaults.
type Options struct {
	MaxSpoolBytes  int
	MaxChunkBytes  int
	MaxExitedProcs int
}

// Manager tracks exec'd processes for one hosted actor and serves the
// ProcessService / FileSystemService worker RPCs.
type Manager struct {
	backend Backend
	opts    Options
	nextID  atomic.Uint64

	baseCtx    context.Context
	baseCancel context.CancelFunc

	mu         sync.Mutex
	closed     bool
	procs      map[string]*managedProcess
	exitedRing []string
}

type managedProcess struct {
	handle  ExecHandle
	stdinMu sync.Mutex

	mu           sync.Mutex
	cond         *sync.Cond
	id           string
	command      []string
	pid          int32
	state        ateworkerpb.ProcessState
	exitCode     int32
	waitErr      error
	startedAt    time.Time
	finishedAt   time.Time
	stdinEnabled bool
	stdinClosed  bool
	stdout       []byte
	stderr       []byte
	stdoutDone   bool
	stderrDone   bool
	waitDone     bool
	cancel       context.CancelFunc
}

// NewManager creates a Manager backed by b.
func NewManager(b Backend, opts Options) *Manager {
	if opts.MaxSpoolBytes <= 0 {
		opts.MaxSpoolBytes = DefaultMaxSpoolBytes
	}
	if opts.MaxChunkBytes <= 0 {
		opts.MaxChunkBytes = DefaultMaxChunkBytes
	}
	if opts.MaxExitedProcs <= 0 {
		opts.MaxExitedProcs = DefaultMaxExitedProcs
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		backend:    b,
		opts:       opts,
		baseCtx:    ctx,
		baseCancel: cancel,
		procs:      make(map[string]*managedProcess),
	}
}

// Close cancels all managed processes and closes their handles.
func (m *Manager) Close() error {
	m.baseCancel()
	m.mu.Lock()
	m.closed = true
	handles := make([]*managedProcess, 0, len(m.procs))
	for _, mp := range m.procs {
		handles = append(handles, mp)
	}
	m.procs = make(map[string]*managedProcess)
	m.exitedRing = nil
	m.mu.Unlock()

	var firstErr error
	for _, mp := range handles {
		mp.cancel()
		mp.mu.Lock()
		mp.cond.Broadcast()
		mp.mu.Unlock()
		if err := mp.handle.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// StartProcess spawns a process and starts background stdout/stderr/wait pumps.
func (m *Manager) StartProcess(ctx context.Context, req *ateworkerpb.StartProcessRequest) (*ateworkerpb.Process, error) {
	if len(req.GetCommand()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	var timeout time.Duration
	if d := req.GetTimeout(); d != nil {
		timeout = d.AsDuration()
		if timeout < 0 {
			return nil, status.Error(codes.InvalidArgument, "timeout must be non-negative")
		}
	}

	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return nil, status.Error(codes.FailedPrecondition, "actor process manager is closed")
	}

	h, err := m.backend.Exec(ctx, ExecSpec{
		ContainerName: req.GetContainerName(),
		Command:       req.GetCommand(),
		Cwd:           req.GetCwd(),
		Env:           req.GetEnv(),
		Stdin:         req.GetStdin(),
	})
	if err != nil {
		return nil, err
	}

	if !req.GetStdin() {
		if err := h.CloseStdin(ctx); err != nil {
			_ = h.Close()
			return nil, fmt.Errorf("closing stdin: %w", err)
		}
	}

	id := fmt.Sprintf("proc-%d", m.nextID.Add(1))
	procCtx, cancel := context.WithCancel(m.baseCtx)
	mp := &managedProcess{
		handle:       h,
		id:           id,
		command:      slices.Clone(req.GetCommand()),
		pid:          h.PID(),
		state:        ateworkerpb.ProcessState_PROCESS_STATE_RUNNING,
		startedAt:    time.Now(),
		stdinEnabled: req.GetStdin(),
		stdinClosed:  !req.GetStdin(),
		cancel:       cancel,
	}
	mp.cond = sync.NewCond(&mp.mu)

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		cancel()
		_ = h.Close()
		return nil, status.Error(codes.FailedPrecondition, "actor process manager is closed")
	}
	m.procs[id] = mp
	m.mu.Unlock()

	go m.pumpStream(mp, h.Stdout(), false)
	go m.pumpStream(mp, h.Stderr(), true)
	go m.waitProcess(procCtx, mp, timeout)

	return mp.protoSnapshot(), nil
}

func (m *Manager) pumpStream(mp *managedProcess, r io.Reader, stderr bool) {
	defer func() {
		mp.mu.Lock()
		if stderr {
			mp.stderrDone = true
		} else {
			mp.stdoutDone = true
		}
		mp.cond.Broadcast()
		mp.mu.Unlock()
	}()
	if r == nil {
		return
	}
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			mp.mu.Lock()
			if stderr {
				mp.stderr = appendBounded(mp.stderr, buf[:n], m.opts.MaxSpoolBytes)
			} else {
				mp.stdout = appendBounded(mp.stdout, buf[:n], m.opts.MaxSpoolBytes)
			}
			mp.cond.Broadcast()
			mp.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func appendBounded(dst, src []byte, limit int) []byte {
	rem := limit - len(dst)
	if rem <= 0 {
		return dst
	}
	if len(src) > rem {
		src = src[:rem]
	}
	return append(dst, src...)
}

func (m *Manager) waitProcess(ctx context.Context, mp *managedProcess, timeout time.Duration) {
	var timer *time.Timer
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() {
			sigCtx, cancel := context.WithTimeout(ctx, signalTimeout)
			defer cancel()
			_ = mp.handle.Signal(sigCtx, syscall.SIGKILL)
		})
		defer timer.Stop()
	}

	exitCode, err := mp.handle.Wait(ctx)
	mp.mu.Lock()
	mp.waitErr = err
	if err == nil {
		mp.exitCode = exitCode
	}
	mp.state = ateworkerpb.ProcessState_PROCESS_STATE_EXITED
	mp.finishedAt = time.Now()
	mp.waitDone = true
	mp.cond.Broadcast()
	mp.mu.Unlock()

	m.recordExited(mp.id)
}

func (m *Manager) recordExited(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.exitedRing = append(m.exitedRing, id)
	for len(m.exitedRing) > m.opts.MaxExitedProcs {
		evict := m.exitedRing[0]
		m.exitedRing = m.exitedRing[1:]
		if old, ok := m.procs[evict]; ok {
			delete(m.procs, evict)
			old.cancel()
			_ = old.handle.Close()
		}
	}
}

func (mp *managedProcess) protoSnapshot() *ateworkerpb.Process {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.protoSnapshotLocked()
}

func (mp *managedProcess) protoSnapshotLocked() *ateworkerpb.Process {
	p := &ateworkerpb.Process{
		ProcessId: mp.id,
		Command:   slices.Clone(mp.command),
		Pid:       mp.pid,
		State:     mp.state,
		ExitCode:  mp.exitCode,
		StartedAt: timestamppb.New(mp.startedAt),
	}
	if !mp.finishedAt.IsZero() {
		p.FinishedAt = timestamppb.New(mp.finishedAt)
	}
	return p
}

func (m *Manager) get(id string) (*managedProcess, error) {
	m.mu.Lock()
	mp, ok := m.procs[id]
	m.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "process %q not found", id)
	}
	return mp, nil
}

// GetProcess returns the current state of processID.
func (m *Manager) GetProcess(processID string) (*ateworkerpb.Process, error) {
	mp, err := m.get(processID)
	if err != nil {
		return nil, err
	}
	return mp.protoSnapshot(), nil
}

// WriteProcessInput writes data to stdin and optionally closes it.
func (m *Manager) WriteProcessInput(ctx context.Context, processID string, data []byte, closeStdin bool) (int64, error) {
	mp, err := m.get(processID)
	if err != nil {
		return 0, err
	}

	mp.stdinMu.Lock()
	defer mp.stdinMu.Unlock()

	mp.mu.Lock()
	enabled, closed := mp.stdinEnabled, mp.stdinClosed
	mp.mu.Unlock()
	if !enabled {
		return 0, status.Error(codes.FailedPrecondition, "process stdin is not enabled")
	}
	if closed {
		return 0, status.Error(codes.FailedPrecondition, "process stdin is already closed")
	}

	var written int64
	for len(data) > 0 {
		chunk := data
		if len(chunk) > m.opts.MaxChunkBytes {
			chunk = chunk[:m.opts.MaxChunkBytes]
		}
		n, err := mp.handle.WriteStdin(ctx, chunk)
		if n < 0 || n > len(chunk) {
			return written, fmt.Errorf("invalid WriteStdin byte count %d for %d-byte chunk", n, len(chunk))
		}
		written += int64(n)
		data = data[n:]
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	if closeStdin {
		if err := mp.handle.CloseStdin(ctx); err != nil {
			return written, err
		}
		mp.mu.Lock()
		mp.stdinClosed = true
		mp.mu.Unlock()
	}
	return written, nil
}

// SignalProcess delivers sig to processID and returns its state snapshot.
func (m *Manager) SignalProcess(ctx context.Context, processID string, sig ateworkerpb.Signal) (*ateworkerpb.Process, error) {
	if sig <= 0 {
		return nil, status.Error(codes.InvalidArgument, "signal must be positive")
	}
	mp, err := m.get(processID)
	if err != nil {
		return nil, err
	}
	if err := mp.handle.Signal(ctx, syscall.Signal(sig)); err != nil {
		return nil, err
	}
	return mp.protoSnapshot(), nil
}

// StreamProcessOutput streams stdout/stderr from byte offsets and, when follow
// is true, blocks until the process exits and all spooled output is emitted.
func (m *Manager) StreamProcessOutput(ctx context.Context, req *ateworkerpb.StreamProcessOutputRequest, emit func(*ateworkerpb.ProcessOutput) error) error {
	stdoutOff := req.GetStdoutOffset()
	stderrOff := req.GetStderrOffset()
	if stdoutOff < 0 || stderrOff < 0 {
		return status.Error(codes.InvalidArgument, "stdout_offset and stderr_offset must be non-negative")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	mp, err := m.get(req.GetProcessId())
	if err != nil {
		return err
	}
	stopWatch := context.AfterFunc(ctx, func() {
		mp.mu.Lock()
		mp.cond.Broadcast()
		mp.mu.Unlock()
	})
	defer stopWatch()

	for {
		outChunk, errChunk, exitProto, waitErr, done, waitNeeded := m.nextOutputBatch(mp, &stdoutOff, &stderrOff, req.GetFollow())

		if len(outChunk) > 0 {
			if err := emit(&ateworkerpb.ProcessOutput{Output: &ateworkerpb.ProcessOutput_Stdout{Stdout: outChunk}}); err != nil {
				return err
			}
		}
		if len(errChunk) > 0 {
			if err := emit(&ateworkerpb.ProcessOutput{Output: &ateworkerpb.ProcessOutput_Stderr{Stderr: errChunk}}); err != nil {
				return err
			}
		}
		if done {
			if waitErr != nil {
				return waitErr
			}
			return emit(&ateworkerpb.ProcessOutput{Output: &ateworkerpb.ProcessOutput_Exit{Exit: exitProto}})
		}
		if !req.GetFollow() && len(outChunk) == 0 && len(errChunk) == 0 {
			return nil
		}
		if waitNeeded {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
}

func (m *Manager) nextOutputBatch(mp *managedProcess, stdoutOff, stderrOff *int64, follow bool) (outChunk, errChunk []byte, exitProto *ateworkerpb.Process, waitErr error, done, waited bool) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	for {
		if *stdoutOff < int64(len(mp.stdout)) {
			end := min(int64(len(mp.stdout)), *stdoutOff+int64(m.opts.MaxChunkBytes))
			outChunk = slices.Clone(mp.stdout[*stdoutOff:end])
			*stdoutOff = end
		}
		if *stderrOff < int64(len(mp.stderr)) {
			end := min(int64(len(mp.stderr)), *stderrOff+int64(m.opts.MaxChunkBytes))
			errChunk = slices.Clone(mp.stderr[*stderrOff:end])
			*stderrOff = end
		}
		finished := mp.waitDone && mp.stdoutDone && mp.stderrDone
		hasMore := *stdoutOff < int64(len(mp.stdout)) || *stderrOff < int64(len(mp.stderr))
		if len(outChunk) > 0 || len(errChunk) > 0 {
			if finished && !hasMore {
				return outChunk, errChunk, mp.protoSnapshotLocked(), mp.waitErr, true, waited
			}
			return outChunk, errChunk, nil, nil, false, waited
		}
		if finished {
			return nil, nil, mp.protoSnapshotLocked(), mp.waitErr, true, waited
		}
		if !follow {
			return nil, nil, nil, nil, false, waited
		}
		if m.baseCtx.Err() != nil {
			return nil, nil, nil, errors.New("process manager closed"), true, waited
		}
		waited = true
		mp.cond.Wait()
		// Return to caller after wakeup so it can check ctx.Err() before re-waiting.
		if *stdoutOff >= int64(len(mp.stdout)) && *stderrOff >= int64(len(mp.stderr)) && !(mp.waitDone && mp.stdoutDone && mp.stderrDone) {
			return nil, nil, nil, nil, false, true
		}
	}
}

// ReadFile delegates to the runtime Backend after basic validation.
func (m *Manager) ReadFile(ctx context.Context, req *ateworkerpb.ReadFileRequest, emit func([]byte) error) error {
	if req.GetPath() == "" {
		return status.Error(codes.InvalidArgument, "path is required")
	}
	return m.backend.ReadFile(ctx, req.GetContainerName(), req.GetPath(), req.GetMode(), emit)
}

// WriteFile delegates to the runtime Backend after basic validation.
func (m *Manager) WriteFile(ctx context.Context, containerName, path string, mode uint32, seekOffset int64, r io.Reader) (int64, error) {
	if path == "" {
		return 0, status.Error(codes.InvalidArgument, "path is required")
	}
	if seekOffset < 0 {
		return 0, status.Error(codes.InvalidArgument, "seek_offset must be non-negative")
	}
	return m.backend.WriteFile(ctx, containerName, path, mode, seekOffset, r)
}
