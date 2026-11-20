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
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
)

// ProcessState mirrors ateenv.v1alpha.ProcessState.
type ProcessState int

const (
	ProcessStateRunning ProcessState = 1
	ProcessStateExited  ProcessState = 2
)

// Process mirrors ateenv.v1alpha.Process.
// Note: PID is always 0 when backed solely by kata-agent's ExecProcess,
// because ExecProcess returns google.protobuf.Empty.
type Process struct {
	ProcessID  string
	Command    []string
	PID        int32
	State      ProcessState
	ExitCode   int32
	StartedAt  time.Time
	FinishedAt time.Time
}

// StartProcessOpts mirrors ateenv.v1alpha.StartProcessRequest.
type StartProcessOpts struct {
	Command []string
	Cwd     string
	Env     []string
	Stdin   bool
	Timeout time.Duration
}

// OutputChunk mirrors ateenv.v1alpha.ProcessOutput.
type OutputChunk struct {
	Stdout []byte
	Stderr []byte
	Exit   *Process
}

// ProcessManager implements the ateenv.v1alpha.ProcessService semantics on top
// of a kata.AgentClient (vsock 1024, no debug_console required).
type ProcessManager struct {
	ac          *AgentClient
	containerID string
	nextID      atomic.Uint64

	mu    sync.Mutex
	procs map[string]*managedProcess
}

type managedProcess struct {
	mu           sync.Mutex
	cond         *sync.Cond
	proc         Process
	stdinEnabled bool
	stdinClosed  bool
	stdout       []byte
	stderr       []byte
	stdoutDone   bool
	stderrDone   bool
	waitDone     bool
	cancel       context.CancelFunc
}

func NewProcessManager(ac *AgentClient, containerID string) *ProcessManager {
	return &ProcessManager{
		ac:          ac,
		containerID: containerID,
		procs:       make(map[string]*managedProcess),
	}
}

func (m *ProcessManager) StartProcess(ctx context.Context, opts StartProcessOpts) (Process, error) {
	if len(opts.Command) == 0 {
		return Process{}, errors.New("command is required")
	}
	id := fmt.Sprintf("exec-%d", m.nextID.Add(1))
	cwd := opts.Cwd
	if cwd == "" {
		cwd = "/"
	}
	req := &agentpb.ExecProcessRequest{
		ContainerId: m.containerID,
		ExecId:      id,
		Process: &agentpb.Process{
			Args: opts.Command,
			Cwd:  cwd,
			Env:  opts.Env,
		},
	}
	if err := m.ac.ExecProcess(ctx, req); err != nil {
		return Process{}, err
	}
	if !opts.Stdin {
		_ = m.ac.CloseStdin(ctx, m.containerID, id)
	}

	procCtx, cancel := context.WithCancel(context.Background())
	mp := &managedProcess{
		proc: Process{
			ProcessID: id,
			Command:   slices.Clone(opts.Command),
			State:     ProcessStateRunning,
			StartedAt: time.Now(),
		},
		stdinEnabled: opts.Stdin,
		stdinClosed:  !opts.Stdin,
		cancel:       cancel,
	}
	mp.cond = sync.NewCond(&mp.mu)

	m.mu.Lock()
	m.procs[id] = mp
	m.mu.Unlock()

	go m.pumpStream(procCtx, mp, false)
	go m.pumpStream(procCtx, mp, true)
	go m.waitProcess(procCtx, mp, opts.Timeout)

	return mp.snapshot(), nil
}

func (m *ProcessManager) pumpStream(ctx context.Context, mp *managedProcess, stderr bool) {
	r := NewStdioReader(ctx, m.ac, m.containerID, mp.proc.ProcessID, stderr)
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			mp.mu.Lock()
			if stderr {
				mp.stderr = append(mp.stderr, buf[:n]...)
			} else {
				mp.stdout = append(mp.stdout, buf[:n]...)
			}
			mp.cond.Broadcast()
			mp.mu.Unlock()
		}
		if err != nil {
			mp.mu.Lock()
			if stderr {
				mp.stderrDone = true
			} else {
				mp.stdoutDone = true
			}
			mp.cond.Broadcast()
			mp.mu.Unlock()
			return
		}
	}
}

func (m *ProcessManager) waitProcess(ctx context.Context, mp *managedProcess, timeout time.Duration) {
	var timer *time.Timer
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() {
			_ = m.ac.SignalProcess(context.Background(), m.containerID, mp.proc.ProcessID, uint32(syscall.SIGKILL))
		})
		defer timer.Stop()
	}

	status, err := m.ac.WaitProcess(ctx, m.containerID, mp.proc.ProcessID)
	mp.mu.Lock()
	if err == nil {
		mp.proc.ExitCode = status
	} else {
		mp.proc.ExitCode = -1
	}
	mp.proc.State = ProcessStateExited
	mp.proc.FinishedAt = time.Now()
	mp.waitDone = true
	mp.cond.Broadcast()
	mp.mu.Unlock()
}

func (mp *managedProcess) snapshot() Process {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	p := mp.proc
	p.Command = slices.Clone(p.Command)
	return p
}

func (m *ProcessManager) get(id string) (*managedProcess, error) {
	m.mu.Lock()
	mp, ok := m.procs[id]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("process %q not found", id)
	}
	return mp, nil
}

func (m *ProcessManager) GetProcess(id string) (Process, error) {
	mp, err := m.get(id)
	if err != nil {
		return Process{}, err
	}
	return mp.snapshot(), nil
}

func (m *ProcessManager) WriteProcessInput(ctx context.Context, id string, data []byte, closeStdin bool) (int64, error) {
	mp, err := m.get(id)
	if err != nil {
		return 0, err
	}
	mp.mu.Lock()
	if !mp.stdinEnabled || mp.stdinClosed {
		mp.mu.Unlock()
		return 0, errors.New("stdin is not open")
	}
	mp.mu.Unlock()

	var written int64
	for len(data) > 0 {
		n, err := m.ac.WriteStdin(ctx, m.containerID, id, data)
		if err != nil {
			return written, err
		}
		written += int64(n)
		data = data[n:]
	}
	if closeStdin {
		if err := m.ac.CloseStdin(ctx, m.containerID, id); err != nil {
			return written, err
		}
		mp.mu.Lock()
		mp.stdinClosed = true
		mp.mu.Unlock()
	}
	return written, nil
}

// SignalProcess delivers sig to the exec'd process.
// Gap vs guest.proto: kata-agent's SignalProcess calls kill(pid, sig) rather
// than kill(-pgid, sig), so grandchildren in the process group are not signaled.
func (m *ProcessManager) SignalProcess(ctx context.Context, id string, sig syscall.Signal) (Process, error) {
	mp, err := m.get(id)
	if err != nil {
		return Process{}, err
	}
	if err := m.ac.SignalProcess(ctx, m.containerID, id, uint32(sig)); err != nil {
		return Process{}, err
	}
	return mp.snapshot(), nil
}

// StreamProcessOutput replays spooled stdout/stderr from the given byte offsets
// and, when follow is true, blocks until the process exits and all output is
// delivered, ending with an Exit chunk.
func (m *ProcessManager) StreamProcessOutput(ctx context.Context, id string, stdoutOff, stderrOff int64, follow bool, emit func(OutputChunk) error) error {
	mp, err := m.get(id)
	if err != nil {
		return err
	}
	stopWatch := context.AfterFunc(ctx, func() {
		mp.mu.Lock()
		mp.cond.Broadcast()
		mp.mu.Unlock()
	})
	defer stopWatch()

	mp.mu.Lock()
	defer mp.mu.Unlock()

	for {
		var outChunk, errChunk []byte
		if stdoutOff < int64(len(mp.stdout)) {
			outChunk = slices.Clone(mp.stdout[stdoutOff:])
			stdoutOff = int64(len(mp.stdout))
		}
		if stderrOff < int64(len(mp.stderr)) {
			errChunk = slices.Clone(mp.stderr[stderrOff:])
			stderrOff = int64(len(mp.stderr))
		}
		done := mp.waitDone && mp.stdoutDone && mp.stderrDone
		var exitCopy *Process
		if done {
			p := mp.proc
			p.Command = slices.Clone(p.Command)
			exitCopy = &p
		}

		mp.mu.Unlock()
		if len(outChunk) > 0 {
			if err := emit(OutputChunk{Stdout: outChunk}); err != nil {
				mp.mu.Lock()
				return err
			}
		}
		if len(errChunk) > 0 {
			if err := emit(OutputChunk{Stderr: errChunk}); err != nil {
				mp.mu.Lock()
				return err
			}
		}
		if done {
			if err := emit(OutputChunk{Exit: exitCopy}); err != nil {
				mp.mu.Lock()
				return err
			}
			mp.mu.Lock()
			return nil
		}
		if !follow {
			mp.mu.Lock()
			return nil
		}
		mp.mu.Lock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if stdoutOff >= int64(len(mp.stdout)) && stderrOff >= int64(len(mp.stderr)) && !(mp.waitDone && mp.stdoutDone && mp.stderrDone) {
			mp.cond.Wait()
		}
	}
}

// ReadFileViaExec is a minimal POC showing ReadFile over ExecProcess without tar.
// In production this would invoke a read-only worker-injected static helper
// instead of /bin/cat so distroless images work and no image binary is trusted.
func (m *ProcessManager) ReadFileViaExec(ctx context.Context, path string) ([]byte, error) {
	p, err := m.StartProcess(ctx, StartProcessOpts{Command: []string{"cat", "--", path}})
	if err != nil {
		return nil, err
	}
	var out, errBuf []byte
	var exit *Process
	if err := m.StreamProcessOutput(ctx, p.ProcessID, 0, 0, true, func(c OutputChunk) error {
		out = append(out, c.Stdout...)
		errBuf = append(errBuf, c.Stderr...)
		if c.Exit != nil {
			exit = c.Exit
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if exit == nil || exit.ExitCode != 0 {
		return nil, fmt.Errorf("read %q failed (exit %v): %s", path, exit, string(errBuf))
	}
	return out, nil
}

// ensure io is used if needed
var _ = io.EOF
