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
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeExecProc struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
	done   chan struct{}
	status int32
}

// startFakeExecAgent runs a ttrpc server implementing the subset of
// grpc.AgentService used by ProcessManager, backed by real os/exec processes.
func startFakeExecAgent(t *testing.T) *AgentClient {
	t.Helper()

	var mu sync.Mutex
	procs := map[string]*fakeExecProc{}

	get := func(id string) (*fakeExecProc, error) {
		mu.Lock()
		defer mu.Unlock()
		p, ok := procs[id]
		if !ok {
			return nil, errors.New("unknown exec_id: " + id)
		}
		return p, nil
	}

	sock := filepath.Join(t.TempDir(), "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv, err := ttrpc.NewServer()
	if err != nil {
		t.Fatalf("ttrpc.NewServer: %v", err)
	}
	srv.Register("grpc.AgentService", map[string]ttrpc.Method{
		"ExecProcess": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.ExecProcessRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			args := req.GetProcess().GetArgs()
			cmd := exec.Command(args[0], args[1:]...)
			if cwd := req.GetProcess().GetCwd(); cwd != "" {
				cmd.Dir = cwd
			}
			if env := req.GetProcess().GetEnv(); len(env) > 0 {
				cmd.Env = append(os.Environ(), env...)
			}
			stdin, err := cmd.StdinPipe()
			if err != nil {
				return nil, err
			}
			outR, outW, err := os.Pipe()
			if err != nil {
				return nil, err
			}
			errR, errW, err := os.Pipe()
			if err != nil {
				outR.Close()
				outW.Close()
				return nil, err
			}
			cmd.Stdout = outW
			cmd.Stderr = errW
			if err := cmd.Start(); err != nil {
				outR.Close()
				outW.Close()
				errR.Close()
				errW.Close()
				return nil, err
			}
			// Close parent's write ends so reads see EOF once the child exits,
			// without cmd.Wait() closing the read ends early.
			_ = outW.Close()
			_ = errW.Close()
			fp := &fakeExecProc{
				cmd:    cmd,
				stdin:  stdin,
				stdout: outR,
				stderr: errR,
				done:   make(chan struct{}),
			}
			go func() {
				err := cmd.Wait()
				if err == nil {
					fp.status = 0
				} else if ee, ok := err.(*exec.ExitError); ok {
					ws := ee.Sys().(syscall.WaitStatus)
					if ws.Signaled() {
						fp.status = int32(128 + ws.Signal())
					} else {
						fp.status = int32(ws.ExitStatus())
					}
				} else {
					fp.status = 1
				}
				close(fp.done)
			}()
			mu.Lock()
			procs[req.GetExecId()] = fp
			mu.Unlock()
			return &emptypb.Empty{}, nil
		},
		"WriteStdin": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.WriteStreamRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			fp, err := get(req.GetExecId())
			if err != nil {
				return nil, err
			}
			n, err := fp.stdin.Write(req.GetData())
			if err != nil {
				return nil, err
			}
			return &agentpb.WriteStreamResponse{Len: uint32(n)}, nil
		},
		"CloseStdin": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.CloseStdinRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			fp, err := get(req.GetExecId())
			if err != nil {
				return nil, err
			}
			_ = fp.stdin.Close()
			return &emptypb.Empty{}, nil
		},
		"ReadStdout": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.ReadStreamRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			fp, err := get(req.GetExecId())
			if err != nil {
				return nil, err
			}
			buf := make([]byte, req.GetLen())
			n, err := fp.stdout.Read(buf)
			if n > 0 {
				return &agentpb.ReadStreamResponse{Data: buf[:n]}, nil
			}
			return nil, err
		},
		"ReadStderr": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.ReadStreamRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			fp, err := get(req.GetExecId())
			if err != nil {
				return nil, err
			}
			buf := make([]byte, req.GetLen())
			n, err := fp.stderr.Read(buf)
			if n > 0 {
				return &agentpb.ReadStreamResponse{Data: buf[:n]}, nil
			}
			return nil, err
		},
		"SignalProcess": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.SignalProcessRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			fp, err := get(req.GetExecId())
			if err != nil {
				return nil, err
			}
			if err := fp.cmd.Process.Signal(syscall.Signal(req.GetSignal())); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return nil, err
			}
			return &emptypb.Empty{}, nil
		},
		"WaitProcess": func(ctx context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.WaitProcessRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			fp, err := get(req.GetExecId())
			if err != nil {
				return nil, err
			}
			select {
			case <-fp.done:
				return &agentpb.WaitProcessResponse{Status: fp.status}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	go func() { _ = srv.Serve(context.Background(), l) }()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ac := &AgentClient{conn: conn, client: ttrpc.NewClient(conn)}
	t.Cleanup(func() { _ = ac.Close() })
	return ac
}

func TestProcessManagerStartStreamReplay(t *testing.T) {
	ac := startFakeExecAgent(t)
	pm := NewProcessManager(ac, "actor-ctr")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p, err := pm.StartProcess(ctx, StartProcessOpts{
		Command: []string{"sh", "-c", "printf 'hello world'; printf 'warn!' >&2"},
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	var stdout, stderr []byte
	var exit *Process
	if err := pm.StreamProcessOutput(ctx, p.ProcessID, 0, 0, true, func(c OutputChunk) error {
		stdout = append(stdout, c.Stdout...)
		stderr = append(stderr, c.Stderr...)
		if c.Exit != nil {
			exit = c.Exit
		}
		return nil
	}); err != nil {
		t.Fatalf("StreamProcessOutput: %v", err)
	}
	if string(stdout) != "hello world" || string(stderr) != "warn!" {
		t.Fatalf("got stdout=%q stderr=%q", stdout, stderr)
	}
	if exit == nil || exit.State != ProcessStateExited || exit.ExitCode != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	// Replay from byte offset 6 on stdout and past end on stderr.
	var replayOut, replayErr []byte
	if err := pm.StreamProcessOutput(ctx, p.ProcessID, 6, 999, false, func(c OutputChunk) error {
		replayOut = append(replayOut, c.Stdout...)
		replayErr = append(replayErr, c.Stderr...)
		return nil
	}); err != nil {
		t.Fatalf("StreamProcessOutput replay: %v", err)
	}
	if string(replayOut) != "world" || len(replayErr) != 0 {
		t.Fatalf("replay got stdout=%q stderr=%q, want world / empty", replayOut, replayErr)
	}
}

func TestProcessManagerStdinAndSignal(t *testing.T) {
	ac := startFakeExecAgent(t)
	pm := NewProcessManager(ac, "actor-ctr")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stdin roundtrip via cat.
	p, err := pm.StartProcess(ctx, StartProcessOpts{
		Command: []string{"cat"},
		Stdin:   true,
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	n, err := pm.WriteProcessInput(ctx, p.ProcessID, []byte("ping-pong"), true)
	if err != nil || n != 9 {
		t.Fatalf("WriteProcessInput: n=%d err=%v", n, err)
	}
	var out []byte
	if err := pm.StreamProcessOutput(ctx, p.ProcessID, 0, 0, true, func(c OutputChunk) error {
		out = append(out, c.Stdout...)
		return nil
	}); err != nil {
		t.Fatalf("StreamProcessOutput: %v", err)
	}
	if string(out) != "ping-pong" {
		t.Fatalf("cat echoed %q, want ping-pong", out)
	}

	// SignalProcess + timeout.
	sleeper, err := pm.StartProcess(ctx, StartProcessOpts{
		Command: []string{"sleep", "30"},
	})
	if err != nil {
		t.Fatalf("StartProcess sleep: %v", err)
	}
	if _, err := pm.SignalProcess(ctx, sleeper.ProcessID, syscall.SIGTERM); err != nil {
		t.Fatalf("SignalProcess: %v", err)
	}
	var exit *Process
	if err := pm.StreamProcessOutput(ctx, sleeper.ProcessID, 0, 0, true, func(c OutputChunk) error {
		if c.Exit != nil {
			exit = c.Exit
		}
		return nil
	}); err != nil {
		t.Fatalf("StreamProcessOutput sleep: %v", err)
	}
	if exit == nil || exit.ExitCode != int32(128+syscall.SIGTERM) {
		t.Fatalf("got exit=%+v, want exit_code=%d", exit, 128+syscall.SIGTERM)
	}
}

func TestProcessManagerReadFileViaExec(t *testing.T) {
	ac := startFakeExecAgent(t)
	pm := NewProcessManager(ac, "actor-ctr")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f := filepath.Join(t.TempDir(), "sample.bin")
	want := []byte("binary\x00payload\nline2")
	if err := os.WriteFile(f, want, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := pm.ReadFileViaExec(ctx, f)
	if err != nil {
		t.Fatalf("ReadFileViaExec: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadFileViaExec = %q, want %q", got, want)
	}
}
