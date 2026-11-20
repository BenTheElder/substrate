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
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"go.ate.dev/substrate/internal/ateworker/procman"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestMain lets this test binary double as the in-container fshelper when
// invoked with --fshelper, avoiding any external binary build in unit tests.
func TestMain(m *testing.M) {
	if len(os.Args) >= 2 && os.Args[1] == "--fshelper" {
		os.Exit(procman.RunFSHelper(os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

type fakeExecProc struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *os.File
	stderr *os.File
	done   chan struct{}
	status int32
}

type fakeExecAgentState struct {
	mu       sync.Mutex
	lastReq  *agentpb.ExecProcessRequest
	procs    map[string]*fakeExecProc
	expected string
}

func startFakeExecAgent(t *testing.T, expectedCID string) (*AgentClient, *fakeExecAgentState) {
	t.Helper()

	st := &fakeExecAgentState{
		procs:    map[string]*fakeExecProc{},
		expected: expectedCID,
	}

	get := func(cid, id string) (*fakeExecProc, error) {
		if st.expected != "" && cid != st.expected {
			return nil, errors.New("unexpected container_id: " + cid)
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		p, ok := st.procs[id]
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
			if st.expected != "" && req.GetContainerId() != st.expected {
				return nil, errors.New("unexpected container_id: " + req.GetContainerId())
			}
			args := req.GetProcess().GetArgs()
			cmd := exec.Command(args[0], args[1:]...)
			if cwd := req.GetProcess().GetCwd(); cwd != "" {
				cmd.Dir = cwd
			}
			cmd.Env = req.GetProcess().GetEnv()

			stdin, err := cmd.StdinPipe()
			if err != nil {
				return nil, err
			}
			outR, outW, err := os.Pipe()
			if err != nil {
				_ = stdin.Close()
				return nil, err
			}
			errR, errW, err := os.Pipe()
			if err != nil {
				_ = stdin.Close()
				_ = outR.Close()
				_ = outW.Close()
				return nil, err
			}
			cmd.Stdout = outW
			cmd.Stderr = errW
			if err := cmd.Start(); err != nil {
				_ = stdin.Close()
				_ = outR.Close()
				_ = outW.Close()
				_ = errR.Close()
				_ = errW.Close()
				return nil, err
			}
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
			st.mu.Lock()
			st.lastReq = &req
			st.procs[req.GetExecId()] = fp
			st.mu.Unlock()
			return &emptypb.Empty{}, nil
		},
		"WriteStdin": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.WriteStreamRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			fp, err := get(req.GetContainerId(), req.GetExecId())
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
			fp, err := get(req.GetContainerId(), req.GetExecId())
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
			fp, err := get(req.GetContainerId(), req.GetExecId())
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
			fp, err := get(req.GetContainerId(), req.GetExecId())
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
			fp, err := get(req.GetContainerId(), req.GetExecId())
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
			fp, err := get(req.GetContainerId(), req.GetExecId())
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
	t.Cleanup(func() {
		_ = srv.Close()
		st.mu.Lock()
		defer st.mu.Unlock()
		for _, fp := range st.procs {
			if fp.cmd.Process != nil {
				_ = fp.cmd.Process.Kill()
			}
			_ = fp.stdin.Close()
			_ = fp.stdout.Close()
			_ = fp.stderr.Close()
		}
	})

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ac := &AgentClient{conn: conn, client: ttrpc.NewClient(conn)}
	t.Cleanup(func() { _ = ac.Close() })
	return ac, st
}

func TestExecBackendProcessAndFileAPI(t *testing.T) {
	ac, st := startFakeExecAgent(t, "app")
	selfExe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	workDir := t.TempDir()
	backend := NewExecBackend(ac, ExecBackendOptions{
		DefaultContainerID: "app",
		BaseProcesses: map[string]*agentpb.Process{
			"app": {
				Cwd:             workDir,
				Env:             []string{"PATH=" + os.Getenv("PATH"), "BASE_VAR=from-oci"},
				User:            &agentpb.User{UID: 1000, GID: 1000},
				NoNewPrivileges: true,
			},
		},
		FSHelperCmd: []string{selfExe, "--fshelper"},
	})
	mgr := procman.NewManager(backend, procman.Options{MaxChunkBytes: 4})
	t.Cleanup(func() { _ = mgr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. StartProcess inherits OCI security context + merges Env + defaults Cwd.
	p, err := mgr.StartProcess(ctx, &ateworkerpb.StartProcessRequest{
		Command: []string{"sh", "-c", "printf '%s|%s|%s' \"$BASE_VAR\" \"$EXTRA\" \"$(pwd)\"; printf 'err!' >&2"},
		Env:     map[string]string{"EXTRA": "override"},
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	st.mu.Lock()
	lastProc := st.lastReq.GetProcess()
	st.mu.Unlock()
	if !lastProc.GetNoNewPrivileges() || lastProc.GetUser().GetUID() != 1000 {
		t.Fatalf("OCI security context not inherited: %+v", lastProc)
	}

	var stdout, stderr []byte
	var exit *ateworkerpb.Process
	if err := mgr.StreamProcessOutput(ctx, &ateworkerpb.StreamProcessOutputRequest{
		ProcessId: p.GetProcessId(),
		Follow:    true,
	}, func(o *ateworkerpb.ProcessOutput) error {
		switch v := o.GetOutput().(type) {
		case *ateworkerpb.ProcessOutput_Stdout:
			stdout = append(stdout, v.Stdout...)
		case *ateworkerpb.ProcessOutput_Stderr:
			stderr = append(stderr, v.Stderr...)
		case *ateworkerpb.ProcessOutput_Exit:
			exit = v.Exit
		}
		return nil
	}); err != nil {
		t.Fatalf("StreamProcessOutput: %v", err)
	}
	wantOut := "from-oci|override|" + workDir
	if string(stdout) != wantOut || string(stderr) != "err!" {
		t.Fatalf("got stdout=%q stderr=%q, want %q / %q", stdout, stderr, wantOut, "err!")
	}
	if exit == nil || exit.GetState() != ateworkerpb.ProcessState_PROCESS_STATE_EXITED || exit.GetExitCode() != 0 {
		t.Fatalf("unexpected exit: %+v", exit)
	}

	// Offset replay + negative offset rejection.
	var replay []byte
	if err := mgr.StreamProcessOutput(ctx, &ateworkerpb.StreamProcessOutputRequest{
		ProcessId:    p.GetProcessId(),
		StdoutOffset: 9,
		StderrOffset: 999,
		Follow:       false,
	}, func(o *ateworkerpb.ProcessOutput) error {
		replay = append(replay, o.GetStdout()...)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if string(replay) != "override|"+workDir {
		t.Fatalf("replay = %q, want %q", replay, "override|"+workDir)
	}
	if err := mgr.StreamProcessOutput(ctx, &ateworkerpb.StreamProcessOutputRequest{
		ProcessId:    p.GetProcessId(),
		StdoutOffset: -1,
	}, func(*ateworkerpb.ProcessOutput) error { return nil }); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative offset err = %v, want InvalidArgument", err)
	}

	// 2. WriteProcessInput (stdin + close) + rejection when stdin=false.
	if _, err := mgr.WriteProcessInput(ctx, p.GetProcessId(), []byte("x"), false); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stdin=false err = %v, want FailedPrecondition", err)
	}
	catProc, err := mgr.StartProcess(ctx, &ateworkerpb.StartProcessRequest{
		Command: []string{"cat"},
		Stdin:   true,
	})
	if err != nil {
		t.Fatalf("StartProcess cat: %v", err)
	}
	if n, err := mgr.WriteProcessInput(ctx, catProc.GetProcessId(), []byte("hello-stdin"), true); err != nil || n != 11 {
		t.Fatalf("WriteProcessInput: n=%d err=%v", n, err)
	}
	var catOut []byte
	if err := mgr.StreamProcessOutput(ctx, &ateworkerpb.StreamProcessOutputRequest{
		ProcessId: catProc.GetProcessId(),
		Follow:    true,
	}, func(o *ateworkerpb.ProcessOutput) error {
		catOut = append(catOut, o.GetStdout()...)
		return nil
	}); err != nil {
		t.Fatalf("StreamProcessOutput cat: %v", err)
	}
	if string(catOut) != "hello-stdin" {
		t.Fatalf("catOut = %q, want hello-stdin", catOut)
	}

	// 3. Timeout kills long-running process with SIGKILL.
	timed, err := mgr.StartProcess(ctx, &ateworkerpb.StartProcessRequest{
		Command: []string{"sleep", "30"},
		Timeout: durationpb.New(80 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("StartProcess timed: %v", err)
	}
	var timedExit *ateworkerpb.Process
	if err := mgr.StreamProcessOutput(ctx, &ateworkerpb.StreamProcessOutputRequest{
		ProcessId: timed.GetProcessId(),
		Follow:    true,
	}, func(o *ateworkerpb.ProcessOutput) error {
		if e := o.GetExit(); e != nil {
			timedExit = e
		}
		return nil
	}); err != nil {
		t.Fatalf("StreamProcessOutput timed: %v", err)
	}
	if timedExit == nil || timedExit.GetExitCode() != int32(128+syscall.SIGKILL) {
		t.Fatalf("timedExit = %+v, want exit_code=%d", timedExit, 128+syscall.SIGKILL)
	}

	// 4. FileSystemService: WriteFile (truncate + seek_offset) & ReadFile (binary + create-if-missing + NotFound).
	target := filepath.Join(workDir, "sub", "data.bin")
	if _, err := mgr.WriteFile(ctx, "app", target, 0o600, 0, bytes.NewReader([]byte("hello\x00world"))); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// Patch bytes at seek_offset=6 without truncating.
	if _, err := mgr.WriteFile(ctx, "app", target, 0o600, 6, bytes.NewReader([]byte("KATA!"))); err != nil {
		t.Fatalf("WriteFile seek: %v", err)
	}
	var gotFile []byte
	if err := mgr.ReadFile(ctx, &ateworkerpb.ReadFileRequest{
		ContainerName: "app",
		Path:          target,
	}, func(chunk []byte) error {
		gotFile = append(gotFile, chunk...)
		return nil
	}); err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(gotFile, []byte("hello\x00KATA!")) {
		t.Fatalf("ReadFile = %q, want %q", gotFile, "hello\x00KATA!")
	}

	// Missing file with mode == 0 -> NotFound.
	missing := filepath.Join(workDir, "missing.txt")
	if err := mgr.ReadFile(ctx, &ateworkerpb.ReadFileRequest{
		ContainerName: "app",
		Path:          missing,
	}, func([]byte) error { return nil }); status.Code(err) != codes.NotFound {
		t.Fatalf("ReadFile missing err = %v, want NotFound", err)
	}
	// Missing file with mode != 0 -> creates empty file and returns empty stream.
	var emptyChunks [][]byte
	if err := mgr.ReadFile(ctx, &ateworkerpb.ReadFileRequest{
		ContainerName: "app",
		Path:          missing,
		Mode:          0o644,
	}, func(c []byte) error {
		emptyChunks = append(emptyChunks, slices.Clone(c))
		return nil
	}); err != nil {
		t.Fatalf("ReadFile create-if-missing: %v", err)
	}
	if len(emptyChunks) != 0 {
		t.Fatalf("expected 0 chunks for newly created file, got %d", len(emptyChunks))
	}
	if _, err := os.Stat(missing); err != nil {
		t.Fatalf("expected %q to be created: %v", missing, err)
	}
}
