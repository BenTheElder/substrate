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
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ateworker/procman"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestMain lets the test binary emulate both `runsc` (exec/kill/read) and the
// --exec-fd fshelper binary without external build steps.
func TestMain(m *testing.M) {
	if len(os.Args) >= 2 && os.Args[1] == "--fshelper" {
		os.Exit(procman.RunFSHelper(os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	}
	if os.Getenv("FAKE_RUNSC_MODE") == "1" {
		os.Exit(runFakeRunsc(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// runFakeRunsc implements the subset of runsc subcommands used by gvisorBackend:
//
//	-root <dir> exec --internal-pid-file <f> [--cwd <d>] [--env k=v] [--exec-fd 3] <cid> <cmd...>
//	-root <dir> kill --pgid <pgid> <cid> <sig>
//	-root <dir> read <cid> <path>
func runFakeRunsc(args []string) int {
	// Skip global flags like -root <dir>.
	i := 0
	for i < len(args) && strings.HasPrefix(args[i], "-") {
		if args[i] == "-root" && i+1 < len(args) {
			i += 2
			continue
		}
		i++
	}
	if i >= len(args) {
		fmt.Fprintln(os.Stderr, "missing subcommand")
		return 2
	}
	sub := args[i]
	rest := args[i+1:]

	switch sub {
	case "read":
		if len(rest) != 2 {
			fmt.Fprintln(os.Stderr, "usage: read <cid> <path>")
			return 2
		}
		f, err := os.Open(rest[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer f.Close()
		if _, err := io.Copy(os.Stdout, f); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0

	case "kill":
		var pgid int
		j := 0
		for j < len(rest) && strings.HasPrefix(rest[j], "-") {
			if rest[j] == "--pgid" && j+1 < len(rest) {
				v, err := strconv.Atoi(rest[j+1])
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return 2
				}
				pgid = v
				j += 2
				continue
			}
			j++
		}
		if len(rest)-j < 2 || pgid <= 0 {
			fmt.Fprintln(os.Stderr, "usage: kill --pgid <pgid> <cid> <sig>")
			return 2
		}
		sigNum, err := strconv.Atoi(rest[j+1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if err := syscall.Kill(-pgid, syscall.Signal(sigNum)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0

	case "exec":
		var pidFile, cwd string
		var env []string
		var execFD int
		j := 0
		for j < len(rest) && strings.HasPrefix(rest[j], "-") {
			switch rest[j] {
			case "--internal-pid-file":
				pidFile = rest[j+1]
				j += 2
			case "--cwd":
				cwd = rest[j+1]
				j += 2
			case "--env":
				env = append(env, rest[j+1])
				j += 2
			case "--exec-fd":
				v, err := strconv.Atoi(rest[j+1])
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return 2
				}
				execFD = v
				j += 2
			default:
				j++
			}
		}
		// rest[j] is cid, rest[j+1:] is command.
		if len(rest)-j < 2 {
			fmt.Fprintln(os.Stderr, "usage: exec [flags] <cid> <cmd...>")
			return 2
		}
		cmdArgs := rest[j+1:]
		exePath := cmdArgs[0]
		if execFD > 0 {
			// Emulate runsc --exec-fd by executing /proc/self/fd/<fd>.
			exePath = fmt.Sprintf("/proc/self/fd/%d", execFD)
		}
		c := exec.Command(exePath, cmdArgs[1:]...)
		if cwd != "" {
			c.Dir = cwd
		}
		c.Env = append(os.Environ(), env...)
		c.Env = append(c.Env, "FAKE_RUNSC_MODE=0")
		c.Stdin = os.Stdin
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := c.Start(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if pidFile != "" {
			_ = os.WriteFile(pidFile, []byte(strconv.Itoa(c.Process.Pid)), 0o600)
		}
		err := c.Wait()
		if err == nil {
			return 0
		}
		if ee, ok := err.(*exec.ExitError); ok {
			ws := ee.Sys().(syscall.WaitStatus)
			if ws.Signaled() {
				// Re-raise the same signal on ourselves so the parent observes WaitStatus.Signaled().
				_ = syscall.Kill(os.Getpid(), ws.Signal())
				return int(128 + ws.Signal())
			}
			return ws.ExitStatus()
		}
		return 1
	}
	fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", sub)
	return 2
}

func TestGVisorWorkerGRPCExecAndFiles(t *testing.T) {
	selfExe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	// Create a tiny wrapper script that sets FAKE_RUNSC_MODE=1 and execs selfExe.
	wrapper := filepath.Join(t.TempDir(), "fake-runsc")
	script := fmt.Sprintf("#!/bin/sh\nFAKE_RUNSC_MODE=1 exec %q \"$@\"\n", selfExe)
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake-runsc: %v", err)
	}

	svc := &AteWorkerService{
		actors:    map[string]*hostedActor{},
		maxActors: 4,
	}
	flags, err := newFlagStore(t.Context(), "")
	if err != nil {
		t.Fatalf("newFlagStore: %v", err)
	}
	rcmd := &runsc{
		path:      wrapper,
		actorUID:  "actor-1",
		actorDirs: &ateworkerpb.ActorDirs{RootDir: t.TempDir()},
		flags:     flags,
	}
	backend := &gvisorBackend{
		rcmd:             rcmd,
		defaultContainer: "app",
		fsHelperHostPath: selfExe,
		fsHelperArgs:     []string{"--fshelper"},
	}
	mgr := procman.NewManager(backend, procman.Options{})
	t.Cleanup(func() { _ = mgr.Close() })

	svc.actors["actor-1"] = &hostedActor{
		attribution: resources.ActorAttribution{UID: "actor-1"},
		procs:       mgr,
	}

	sock := filepath.Join(t.TempDir(), "worker.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	ateworkerpb.RegisterWorkerServer(srv, svc)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := ateworkerpb.NewWorkerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. StartProcess reports real in-container PID + SignalProcess kills the whole process group via --pgid.
	proc, err := client.StartProcess(ctx, &ateworkerpb.StartProcessRequest{
		ActorUid: "actor-1",
		Command:  []string{"sh", "-c", "sleep 30 & wait"},
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if proc.GetPid() <= 0 {
		t.Fatalf("expected positive internal PID from --internal-pid-file, got %d", proc.GetPid())
	}
	if _, err := client.SignalProcess(ctx, &ateworkerpb.SignalProcessRequest{
		ActorUid:  "actor-1",
		ProcessId: proc.GetProcessId(),
		Signal:    ateworkerpb.Signal_SIGNAL_TERM,
	}); err != nil {
		t.Fatalf("SignalProcess: %v", err)
	}
	stream, err := client.StreamProcessOutput(ctx, &ateworkerpb.StreamProcessOutputRequest{
		ActorUid:  "actor-1",
		ProcessId: proc.GetProcessId(),
		Follow:    true,
	})
	if err != nil {
		t.Fatalf("StreamProcessOutput: %v", err)
	}
	var gotExit *ateworkerpb.Process
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("StreamProcessOutput Recv: %v", err)
		}
		if e := msg.GetExit(); e != nil {
			gotExit = e
		}
	}
	if gotExit == nil || gotExit.GetExitCode() != int32(128+syscall.SIGTERM) {
		t.Fatalf("gotExit = %+v, want exit_code=%d", gotExit, 128+syscall.SIGTERM)
	}

	// 2. WriteFile via --exec-fd (truncate + seek_offset) + native ReadFile via `runsc read`.
	target := filepath.Join(t.TempDir(), "nested", "payload.bin")
	wstream, err := client.WriteFile(ctx)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := wstream.Send(&ateworkerpb.WriteFileRequest{
		ActorUid: "actor-1",
		Path:     target,
		Mode:     0o640,
		Chunk:    []byte("abc\x00def"),
	}); err != nil {
		t.Fatalf("WriteFile Send 1: %v", err)
	}
	if err := wstream.Send(&ateworkerpb.WriteFileRequest{
		Chunk: []byte("ghi"),
	}); err != nil {
		t.Fatalf("WriteFile Send 2: %v", err)
	}
	wresp, err := wstream.CloseAndRecv()
	if err != nil || wresp.GetBytesWritten() != 10 {
		t.Fatalf("WriteFile resp=%+v err=%v", wresp, err)
	}

	// Partial write at seek_offset=4.
	wstream2, err := client.WriteFile(ctx)
	if err != nil {
		t.Fatalf("WriteFile 2: %v", err)
	}
	if err := wstream2.Send(&ateworkerpb.WriteFileRequest{
		ActorUid:   "actor-1",
		Path:       target,
		SeekOffset: 4,
		Chunk:      []byte("XYZ"),
	}); err != nil {
		t.Fatalf("WriteFile 2 Send: %v", err)
	}
	if _, err := wstream2.CloseAndRecv(); err != nil {
		t.Fatalf("WriteFile 2 CloseAndRecv: %v", err)
	}

	// Read back via native `runsc read` (mode == 0).
	rstream, err := client.ReadFile(ctx, &ateworkerpb.ReadFileRequest{
		ActorUid: "actor-1",
		Path:     target,
	})
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var got bytes.Buffer
	for {
		msg, err := rstream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadFile Recv: %v", err)
		}
		got.Write(msg.GetChunk())
	}
	if !bytes.Equal(got.Bytes(), []byte("abc\x00XYZghi")) {
		t.Fatalf("ReadFile = %q, want %q", got.Bytes(), "abc\x00XYZghi")
	}

	// Missing file -> NotFound.
	rstreamMissing, err := client.ReadFile(ctx, &ateworkerpb.ReadFileRequest{
		ActorUid: "actor-1",
		Path:     filepath.Join(t.TempDir(), "nope"),
	})
	if err != nil {
		t.Fatalf("ReadFile missing: %v", err)
	}
	if _, err := rstreamMissing.Recv(); status.Code(err) != codes.NotFound {
		t.Fatalf("ReadFile missing err = %v, want NotFound", err)
	}
}
