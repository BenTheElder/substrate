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

package procman

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RunFSHelper implements the tiny in-container helper protocol for ReadFile and
// WriteFile without tar:
//
//	read  <path> <octal-mode>
//	write <path> <octal-mode> <seek-offset>
//
// Raw file bytes stream over stdout (read) or stdin (write); errors go to stderr.
func RunFSHelper(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) < 3 {
		fmt.Fprintln(stderr, "usage: fshelper read <path> <mode> | write <path> <mode> <seek_offset>")
		return 2
	}
	switch args[0] {
	case "read":
		mode, err := strconv.ParseUint(args[2], 8, 32)
		if err != nil {
			fmt.Fprintf(stderr, "invalid mode %q: %v\n", args[2], err)
			return 2
		}
		if err := helperReadFile(args[1], os.FileMode(mode), stdout); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				fmt.Fprintf(stderr, "not found: %v\n", err)
				return 3
			}
			fmt.Fprintf(stderr, "read failed: %v\n", err)
			return 1
		}
		return 0
	case "write":
		if len(args) < 4 {
			fmt.Fprintln(stderr, "usage: fshelper write <path> <mode> <seek_offset>")
			return 2
		}
		mode, err := strconv.ParseUint(args[2], 8, 32)
		if err != nil {
			fmt.Fprintf(stderr, "invalid mode %q: %v\n", args[2], err)
			return 2
		}
		offset, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil || offset < 0 {
			fmt.Fprintf(stderr, "invalid seek_offset %q\n", args[3])
			return 2
		}
		if err := helperWriteFile(args[1], os.FileMode(mode), offset, stdin); err != nil {
			fmt.Fprintf(stderr, "write failed: %v\n", err)
			return 1
		}
		return 0
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\n", args[0])
		return 2
	}
}

func helperReadFile(path string, mode os.FileMode, stdout io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && mode != 0 {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return err
			}
			cf, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode&0o777)
			if err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			if cf != nil {
				return cf.Close()
			}
			return nil
		}
		return err
	}
	defer f.Close()
	_, err = io.Copy(stdout, f)
	return err
}

func helperWriteFile(path string, mode os.FileMode, seekOffset int64, stdin io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	perm := mode & 0o777
	if perm == 0 {
		perm = 0o644
	}
	flags := os.O_CREATE | os.O_WRONLY
	if seekOffset == 0 {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return err
	}
	defer f.Close()
	if seekOffset > 0 {
		if _, err := f.Seek(seekOffset, io.SeekStart); err != nil {
			return err
		}
	}
	if _, err := io.Copy(f, stdin); err != nil {
		return err
	}
	return f.Close()
}

// ReadFileOverExec streams a file directly from an ExecHandle without spooling
// the whole file into memory or leaving an entry in Manager.procs. On context
// cancellation it sends SIGKILL so reads of FIFOs or /dev/zero cannot leak.
func ReadFileOverExec(ctx context.Context, h ExecHandle, emit func([]byte) error) error {
	defer h.Close()
	_ = h.CloseStdin(ctx)

	stopKill := context.AfterFunc(ctx, func() {
		sigCtx, cancel := context.WithTimeout(context.Background(), signalTimeout)
		defer cancel()
		_ = h.Signal(sigCtx, syscall.SIGKILL)
	})
	defer stopKill()

	var stderrBuf bytes.Buffer
	errDone := make(chan struct{})
	go func() {
		defer close(errDone)
		if r := h.Stderr(); r != nil {
			_, _ = io.Copy(&stderrBuf, io.LimitReader(r, 8192))
		}
	}()

	streamErr := StreamReaderToEmit(ctx, h.Stdout(), DefaultMaxChunkBytes, emit)
	<-errDone
	exitCode, waitErr := h.Wait(ctx)
	if streamErr != nil {
		return streamErr
	}
	if waitErr != nil {
		return waitErr
	}
	if exitCode == 3 {
		return status.Errorf(codes.NotFound, "%s", bytes.TrimSpace(stderrBuf.Bytes()))
	}
	if exitCode != 0 {
		return status.Errorf(codes.Internal, "read helper exited %d: %s", exitCode, bytes.TrimSpace(stderrBuf.Bytes()))
	}
	return nil
}

// WriteFileOverExec streams r directly to an ExecHandle's stdin without
// buffering the whole file in memory.
func WriteFileOverExec(ctx context.Context, h ExecHandle, r io.Reader) (int64, error) {
	defer h.Close()

	stopKill := context.AfterFunc(ctx, func() {
		sigCtx, cancel := context.WithTimeout(context.Background(), signalTimeout)
		defer cancel()
		_ = h.Signal(sigCtx, syscall.SIGKILL)
	})
	defer stopKill()

	var stderrBuf bytes.Buffer
	errDone := make(chan struct{})
	go func() {
		defer close(errDone)
		if er := h.Stderr(); er != nil {
			_, _ = io.Copy(&stderrBuf, io.LimitReader(er, 8192))
		}
	}()

	var total int64
	buf := make([]byte, DefaultMaxChunkBytes)
	var writeErr error
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			for len(chunk) > 0 {
				w, werr := h.WriteStdin(ctx, chunk)
				if w < 0 || w > len(chunk) {
					writeErr = fmt.Errorf("invalid WriteStdin byte count %d", w)
					break
				}
				total += int64(w)
				chunk = chunk[w:]
				if werr != nil {
					writeErr = werr
					break
				}
				if w == 0 {
					writeErr = io.ErrShortWrite
					break
				}
			}
			if writeErr != nil {
				break
			}
		}
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			writeErr = rerr
			break
		}
	}
	if cerr := h.CloseStdin(ctx); writeErr == nil && cerr != nil {
		writeErr = cerr
	}
	<-errDone
	exitCode, waitErr := h.Wait(ctx)
	if writeErr != nil {
		return total, writeErr
	}
	if waitErr != nil {
		return total, waitErr
	}
	if exitCode != 0 {
		return total, status.Errorf(codes.Internal, "write helper exited %d: %s", exitCode, bytes.TrimSpace(stderrBuf.Bytes()))
	}
	return total, nil
}
