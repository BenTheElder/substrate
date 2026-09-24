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

package childreap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// gonePoll is how often Watch.WaitGone checks back while it waits.
const gonePoll = 5 * time.Millisecond

// Watch follows one process through a pidfd, so a later process given the
// same PID cannot be mistaken for it.
type Watch struct {
	pid int
	fd  int
}

// WatchPID starts following pid. Close the Watch when done.
func WatchPID(pid int) (*Watch, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("while opening a pidfd for %d: %w", pid, err)
	}
	return &Watch{pid: pid, fd: fd}, nil
}

// Close releases the pidfd.
func (w *Watch) Close() error {
	return unix.Close(w.fd)
}

// WaitGone waits until the process has exited and its PID no longer exists.
// A child of this process is reaped here; any other is left to its parent.
func (w *Watch) WaitGone(ctx context.Context) error {
	// The pidfd becomes readable once the process exits.
	for {
		fds := []unix.PollFd{{Fd: int32(w.fd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, int(gonePoll/time.Millisecond))
		if err != nil && !errors.Is(err, unix.EINTR) {
			return fmt.Errorf("while waiting for %d to exit: %w", w.pid, err)
		}
		if n > 0 {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	// ECHILD: it is not a child, or a Reaper got to it first.
	var info unix.Siginfo
	if err := unix.Waitid(unix.P_PIDFD, w.fd, &info, unix.WEXITED|unix.WNOHANG, nil); err != nil && !errors.Is(err, unix.ECHILD) {
		return fmt.Errorf("while reaping %d: %w", w.pid, err)
	}
	// Any other parent reaps it on its own schedule. A PID reused straight
	// after would only make this wait out ctx.
	for {
		if err := unix.Kill(w.pid, 0); errors.Is(err, unix.ESRCH) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(gonePoll):
		}
	}
}
