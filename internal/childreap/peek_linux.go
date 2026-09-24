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
	"unsafe"

	"golang.org/x/sys/unix"
)

// peekExited returns the PID of an exited child without reaping it, or 0 if
// no child has exited.
func peekExited() (int, error) {
	var info unix.Siginfo
	if err := unix.Waitid(unix.P_ALL, 0, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil); err != nil {
		return 0, err
	}
	return siginfoPID(&info), nil
}

// siginfoPID reads si_pid, which x/sys/unix does not expose. It follows
// si_signo, si_errno and si_code, padded to pointer alignment.
func siginfoPID(info *unix.Siginfo) int {
	offset := 3 * unsafe.Sizeof(int32(0))
	if align := unsafe.Alignof(uintptr(0)); offset%align != 0 {
		offset += align - offset%align
	}
	return int(*(*int32)(unsafe.Add(unsafe.Pointer(info), offset)))
}
