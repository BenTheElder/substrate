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
	"errors"

	"golang.org/x/sys/unix"
)

// maxKernelCopy caps a single copy_file_range request. The syscall takes an int
// length, and a bounded request keeps one call from monopolising the thread.
const maxKernelCopy = 1 << 30

// kernelCopyRange copies up to length bytes at off from srcFd to dstFd without the
// data crossing into userspace, and reports how much it copied (short copies are
// normal, so callers must loop).
//
// It reports errKernelCopyUnsupported when the kernel or filesystem cannot do the
// copy — most commonly EXDEV, when source and destination are on different
// filesystems — so the caller can fall back to a userspace copy.
func kernelCopyRange(srcFd, dstFd int, off, length int64) (int64, error) {
	if length > maxKernelCopy {
		length = maxKernelCopy
	}
	roff, woff := off, off
	n, err := unix.CopyFileRange(srcFd, &roff, dstFd, &woff, int(length), 0)
	if err != nil {
		switch {
		case errors.Is(err, unix.ENOSYS), // pre-4.5 kernel, or blocked by seccomp
			errors.Is(err, unix.EXDEV),      // different filesystems
			errors.Is(err, unix.EOPNOTSUPP), // filesystem does not implement it
			errors.Is(err, unix.EPERM),      // e.g. append-only destination
			errors.Is(err, unix.EINVAL),     // ranges or flags this kernel rejects
			errors.Is(err, unix.EBADF):      // not both regular files
			return 0, errKernelCopyUnsupported
		}
		return 0, err
	}
	if n == 0 {
		// No error and no progress: treat as unsupported rather than spin.
		return 0, errKernelCopyUnsupported
	}
	return int64(n), nil
}
