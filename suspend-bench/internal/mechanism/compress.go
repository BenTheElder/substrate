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

package mechanism

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// memoryRangesFile is cloud-hypervisor's guest-RAM dump inside a snapshot dir.
// It dominates snapshot size, so it is the only file we (de)compress.
const memoryRangesFile = "memory-ranges"

// compressMemoryRanges compresses <dir>/memory-ranges in place using the given
// algorithm ("zstd"/"lz4"), removing the original. "none"/"" is a no-op.
// Returns the compressed size (or the untouched size for "none").
func compressMemoryRanges(ctx context.Context, dir, algo string) (int64, error) {
	src := filepath.Join(dir, memoryRangesFile)
	if algo == "" || algo == "none" {
		if st, err := os.Stat(src); err == nil {
			return st.Size(), nil
		}
		return -1, nil
	}
	ext, err := algoExt(algo)
	if err != nil {
		return -1, err
	}
	// `zstd -q --rm -o out in` / `lz4 -q --rm in out`; use stdin/stdout to keep
	// the invocation uniform and let the tool stream.
	dst := src + ext
	out, err := os.Create(dst)
	if err != nil {
		return -1, err
	}
	defer out.Close()
	in, err := os.Open(src)
	if err != nil {
		return -1, err
	}
	defer in.Close()
	cmd := exec.CommandContext(ctx, algo, "-q", "-c")
	cmd.Stdin = in
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return -1, fmt.Errorf("compress %s: %w", algo, err)
	}
	if err := os.Remove(src); err != nil {
		return -1, err
	}
	st, err := os.Stat(dst)
	if err != nil {
		return -1, err
	}
	return st.Size(), nil
}

// decompressMemoryRanges reverses compressMemoryRanges so the dir holds a raw
// memory-ranges that cloud-hypervisor can restore from.
func decompressMemoryRanges(ctx context.Context, dir, algo string) error {
	if algo == "" || algo == "none" {
		return nil
	}
	ext, err := algoExt(algo)
	if err != nil {
		return err
	}
	src := filepath.Join(dir, memoryRangesFile+ext)
	dst := filepath.Join(dir, memoryRangesFile)
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	cmd := exec.CommandContext(ctx, algo, "-q", "-d", "-c")
	cmd.Stdin = in
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("decompress %s: %w", algo, err)
	}
	return os.Remove(src)
}

func algoExt(algo string) (string, error) {
	switch algo {
	case "zstd":
		return ".zst", nil
	case "lz4":
		return ".lz4", nil
	default:
		return "", fmt.Errorf("unknown compression %q", algo)
	}
}
