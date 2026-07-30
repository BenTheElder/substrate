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

package ch

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// PackOptions tunes how populated extents are grouped into memory ranges.
type PackOptions struct {
	// CoalesceGap merges extents separated by at most this many bytes of hole.
	// Larger values trade a little needless zero-fill for fewer ranges.
	CoalesceGap int64
	// Align rounds every extent outward to this boundary. It must be a power of
	// two and at least the restoring host's page size: cloud-hypervisor rejects a
	// range table whose gpa or length is not page-aligned, and the restoring host
	// may use a larger page size than the host that wrote the snapshot.
	Align int64
}

// DefaultPackOptions coalesces across holes up to 1MiB and aligns to 2MiB, which
// on a typical actor collapses ~40 extents into a handful of ranges while wasting
// only a few MiB of zero-fill, and is aligned for any page size up to 2MiB.
var DefaultPackOptions = PackOptions{CoalesceGap: 1 << 20, Align: 2 << 20}

// PackStats reports what WritePackedSnapshot did.
type PackStats struct {
	SrcRanges, DstRanges int
	SrcBytes, DstBytes   int64
}

func (s PackStats) String() string {
	return fmt.Sprintf("ranges %d->%d, declared memory %dMiB->%dMiB",
		s.SrcRanges, s.DstRanges, s.SrcBytes>>20, s.DstBytes>>20)
}

// WritePackedSnapshot copies the cloud-hypervisor snapshot in srcDir to dstDir with
// its memory range table narrowed to the regions that actually hold data.
//
// A snapshot's table normally declares guest RAM as one range spanning everything,
// with the unwritten parts left as holes in the memory-ranges file. On an OnDemand
// restore cloud-hypervisor registers userfaultfd over every declared range and
// faults each page in, so the holes cost a page of zeroes each: a 2GiB guest holding
// ~160MiB of data materializes 2GiB of resident memory. Narrowing the table to the
// populated extents leaves the holes unregistered, so the guest gets them from the
// kernel as ordinary zero-filled shmem pages, on touch, and only if touched.
//
// Reading a hole and reading an unregistered page both yield zeroes, so the guest
// sees identical memory either way.
//
// The memory-ranges file is repacked because cloud-hypervisor locates a range's
// contents by the running sum of the preceding ranges' lengths, not by guest
// address. dstDir gets its own copy of every other snapshot file; srcDir is left
// untouched, so it remains usable as a merge base whose layout matches the deltas
// cloud-hypervisor writes.
func WritePackedSnapshot(srcDir, dstDir string, opt PackOptions) (PackStats, error) {
	var stats PackStats
	if opt.Align <= 0 || opt.Align&(opt.Align-1) != 0 {
		return stats, fmt.Errorf("WritePackedSnapshot: Align must be a power of two, got %d", opt.Align)
	}

	state, err := readJSONFile(filepath.Join(srcDir, snapshotStateFile))
	if err != nil {
		return stats, err
	}
	mem, err := memoryManagerState(state)
	if err != nil {
		return stats, err
	}
	srcRanges, err := memoryRanges(mem)
	if err != nil {
		return stats, err
	}

	memPath := filepath.Join(srcDir, snapshotMemoryFile)
	src, err := os.Open(memPath)
	if err != nil {
		return stats, fmt.Errorf("open %q: %w", memPath, err)
	}
	defer src.Close()

	packed, err := populatedExtents(src, srcRanges, opt)
	if err != nil {
		return stats, err
	}
	if len(packed) == 0 {
		// Nothing is populated, or the filesystem cannot report holes. Fall back to
		// the original layout rather than handing cloud-hypervisor an empty table.
		packed = wholeExtents(srcRanges)
	}

	if err := os.MkdirAll(dstDir, 0o700); err != nil {
		return stats, err
	}
	if err := writePackedMemory(src, packed, filepath.Join(dstDir, snapshotMemoryFile)); err != nil {
		return stats, err
	}
	if err := setMemoryRanges(state, mem, packed); err != nil {
		return stats, err
	}
	if err := writeJSONFile(filepath.Join(dstDir, snapshotStateFile), state); err != nil {
		return stats, err
	}
	if err := copyOtherSnapshotFiles(srcDir, dstDir); err != nil {
		return stats, err
	}

	stats.SrcRanges, stats.DstRanges = len(srcRanges), len(packed)
	for _, r := range srcRanges {
		stats.SrcBytes += r.Length
	}
	for _, e := range packed {
		stats.DstBytes += e.length
	}
	return stats, nil
}

const (
	snapshotStateFile  = "state.json"
	snapshotMemoryFile = "memory-ranges"
)

// memRange mirrors cloud-hypervisor's MemoryRange.
type memRange struct {
	GPA    int64
	Length int64
}

// extent is a populated span of guest memory together with where its bytes live in
// the source memory file. Tracking the source offset explicitly keeps the packing
// correct for a table of more than one range, where guest address and file offset
// are not related by a single constant.
type extent struct {
	gpa    int64
	length int64
	srcOff int64
}

func wholeExtents(table []memRange) []extent {
	out := make([]extent, 0, len(table))
	var cursor int64
	for _, r := range table {
		out = append(out, extent{gpa: r.GPA, length: r.Length, srcOff: cursor})
		cursor += r.Length
	}
	return out
}

// populatedExtents finds the data extents of the memory file and maps them back
// onto guest addresses, then aligns and coalesces them per opt.
func populatedExtents(f *os.File, table []memRange, opt PackOptions) ([]extent, error) {
	var out []extent
	var cursor int64 // running file offset of the current range, as CH lays them out
	for _, r := range table {
		end := cursor + r.Length
		for off := cursor; off < end; {
			dataOff, err := unix.Seek(int(f.Fd()), off, unix.SEEK_DATA)
			if err != nil {
				// ENXIO means no data remains in the file. Any other error means the
				// filesystem cannot report holes, so give up on narrowing entirely
				// rather than describing a subset of a range we cannot inspect.
				if err == unix.ENXIO {
					break
				}
				return nil, fmt.Errorf("SEEK_DATA at %d: %w", off, err)
			}
			if dataOff >= end {
				break
			}
			holeOff, err := unix.Seek(int(f.Fd()), dataOff, unix.SEEK_HOLE)
			if err != nil {
				return nil, fmt.Errorf("SEEK_HOLE at %d: %w", dataOff, err)
			}
			if holeOff > end {
				holeOff = end
			}

			// Align outward, clamped to the enclosing range so we never describe
			// memory outside it.
			lo, hi := dataOff, holeOff
			if a := alignDown(lo-cursor, opt.Align) + cursor; a >= cursor {
				lo = a
			}
			if a := alignUp(hi-cursor, opt.Align) + cursor; a <= end {
				hi = a
			} else {
				hi = end
			}
			out = append(out, extent{gpa: r.GPA + (lo - cursor), length: hi - lo, srcOff: lo})
			off = holeOff
		}
		cursor = end
	}

	sort.Slice(out, func(i, j int) bool { return out[i].gpa < out[j].gpa })
	return coalesce(out, opt.CoalesceGap), nil
}

// coalesce merges extents that overlap or are separated by at most gap bytes.
// Extents are only merged when guest addresses and source offsets advance in
// lockstep, i.e. when both came from the same range of the original table;
// otherwise the merged span could not be copied as one contiguous read.
func coalesce(in []extent, gap int64) []extent {
	var out []extent
	for _, e := range in {
		if n := len(out); n > 0 {
			prev := &out[n-1]
			prevEnd := prev.gpa + prev.length
			contiguous := e.srcOff-prev.srcOff == e.gpa-prev.gpa
			if contiguous && e.gpa <= prevEnd+gap {
				if end := e.gpa + e.length; end > prevEnd {
					prev.length = end - prev.gpa
				}
				continue
			}
		}
		out = append(out, e)
	}
	return out
}

func alignDown(v, align int64) int64 { return v &^ (align - 1) }
func alignUp(v, align int64) int64   { return alignDown(v+align-1, align) }

// writePackedMemory writes each extent's contents back to back, the layout
// cloud-hypervisor expects when it walks the table.
func writePackedMemory(src *os.File, extents []extent, dstPath string) error {
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create %q: %w", dstPath, err)
	}
	defer dst.Close()

	for _, e := range extents {
		n, err := io.Copy(dst, io.NewSectionReader(src, e.srcOff, e.length))
		if err != nil {
			return fmt.Errorf("copying gpa=%#x len=%d: %w", e.gpa, e.length, err)
		}
		if n != e.length {
			// The source is shorter than its table claims; the restore would read
			// another extent's bytes for this one.
			return fmt.Errorf("short copy for gpa=%#x: wrote %d of %d", e.gpa, n, e.length)
		}
	}
	return dst.Close()
}

// copyOtherSnapshotFiles copies every file except the two the pack rewrites.
func copyOtherSnapshotFiles(srcDir, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == snapshotStateFile || e.Name() == snapshotMemoryFile {
			continue
		}
		b, err := os.ReadFile(filepath.Join(srcDir, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dstDir, e.Name()), b, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// --- state.json plumbing -----------------------------------------------------
//
// The memory range table is nested inside state.json as a JSON *string* holding the
// memory manager's serialized state:
//
//	snapshots["memory-manager"].snapshot_data.state
//	  = "{\"memory_ranges\":{\"data\":[{\"gpa\":...,\"length\":...}]}, ...}"
//
// Everything is decoded with json.Number so unrelated 64-bit fields survive the
// round trip that a float64 decode would corrupt.

func readJSONFile(path string) (map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("decoding %q: %w", path, err)
	}
	return m, nil
}

func writeJSONFile(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// memoryManagerState returns the decoded inner memory manager state object.
func memoryManagerState(state map[string]any) (map[string]any, error) {
	raw, err := memoryManagerStateString(state)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var inner map[string]any
	if err := dec.Decode(&inner); err != nil {
		return nil, fmt.Errorf("decoding memory-manager state: %w", err)
	}
	return inner, nil
}

func memoryManagerStateString(state map[string]any) (string, error) {
	data, err := memoryManagerSnapshotData(state)
	if err != nil {
		return "", err
	}
	raw, ok := data["state"].(string)
	if !ok {
		return "", fmt.Errorf("state.json: memory-manager snapshot_data.state is not a string")
	}
	return raw, nil
}

func memoryManagerSnapshotData(state map[string]any) (map[string]any, error) {
	snaps, ok := state["snapshots"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("state.json: no snapshots object")
	}
	mm, ok := snaps["memory-manager"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("state.json: no memory-manager snapshot")
	}
	data, ok := mm["snapshot_data"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("state.json: memory-manager has no snapshot_data")
	}
	return data, nil
}

func memoryRanges(mem map[string]any) ([]memRange, error) {
	mr, ok := mem["memory_ranges"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("memory-manager state: no memory_ranges")
	}
	list, ok := mr["data"].([]any)
	if !ok {
		return nil, fmt.Errorf("memory-manager state: memory_ranges.data is not a list")
	}
	out := make([]memRange, 0, len(list))
	for i, e := range list {
		m, ok := e.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("memory_ranges.data[%d] is not an object", i)
		}
		gpa, err := jsonInt(m["gpa"])
		if err != nil {
			return nil, fmt.Errorf("memory_ranges.data[%d].gpa: %w", i, err)
		}
		length, err := jsonInt(m["length"])
		if err != nil {
			return nil, fmt.Errorf("memory_ranges.data[%d].length: %w", i, err)
		}
		out = append(out, memRange{GPA: gpa, Length: length})
	}
	return out, nil
}

// setMemoryRanges writes extents back into the nested state string.
func setMemoryRanges(state, mem map[string]any, extents []extent) error {
	list := make([]any, 0, len(extents))
	for _, e := range extents {
		list = append(list, map[string]any{
			"gpa":    json.Number(fmt.Sprint(e.gpa)),
			"length": json.Number(fmt.Sprint(e.length)),
		})
	}
	mem["memory_ranges"] = map[string]any{"data": list}

	inner, err := json.Marshal(mem)
	if err != nil {
		return err
	}
	data, err := memoryManagerSnapshotData(state)
	if err != nil {
		return err
	}
	data["state"] = string(inner)
	return nil
}

func jsonInt(v any) (int64, error) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, fmt.Errorf("not a number: %T", v)
	}
	return n.Int64()
}
