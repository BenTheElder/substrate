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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const testGuestBase = 1 << 30 // where CH places guest RAM, as in a real snapshot

// writeTestSnapshot builds a snapshot directory whose memory file has data only at
// the given guest offsets, leaving the rest as holes.
func writeTestSnapshot(t *testing.T, dir string, table []memRange, data map[int64][]byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	var total int64
	for _, r := range table {
		total += r.Length
	}
	f, err := os.OpenFile(filepath.Join(dir, snapshotMemoryFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Size the file first so unwritten regions become holes.
	if err := f.Truncate(total); err != nil {
		t.Fatal(err)
	}
	for off, b := range data {
		if _, err := f.WriteAt(b, off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}

	inner, err := json.Marshal(map[string]any{
		"memory_ranges": map[string]any{"data": rangesToJSON(table)},
		// A field wide enough to be mangled by a float64 round trip.
		"start_of_device_area": json.Number("18446744073709551615"),
	})
	if err != nil {
		t.Fatal(err)
	}
	state := map[string]any{
		"snapshots": map[string]any{
			"memory-manager": map[string]any{
				"snapshots":     map[string]any{},
				"snapshot_data": map[string]any{"state": string(inner)},
			},
		},
	}
	if err := writeJSONFile(filepath.Join(dir, snapshotStateFile), state); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"unrelated":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
}

func rangesToJSON(table []memRange) []any {
	out := make([]any, 0, len(table))
	for _, r := range table {
		out = append(out, map[string]any{
			"gpa":    json.Number(fmt.Sprint(r.GPA)),
			"length": json.Number(fmt.Sprint(r.Length)),
		})
	}
	return out
}

// reconstruct rebuilds guest memory the way cloud-hypervisor does on restore: walk
// the table, read each range's bytes from the running file offset, and place them at
// the range's guest address. Everything not covered by the table reads as zeroes.
func reconstruct(t *testing.T, dir string, guestSize int64) []byte {
	t.Helper()
	state, err := readJSONFile(filepath.Join(dir, snapshotStateFile))
	if err != nil {
		t.Fatal(err)
	}
	mem, err := memoryManagerState(state)
	if err != nil {
		t.Fatal(err)
	}
	table, err := memoryRanges(mem)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, snapshotMemoryFile))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	out := make([]byte, guestSize)
	var cursor int64
	for _, r := range table {
		buf := make([]byte, r.Length)
		if _, err := f.ReadAt(buf, cursor); err != nil {
			t.Fatalf("reading range gpa=%#x len=%d at %d: %v", r.GPA, r.Length, cursor, err)
		}
		copy(out[r.GPA-testGuestBase:], buf)
		cursor += r.Length
	}
	return out
}

func TestWritePackedSnapshotPreservesGuestMemory(t *testing.T) {
	const guestSize = 64 << 20
	src := filepath.Join(t.TempDir(), "src")
	dst := filepath.Join(t.TempDir(), "dst")

	// Data at the very start, somewhere in the middle, and at the very end, so the
	// packing has to handle both edges of the range.
	table := []memRange{{GPA: testGuestBase, Length: guestSize}}
	data := map[int64][]byte{
		0:                     bytes.Repeat([]byte{0xAA}, 8<<10),
		20 << 20:              bytes.Repeat([]byte{0xBB}, 4<<10),
		guestSize - (4 << 10): bytes.Repeat([]byte{0xCC}, 4<<10),
	}
	writeTestSnapshot(t, src, table, data)

	want := reconstruct(t, src, guestSize)

	stats, err := WritePackedSnapshot(src, dst, DefaultPackOptions)
	if err != nil {
		t.Fatalf("WritePackedSnapshot: %v", err)
	}
	got := reconstruct(t, dst, guestSize)

	if !bytes.Equal(want, got) {
		t.Errorf("packed snapshot reconstructs different guest memory (%s)", stats)
		for i := range want {
			if want[i] != got[i] {
				t.Fatalf("first difference at guest offset %#x: want %#x, got %#x", i, want[i], got[i])
			}
		}
	}

	if stats.DstBytes >= stats.SrcBytes {
		// Not a hard failure: a filesystem that cannot report holes legitimately
		// falls back to the original layout.
		t.Logf("no narrowing achieved (%s); does %s support SEEK_HOLE?", stats, os.TempDir())
	} else {
		t.Logf("narrowed: %s", stats)
	}
}

func TestWritePackedSnapshotKeepsWideFields(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	dst := filepath.Join(t.TempDir(), "dst")
	writeTestSnapshot(t, src, []memRange{{GPA: testGuestBase, Length: 8 << 20}},
		map[int64][]byte{0: bytes.Repeat([]byte{1}, 4<<10)})

	if _, err := WritePackedSnapshot(src, dst, DefaultPackOptions); err != nil {
		t.Fatal(err)
	}

	state, err := readJSONFile(filepath.Join(dst, snapshotStateFile))
	if err != nil {
		t.Fatal(err)
	}
	mem, err := memoryManagerState(state)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(mem["start_of_device_area"]); got != "18446744073709551615" {
		t.Errorf("wide field corrupted by the round trip: got %s", got)
	}
	if _, err := os.Stat(filepath.Join(dst, "config.json")); err != nil {
		t.Errorf("config.json not copied: %v", err)
	}
}

func TestCoalesceOnlyMergesContiguousSource(t *testing.T) {
	// Two extents adjacent in guest address space but from different source ranges
	// (their source offsets do not advance in lockstep) must not be merged.
	in := []extent{
		{gpa: testGuestBase, length: 4 << 10, srcOff: 0},
		{gpa: testGuestBase + (4 << 10), length: 4 << 10, srcOff: 1 << 20},
	}
	if got := coalesce(in, 1<<20); len(got) != 2 {
		t.Errorf("merged extents that are not contiguous in the source file: %+v", got)
	}

	// The same extents laid out contiguously in the source do merge.
	in[1].srcOff = 4 << 10
	if got := coalesce(in, 1<<20); len(got) != 1 {
		t.Errorf("failed to merge contiguous extents: %+v", got)
	}
}
