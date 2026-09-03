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

package ateomcapacity

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cpu        string
		memory     string
		wantCPU    int64
		wantMemory int64
	}{
		{name: "limits set", cpu: "2000", memory: "4294967296", wantCPU: 2000, wantMemory: 4294967296},
		{name: "unparseable is none", cpu: "2Gi", memory: "", wantCPU: 0, wantMemory: 0},
		{name: "negative is none", cpu: "-1", memory: "-1", wantCPU: 0, wantMemory: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(CPULimitEnv, tc.cpu)
			t.Setenv(MemoryLimitEnv, tc.memory)

			got := FromEnv()
			if got.GetActors() != actorsPerAteom {
				t.Errorf("actors = %d, want %d", got.GetActors(), actorsPerAteom)
			}
			if got.GetCpuMilli() != tc.wantCPU {
				t.Errorf("cpu_milli = %d, want %d", got.GetCpuMilli(), tc.wantCPU)
			}
			if got.GetMemoryBytes() != tc.wantMemory {
				t.Errorf("memory_bytes = %d, want %d", got.GetMemoryBytes(), tc.wantMemory)
			}
		})
	}
}

func TestFromEnvUnset(t *testing.T) {
	// t.Setenv first so the originals are restored for other tests.
	t.Setenv(CPULimitEnv, "")
	t.Setenv(MemoryLimitEnv, "")
	os.Unsetenv(CPULimitEnv)
	os.Unsetenv(MemoryLimitEnv)

	got := FromEnv()
	if got.GetCpuMilli() != 0 || got.GetMemoryBytes() != 0 {
		t.Errorf("unset environment reported %v, want no compute", got)
	}
	if got.GetActors() != actorsPerAteom {
		t.Errorf("actors = %d, want %d", got.GetActors(), actorsPerAteom)
	}
}

// reportSeam swaps the one-shot call out so the retry loop can be exercised
// without a socket or certificates.
func TestReportRetriesUntilAccepted(t *testing.T) {
	attempts := 0
	send := func() error {
		attempts++
		if attempts < 3 {
			return errors.New("worker record does not exist yet")
		}
		return nil
	}
	if err := retryReport(context.Background(), send, time.Millisecond); err != nil {
		t.Fatalf("retryReport() failed: %v", err)
	}
	if attempts != 3 {
		t.Errorf("gave up after %d attempts, want 3", attempts)
	}
}

func TestReportStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	send := func() error {
		attempts++
		if attempts == 2 {
			cancel()
		}
		return errors.New("still failing")
	}
	if err := retryReport(ctx, send, time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Errorf("retryReport() = %v, want context.Canceled", err)
	}
}
