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
	"context"
	"testing"
	"time"
)

// TestWaitForInFlightDeadline pins that shutdown gives up rather than hanging
// when an RPC will not finish.
func TestWaitForInFlightDeadline(t *testing.T) {
	s := &AteomService{}
	s.inFlight.Add(1)
	defer s.inFlight.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if s.waitForInFlight(ctx) {
		t.Error("waitForInFlight() = true with an RPC still in flight, want false")
	}

	// And returns promptly once nothing is running.
	drained := &AteomService{}
	if !drained.waitForInFlight(context.Background()) {
		t.Error("waitForInFlight() = false with nothing in flight, want true")
	}
}
