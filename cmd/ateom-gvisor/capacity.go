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

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// GetCapacity reports how many Actors this ateom can host.
//
// It answers from the slot allocator this process actually uses, so the number
// cannot drift from what hostActor will admit: both come from the same pod-side
// plan, the prefix every actor takes an address out of. Move the prefix with
// --actor-pod-subnet and the reported ceiling follows, with nothing to
// reconfigure on the control-plane side. An ateom built to host one Actor
// answers one, which is what lets the control plane learn a worker's ceiling
// instead of asserting it.
func (s *AteomService) GetCapacity(context.Context, *ateompb.GetCapacityRequest) (*ateompb.GetCapacityResponse, error) {
	return &ateompb.GetCapacityResponse{ActorSlots: int32(s.podSidePlan.Slots())}, nil
}
