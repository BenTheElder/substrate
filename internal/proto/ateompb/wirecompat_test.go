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

package ateompb

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// atelet and the ateoms it polls are separate images in separate pods, so a
// rollout routinely has one talking to the other version. GetActiveWorkloadStats
// answers on field 1 either way: it held a single WorkloadStatsSample before an
// ateom could execute several, and singular and repeated of the same message
// type are wire-compatible.
//
// Built by hand rather than from the old generated type, which no longer
// exists: field 1, wire type 2, carrying a marshaled WorkloadStatsSample is
// exactly what an ateom that predates the set puts on the wire.
func oldSingleSampleResponse(t *testing.T, sample *WorkloadStatsSample) []byte {
	t.Helper()
	body, err := proto.Marshal(sample)
	if err != nil {
		t.Fatalf("marshaling sample: %v", err)
	}
	return protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), body)
}

func TestActiveWorkloadStatsReadsAnOlderAteomsSingleSample(t *testing.T) {
	sample := &WorkloadStatsSample{ActorUid: "actor-1"}

	var got GetActiveWorkloadStatsResponse
	if err := proto.Unmarshal(oldSingleSampleResponse(t, sample), &got); err != nil {
		t.Fatalf("unmarshaling an older ateom's response: %v", err)
	}

	if n := len(got.GetSamples()); n != 1 {
		t.Fatalf("samples = %d, want the single sample to arrive as a set of one", n)
	}
	if uid := got.GetSamples()[0].GetActorUid(); uid != "actor-1" {
		t.Errorf("samples[0].actor_uid = %q, want %q", uid, "actor-1")
	}
	// Silence is the failure this guards against: no sample AND no reason is a
	// state the rpc says cannot happen, and is what a reserved field 1 produced.
	if got.GetNoSampleReason() != NoSampleReason_NO_SAMPLE_REASON_UNSPECIFIED {
		t.Errorf("no_sample_reason = %v, want it unset when a sample arrived", got.GetNoSampleReason())
	}
}

// And the other direction: an older atelet reading a new ateom takes the first
// sample off field 1, because that is where it still is.
func TestOlderAteletReadsTheFirstSampleOfASet(t *testing.T) {
	body, err := proto.Marshal(&GetActiveWorkloadStatsResponse{
		Samples: []*WorkloadStatsSample{{ActorUid: "actor-1"}, {ActorUid: "actor-2"}},
	})
	if err != nil {
		t.Fatalf("marshaling: %v", err)
	}

	num, typ, n := protowire.ConsumeTag(body)
	if n < 0 {
		t.Fatalf("consuming tag: %v", protowire.ParseError(n))
	}
	if num != 1 || typ != protowire.BytesType {
		t.Fatalf("first field = %d (type %d), want field 1 as bytes, where the singular sample was", num, typ)
	}
	payload, n := protowire.ConsumeBytes(body[n:])
	if n < 0 {
		t.Fatalf("consuming bytes: %v", protowire.ParseError(n))
	}
	var first WorkloadStatsSample
	if err := proto.Unmarshal(payload, &first); err != nil {
		t.Fatalf("unmarshaling the first sample: %v", err)
	}
	if first.GetActorUid() != "actor-1" {
		t.Errorf("first sample actor_uid = %q, want %q", first.GetActorUid(), "actor-1")
	}
}
