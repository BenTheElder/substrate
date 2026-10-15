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

package workerstats

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

func TestPendingSample(t *testing.T) {
	active := &resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: "atespace-a", Name: "actor-b"},
		UID:              "uid-c",
		TemplateAtespace: "template-ns-d",
		TemplateName:     "template-name-e",
	}
	before := time.Now().UnixNano()
	got := PendingSample(active, "microvm")
	after := time.Now().UnixNano()

	if ts := got.GetObservedAtUnixNano(); ts < before || ts > after {
		t.Errorf("ObservedAtUnixNano = %d, want within [%d, %d]", ts, before, after)
	}
	want := &ateworkerpb.WorkloadStatsSample{
		Atespace:              "atespace-a",
		ActorName:             "actor-b",
		ActorUid:              "uid-c",
		ActorTemplateAtespace: "template-ns-d",
		ActorTemplateName:     "template-name-e",
		SandboxClass:          "microvm",
	}
	if diff := cmp.Diff(want, got, protocmp.Transform(), protocmp.IgnoreFields(&ateworkerpb.WorkloadStatsSample{}, "observed_at_unix_nano")); diff != "" {
		t.Errorf("PendingSample() mismatch (-want +got):\n%s", diff)
	}
}
