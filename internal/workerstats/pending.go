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
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// PendingSample reports a workload we have no measurements for yet
// Source is left unspecified, which the sample contract defines as "not measured"
func PendingSample(active *resources.ActorAttribution, sandboxClass string) *ateworkerpb.WorkloadStatsSample {
	return &ateworkerpb.WorkloadStatsSample{
		Atespace:              active.Ref.Atespace,
		ActorName:             active.Ref.Name,
		ActorUid:              active.UID,
		ActorTemplateAtespace: active.TemplateAtespace,
		ActorTemplateName:     active.TemplateName,
		SandboxClass:          sandboxClass,
		ObservedAtUnixNano:    time.Now().UnixNano(),
	}
}
