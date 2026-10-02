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

// Package ateomstats holds the pieces both ateom runtimes need to answer

package ateomstats

import (
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// PendingSample is a workload with no numbers to give yet, as the discovery
// read reports it: attribution and the runtime family, measurements absent --
// source stays STATS_SOURCE_UNSPECIFIED, which the sample's contract defines
// as "not measured" rather than "measured as zero".
func PendingSample(active *resources.ActorAttribution, class ateompb.SandboxClass) *ateompb.WorkloadStatsSample {
	return &ateompb.WorkloadStatsSample{
		Atespace:              active.Ref.Atespace,
		ActorName:             active.Ref.Name,
		ActorUid:              active.UID,
		ActorTemplateAtespace: active.TemplateAtespace,
		ActorTemplateName:     active.TemplateName,

		SandboxClass: class,

		ObservedAtUnixNano: time.Now().UnixNano(),
	}
}
