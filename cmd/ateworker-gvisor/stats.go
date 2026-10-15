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
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateworker-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateworker"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// defaultCgroupRoot is the worker pod's own cgroup scope. The worker runs in a
// private cgroup namespace, so this path is the pod's cgroup rather than the
// host root, and runsc's per-container leaves are its direct children (see
// cgroup.Delegate).
const defaultCgroupRoot = "/sys/fs/cgroup"

// sandboxCgroupContainer is the container whose cgroup leaf accounts for the
// sandbox as a whole.
//
// gVisor runs every container of a sandbox as threads inside one host process,
// the sentry. runsc starts that process from the root container's create and
// from inside that container's cgroup — container.createRoot wraps the sandbox
// and gofer spawn in cgroup.RunInCgroup — so the sentry lands in the leaf of
// the pause container, the first one RunWorkload and RestoreWorkload create.
//
// The leaf is a direct child of the delegated scope rather than of ateworker's own
// cgroup, because runsc resolves cgroupsPath against the parent of the cgroup
// it is running in: cgroup v2 forbids a cgroup from holding processes and
// delegating controllers to children at once, so runsc walks up one level to
// find a directory it is allowed to create in. cgroup.Delegate moves
// ateworker into /sys/fs/cgroup/ateworker precisely so that one level up is the
// delegated scope.
//
// The actor's own containers get leaves too, since cmdCreate shapes a
// cgroupsPath into each of their specs, but those leaves stay empty by
// design. gVisor's setupCgroupForSubcontainer creates them with empty resources
// and explains why: "Since subcontainers run exclusively inside the sandbox,
// subcontainer cgroups on the host have no effect on them. However, some tools
// (e.g. cAdvisor) uses cgroups paths to discover new containers and report
// stats for them." They are discovery markers, not accounting.
//
// So the actor's memory and CPU are the sentry's and are charged here. That is
// why a sample is attributed to the actor rather than to a container, and why
// this reads one leaf instead of summing them — the others have nothing in them
// to sum.
//
// What the leaf holds besides the actor's own work: the sentry's own overhead
// (its Go heap, page tables, netstack) and the gofers. Process listings taken
// on a live node in #161 put runsc-sandbox and both gofers — the pause
// container's and the actor container's — in the pause cgroup. Those runs
// predate #496, so they establish the leaf name and the fact that everything
// lands in one leaf, not the absolute path, which #496's delegation moved under
// the pod scope.
//
// So this measures the sandbox, not the actor's processes in isolation, which
// is what the proto means by "the unit of measurement is the SANDBOX". Sizing
// and chargeback want that number, since the sentry's overhead is a cost the
// node pays for running this actor, but it is not comparable to a container
// figure from a runc-based runtime, and a mostly idle actor reads as a nonzero
// floor. Splitting the actor's share out would need the sentry's own
// accounting, which the proto already says is not reported here.
//
// ocispec.GVisorCgroupLeaf supplies the leaf name for both shaping and stats.
const sandboxCgroupContainer = ocispec.PauseContainer

// Sample reads the sandbox cgroup. It finds the cgroup by UID alone, so it
// measures the boot too, before Start returns a sandbox.
func (r *gvisorRuntime) Sample(_ context.Context, active resources.ActorAttribution, _ ateworker.Sandbox) (ateworker.Reading, error) {
	read := r.readSandboxCgroup
	if read == nil {
		read = cgroupstats.Read
	}
	observedAt := time.Now()
	sample, err := read(filepath.Join(r.cgroupRoot, ocispec.GVisorCgroupLeaf(active.UID, sandboxCgroupContainer)))
	if err != nil {
		// Most often a poll landing in the boot, before runsc has created the
		// leaf, or a sandbox that went away underneath the read.
		if errors.Is(err, fs.ErrNotExist) {
			return ateworker.Reading{}, fmt.Errorf("%w: no sandbox cgroup to measure yet", ateworker.ErrNoSample)
		}
		return ateworker.Reading{}, fmt.Errorf("reading sandbox cgroup: %w", err)
	}

	return ateworker.Reading{
		Sample: &ateworkerpb.WorkloadStatsSample{
			Atespace:              active.Ref.Atespace,
			ActorName:             active.Ref.Name,
			ActorUid:              active.UID,
			ActorTemplateAtespace: active.TemplateAtespace,
			ActorTemplateName:     active.TemplateName,

			SandboxClass: string(atev1alpha1.SandboxClassGvisor),
			Source:       ateworkerpb.StatsSource_STATS_SOURCE_CGROUP,

			MemoryCurrentBytes:    sample.MemoryCurrentBytes,
			MemoryPeakBytes:       sample.MemoryPeakBytes,
			MemoryWorkingSetBytes: sample.MemoryWorkingSetBytes,
			CpuUsageUsec:          sample.CPUUsageUsec,

			ObservedAtUnixNano: observedAt.UnixNano(),
		},
		// oom_kill counts over the cgroup's lifetime, so a leaf left behind by
		// an earlier activation of the same actor carries its count forward.
		Dead:      sample.Empty,
		DeadAttrs: []slog.Attr{slog.Uint64(string(ateattr.SandboxOOMKillsKey), sample.OOMKills)},
	}, nil
}
