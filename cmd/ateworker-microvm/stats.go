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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateworker-microvm/internal/agentstats"
	"github.com/agent-substrate/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateworker"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// statsCallTimeout bounds one container's guest-agent call. The RPC is polled
// on a timer, so it must fail fast rather than pile pollers up behind a guest
// that has stopped answering: a missed sample is cheap, a stuck handler is not.
// Generous next to a healthy read, which is a vsock round trip and four small
// file reads inside the guest, and far short of the lifecycle calls' 20-30s.
const statsCallTimeout = 2 * time.Second

// containerStatsReader is the one guest-agent call Sample makes.
// *kata.AgentClient satisfies it; the narrow interface is what lets Sample be
// tested without a live micro-VM.
type containerStatsReader interface {
	StatsContainer(ctx context.Context, containerID string) (*agentpb.CgroupStats, error)
}

// guestStatsTarget is everything Sample needs to read a live guest.
type guestStatsTarget struct {
	// actorUID is the actor these containers belong to. Checked against the
	// attribution before anything is reported, so a target left behind by a
	// transition cannot file one actor's numbers under another's name.
	actorUID string

	// agent is the kata-agent client the actor's log forwarding already keeps
	// open for its lifetime, borrowed rather than dialed again. ttrpc
	// multiplexes, so a poll and the forwarding reads share it safely.
	agent containerStatsReader

	// workloadIDs are the guest containers to sum, one per actor container.
	// Each actor container exists in the guest as TWO kata containers (see
	// the retired guest-overlay design): a "carrier", whose only job is to make the agent
	// bind the read-only image rootfs at a fixed guest path, and the overlay
	// WORKLOAD, which lays a writable upper over it and runs the container's
	// actual process. Only workload ids belong here: a carrier is created but
	// never started, so no process ever runs in it and its cgroup has nothing
	// to add to the sum.
	workloadIDs []string
}

// errStaleGuestTarget is the one bug-shaped failure Sample can return: the
// published guest target and the attribution disagree, which should never
// happen.
var errStaleGuestTarget = errors.New("guest agent connection belongs to a different actor")

// Sample reads the guest's container cgroups through the agent. The host
// cgroup holds cloud-hypervisor, whose memory is the guest RAM it took at boot
// and reads the same for an idle actor and a saturated one; the guest kernel is
// what accounts for the workload.
//
// With one exception its errors mean "no numbers right now": a guest that has
// stopped answering is routine here, and a vsock call offers no error type that
// separates "gone" from "broken". The exception is errStaleGuestTarget.
func (s *microvmRuntime) Sample(ctx context.Context, active resources.ActorAttribution, sb ateworker.Sandbox) (ateworker.Reading, error) {
	// No guest to ask yet, usually a poll landing in the boot or the restore, or
	// one already torn down.
	ra, _ := sb.(*runningActor)
	var target *guestStatsTarget
	if ra != nil {
		target = ra.statsTarget()
	}
	if target == nil {
		return ateworker.Reading{}, fmt.Errorf("%w: no guest agent connection to measure yet", ateworker.ErrNoSample)
	}
	// The target is published alongside the attribution, so this should be
	// unreachable; if the two ever disagree, decline rather than report a
	// stale guest's numbers under the requested actor's name.
	if target.actorUID != active.UID {
		return ateworker.Reading{}, fmt.Errorf("%w: %q, not %q", errStaleGuestTarget, target.actorUID, active.UID)
	}

	observedAt := time.Now()
	sample, cpuByContainer, err := sumContainerStats(ctx, target)
	if err != nil {
		return ateworker.Reading{}, fmt.Errorf("%w: no container stats from the guest agent: %w", ateworker.ErrNoSample, err)
	}

	return ateworker.Reading{
		Sample: &ateworkerpb.WorkloadStatsSample{
			Atespace:              active.Ref.Atespace,
			ActorName:             active.Ref.Name,
			ActorUid:              active.UID,
			ActorTemplateAtespace: active.TemplateAtespace,
			ActorTemplateName:     active.TemplateName,

			SandboxClass: string(atev1alpha1.SandboxClassMicroVM),
			Source:       ateworkerpb.StatsSource_STATS_SOURCE_GUEST_AGENT,

			MemoryCurrentBytes:    sample.MemoryCurrentBytes,
			MemoryPeakBytes:       sample.MemoryPeakBytes,
			MemoryWorkingSetBytes: sample.MemoryWorkingSetBytes,
			CpuUsageUsec:          sample.CPUUsageUsec,

			ObservedAtUnixNano: observedAt.UnixNano(),
		},
		CPUByContainer: cpuByContainer,
	}, nil
}

// sumContainerStats reads every container of the actor and adds them up. It
// also returns each container's CPU by id, so the activation can tell a
// container it could not read from one whose usage fell.
//
// A container the agent cannot report contributes nothing instead of failing
// the sample. That is not only the "a partial reading beats none" trade the
// gVisor side makes for a missing cgroup file — for the common way this
// happens, zero is the correct contribution rather than a fallback: a container
// that has exited took its guest cgroup with it and consumes nothing from here
// on. Failing the actor's telemetry because one sidecar is gone would be the
// wrong answer.
//
// It fails when no container could be read at all, which is the guest as a
// whole not answering rather than one container being gone, and returns the
// last error so the caller can say why. It also fails when ctx ends before
// every container answered, so a deadline never yields a partial sum.
//
// The loop as a whole is bounded by ctx — the caller's RPC deadline — with
// maxActorContainers * statsCallTimeout as the ceiling when the caller set
// none. statsCallTimeout is per call so that one hung container read cannot
// eat the budget the remaining containers still need.
func sumContainerStats(ctx context.Context, target *guestStatsTarget) (agentstats.Sample, map[string]uint64, error) {
	var (
		total   agentstats.Sample
		cpu     = make(map[string]uint64, len(target.workloadIDs))
		lastErr error
	)
	for _, id := range target.workloadIDs {
		if err := ctx.Err(); err != nil {
			// The caller is gone; the per-call ctx below would fail instantly
			// anyway, so stop burning through the remaining containers.
			return agentstats.Sample{}, nil, err
		}
		callCtx, cancel := context.WithTimeout(ctx, statsCallTimeout)
		cs, err := target.agent.StatsContainer(callCtx, id)
		cancel()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// ctx ended during this call: report that rather than a sum
				// missing this container.
				return agentstats.Sample{}, nil, ctxErr
			}
			lastErr = err
			continue
		}
		one := agentstats.FromCgroupStats(cs)
		cpu[id] = one.CPUUsageUsec
		total = total.Plus(one)
	}

	if len(cpu) == 0 {
		if lastErr == nil {
			// No containers to read. The actor is up with an empty container
			// list, which the boot path does not produce, so treat it as "no
			// numbers" rather than reporting a confident zero.
			lastErr = errors.New("no containers to measure")
		}
		return agentstats.Sample{}, nil, lastErr
	}
	return total, cpu, nil
}
