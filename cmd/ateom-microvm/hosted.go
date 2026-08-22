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
	"fmt"
	"log/slog"
	"net/url"

	"github.com/agent-substrate/substrate/internal/actornet"
	"github.com/agent-substrate/substrate/internal/resources"
)

// hostedActor is everything this ateom holds for one actor it is running.
//
// It replaced three pieces of per-process state that each assumed a single
// actor: the attribution slot, the guest-stats target, and the entry in the
// running map. They are together here because they are three views of one
// actor's lifetime, and keeping them apart was only tenable while there could
// be at most one of each.
//
// The fields become true at different points, which is the whole reason they
// are separate fields rather than one struct built at boot:
//
//   - attribution exists from the moment the ateom accepts the actor,
//     deliberately including a boot that never finishes, so a stats poll landing
//     mid-boot is attributable and a workload that dies during boot is not
//     anonymous.
//   - network exists once the veth is up, before the guest is.
//   - vm exists once cloud-hypervisor is running.
//   - guest exists only once the containers are up and the agent is answering.
type hostedActor struct {
	// attribution is set once at hostActor and never mutated, so it is safe to
	// read through the pointer without holding actorsMu.
	attribution resources.ActorAttribution

	// slot is the worker-local index this actor holds. Recorded HERE, at
	// registration, rather than only on network below: the slot has to be
	// visible to a concurrent freeSlotLocked from the moment it is taken, and
	// network is nil until SetupActorNetwork returns. Guarded by actorsMu.
	slot int

	// network is this actor's namespace, veth, and pod-side address. Nil until
	// the veth is up; slot is what says which one it will be.
	network *actornet.ActorNetwork

	// vm is the live micro-VM, kept so CheckpointWorkload can pause, snapshot,
	// and tear down the same sandbox RestoreWorkload relaunched. Nil until the
	// boot publishes it. Guarded by actorsMu.
	vm *runningActor

	// guest is what GetWorkloadStats measures with. Nil whenever there is no
	// guest to ask -- before the containers are up, after teardownActor, and for
	// the rest of an activation whose post-restore agent dial failed. Non-nil
	// here implies attribution is set, never the reverse. Guarded by actorsMu.
	guest *guestStatsTarget
}

// upstream is where atunnel reaches this actor. Every actor believes it holds
// the same address -- it is frozen into the guest's snapshot, which is exactly
// why it cannot be allocated per actor -- so the worker addresses it by the
// pod-side one the network rewrite gives it instead.
func (h *hostedActor) upstream() (*url.URL, error) {
	return url.Parse(fmt.Sprintf("http://%s:%d", h.network.PodSideIP, actorHTTPPort))
}

// actorHTTPPort is the in-guest HTTP port atunnel proxies actor ingress to.
const actorHTTPPort = 80

// hostActor registers an actor as hosted and gives it a network. The slot it
// allocates fixes the actor's veth name, tap name, and pod-side address for as
// long as it is here, and is reusable once it leaves.
func (s *AteomService) hostActor(ctx context.Context, attribution resources.ActorAttribution, tunneledEgress bool) (*hostedActor, error) {
	uid := attribution.UID
	if uid == "" {
		return nil, fmt.Errorf("actor UID is required")
	}

	// Clear anything left from a previous activation of THIS actor before taking
	// a slot for it. Other actors here are untouched: a second activation is now
	// the ordinary case, not a conflict to resolve.
	if err := s.unhostActor(ctx, uid); err != nil {
		return nil, fmt.Errorf("while clearing stale state for actor %s: %w", uid, err)
	}

	s.actorsMu.Lock()
	slot, err := s.freeSlotLocked()
	if err != nil {
		s.actorsMu.Unlock()
		return nil, err
	}
	// Registered WITH ITS SLOT before the network exists, so the slot is
	// genuinely reserved against a concurrent activation, and so a stats poll
	// during setup can still attribute what it finds.
	hosted := &hostedActor{attribution: attribution, slot: slot}
	s.actors[uid] = hosted
	s.actorsMu.Unlock()

	network, err := actornet.SetupActorNetwork(ctx, actornet.ActorNetworkConfig{
		ActorUID: uid,
		Slot:     slot,
		// Fixed, unlike gVisor's kernel-random veth MAC: a CH snapshot freezes
		// the guest kernel's ARP entry for the gateway, so a new veth pair with
		// a random MAC would blackhole guest egress until that entry expired.
		HostVethHWAddr: hostVethHWAddr,
		// The interior namespace is created fresh per activation, but a previous
		// activation of this actor can have left the kata tap behind in it.
		SweepInteriorLinks: true,
		// Only this actor's egress is redirected into atunnel, and only when it
		// asked for it: the rule is keyed on its pod-side address.
		TunneledEgress: tunneledEgress,
	})
	if err != nil {
		// Drop the reservation; the actor is not here after all.
		s.actorsMu.Lock()
		delete(s.actors, uid)
		s.actorsMu.Unlock()
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}

	s.actorsMu.Lock()
	hosted.network = network
	s.actorsMu.Unlock()

	// The rules describe the whole set, so they are reapplied whenever it
	// changes rather than edited for one actor.
	if err := s.applyNetworkRules(); err != nil {
		if cleanupErr := s.unhostActor(ctx, uid); cleanupErr != nil {
			slog.WarnContext(ctx, "Failed to clean up actor after rule application failed",
				slog.String("actorUID", uid), slog.Any("err", cleanupErr))
		}
		return nil, err
	}
	return hosted, nil
}

// unhostActor removes an actor and its network, and reapplies the rules for
// those that remain. Idempotent: an actor that is not here is already gone,
// which is what makes it safe on the failure paths that call it.
//
// It does NOT stop the actor's micro-VM. Every caller has already torn the VM
// down (teardownActor) or never got one up, and killing cloud-hypervisor from
// the same call that frees a slot would make the failure paths, which run this
// speculatively, capable of destroying a healthy guest.
func (s *AteomService) unhostActor(ctx context.Context, actorUID string) error {
	s.actorsMu.Lock()
	hosted, ok := s.actors[actorUID]
	delete(s.actors, actorUID)
	s.actorsMu.Unlock()
	if !ok {
		return nil
	}

	var err error
	if hosted.network != nil {
		err = actornet.CleanupActorNetwork(ctx, hosted.network)
	}
	// Reapply even when teardown failed: the actor is out of the set either way,
	// and leaving its rules in place would send the next actor to take the slot
	// down a path built for its predecessor.
	if ruleErr := s.applyNetworkRules(); ruleErr != nil && err == nil {
		err = ruleErr
	}
	return err
}

// lookupActor returns the actor with this UID, or nil. Takes only actorsMu, not
// an actor's lifecycle lock, so a stats poll landing in the middle of a boot or
// checkpoint answers immediately instead of parking for its duration.
func (s *AteomService) lookupActor(actorUID string) *hostedActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	return s.actors[actorUID]
}

// hostedActors is a snapshot of everything running here.
func (s *AteomService) hostedActors() []*hostedActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	out := make([]*hostedActor, 0, len(s.actors))
	for _, hosted := range s.actors {
		out = append(out, hosted)
	}
	return out
}

// setVM records the live micro-VM for an actor once it is running.
func (s *AteomService) setVM(actorUID string, vm *runningActor) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	if hosted, ok := s.actors[actorUID]; ok {
		hosted.vm = vm
	}
}

// vmFor returns the actor's live micro-VM, or nil if there is not one.
func (s *AteomService) vmFor(actorUID string) *runningActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	if hosted, ok := s.actors[actorUID]; ok {
		return hosted.vm
	}
	return nil
}

// setGuestTarget publishes (or with nil, withdraws) the guest connection
// GetWorkloadStats measures through.
func (s *AteomService) setGuestTarget(actorUID string, target *guestStatsTarget) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	if hosted, ok := s.actors[actorUID]; ok {
		hosted.guest = target
	}
}

// guestTarget returns the actor's guest connection, copied out under the lock
// so the caller's guest-agent calls -- a vsock round trip apiece -- never run
// with actorsMu held.
func (s *AteomService) guestTarget(hosted *hostedActor) *guestStatsTarget {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	return hosted.guest
}

// freeSlotLocked returns the lowest slot no hosted actor is using. Lowest-free
// rather than a counter so slots are reused as actors come and go, which keeps
// veth names, tap names, and pod-side addresses within a small range no matter
// how many actors a long-lived worker has cycled through.
func (s *AteomService) freeSlotLocked() (int, error) {
	taken := make(map[int]bool, len(s.actors))
	for _, hosted := range s.actors {
		// Every registered actor, whether or not its network exists yet. Keying
		// off network instead was a race: an actor registered but still inside
		// SetupActorNetwork marked nothing, so concurrent activations all chose
		// the lowest slot, built the same veth name, and every loser failed with
		// "bringing up host veth: no such device". Sequentially it never
		// appeared; twelve at once hit it within ten actors.
		taken[hosted.slot] = true
	}
	for slot := range actornet.MaxActorSlots {
		if !taken[slot] {
			return slot, nil
		}
	}
	return 0, fmt.Errorf("no free actor slot: all %d are in use", actornet.MaxActorSlots)
}

// applyNetworkRules rebuilds the worker's nftables ruleset from the actors it is
// currently hosting.
func (s *AteomService) applyNetworkRules() error {
	hosted := s.hostedActors()
	networks := make([]*actornet.ActorNetwork, 0, len(hosted))
	for _, h := range hosted {
		if h.network != nil {
			networks = append(networks, h.network)
		}
	}
	// The ruleset is the one thing an activation shares with the others, so it
	// gets its own short lock rather than relying on activations being
	// serialized -- they no longer are.
	s.rulesMu.Lock()
	defer s.rulesMu.Unlock()
	if err := actornet.ApplyActorNetworkRules(networks, s.atunnelEgressPort); err != nil {
		return fmt.Errorf("while applying actor network rules: %w", err)
	}
	return nil
}
