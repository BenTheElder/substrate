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

package ateworker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateworker/actornet"
	"github.com/agent-substrate/substrate/internal/atunnel/ingress"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/workerstats"
)

// hostedActor holds one actor's attribution, network, and sandbox.
type hostedActor struct {
	// Immutable after admission.
	attribution resources.ActorAttribution
	// usage is this activation's epoch, CPU baseline, and latest sample.
	usage *workerstats.Activation
	// deadReported keeps the usage sweep to one dead-sandbox warning per
	// activation.
	deadReported atomic.Bool

	// Guarded by actorsMu.
	network *actornet.SandboxSession
	// Nil until Start returns.
	sandbox Sandbox
}

// admitActor reserves capacity before network setup. An actor that is already
// hosted keeps its slot; its old network is returned for the caller to close.
// resumesCounters is set when the sandbox's CPU counters carry over from a
// snapshot instead of starting at zero.
func (s *service) admitActor(attribution resources.ActorAttribution, resumesCounters bool) (*hostedActor, *actornet.SandboxSession, error) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	var stale *actornet.SandboxSession
	if old, ok := s.actors[attribution.UID]; ok {
		stale = old.network
	} else if len(s.actors)+s.draining >= s.maxActors {
		return nil, nil, apierror.ResourceExhausted("worker is full: %d actors", s.maxActors)
	}
	hosted := &hostedActor{attribution: attribution, usage: workerstats.NewActivation(time.Now(), resumesCounters)}
	s.actors[attribution.UID] = hosted
	return hosted, stale, nil
}

// hostActor sets up the actor's network, replacing any stale one.
func (s *service) hostActor(ctx context.Context, attribution resources.ActorAttribution, resumesCounters bool) (*hostedActor, error) {
	uid := attribution.UID
	if uid == "" {
		return nil, fmt.Errorf("actor UID is required")
	}

	hosted, stale, err := s.admitActor(attribution, resumesCounters)
	if err != nil {
		return nil, err
	}
	if stale != nil {
		if err := stale.Close(ctx); err != nil {
			s.forgetActor(uid)
			return nil, fmt.Errorf("while clearing stale state for actor %s: %w", uid, err)
		}
	}

	session, err := actornet.ServeSandbox(ctx, actornet.SandboxNetworkConfig{
		ActorUID:   uid,
		Veth:       s.cfg.Veth,
		EgressPort: s.tunnel.EgressPort,
		DNSPolicy:  s.tunnel.ActorEgressPolicy(attribution),
	}, s.tunnel.Egress, s.tunnel.DNSRelay)
	if err != nil {
		s.forgetActor(uid)
		return nil, fmt.Errorf("while setting up the sandbox network: %w", err)
	}

	s.actorsMu.Lock()
	hosted.network = session
	s.actorsMu.Unlock()
	return hosted, nil
}

// forgetActor drops an actor whose network never came up.
func (s *service) forgetActor(actorUID string) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	delete(s.actors, actorUID)
}

// unhostActor removes an actor and its network. Repeated calls are safe. The
// caller must stop the sandbox first.
func (s *service) unhostActor(ctx context.Context, actorUID string) error {
	s.actorsMu.Lock()
	hosted, ok := s.actors[actorUID]
	delete(s.actors, actorUID)
	if ok {
		// Retain capacity until network cleanup completes.
		s.draining++
	}
	s.actorsMu.Unlock()
	if !ok {
		return nil
	}
	defer func() {
		s.actorsMu.Lock()
		s.draining--
		s.actorsMu.Unlock()
	}()

	if hosted.network == nil {
		return nil
	}
	if err := hosted.network.Close(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to remove the sandbox network", slog.Any("err", err))
		return err
	}
	return nil
}

// lookupActor returns the actor or nil without taking its lifecycle lock.
func (s *service) lookupActor(actorUID string) *hostedActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	return s.actors[actorUID]
}

// hostedActors returns a snapshot of the actor map.
func (s *service) hostedActors() []*hostedActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	out := make([]*hostedActor, 0, len(s.actors))
	for _, hosted := range s.actors {
		out = append(out, hosted)
	}
	return out
}

// sandboxOf is h's sandbox, or nil.
func (s *service) sandboxOf(h *hostedActor) Sandbox {
	if h == nil {
		return nil
	}
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	return h.sandbox
}

// setSandbox sets or clears h's sandbox.
func (s *service) setSandbox(h *hostedActor, sb Sandbox) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	h.sandbox = sb
}

// sandboxDialer connects to the actor inside its network, looking it up on
// each dial.
func (s *service) sandboxDialer(actorUID string) ingress.DialFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		s.actorsMu.RLock()
		var session *actornet.SandboxSession
		if hosted := s.actors[actorUID]; hosted != nil {
			session = hosted.network
		}
		s.actorsMu.RUnlock()
		if session == nil {
			return nil, fmt.Errorf("actor %s is not hosted here", actorUID)
		}
		return session.Dialer()(ctx, network, address)
	}
}
