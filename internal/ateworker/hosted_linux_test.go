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
	"testing"

	"github.com/agent-substrate/substrate/internal/ateworker/tunnel"
	"github.com/agent-substrate/substrate/internal/atunnel/egress"
	"github.com/agent-substrate/substrate/internal/linuxsys"
	"github.com/agent-substrate/substrate/internal/netns"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/roottest"
)

// Hosting an actor that is already hosted replaces its network rather than
// failing on the namespace name it still holds.
func TestHostActorReplacesSameActor(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()
	egressProxy, err := egress.New(linuxsys.TCPOriginalDestination)
	if err != nil {
		t.Fatal(err)
	}
	s := newTestService(&fakeRuntime{})
	s.tunnel = &tunnel.Tunnel{Egress: egressProxy, EgressPort: 15001}
	s.maxActors = 1
	const actorUID = "worker-network-replace"
	t.Cleanup(func() {
		if err := s.unhostActor(ctx, actorUID); err != nil {
			t.Error(err)
		}
	})
	var hosted *hostedActor
	for range 2 {
		if hosted, err = s.hostActor(ctx, resources.ActorAttribution{UID: actorUID}, false); err != nil {
			t.Fatal(err)
		}
	}
	named, err := netns.GetFromName(nodepath.ActorNetNSName(actorUID))
	if err != nil {
		t.Fatalf("opening replacement namespace by name: %v", err)
	}
	defer named.Close()
	if !named.Equal(hosted.network.Network.GatewayNetNS) {
		t.Fatal("namespace name does not refer to the replacement")
	}
}
