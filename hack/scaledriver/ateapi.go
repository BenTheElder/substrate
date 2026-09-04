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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var (
	ateapiEndpoint = pflag.String("ateapi-endpoint", "api.ate-system.svc:443", "ateapi gRPC target for --via=ateapi.")
	ateapiToken    = pflag.String("ateapi-token-file", "/var/run/secrets/ate/token", "Projected service account token presented to ateapi.")
)

// driveViaAteapi runs the same ramp through the control plane instead of
// straight at the node, so the two can be subtracted.
//
// It deliberately reuses internal/ateclient -- the client kubectl-ate itself
// builds -- rather than dialing ateapi by hand. The number wanted here is what
// the control plane costs an ordinary caller, and a hand-rolled client would
// quietly measure a different one.
//
// What it does NOT reuse is kubectl-ate's process model. The shell harness
// spawns two 70MB binaries per actor, each doing its own TLS handshake and
// ClusterTrustBundle fetch, and at any real concurrency that dominates
// everything it is supposed to be measuring -- an earlier run had 32 of them
// DoS'ing the Kubernetes API server. One process, one connection, N concurrent
// RPCs is the same API from the server's point of view and none of that from
// the client's.
func driveViaAteapi(ctx context.Context) error {
	client, err := ateclient.NewClient(ctx, "", "", *ateapiEndpoint, *ateapiToken, false)
	if err != nil {
		return fmt.Errorf("dial ateapi at %s: %w", *ateapiEndpoint, err)
	}
	defer client.Close()
	fmt.Fprintf(os.Stderr, "ateapi %s, atespace %s\n", *ateapiEndpoint, *atespace)

	if _, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: *atespace}},
	}); err != nil {
		// Already there is the ordinary case on a re-run.
		fmt.Fprintf(os.Stderr, "create atespace %s: %v (continuing)\n", *atespace, err)
	}

	if *mode == "terminate" {
		return drive(ctx, func(ctx context.Context, n int) error {
			_, err := client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: *atespace, Name: actorName(n)},
			})
			return err
		})
	}

	return drive(ctx, func(ctx context.Context, n int) error {
		name := actorName(n)
		// Create then resume, which is what activating an actor through the API
		// is. Both are timed together: splitting them would measure two RPCs
		// rather than the thing a caller actually waits for.
		if _, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: *atespace, Name: name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: *templateNamespace, Name: *templateName},
			},
		}); err != nil && status.Code(err) != codes.AlreadyExists {
			// An actor left by a previous run is resumed again, which is what
			// --name-prefix documents and what a resume-only measurement needs.
			return fmt.Errorf("create: %w", err)
		}
		if _, err := client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: *atespace, Name: name},
		}); err != nil {
			return fmt.Errorf("resume: %w", err)
		}
		return nil
	})
}
