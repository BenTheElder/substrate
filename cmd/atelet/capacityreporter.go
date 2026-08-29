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
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// capacityReportInterval is how often the node's ateoms are re-asked what they
// can hold.
//
// Capacity is near-static -- it changes when an ateom is replaced by a build
// that admits a different number -- so this is a convergence loop, not a
// measurement. It is short enough that a worker becomes schedulable at its real
// ceiling promptly after starting, and the control plane skips the write when
// nothing changed, so the steady-state cost is one read per worker per tick.
const capacityReportInterval = 30 * time.Second

// capacityClient is the ateom half of a report: what it can hold.
type capacityClient interface {
	GetCapacity(ctx context.Context, in *ateompb.GetCapacityRequest, opts ...grpc.CallOption) (*ateompb.GetCapacityResponse, error)
}

// capacityReporter tells the control plane what each of the node's workers can
// hold, by asking the ateoms themselves.
//
// The ceiling belongs to the ateom: it owns the slot allocator, so it is the
// only thing that knows what hostActor will admit, and an ateom built to host
// one Actor reports one however the pool that started it is configured. atelet
// is the messenger because it is the process on this node with an authenticated
// connection to ateapi.
//
// Discovery is the same filesystem registry the stats poller uses: every ateom
// creates its socket directory at boot, so one readdir names the node's
// workers, and the directory name IS the worker pod UID, which is the Worker's
// name. Nothing is held between sweeps, so an atelet restart loses nothing.
type capacityReporter struct {
	// interval between the end of one sweep and the start of the next.
	interval time.Duration

	// ateomsDir is the directory whose entries are worker pod UIDs
	// (ateompath.AteomsDir on a real node; a fixture in tests).
	ateomsDir string

	// dial returns a capacity client for one ateom plus the closer that
	// releases its connection. One connection per probe, for the same reason
	// the stats sweep does it: a cache saves nothing at this rate, and sweeping
	// stale sockets through the lifecycle RPCs' shared cache would let this
	// evict connections they are using.
	dial func(ctx context.Context, podUID string) (capacityClient, io.Closer, error)

	// report hands one worker's capacity to ateapi.
	report func(ctx context.Context, in *ateapipb.SetWorkerCapacityRequest, opts ...grpc.CallOption) (*ateapipb.SetWorkerCapacityResponse, error)
}

// run sweeps until ctx is cancelled, starting with an immediate sweep so a
// restarted atelet does not leave its workers at the default ceiling for an
// interval.
func (r *capacityReporter) run(ctx context.Context) {
	slog.InfoContext(ctx, "Worker capacity reporter starting", slog.Duration("interval", r.interval))
	for {
		r.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.interval):
		}
	}
}

// tick asks every ateom on the node what it can hold and reports each answer.
//
// The tolerance rule is the stats sweep's, for the same reasons: any failure to
// dial or call an entry means "not a target this tick" rather than an error.
// That covers stale directories left by deleted worker pods, ateoms that have
// created their directory but are not yet listening, and workers torn down
// mid-sweep. A Worker the control plane has not registered yet is the same kind
// of ordinary miss -- the ateom's directory can exist before the Worker record
// does -- so NOT_FOUND is a debug line and the next sweep picks it up.
func (r *capacityReporter) tick(ctx context.Context) {
	entries, err := os.ReadDir(r.ateomsDir)
	if err != nil {
		// A node with no ateoms directory yet has no workers to report on; the
		// first RunWorkload dispatch creates it.
		slog.DebugContext(ctx, "Capacity sweep: no ateoms directory", slog.Any("err", err))
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		r.reportOne(ctx, entry.Name())
	}
}

// reportOne asks a single ateom what it can hold and passes that on. The pod
// UID naming the ateom's directory is also the Worker's name.
func (r *capacityReporter) reportOne(ctx context.Context, podUID string) {
	client, closer, err := r.dial(ctx, podUID)
	if err != nil {
		slog.DebugContext(ctx, "Capacity sweep: cannot dial ateom", slog.String("worker", podUID), slog.Any("err", err))
		return
	}
	defer closer.Close()

	got, err := client.GetCapacity(ctx, &ateompb.GetCapacityRequest{})
	if err != nil {
		slog.DebugContext(ctx, "Capacity sweep: ateom did not answer", slog.String("worker", podUID), slog.Any("err", err))
		return
	}
	slots := got.GetActorSlots()
	if slots <= 0 {
		// The ateom declines to state a ceiling. Leave whatever the control
		// plane already believes rather than reporting a zero it would have to
		// interpret.
		slog.DebugContext(ctx, "Capacity sweep: ateom reports no ceiling", slog.String("worker", podUID))
		return
	}

	if _, err := r.report(ctx, &ateapipb.SetWorkerCapacityRequest{
		Worker:   &ateapipb.ObjectRef{Name: podUID},
		Capacity: &ateapipb.WorkerCapacity{Actors: slots},
	}); err != nil {
		// NOT_FOUND is the ordinary race with worker registration, and ABORTED
		// is a lost write race; both resolve on the next sweep.
		switch status.Code(err) {
		case codes.NotFound, codes.Aborted:
			slog.DebugContext(ctx, "Capacity sweep: report not applied yet",
				slog.String("worker", podUID), slog.Any("err", err))
		default:
			slog.WarnContext(ctx, "Capacity sweep: reporting worker capacity failed",
				slog.String("worker", podUID), slog.Any("err", err))
		}
	}
}

// startCapacityReporter assembles the reporter and starts it. Split from main's
// boot sequence the way startStatsPoller is, so the sweep has one entry point.
//
// It dials its own per-probe connections to the ateoms for the reason
// capacityReporter.dial gives, and reuses the ateapi connection main already
// holds: the report authenticates as this atelet, which is exactly the identity
// that connection carries.
func startCapacityReporter(ctx context.Context, ateapiConn grpc.ClientConnInterface) {
	client := ateapipb.NewWorkerServiceClient(ateapiConn)
	reporter := &capacityReporter{
		interval:  capacityReportInterval,
		ateomsDir: ateompath.AteomsDir(),
		dial: func(_ context.Context, podUID string) (capacityClient, io.Closer, error) {
			conn, err := grpc.NewClient(
				"unix://"+ateompath.AteomSocketPath(podUID),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			)
			if err != nil {
				return nil, nil, err
			}
			return ateompb.NewAteomClient(conn), conn, nil
		},
		report: client.SetWorkerCapacity,
	}
	go reporter.run(ctx)
}
