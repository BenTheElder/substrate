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

package actoridentity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ReportWorkerCapacity records what a Worker can hold, as reported by the
// atelet herding it. Authorized like MintCert: the calling atelet must run on
// the Worker's node. An unchanged report is not an update, so a reporter on a
// timer costs a read.
func (s *Server) ReportWorkerCapacity(ctx context.Context, req *ateapipb.ReportWorkerCapacityRequest) (*ateapipb.ReportWorkerCapacityResponse, error) {
	caller, err := authenticateAtelet(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateWorkerRef(req.GetWorker()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid worker: %v", err)
	}
	reported := req.GetCapacity()
	if reported == nil {
		return nil, status.Error(codes.InvalidArgument, "capacity is required")
	}
	name := req.GetWorker().GetName()

	// Authoritative, not cached: this decides a write, and a stale read would
	// reject a reporter whose Worker was just created.
	worker, err := s.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
		}
		return nil, fmt.Errorf("while fetching worker %s: %w", name, err)
	}
	if worker.GetNodeName() != caller.nodeName {
		// Same answer as an absent Worker: a caller learns nothing about
		// Workers elsewhere, including whether they exist.
		slog.WarnContext(ctx, "Refusing a capacity report for a worker on another node",
			slog.String("worker", name),
			slog.String("worker_node", worker.GetNodeName()),
			slog.String("caller_node", caller.nodeName),
			slog.String("caller_pod", caller.podName))
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	}

	merged := mergeReportedCapacity(worker.GetCapacity(), reported)
	if proto.Equal(worker.GetCapacity(), merged) {
		return &ateapipb.ReportWorkerCapacityResponse{Worker: worker}, nil
	}

	updated, err := s.store.UpdateWorker(ctx, name, store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Capacity = mergeReportedCapacity(toUpdate.GetCapacity(), reported)
		return nil
	})
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrVersionConflict):
		// The reporter re-sends on a timer, so a lost race needs no retry.
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	default:
		return nil, fmt.Errorf("while recording capacity for worker %s: %w", name, err)
	}
	slog.InfoContext(ctx, "Worker reported its capacity",
		slog.String("worker", name),
		slog.String("was", worker.GetCapacity().String()),
		slog.String("now", updated.GetCapacity().String()))
	return &ateapipb.ReportWorkerCapacityResponse{Worker: updated}, nil
}

// mergeReportedCapacity overlays the dimensions a reporter set onto what is
// recorded. A reporter states only what it can observe, so an unset dimension
// means "no opinion" and keeps what is there.
func mergeReportedCapacity(current, reported *ateapipb.WorkerCapacity) *ateapipb.WorkerCapacity {
	merged := proto.Clone(current).(*ateapipb.WorkerCapacity)
	if merged == nil {
		merged = &ateapipb.WorkerCapacity{}
	}
	if reported.GetActors() != 0 {
		merged.Actors = reported.GetActors()
	}
	if reported.GetCpuMilli() != 0 {
		merged.CpuMilli = reported.GetCpuMilli()
	}
	if reported.GetMemoryBytes() != 0 {
		merged.MemoryBytes = reported.GetMemoryBytes()
	}
	return merged
}
