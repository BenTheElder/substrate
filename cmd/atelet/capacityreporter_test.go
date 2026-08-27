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
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeCapacityClient struct {
	slots int32
	err   error
}

func (f fakeCapacityClient) GetCapacity(context.Context, *ateompb.GetCapacityRequest, ...grpc.CallOption) (*ateompb.GetCapacityResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &ateompb.GetCapacityResponse{ActorSlots: f.slots}, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// ateomsFixture builds a directory shaped like the node's ateom registry: one
// directory per worker pod UID, which is what a sweep discovers.
func ateomsFixture(t *testing.T, podUIDs ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, uid := range podUIDs {
		if err := os.MkdirAll(filepath.Join(dir, uid), 0o700); err != nil {
			t.Fatalf("creating ateom dir %s: %v", uid, err)
		}
	}
	return dir
}

// reporterFor wires a reporter over the fixture, recording what it reports.
func reporterFor(dir string, clients map[string]capacityClient, reportErr error) (*capacityReporter, *[]*ateapipb.ReportWorkerCapacityRequest) {
	var got []*ateapipb.ReportWorkerCapacityRequest
	r := &capacityReporter{
		ateomsDir: dir,
		dial: func(_ context.Context, podUID string) (capacityClient, io.Closer, error) {
			c, ok := clients[podUID]
			if !ok {
				return nil, nil, errors.New("no such ateom")
			}
			return c, nopCloser{}, nil
		},
		report: func(_ context.Context, in *ateapipb.ReportWorkerCapacityRequest, _ ...grpc.CallOption) (*ateapipb.ReportWorkerCapacityResponse, error) {
			got = append(got, in)
			return &ateapipb.ReportWorkerCapacityResponse{}, reportErr
		},
	}
	return r, &got
}

// The sweep names its targets from the directory alone, and the directory name
// is the Worker's name.
func TestCapacityReporterReportsEachAteom(t *testing.T) {
	dir := ateomsFixture(t, "uid-a", "uid-b")
	r, got := reporterFor(dir, map[string]capacityClient{
		"uid-a": fakeCapacityClient{slots: 4094},
		"uid-b": fakeCapacityClient{slots: 1},
	}, nil)

	r.tick(context.Background())

	if len(*got) != 2 {
		t.Fatalf("reported %d workers, want 2", len(*got))
	}
	byName := map[string]int32{}
	for _, req := range *got {
		byName[req.GetWorker().GetName()] = req.GetCapacity().GetActors()
	}
	if byName["uid-a"] != 4094 || byName["uid-b"] != 1 {
		t.Errorf("reported %v, want uid-a=4094 uid-b=1", byName)
	}
	var names []string
	for n := range byName {
		names = append(names, n)
	}
	sort.Strings(names)
	if names[0] != "uid-a" || names[1] != "uid-b" {
		t.Errorf("worker names = %v", names)
	}
}

// An ateom that cannot be reached is not a target this tick. That covers stale
// directories from deleted pods and ateoms that have not started listening, so
// one unreachable entry must not stop the sweep.
func TestCapacityReporterSkipsUnreachableAteoms(t *testing.T) {
	dir := ateomsFixture(t, "gone", "alive")
	r, got := reporterFor(dir, map[string]capacityClient{
		"alive": fakeCapacityClient{slots: 8},
	}, nil)

	r.tick(context.Background())

	if len(*got) != 1 || (*got)[0].GetWorker().GetName() != "alive" {
		t.Fatalf("reported %d workers (%v), want only alive", len(*got), *got)
	}
}

// An ateom that answers with an error, and one that declines to state a
// ceiling, both leave the control plane's view alone rather than reporting a
// zero it would have to interpret.
func TestCapacityReporterReportsNothingWithoutACeiling(t *testing.T) {
	for _, tc := range []struct {
		name   string
		client capacityClient
	}{
		{"rpc failed", fakeCapacityClient{err: errors.New("boom")}},
		{"no ceiling", fakeCapacityClient{slots: 0}},
		{"negative", fakeCapacityClient{slots: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := ateomsFixture(t, "uid-a")
			r, got := reporterFor(dir, map[string]capacityClient{"uid-a": tc.client}, nil)
			r.tick(context.Background())
			if len(*got) != 0 {
				t.Errorf("reported %v, want nothing", *got)
			}
		})
	}
}

// A Worker the control plane has not registered yet is an ordinary race: the
// ateom's directory can exist before the Worker record does. The sweep must
// carry on and leave it for the next one.
func TestCapacityReporterToleratesUnregisteredWorker(t *testing.T) {
	dir := ateomsFixture(t, "uid-a", "uid-b")
	r, got := reporterFor(dir, map[string]capacityClient{
		"uid-a": fakeCapacityClient{slots: 2},
		"uid-b": fakeCapacityClient{slots: 2},
	}, status.Error(codes.NotFound, "Worker not found"))

	r.tick(context.Background())

	if len(*got) != 2 {
		t.Errorf("attempted %d reports, want both attempted despite NOT_FOUND", len(*got))
	}
}

// A node with no ateoms directory has no workers to report on, which is the
// state every node starts in.
func TestCapacityReporterToleratesMissingDir(t *testing.T) {
	r, got := reporterFor(filepath.Join(t.TempDir(), "absent"), nil, nil)
	r.tick(context.Background())
	if len(*got) != 0 {
		t.Errorf("reported %v, want nothing", *got)
	}
}

// Files alongside the per-ateom directories are not workers.
func TestCapacityReporterIgnoresNonDirectories(t *testing.T) {
	dir := ateomsFixture(t, "uid-a")
	if err := os.WriteFile(filepath.Join(dir, "stray-file"), []byte("x"), 0o600); err != nil {
		t.Fatalf("writing stray file: %v", err)
	}
	r, got := reporterFor(dir, map[string]capacityClient{"uid-a": fakeCapacityClient{slots: 3}}, nil)

	r.tick(context.Background())

	if len(*got) != 1 || (*got)[0].GetWorker().GetName() != "uid-a" {
		t.Errorf("reported %v, want only uid-a", *got)
	}
}
