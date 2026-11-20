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

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/procman"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

func (s *AteWorkerService) setProcManager(actorUID string, mgr *procman.Manager) {
	s.actorsMu.Lock()
	var old *procman.Manager
	if h, ok := s.actors[actorUID]; ok {
		old = h.procs
		h.procs = mgr
	}
	s.actorsMu.Unlock()
	if old != nil && old != mgr {
		_ = old.Close()
	}
}

func (s *AteWorkerService) lookupProcManager(actorUID string) (*procman.Manager, error) {
	if actorUID == "" {
		return nil, apierror.InvalidArgument("actor_uid is required")
	}
	s.actorsMu.RLock()
	h := s.actors[actorUID]
	var mgr *procman.Manager
	if h != nil {
		mgr = h.procs
	}
	s.actorsMu.RUnlock()
	if h == nil {
		return nil, apierror.NotFound("worker is not executing actor %q", actorUID)
	}
	if mgr == nil {
		return nil, apierror.FailedPrecondition("actor %q guest agent is not ready for exec", actorUID)
	}
	return mgr, nil
}

func (s *AteWorkerService) StartProcess(ctx context.Context, req *ateworkerpb.StartProcessRequest) (*ateworkerpb.Process, error) {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return nil, err
	}
	return mgr.StartProcess(ctx, req)
}

func (s *AteWorkerService) GetProcess(_ context.Context, req *ateworkerpb.GetProcessRequest) (*ateworkerpb.Process, error) {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return nil, err
	}
	return mgr.GetProcess(req.GetProcessId())
}

func (s *AteWorkerService) StreamProcessOutput(req *ateworkerpb.StreamProcessOutputRequest, stream ateworkerpb.Worker_StreamProcessOutputServer) error {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return err
	}
	return mgr.StreamProcessOutput(stream.Context(), req, stream.Send)
}

func (s *AteWorkerService) WriteProcessInput(stream ateworkerpb.Worker_WriteProcessInputServer) error {
	return procman.ServeWriteProcessInput(stream, s.lookupProcManager)
}

func (s *AteWorkerService) SignalProcess(ctx context.Context, req *ateworkerpb.SignalProcessRequest) (*ateworkerpb.Process, error) {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return nil, err
	}
	return mgr.SignalProcess(ctx, req.GetProcessId(), req.GetSignal())
}

func (s *AteWorkerService) ReadFile(req *ateworkerpb.ReadFileRequest, stream ateworkerpb.Worker_ReadFileServer) error {
	mgr, err := s.lookupProcManager(req.GetActorUid())
	if err != nil {
		return err
	}
	return mgr.ReadFile(stream.Context(), req, func(chunk []byte) error {
		return stream.Send(&ateworkerpb.ReadFileResponse{Chunk: chunk})
	})
}

func (s *AteWorkerService) WriteFile(stream ateworkerpb.Worker_WriteFileServer) error {
	return procman.ServeWriteFile(stream, s.lookupProcManager)
}
