//go:build !linux

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
)

type runsc struct {
	path                   string
	actorTemplateNamespace string
	actorTemplateName      string
	actorID                string
}

var errUnsupported = errors.New("local runsc execution is only supported on Linux")

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string) error {
	return errUnsupported
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	return errUnsupported
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string) error {
	return errUnsupported
}

func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	return errUnsupported
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	return errUnsupported
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
	return errUnsupported
}

func (r *runsc) cmdPause(ctx context.Context, containerName string) error {
	return errUnsupported
}

func (r *runsc) cmdResume(ctx context.Context, containerName string) error {
	return errUnsupported
}

func setupHostSwap(ctx context.Context) error {
	return nil
}
