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
	"github.com/agent-substrate/substrate/internal/activation"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
)

// setupBundleRootfs composes one container's rootfs, timed. Worth its own
// wrapper rather than four inline closures: this is the step that takes the
// pod's mount lock, and at thousands of actors the worker's mount namespace
// holds thousands of overlays, so whether it stays flat is a question the
// breakdown has to be able to answer. (Measured: it does not. One mount costs
// ~0.5ms on an empty worker and ~18ms on a full one.)
func setupBundleRootfs(act *activation.Activation, actorUID, containerName string) error {
	return act.Step(ateattr.ActivationPhaseBundleRootfs, func() error {
		return imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(actorUID, containerName))
	})
}
