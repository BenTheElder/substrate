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

// Command ateworker-gvisor runs actors in gVisor sandboxes.
package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/cmd/ateworker-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateworker"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var config = ateworker.Config{
	ServiceName:  "ateworker-gvisor",
	SandboxClass: string(atev1alpha1.SandboxClassGvisor),
	// gVisor keeps rootfs changes inside its memory checkpoint, so it cannot
	// capture ROOTFS without MEMORY.
	Fidelities: []ateworkerpb.SnapshotFidelity{
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
	},
	Veth: true,
}

func main() {
	ateworker.Main(config, newRuntime)
}

func flagsConfigPath() string {
	path := "internal_flags.json"
	if koDataPath := os.Getenv("KO_DATA_PATH"); koDataPath != "" {
		path = filepath.Join(koDataPath, path)
	}
	return path
}

// gvisorRuntime runs each actor as a runsc sandbox.
type gvisorRuntime struct {
	actorLogger *actorlog.ActorLogger
	// flags holds the watched JSON flag configuration.
	flags *flagStore

	// cgroupRoot is where the sandbox's cgroup v2 leaves live, which
	// cgroup.Delegate prepares. A field so tests can point it at a fixture tree.
	cgroupRoot string
	// readSandboxCgroup overrides cgroupstats.Read when set. Only tests set it.
	readSandboxCgroup func(dir string) (cgroupstats.Sample, error)
}

var _ ateworker.Runtime = (*gvisorRuntime)(nil)

func newRuntime(ctx context.Context, env ateworker.Env) (ateworker.Runtime, error) {
	flags, err := newFlagStore(ctx, flagsConfigPath())
	if err != nil {
		return nil, err
	}
	return &gvisorRuntime{
		actorLogger: env.ActorLogger,
		flags:       flags,
		cgroupRoot:  defaultCgroupRoot,
	}, nil
}

// Validate ensures we only execute runsc from the static files dir.
func (r *gvisorRuntime) Validate(a *ateworker.Actor) error {
	if errs := resources.ValidateRuntimeAssetPath(nodepath.StaticFilesDir, a.RunscPath, field.NewPath("runsc_path")); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// runsc drives the actor's sandbox. Checkpoint and teardown apply no sizing,
// so a.Size is zero there.
func (r *gvisorRuntime) runsc(a *ateworker.Actor) *runsc {
	return &runsc{
		path:           a.RunscPath,
		actorUID:       a.Attribution.UID,
		actorDirs:      a.Dirs,
		size:           a.Size,
		durableVolumes: durableVolumeNames(a.Spec),
		containers:     a.Containers(),
		flags:          r.flags,
	}
}
