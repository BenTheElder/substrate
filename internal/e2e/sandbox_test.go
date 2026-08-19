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

package e2e

import (
	"os"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// fixtureManifests are every template RenderFixtureManifest is asked to render.
var fixtureManifests = []string{
	"internal/e2e/fixtures/probe/probe.yaml.tmpl",
	"internal/e2e/fixtures/probe/probe-sized.yaml.tmpl",
}

// renderedFixture is the subset of a rendered fixture document the assertions
// below care about. Every field is optional: one document is a Namespace.
type renderedFixture struct {
	Kind string `json:"kind"`
	Spec struct {
		AteomImage        string `json:"ateomImage"`
		SandboxClass      string `json:"sandboxClass"`
		SandboxConfigName string `json:"sandboxConfigName"`
		Resources         *struct {
			Limits map[string]string `json:"limits"`
		} `json:"resources"`
		SnapshotsConfig struct {
			Location string `json:"location"`
		} `json:"snapshotsConfig"`
	} `json:"spec"`
}

// decodeFixture renders a manifest and returns its documents by kind. It fails
// the test on any document that is not valid YAML, which is the thing most
// likely to break: the runtime blocks are injected as pre-indented text.
func decodeFixture(t *testing.T, relPath string) map[string]renderedFixture {
	t.Helper()
	raw, err := os.ReadFile(RenderFixtureManifest(t, relPath, "test-bucket"))
	if err != nil {
		t.Fatalf("reading the rendered %s: %v", relPath, err)
	}
	byKind := map[string]renderedFixture{}
	for doc := range strings.SplitSeq(string(raw), "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var out renderedFixture
		if err := yaml.Unmarshal([]byte(doc), &out); err != nil {
			t.Fatalf("rendered %s is not valid YAML: %v\n%s", relPath, err, doc)
		}
		byKind[out.Kind] = out
	}
	return byKind
}

// TestRenderFixtureManifest_GVisor pins the default rendering: every micro-VM
// block is gone and no placeholder survives, so the gVisor lane keeps applying
// exactly what it applied before the templates were parameterized.
func TestRenderFixtureManifest_GVisor(t *testing.T) {
	t.Setenv(sandboxClassEnv, "")
	for _, relPath := range fixtureManifests {
		t.Run(relPath, func(t *testing.T) {
			docs := decodeFixture(t, relPath)

			pool := docs["WorkerPool"]
			if !strings.HasSuffix(pool.Spec.AteomImage, "/cmd/ateom-gvisor") {
				t.Errorf("WorkerPool ateomImage = %q, want the gVisor ateom", pool.Spec.AteomImage)
			}
			if pool.Spec.SandboxClass != "" || pool.Spec.SandboxConfigName != "" {
				t.Errorf("WorkerPool carries micro-VM runtime fields: class=%q config=%q",
					pool.Spec.SandboxClass, pool.Spec.SandboxConfigName)
			}

			template := docs["ActorTemplate"]
			if template.Spec.SandboxClass != "" {
				t.Errorf("ActorTemplate sandboxClass = %q, want unset for gVisor", template.Spec.SandboxClass)
			}
			// An inline placeholder with an empty value must substitute, not
			// delete its line: the location is what the golden snapshot needs.
			if want := "gs://test-bucket/"; !strings.HasPrefix(template.Spec.SnapshotsConfig.Location, want) {
				t.Errorf("ActorTemplate snapshot location = %q, want it to start with %q",
					template.Spec.SnapshotsConfig.Location, want)
			}
			if strings.HasSuffix(template.Spec.SnapshotsConfig.Location, "-microvm/") {
				t.Errorf("ActorTemplate snapshot location = %q, want no micro-VM suffix",
					template.Spec.SnapshotsConfig.Location)
			}
		})
	}
}

// TestRenderFixtureManifest_MicroVM pins the micro-VM rendering: the pool names
// the cluster-wide SandboxConfig, the template matches its class, and the
// snapshots land under their own prefix.
func TestRenderFixtureManifest_MicroVM(t *testing.T) {
	t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
	for _, relPath := range fixtureManifests {
		t.Run(relPath, func(t *testing.T) {
			docs := decodeFixture(t, relPath)

			pool := docs["WorkerPool"]
			if !strings.HasSuffix(pool.Spec.AteomImage, "/cmd/ateom-microvm") {
				t.Errorf("WorkerPool ateomImage = %q, want the micro-VM ateom", pool.Spec.AteomImage)
			}
			if pool.Spec.SandboxClass != SandboxClassMicroVM || pool.Spec.SandboxConfigName != "microvm" {
				t.Errorf("WorkerPool runtime = class %q / config %q, want microvm / microvm",
					pool.Spec.SandboxClass, pool.Spec.SandboxConfigName)
			}

			template := docs["ActorTemplate"]
			if template.Spec.SandboxClass != SandboxClassMicroVM {
				t.Errorf("ActorTemplate sandboxClass = %q, want %q — it must match the pool's or no worker is eligible",
					template.Spec.SandboxClass, SandboxClassMicroVM)
			}
			// Undeclared limits boot the guest at the kata config default
			// (2GiB), which does not fit beside the demo pools on one kind node.
			if template.Spec.Resources == nil || template.Spec.Resources.Limits["memory"] == "" {
				t.Errorf("ActorTemplate declares no memory limit, so the guest would boot at the kata default: %+v", template.Spec.Resources)
			}
			if want := "-microvm/"; !strings.HasSuffix(template.Spec.SnapshotsConfig.Location, want) {
				t.Errorf("ActorTemplate snapshot location = %q, want it to end with %q",
					template.Spec.SnapshotsConfig.Location, want)
			}
		})
	}
}

// TestCounterFixture covers the knob every suite reads: the class picks the
// fixture, and the explicit environment overrides still win.
func TestCounterFixture(t *testing.T) {
	t.Run("gvisor", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, "")
		if got := CounterFixture(); got.Namespace != "ate-demo-counter" || got.Name != "counter" {
			t.Errorf("CounterFixture() = %+v, want the gVisor counter demo", got)
		}
	})
	t.Run("microvm", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
		if got := CounterFixture(); got.Namespace != "ate-demo-counter-microvm" || got.Name != "counter-microvm" {
			t.Errorf("CounterFixture() = %+v, want the micro-VM counter demo", got)
		}
		if got := EgressFixture(); got.Namespace != "ate-demo-egress-microvm" || got.Name != "egress-microvm" {
			t.Errorf("EgressFixture() = %+v, want the micro-VM egress demo", got)
		}
	})
	t.Run("explicit override wins", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
		t.Setenv("E2E_TEMPLATE_NAMESPACE", "elsewhere")
		t.Setenv("E2E_TEMPLATE_NAME", "other")
		if got := CounterFixture(); got.Namespace != "elsewhere" || got.Name != "other" {
			t.Errorf("CounterFixture() = %+v, want the environment override", got)
		}
	})
}
