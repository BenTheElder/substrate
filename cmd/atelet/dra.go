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
	"log/slog"
	"os"

	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/dra"
)

const (
	// hostDevRoot is where the node's /dev is mounted into atelet (see
	// manifests/ate-install/atelet.yaml), read only to detect which device nodes
	// exist; workers are handed the real host paths, which the runtime resolves.
	hostDevRoot = "/host/dev"

	// cdiSpecDir is a directory the container runtime scans for CDI specs.
	cdiSpecDir = "/var/run/cdi"

	// kubeletRegistrarDir is where kubelet watches for plugin registration
	// sockets. Its absence means there is nobody to register with.
	kubeletRegistrarDir = "/var/lib/kubelet/plugins_registry"
)

// startDRADriver offers the sandbox host devices present on this node through
// dynamic resource allocation, in the background for the lifetime of ctx. This
// is what lets a worker be granted /dev/kvm without running privileged. atelet
// already runs per node, so it hosts the driver rather than adding a DaemonSet.
//
// Failures are logged, never fatal: a node that cannot offer devices still runs
// every sandbox class that needs none.
func startDRADriver(ctx context.Context, kube kubernetes.Interface) {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		slog.ErrorContext(ctx, "NODE_NAME unset; not offering host devices")
		return
	}
	// atelet runs in environments that do not mount the kubelet directories
	// (tests, minimal installs), where retrying forever would just be noise.
	if _, err := os.Stat(kubeletRegistrarDir); err != nil {
		slog.InfoContext(ctx, "Kubelet plugin registry unavailable; not offering host devices",
			slog.String("path", kubeletRegistrarDir), slog.Any("err", err))
		return
	}

	devices := dra.Available(dra.SandboxDevices, hostDevRoot)
	if len(devices) == 0 {
		slog.InfoContext(ctx, "No sandbox host devices present on this node; not offering any",
			slog.String("devRoot", hostDevRoot))
		return
	}

	helper, err := dra.Start(ctx, dra.Options{
		KubeClient: kube,
		NodeName:   nodeName,
		Devices:    devices,
		CDIDir:     cdiSpecDir,
	})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to start DRA driver", slog.Any("err", err))
		return
	}
	names := make([]string, 0, len(devices))
	for _, d := range devices {
		names = append(names, d.Name)
	}
	slog.InfoContext(ctx, "DRA driver offering host devices",
		slog.String("driver", dra.DriverName), slog.Any("devices", names))

	go func() {
		<-ctx.Done()
		helper.Stop()
	}()
}
