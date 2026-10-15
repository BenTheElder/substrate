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

// The phase names this runtime adds to the worker's timing records. The
// app_* phases are summed over the application containers, which are restored
// one after another.
const (
	phasePauseRootfs  = "pause_rootfs"
	phasePauseCreate  = "pause_create"
	phasePauseRestore = "pause_restore"
	phaseAppRootfs    = "app_rootfs"
	phaseAppCreate    = "app_create"
	phaseAppRestore   = "app_restore"

	phaseCheckpoint = "checkpoint"
	phaseDurableDir = "durable_dir"
)
