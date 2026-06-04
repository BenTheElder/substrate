#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Prepare the cgroup v2 tree the harness uses for per-instance memory accounting
# and swap-suspend. Each VMM/sandbox is launched directly into its own leaf
# (suspend-bench/<id>) so cgroup v2's "charges don't migrate on move" rule never
# bites us (the poc2 finding) — we drive memory.reclaim / memory.swap.max on the
# leaf where the pages were first faulted.
#
# Idempotent.
set -euo pipefail

log() { printf '\033[1;34m[cgroup]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[cgroup] FATAL:\033[0m %s\n' "$*" >&2; exit 1; }

CG_ROOT=/sys/fs/cgroup
PARENT="${CG_ROOT}/suspend-bench"

# Confirm unified cgroup v2.
grep -q cgroup2 /proc/filesystems || die "cgroup v2 not available"
[[ -f "${CG_ROOT}/cgroup.controllers" ]] || die "${CG_ROOT} is not a cgroup v2 root"

# Enable +memory at the root so children can be delegated the memory controller.
if ! grep -qw memory "${CG_ROOT}/cgroup.subtree_control"; then
  echo "+memory" > "${CG_ROOT}/cgroup.subtree_control" 2>/dev/null \
    || log "warn: could not add +memory to root subtree_control (may already be managed by systemd)"
fi

mkdir -p "${PARENT}"
# Delegate memory down to the per-instance leaves the harness will create.
echo "+memory" > "${PARENT}/cgroup.subtree_control"

grep -qw memory "${PARENT}/cgroup.controllers" \
  || die "memory controller not delegated to ${PARENT}; check root subtree_control / systemd delegation"

log "cgroup parent ready: ${PARENT} (controllers: $(cat ${PARENT}/cgroup.controllers))"
log "harness will create leaves ${PARENT}/<instance-id> with memory.reclaim / memory.swap.max"
