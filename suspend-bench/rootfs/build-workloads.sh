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
# One-shot: build the workload container images, the init shim, the CH ext4
# rootfs images, and the gVisor OCI bundles. Run on the bench VM (needs root for
# the loop mount in build-rootfs.sh). Override the image builder via BUILDER
# (docker|podman) and output locations via the *_DIR vars.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"
# BUILDER may be multi-word, e.g. "sudo podman" (rootful, needed when the login
# user has no subuid/subgid ranges for rootless mode).
BUILDER="${BUILDER:-podman}"
read -ra BUILDER_BIN <<< "${BUILDER}"
IMG_DIR="${IMG_DIR:-/mnt/nvme-images/rootfs}"
BUNDLE_DIR="${BUNDLE_DIR:-/mnt/nvme-images/bundles}"
C_SIZE="${C_SIZE:-512MiB}"     # small static workload
NODE_SIZE="${NODE_SIZE:-2GiB}" # node + socat userspace

log() { printf '\033[1;34m[workloads]\033[0m %s\n' "$*"; }

# 1. init shim (static PID1) — needed by build-rootfs.sh.
log "building init shim"
make -C "${ROOT}/workloads/initshim"

# 2. container images.
log "building images with ${BUILDER}"
"${BUILDER_BIN[@]}" build -t suspend-bench/cworkload:latest "${ROOT}/workloads/cworkload"
"${BUILDER_BIN[@]}" build -t suspend-bench/nodeworkload:latest "${ROOT}/workloads/nodeworkload"

# 3. Flatten each image to a rootfs tar (builder-agnostic; podman has no daemon
#    for crane to read, so we export the merged filesystem directly).
export_tar() {
  local img="$1" out="$2" cid
  cid="$("${BUILDER_BIN[@]}" create "${img}")"
  "${BUILDER_BIN[@]}" export "${cid}" -o "${out}"
  "${BUILDER_BIN[@]}" rm "${cid}" >/dev/null
}
TARS="$(mktemp -d)"
trap 'rm -rf "${TARS}"' EXIT
log "exporting rootfs tars"
export_tar suspend-bench/cworkload:latest   "${TARS}/cworkload.tar"
export_tar suspend-bench/nodeworkload:latest "${TARS}/nodeworkload.tar"
sudo chmod 0644 "${TARS}"/*.tar  # rootful builder writes root-owned tars

# 4. CH ext4 rootfs images (need root for the loop mount + the NVMe dir).
log "building CH ext4 rootfs images -> ${IMG_DIR}"
sudo "${HERE}/build-rootfs.sh" "${TARS}/cworkload.tar"   "${IMG_DIR}/cworkload.img"   "${C_SIZE}"
sudo "${HERE}/build-rootfs.sh" "${TARS}/nodeworkload.tar" "${IMG_DIR}/nodeworkload.img" "${NODE_SIZE}"

# 5. gVisor OCI bundles (sudo: the NVMe mount is root-owned).
log "building gVisor bundles -> ${BUNDLE_DIR}"
sudo "${HERE}/build-bundle.sh" "${TARS}/cworkload.tar"   "${BUNDLE_DIR}/cworkload"
sudo "${HERE}/build-bundle.sh" "${TARS}/nodeworkload.tar" "${BUNDLE_DIR}/nodeworkload"

log "done. Now: ./rootfs/fetch-kernel.sh (if not yet), then sudo bin/bench --smoke"
