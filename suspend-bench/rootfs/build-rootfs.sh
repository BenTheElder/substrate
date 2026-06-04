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
# Build a virtio-blk ext4 rootfs image from a container image. The workload ships
# inside the block image (NOT virtio-fs, which breaks cloud-hypervisor restore),
# and the only host<->guest channel is virtio-vsock (set up by the init shim).
#
# The first argument is the flattened image filesystem, given as EITHER:
#   - a tarball path (e.g. produced by `podman export` / `docker export`), or
#   - an image ref readable by crane (e.g. a registry ref, used when no tar).
# A tar is preferred on the bench VM since podman has no daemon for crane to read.
#
# Usage:
#   build-rootfs.sh <rootfs.tar | image-ref> <output.img> [size]
# Example:
#   build-rootfs.sh /tmp/cworkload.tar /mnt/nvme-images/rootfs/cworkload.img 512MiB
#
# Writes <output.img>.meta.json recording the source so each result row can be
# tied to an exact rootfs. Needs root (loop mount). Overwrites the output image.
set -euo pipefail

log() { printf '\033[1;34m[rootfs]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[rootfs] FATAL:\033[0m %s\n' "$*" >&2; exit 1; }

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${1:?usage: build-rootfs.sh <rootfs.tar | image-ref> <output.img> [size]}"
OUT_IMG="${2:?usage: build-rootfs.sh <rootfs.tar | image-ref> <output.img> [size]}"
SIZE="${3:-1GiB}"

[[ "${EUID}" -eq 0 ]] || die "must run as root (loop mount)"
command -v mkfs.ext4 >/dev/null || die "mkfs.ext4 not installed"

INIT_BIN="${HERE}/../workloads/initshim/init"
[[ -x "${INIT_BIN}" || -f "${INIT_BIN}" ]] || die "init shim not built; run: make -C workloads/initshim"

TMP="$(mktemp -d)"; MNT="${TMP}/mnt"; mkdir -p "${MNT}"
cleanup() { mountpoint -q "${MNT}" && umount "${MNT}"; rm -rf "${TMP}"; }
trap cleanup EXIT

# --- 1. Obtain the flattened image filesystem as a tar. ----------------------
if [[ -f "${SRC}" ]]; then
  log "using rootfs tar ${SRC}"
  cp "${SRC}" "${TMP}/rootfs.tar"
  DIGEST="tar:$(sha256sum "${SRC}" | cut -d' ' -f1)"
else
  command -v crane >/dev/null || die "crane not installed and ${SRC} is not a tar file"
  log "exporting ${SRC} filesystem via crane"
  crane export "${SRC}" "${TMP}/rootfs.tar"
  DIGEST="$(crane digest "${SRC}" 2>/dev/null || echo unknown)"
fi

# --- 2. Make a sparse ext4 image. --------------------------------------------
log "creating ${SIZE} ext4 image at ${OUT_IMG}"
mkdir -p "$(dirname "${OUT_IMG}")"
rm -f "${OUT_IMG}"
truncate -s "${SIZE}" "${OUT_IMG}"
mkfs.ext4 -F -q "${OUT_IMG}"

# --- 3. Populate. ------------------------------------------------------------
mount -o loop "${OUT_IMG}" "${MNT}"
tar -C "${MNT}" -xf "${TMP}/rootfs.tar"

# Install the static PID1 init shim as /sbin/init (the kernel cmdline uses
# init=/sbin/init). It mounts /proc /sys /dev, forks the workload, and runs the
# vsock ping responder.
install -D -m 0755 "${INIT_BIN}" "${MNT}/sbin/init"
mkdir -p "${MNT}/proc" "${MNT}/sys" "${MNT}/dev" "${MNT}/tmp"
sync

# --- 4. Record provenance. ---------------------------------------------------
META="${OUT_IMG}.meta.json"
jq -n --arg ref "${SRC}" --arg digest "${DIGEST}" --arg size "${SIZE}" \
  '{source:$ref, image_digest:$digest, size:$size, fs:"ext4", iface:"virtio-blk"}' \
  > "${META}"
log "rootfs ready: ${OUT_IMG} (digest=${DIGEST}); meta -> ${META}"
