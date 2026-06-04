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
# Build a gVisor OCI bundle dir from a container image: <bundle>/rootfs holds the
# flattened image. The harness generates config.json at boot via `runsc spec`
# (see internal/runtime/gvisor), so we only need the rootfs here.
#
# The first argument is EITHER a tarball (e.g. from `podman export`) or an image
# ref readable by crane.
#
# Usage: build-bundle.sh <rootfs.tar | image-ref> <bundle-dir>
set -euo pipefail

SRC="${1:?usage: build-bundle.sh <rootfs.tar | image-ref> <bundle-dir>}"
BUNDLE="${2:?usage: build-bundle.sh <rootfs.tar | image-ref> <bundle-dir>}"

ROOTFS="${BUNDLE}/rootfs"
rm -rf "${ROOTFS}"
mkdir -p "${ROOTFS}"
if [[ -f "${SRC}" ]]; then
  echo "[bundle] extracting ${SRC} -> ${ROOTFS}"
  tar -C "${ROOTFS}" -xf "${SRC}"
  sha256sum "${SRC}" | cut -d' ' -f1 > "${BUNDLE}/image-digest.txt"
else
  command -v crane >/dev/null || { echo "crane not installed and ${SRC} is not a tar" >&2; exit 1; }
  echo "[bundle] exporting ${SRC} -> ${ROOTFS}"
  crane export "${SRC}" - | tar -C "${ROOTFS}" -x
  crane digest "${SRC}" > "${BUNDLE}/image-digest.txt" 2>/dev/null || true
fi
echo "[bundle] ready: ${BUNDLE}"
