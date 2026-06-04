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
# Fetch a cloud-hypervisor-bootable guest kernel. We reuse Kata Containers'
# published, CH-targeted `vmlinux` (uncompressed ELF, with virtio_blk + virtio_vsock
# built in) rather than building our own — it is exactly what Kata boots under CH.
#
# Output: ./kernel/vmlinux (+ kernel/kernel-metadata.json with the version/sha).
# Idempotent: re-extracts only if missing. Override KATA_VERSION / OUT_DIR via env.
set -euo pipefail

log() { printf '\033[1;34m[kernel]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[kernel] FATAL:\033[0m %s\n' "$*" >&2; exit 1; }

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KATA_VERSION="${KATA_VERSION:-3.20.0}"
OUT_DIR="${OUT_DIR:-${HERE}/kernel}"
VMLINUX="${OUT_DIR}/vmlinux"

mkdir -p "${OUT_DIR}"

if [[ -f "${VMLINUX}" ]]; then
  log "vmlinux already present: ${VMLINUX}"
else
  TARBALL="kata-static-${KATA_VERSION}-amd64.tar.xz"
  URL="https://github.com/kata-containers/kata-containers/releases/download/${KATA_VERSION}/${TARBALL}"
  CACHE="${OUT_DIR}/${TARBALL}"
  TMP="$(mktemp -d)"; trap 'rm -rf "${TMP}"' EXIT
  if [[ ! -f "${CACHE}" ]]; then
    log "downloading ${URL}"
    curl -fsSL -o "${CACHE}" "${URL}"
  else
    log "using cached ${CACHE}"
  fi
  log "extracting vmlinux"
  # Kata ships several kernels; we want the canonical CH one (vmlinux.container
  # symlink -> vmlinux-<ver>, the uncompressed ELF, not the *-confidential /
  # *-nvidia-gpu / *-dragonball variants, not vmlinuz, not *.container files).
  tar -C "${TMP}" -xJf "${CACHE}" --wildcards --no-anchored '*/vmlinux*' || true
  KDIR="${TMP}/opt/kata/share/kata-containers"
  SRC=""
  if [[ -L "${KDIR}/vmlinux.container" ]]; then
    SRC="$(readlink -f "${KDIR}/vmlinux.container" 2>/dev/null || true)"
  fi
  if [[ -z "${SRC}" || ! -f "${SRC}" ]]; then
    SRC="$(find "${KDIR}" -maxdepth 1 -type f -name 'vmlinux-*' \
      ! -name '*.container' ! -name '*confidential*' ! -name '*nvidia*' ! -name '*dragonball*' \
      2>/dev/null | sort | head -1)"
  fi
  [[ -n "${SRC}" && -f "${SRC}" ]] || die "could not find a plain vmlinux-* in ${TARBALL}; inspect layout"
  log "selected $(basename "${SRC}")"
  cp "${SRC}" "${VMLINUX}"
fi

# --- Verify it is what cloud-hypervisor can boot. ---------------------------
FILETYPE="$(file -b "${VMLINUX}")"
case "${FILETYPE}" in
  *ELF*) : ;;  # good: uncompressed ELF vmlinux
  *) die "vmlinux is not an ELF image (got: ${FILETYPE}); cloud-hypervisor needs uncompressed vmlinux, not bzImage" ;;
esac

# Best-effort check that the virtio drivers we depend on are built in (the whole
# design — block rootfs + vsock control plane — relies on these).
if command -v strings >/dev/null 2>&1; then
  for need in virtio_vsock virtio_blk; do
    strings "${VMLINUX}" | grep -qi "${need}" \
      || log "warn: '${need}' string not found in vmlinux; verify the guest can use it"
  done
fi

SHA="$(sha256sum "${VMLINUX}" | cut -d' ' -f1)"
jq -n --arg v "${KATA_VERSION}" --arg sha "${SHA}" --arg file "${FILETYPE}" \
  '{source:"kata-static", kata_version:$v, sha256:$sha, file_type:$file}' \
  > "${OUT_DIR}/kernel-metadata.json"
log "ready: ${VMLINUX} (sha256=${SHA:0:12}…, ${FILETYPE})"
