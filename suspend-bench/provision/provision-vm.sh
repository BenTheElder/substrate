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
# Idempotent one-time host setup for the suspend-bench VM (Debian 13 / trixie,
# n2-standard-16, nested-virt on, 2x local NVMe SSD). Run with sudo.
#
# Installs: base packages, a pinned cloud-hypervisor + ch-remote, a pinned runsc,
# then delegates NVMe / swap+zswap / cgroup setup to the sibling scripts and
# finishes with a sanity gate (boot a trivial CH VM + ping; runsc --version).
#
# All pinned versions are overridable via env. They are RECORDED into
# /var/lib/suspend-bench/host-metadata.json so every result row can be tied to an
# exact toolchain (CH restore/ondemand behavior is version-sensitive).
set -euo pipefail

# --- Pinned versions (override via env). -------------------------------------
# NOTE: cloud-hypervisor must support BOTH features we rely on (v52.0 does, and is
# what FINDINGS.md was measured on):
#   - `memory_restore_mode=ondemand` (userfaultfd lazy restore)
#   - sparse snapshots of memfd-backed RAM (PR #8113) — needs shared=true guest RAM
CH_VERSION="${CH_VERSION:-v52.0}"
RUNSC_VERSION="${RUNSC_VERSION:-latest}"   # gVisor release channel or release-YYYYMMDD.N
KATA_VERSION="${KATA_VERSION:-3.20.0}"     # used by rootfs/fetch-kernel.sh for vmlinux

BIN_DIR="${BIN_DIR:-/usr/local/bin}"
META_DIR="/var/lib/suspend-bench"
META_FILE="${META_DIR}/host-metadata.json"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '\033[1;34m[provision]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[provision] FATAL:\033[0m %s\n' "$*" >&2; exit 1; }

[[ "${EUID}" -eq 0 ]] || die "must run as root (sudo)"

# --- 1. Base packages. -------------------------------------------------------
log "installing base packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
  qemu-utils e2fsprogs mdadm skopeo podman nftables build-essential jq curl ca-certificates \
  socat util-linux cgroup-tools xz-utils file zstd lz4

# crane (go-containerregistry) is not packaged; install if missing.
if ! command -v crane >/dev/null 2>&1; then
  log "installing crane"
  CRANE_VER="${CRANE_VER:-v0.20.2}"
  curl -fsSL "https://github.com/google/go-containerregistry/releases/download/${CRANE_VER}/go-containerregistry_Linux_x86_64.tar.gz" \
    | tar -xz -C "${BIN_DIR}" crane
fi

# --- 2. KVM sanity (nested virt). --------------------------------------------
[[ -e /dev/kvm ]] || die "/dev/kvm missing — is nested virtualization enabled on this VM?"
[[ -r /dev/kvm && -w /dev/kvm ]] || die "/dev/kvm not readable/writable by root"
log "KVM present: $(ls -l /dev/kvm)"

# --- 3. cloud-hypervisor + ch-remote (pinned). -------------------------------
if [[ "$("${BIN_DIR}/cloud-hypervisor" --version 2>/dev/null || true)" != *"${CH_VERSION#v}"* ]]; then
  log "installing cloud-hypervisor ${CH_VERSION}"
  curl -fsSL -o "${BIN_DIR}/cloud-hypervisor" \
    "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CH_VERSION}/cloud-hypervisor-static"
  curl -fsSL -o "${BIN_DIR}/ch-remote" \
    "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CH_VERSION}/ch-remote-static"
  chmod 0755 "${BIN_DIR}/cloud-hypervisor" "${BIN_DIR}/ch-remote"
fi
CH_REPORTED="$("${BIN_DIR}/cloud-hypervisor" --version)"
log "cloud-hypervisor: ${CH_REPORTED}"

# --- 4. runsc (pinned). ------------------------------------------------------
if ! command -v runsc >/dev/null 2>&1; then
  log "installing runsc ${RUNSC_VERSION}"
  curl -fsSL -o "${BIN_DIR}/runsc" \
    "https://storage.googleapis.com/gvisor/releases/release/${RUNSC_VERSION}/x86_64/runsc"
  chmod 0755 "${BIN_DIR}/runsc"
fi
RUNSC_REPORTED="$(runsc --version | head -1)"
log "runsc: ${RUNSC_REPORTED}"

# --- 5. NVMe / swap+zswap / cgroup. ------------------------------------------
KATA_VERSION="${KATA_VERSION}" "${HERE}/setup-nvme.sh"
"${HERE}/setup-swap.sh"
"${HERE}/setup-cgroup.sh"

# --- 6. Record host metadata. ------------------------------------------------
mkdir -p "${META_DIR}"
jq -n \
  --arg ch "${CH_REPORTED}" \
  --arg runsc "${RUNSC_REPORTED}" \
  --arg kata "${KATA_VERSION}" \
  --arg kernel_uname "$(uname -r)" \
  --arg cpu "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')" \
  --argjson nested "$([[ "$(cat /sys/module/kvm_intel/parameters/nested 2>/dev/null || echo N)" =~ ^(Y|1)$ ]] && echo true || echo false)" \
  --arg swappiness "$(cat /proc/sys/vm/swappiness)" \
  --arg zswap "$(cat /sys/module/zswap/parameters/enabled 2>/dev/null || echo unknown)" \
  --arg zswap_compressor "$(cat /sys/module/zswap/parameters/compressor 2>/dev/null || echo unknown)" \
  --arg zswap_zpool "$(cat /sys/module/zswap/parameters/zpool 2>/dev/null || echo unknown)" \
  '{cloud_hypervisor:$ch, runsc:$runsc, kata_version:$kata, host_kernel:$kernel_uname,
    cpu:$cpu, nested_virt:$nested, swappiness:($swappiness|tonumber),
    zswap:{enabled:$zswap, compressor:$zswap_compressor, zpool:$zswap_zpool}}' \
  > "${META_FILE}"
log "wrote host metadata -> ${META_FILE}"

# --- 7. Sanity gate. ---------------------------------------------------------
log "sanity gate: cloud-hypervisor + runsc respond"
"${BIN_DIR}/cloud-hypervisor" --version >/dev/null || die "cloud-hypervisor broken"
runsc --version >/dev/null || die "runsc broken"
log "provisioning complete. Next: ./rootfs/fetch-kernel.sh && ./rootfs/build-rootfs.sh ..."
