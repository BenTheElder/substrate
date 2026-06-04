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
# Create a swapfile on the NVMe swap mount and configure zswap. GCE images ship
# with no swap, which the swap-suspend mechanism requires.
#
# zswap is the "compress-in-RAM" tier that the benchmark toggles on/off (Stage 2
# diagnostic) to attribute any swap win to compression vs the suspend mechanism.
# This script enables it; the harness flips /sys/module/zswap/parameters/enabled
# per-run for the on/off comparison.
#
# !!! IMPORTANT: the stock Debian 13 *cloud* kernel ships with CONFIG_ZSWAP UNSET
# (verified: `zgrep CONFIG_ZSWAP /boot/config-$(uname -r)` -> "is not set"), so
# /sys/module/zswap does not exist and the zswap setup below is a no-op there.
# On that kernel "swap" is plain on-disk swap (no compress-in-RAM). To run the
# zswap diagnostic you need a kernel with CONFIG_ZSWAP=y (e.g. the Debian generic
# linux-image-amd64, or a custom build) — see FINDINGS.md "Open items".
#
# Idempotent. Override SWAP_SIZE_GB / SWAPPINESS / ZSWAP_* via env.
set -euo pipefail

log() { printf '\033[1;34m[swap]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[swap] FATAL:\033[0m %s\n' "$*" >&2; exit 1; }

SWAP_MNT=/mnt/nvme-swap
SWAPFILE="${SWAP_MNT}/swapfile"
# Sized > the largest guest RAM we expect to fully evict (n2-standard-16 has 64GB;
# leave headroom for the host + multiple resident sandboxes).
SWAP_SIZE_GB="${SWAP_SIZE_GB:-48}"
SWAPPINESS="${SWAPPINESS:-60}"
ZSWAP_COMPRESSOR="${ZSWAP_COMPRESSOR:-zstd}"
ZSWAP_ZPOOL="${ZSWAP_ZPOOL:-zsmalloc}"
ZSWAP_MAX_POOL_PERCENT="${ZSWAP_MAX_POOL_PERCENT:-50}"

mountpoint -q "${SWAP_MNT}" || die "${SWAP_MNT} not mounted — run setup-nvme.sh first"

# --- Swapfile. ---------------------------------------------------------------
if swapon --show=NAME --noheadings | grep -qx "${SWAPFILE}"; then
  log "swapfile already active: ${SWAPFILE}"
else
  if [[ ! -f "${SWAPFILE}" ]]; then
    log "creating ${SWAP_SIZE_GB}GiB swapfile at ${SWAPFILE}"
    # fallocate is fine on ext4 for swap (no holes once mkswap formats it).
    fallocate -l "${SWAP_SIZE_GB}G" "${SWAPFILE}"
    chmod 0600 "${SWAPFILE}"
    mkswap "${SWAPFILE}" >/dev/null
  fi
  swapon "${SWAPFILE}"
  log "swapon ${SWAPFILE}"
fi

sysctl -w "vm.swappiness=${SWAPPINESS}" >/dev/null
log "vm.swappiness=${SWAPPINESS}"

# --- zswap. ------------------------------------------------------------------
ZP=/sys/module/zswap/parameters
if [[ -d "${ZP}" ]]; then
  echo "${ZSWAP_COMPRESSOR}"       > "${ZP}/compressor" 2>/dev/null || log "warn: cannot set zswap compressor"
  echo "${ZSWAP_ZPOOL}"            > "${ZP}/zpool"      2>/dev/null || log "warn: cannot set zswap zpool"
  echo "${ZSWAP_MAX_POOL_PERCENT}" > "${ZP}/max_pool_percent" 2>/dev/null || true
  echo 1                           > "${ZP}/enabled"
  log "zswap enabled: compressor=$(cat ${ZP}/compressor) zpool=$(cat ${ZP}/zpool) max_pool_percent=$(cat ${ZP}/max_pool_percent)"
else
  log "warn: ${ZP} not present — zswap module not loaded? swap will be uncompressed (disk only)"
fi

log "swap status:"; swapon --show
