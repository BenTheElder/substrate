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
# Format and mount the 2x local NVMe SSDs:
#   nvme #0 -> /mnt/nvme-images   (snapshots + rootfs images)
#   nvme #1 -> /mnt/nvme-swap     (swapfile target; see setup-swap.sh)
#
# NOTE: the two local NVMe SSDs are used as SEPARATE disks, NOT striped (no RAID0).
# Rationale: keep the snapshot write path and the swap path on independent
# spindles so they don't contend during a run. Each GCE local NVMe does ~0.66 GB/s
# write, so the snapshot path is bounded by ONE disk. If snapshot/restore I/O
# becomes the bottleneck (it is largely hidden by page cache on back-to-back
# runs), RAID0 across both would ~2x throughput — at the cost of sharing the
# stripe with swap. To stripe instead:
#     mdadm --create /dev/md0 --level=0 --raid-devices=2 /dev/nvme0n1 /dev/nvme0n2
#     mkfs.ext4 /dev/md0 && mount /dev/md0 /mnt/nvme-images   # put swap elsewhere
#
# WARNING: local NVMe is EPHEMERAL — wiped on VM stop/restart (and not in fstab,
# so not auto-remounted after a guest reboot; re-run this script). Benchmark
# RESULTS go on the persistent boot disk; only large throwaway artifacts live here.
# Idempotent: skips devices that already carry a filesystem and are mounted.
set -euo pipefail

log() { printf '\033[1;34m[nvme]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[nvme] FATAL:\033[0m %s\n' "$*" >&2; exit 1; }

IMAGES_MNT=/mnt/nvme-images
SWAP_MNT=/mnt/nvme-swap

# Local SSDs on GCE NVMe show up as /dev/nvme0nN with model "nvme_card" and are
# distinct from the persistent boot disk (which is typically a SCSI/virtio /dev/sda
# or an NVMe pd- device). Filter to GCE local-ssd by the by-id google-local-nvme-ssd
# symlinks when present; otherwise fall back to listing nvme namespaces.
mapfile -t LOCAL < <(ls /dev/disk/by-id/google-local-nvme-ssd-* 2>/dev/null | sort || true)
if [[ "${#LOCAL[@]}" -eq 0 ]]; then
  mapfile -t LOCAL < <(lsblk -dn -o NAME,TYPE | awk '$2=="disk" && $1 ~ /^nvme/ {print "/dev/"$1}')
fi
[[ "${#LOCAL[@]}" -ge 2 ]] || die "expected >=2 local NVMe devices, found ${#LOCAL[@]}: ${LOCAL[*]:-none}"
log "local NVMe devices: ${LOCAL[*]}"

# STRIPE=1 -> RAID0 all local NVMe into one volume for max snapshot throughput
# (snapshots + swap colocated on the stripe). Default (unset) keeps them separate
# (snapshots on disk 0, swap on disk 1) so the two I/O paths don't contend.
# Use this to measure whether the snapshot/restore path is local-SSD-bound.
if [[ "${STRIPE:-0}" == "1" ]]; then
  command -v mdadm >/dev/null || die "mdadm not installed (add it in provision-vm.sh)"
  MD=/dev/md0
  if ! mountpoint -q "${IMAGES_MNT}"; then
    if [[ ! -b "${MD}" ]]; then
      log "creating RAID0 ${MD} over ${#LOCAL[@]} devices: ${LOCAL[*]}"
      mdadm --create "${MD}" --level=0 --raid-devices="${#LOCAL[@]}" "${LOCAL[@]}" --run
    fi
    blkid "${MD}" >/dev/null 2>&1 || mkfs.ext4 -F -q "${MD}"
    mkdir -p "${IMAGES_MNT}"
    mount -o noatime "${MD}" "${IMAGES_MNT}"
  fi
  # Colocate swap on the stripe so setup-swap.sh (which uses /mnt/nvme-swap) works
  # unchanged; both snapshot and swap I/O ride the striped volume.
  mkdir -p "${IMAGES_MNT}/snapshots" "${IMAGES_MNT}/rootfs" "${IMAGES_MNT}/swap" "${SWAP_MNT}"
  mountpoint -q "${SWAP_MNT}" || mount --bind "${IMAGES_MNT}/swap" "${SWAP_MNT}"
  log "STRIPED ${MD} -> ${IMAGES_MNT} (swap colocated at ${SWAP_MNT}); $(df -h ${IMAGES_MNT} | tail -1)"
  exit 0
fi

mkfs_mount() {
  local dev="$1" mnt="$2"
  # Resolve symlink to the real device node.
  dev="$(readlink -f "${dev}")"
  if mountpoint -q "${mnt}"; then
    log "${mnt} already mounted"
    return 0
  fi
  if ! blkid "${dev}" >/dev/null 2>&1; then
    log "mkfs.ext4 ${dev}"
    mkfs.ext4 -F -q "${dev}"
  fi
  mkdir -p "${mnt}"
  mount -o noatime "${dev}" "${mnt}"
  log "mounted ${dev} -> ${mnt}"
}

mkfs_mount "${LOCAL[0]}" "${IMAGES_MNT}"
mkfs_mount "${LOCAL[1]}" "${SWAP_MNT}"

mkdir -p "${IMAGES_MNT}/snapshots" "${IMAGES_MNT}/rootfs"
log "NVMe ready: $(df -h "${IMAGES_MNT}" "${SWAP_MNT}" | tail -n +2)"
