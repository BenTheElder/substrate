#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Rebuild kata-agent without the features we do not use and patch it into the guest
# image, in place.
#
# The agent is the single largest thing the guest reads at boot -- 25.2 MiB of ~35 MiB
# with the agent as PID 1, read in full -- and kata's release pipeline builds it with
# the OPA/regorus policy engine and initdata support enabled
# (kata-deploy-binaries.sh defaults AGENT_POLICY=yes even though src/agent/Makefile
# defaults it to no). ateom never sends a policy and never uses initdata, so both are
# dead weight in every boot and every snapshot.
#
# Measured on GKE (amd64, counter demo, medians of 7-9 cold bakes each):
#
#   agent            binary     agent_dial   since_boot   golden snapshot
#   stock (kata's)   30.63 MB     377 ms       498 ms       27.7 MiB
#   this script      18.59 MB     356 ms       468 ms       24.3 MiB
#
# ~41 ms off a ~500 ms cold boot and ~3.5 MiB off the compressed golden, which is
# object-store cost, suspend upload and cross-node resume download.
#
# The image is patched with debugfs rather than a loop mount so this needs no root:
# write the new binary, restore its mode/uid/gid, then let e2fsck settle the bitmaps.
#
# Env: ARCH (arm64|amd64), KATA_VER, IMAGE (the rootfs.img to patch, in place),
#      OPT_LEVEL (default s; z measured the same within noise and is ~0.5 MB smaller,
#      3 is upstream's default and ~2.8 MB larger).

set -o errexit -o nounset -o pipefail

ARCH="${ARCH:-arm64}"
KATA_VER="${KATA_VER:-4.0.0}"
IMAGE="${IMAGE:?IMAGE (path to rootfs.img) is required}"
OPT_LEVEL="${OPT_LEVEL:-s}"
# Partition 1 of the kata image; debugfs addresses the filesystem at this offset.
ROOTFS_OFFSET=3145728
# Staged beside the image rather than under TMPDIR: the build and the patch both run in
# a container with this directory bind-mounted, and macOS's default TMPDIR (/var/folders)
# is not a path Docker Desktop shares, so the mount comes up empty and debugfs fails on a
# rootfs.img that is present on the host. The image's own directory is necessarily
# visible to whoever is assembling it.
WORK="$(mktemp -d "$(dirname "${IMAGE}")/.slim-agent.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

case "$ARCH" in
  arm64|amd64) ;;
  *) echo "unsupported ARCH=$ARCH" >&2; exit 1 ;;
esac

command -v docker >/dev/null || { echo "docker is required to build the agent" >&2; exit 1; }

cp "$IMAGE" "${WORK}/rootfs.img"

echo ">> Building kata-agent ${KATA_VER} (${ARCH}, no policy/initdata, opt-level=${OPT_LEVEL})..."
docker run --rm --platform "linux/${ARCH}" -v "${WORK}:/work" -w /work rust:bookworm bash -eu -c '
  apt-get update -qq
  apt-get install -y -qq build-essential clang pkg-config protobuf-compiler \
    libseccomp-dev libdevmapper-dev git e2fsprogs >/dev/null
  git clone --depth 1 --branch "'"${KATA_VER}"'" \
    https://github.com/kata-containers/kata-containers /work/kata
  cd /work/kata/src/agent
  # SECCOMP stays on: it is what applies a container seccomp profile inside the
  # guest, and it measured free (identical stripped size with and without).
  LIBC=gnu SECCOMP=yes AGENT_POLICY=no INIT_DATA=no \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    CARGO_PROFILE_RELEASE_OPT_LEVEL='"${OPT_LEVEL}"' \
    make
  cp /work/kata/target/*-unknown-linux-gnu/release/kata-agent /work/kata-agent
  strip --strip-all /work/kata-agent

  # Patch it in. kill_file frees the old blocks (rm only unlinks), and sif restores
  # the mode/uid/gid that write does not carry over.
  cd /work
  debugfs -w -f - "rootfs.img?offset='"${ROOTFS_OFFSET}"'" >/dev/null <<EOF
cd /usr/bin
kill_file kata-agent
rm kata-agent
write /work/kata-agent kata-agent
sif kata-agent mode 0100755
sif kata-agent uid 1001
sif kata-agent gid 1001
EOF
  e2fsck -fy "rootfs.img?offset='"${ROOTFS_OFFSET}"'" >/dev/null || true
  debugfs -R "stat /usr/bin/kata-agent" "rootfs.img?offset='"${ROOTFS_OFFSET}"'" 2>/dev/null \
    | grep -E "Mode|Size:"
'

cp "${WORK}/rootfs.img" "$IMAGE"
echo ">> Patched $(basename "$IMAGE"): agent is now $(stat -c %s "${WORK}/kata-agent" 2>/dev/null || stat -f %z "${WORK}/kata-agent") bytes"
