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

# Assemble the micro-VM (kata + cloud-hypervisor) runtime asset set that
# ateom-microvm fetches at runtime (fetch-not-bake). Run this on a Linux
# host of the TARGET arch.
#
# Produces, under $OUT, the five assets named as the ActorTemplate expects, plus
# their sha256 sums (paste into demos/counter/counter-microvm.yaml.tmpl):
#   cloud-hypervisor  virtiofsd-patched  vmlinux  rootfs.img  configuration-clh.toml
#
# NB: the kata containerd shim (containerd-shim-kata-v2) is NOT needed — ateom owns
# the cloud-hypervisor boot and drives the kata-agent directly over ttrpc, replacing
# the shim. Only the guest kernel/image + CH + virtiofsd + base config are fetched.
#
# virtiofsd is built from upstream main: the vhost-0.16 snapshot/restore fix (REPLY_ACK)
# is on main but NOT in any release tag yet, so the kata-bundled virtiofsd (v1.13.3 tag,
# old vhost) hangs CH's restore handshake (confirmed on kind/arm64). main needs no manual
# patch (the bump is already there). Needs rust (rustup) + libcap-ng-dev libseccomp-dev
# pkg-config on the build host.
#
# Env: ARCH (arm64|amd64, default arm64), KATA_VER (3.32.0), CH_VER (v52.0),
#      OUT (default ./microvm-assets-$ARCH).

set -o errexit -o nounset -o pipefail

ARCH="${ARCH:-arm64}"
KATA_VER="${KATA_VER:-3.32.0}"
CH_VER="${CH_VER:-v52.0}"
OUT="${OUT:-$PWD/microvm-assets-$ARCH}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

case "$ARCH" in
  arm64) CH_ASSET="cloud-hypervisor-static-aarch64" ;;
  amd64) CH_ASSET="cloud-hypervisor-static" ;;
  *) echo "unsupported ARCH=$ARCH" >&2; exit 1 ;;
esac

mkdir -p "$OUT"
cd "$WORK"

echo ">> Downloading kata-static ${KATA_VER} (${ARCH})..."
curl -fSL -o kata-static.tar.zst \
  "https://github.com/kata-containers/kata-containers/releases/download/${KATA_VER}/kata-static-${KATA_VER}-${ARCH}.tar.zst"
mkdir -p kata
tar --zstd -xf kata-static.tar.zst -C kata
KROOT="kata/opt/kata"

cp "$(readlink -f "${KROOT}/share/kata-containers/vmlinux.container")" "${OUT}/vmlinux"
cp "$(readlink -f "${KROOT}/share/kata-containers/kata-containers.img")" "${OUT}/rootfs.img"
cp "${KROOT}/share/defaults/kata-containers/configuration-clh.toml" "${OUT}/configuration-clh.toml"

echo ">> Downloading cloud-hypervisor ${CH_VER} (${CH_ASSET})..."
curl -fSL -o "${OUT}/cloud-hypervisor" \
  "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CH_VER}/${CH_ASSET}"
chmod +x "${OUT}/cloud-hypervisor"

echo ">> Building virtiofsd from upstream main (vhost 0.16)..."
# The vhost-0.16 / vhost-user-backend-0.22 snapshot-restore fix (REPLY_ACK) is on
# virtiofsd main but NOT in any release tag yet — so the kata-bundled virtiofsd (built
# from the v1.13.3 TAG, old vhost) still hangs CH's restore handshake. Empirically
# confirmed on kind/arm64: kata-bundled -> vm.restore i/o timeout; main -> works.
# Build current main (no manual patch needed; main already has the bump). Keep the
# asset name virtiofsd-patched so the manifest is unchanged. Build deps (Debian):
# apt-get install -y git libcap-ng-dev libseccomp-dev pkg-config; rust via rustup.
if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found; install rust (rustup) + libcap-ng-dev libseccomp-dev pkg-config" >&2
  exit 1
fi
git clone --depth 1 https://gitlab.com/virtio-fs/virtiofsd.git
(
  cd virtiofsd
  grep -E '^(vhost|vhost-user-backend) =' Cargo.toml   # expect vhost 0.16 / backend 0.22
  cargo build --release
)
cp "virtiofsd/target/release/virtiofsd" "${OUT}/virtiofsd-patched"
chmod +x "${OUT}/virtiofsd-patched"

echo
echo ">> Assets assembled in ${OUT}:"
cd "${OUT}"
for f in cloud-hypervisor virtiofsd-patched vmlinux rootfs.img configuration-clh.toml; do
  [ -f "$f" ] || { echo "MISSING: $f" >&2; exit 1; }
done
"${OUT}/virtiofsd-patched" --version 2>/dev/null | head -1 || true
echo
echo ">> sha256 (paste into demos/counter/counter-microvm.yaml.tmpl runtime.assets):"
sha256sum cloud-hypervisor virtiofsd-patched vmlinux rootfs.img configuration-clh.toml
