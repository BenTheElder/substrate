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
# Self-contained bench runner for the GCE host: cleans any stray runs, installs
# the latest binary (if staged at /tmp/bench.new), runs a small matrix, and
# persists results to ~/suspend-bench/results/. Designed to be triggered by a
# trivial fire-and-forget SSH one-liner (the corp tunnel drops long sessions):
#
#   nohup bash ~/suspend-bench/hack/run-matrix.sh >/tmp/bench.out 2>&1 & echo GO
set -uo pipefail

cd "$(dirname "$0")/.."
RESULTS_DIR=~/suspend-bench/results
mkdir -p "$RESULTS_DIR"
OUT="$RESULTS_DIR/results.jsonl"

# Clean strays from prior (possibly tunnel-killed) runs.
sudo pkill -9 -f bin/bench 2>/dev/null
sudo pkill -9 -f cloud-hypervisor 2>/dev/null
sleep 2
for d in /sys/fs/cgroup/suspend-bench/*/; do sudo rmdir "$d" 2>/dev/null; done
sudo rm -rf /run/suspend-bench/* 2>/dev/null

# Install freshly-staged binary if present.
[ -f /tmp/bench.new ] && install -m0755 /tmp/bench.new ~/suspend-bench/bin/bench

# Small matrix: swap vs checkpoint (copy + ondemand) + coldstart baseline, with
# memfd sparse snapshots, across two working sets. 1 GiB guest. Results stream to
# the persistent boot disk.
sudo ./bin/bench \
  --runtimes ch \
  --mechanisms checkpoint_local,swap,coldstart \
  --restore-modes copy,ondemand \
  --shared-mem \
  --workloads c \
  --working-sets "${WS:-64MiB,256MiB}" \
  --guest-mem "${GUEST_MEM:-1073741824}" \
  --reps "${REPS:-3}" \
  --resume-deadline 30s \
  --cell-timeout 120s \
  --out "$OUT"

echo "MATRIX_DONE -> $OUT"
