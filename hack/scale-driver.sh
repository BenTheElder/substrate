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

# Runs hack/scaledriver in the cluster and collects its CSV.
#
# The companion to hack/scale-density.sh: that one measures the whole system
# through ateapi, this one measures the sandbox with ateapi removed. Run both --
# the gap between them is the control plane's share of an activation.
#
# Usage: hack/scale-driver.sh [count] [parallel]
#   SHARDS=<n> hack/scale-driver.sh <count> <parallel>   splits across n driver pods
#   MODE=terminate hack/scale-driver.sh <count>   tears the same actors back down

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

COUNT="${1:-500}"
PARALLEL="${2:-16}"
# A single driver pod tops out well below a wide fleet. SHARDS splits COUNT
# across that many pods, each driving PARALLEL activations at a time.
SHARDS="${SHARDS:-1}"
# Pins the driver pods to a node pool of their own, so the load generator does
# not compete with the fleet it is measuring. Empty means "anywhere".
DRIVER_NODEPOOL="${DRIVER_NODEPOOL:-}"
DRIVER_NODESELECTOR='{}'
if [[ -n "${DRIVER_NODEPOOL}" ]]; then
  DRIVER_NODESELECTOR="{\"cloud.google.com/gke-nodepool\": \"${DRIVER_NODEPOOL}\"}"
fi
MODE="${MODE:-restore}"
# atelet = measure the sandbox, ateapi = measure the whole system. The gap is
# the control plane.
VIA="${VIA:-atelet}"
NAME_PREFIX="${NAME_PREFIX:-d}"
# Generous by default: the ateom's 30s scores a queued activation as a failed
# one, which reads as a ceiling that is not there.
READYZ_TIMEOUT_SECONDS="${READYZ_TIMEOUT_SECONDS:-300}"
TEMPLATE_NS="${TEMPLATE_NS:-ate-scale-tiny}"
TEMPLATE_NAME="${TEMPLATE_NAME:-tiny}"
# The pool defaults to the template's own fixture; the two only differ if a
# pool is being driven with someone else's template.
POOL_NS="${POOL_NS:-${TEMPLATE_NS}}"
POOL_NAME="${POOL_NAME:-${TEMPLATE_NAME}}"
ATESPACE="${ATESPACE:-scale}"
OUT="${OUT:-/tmp/scale-driver.csv}"

if [[ -z "${KUBECTL_CONTEXT:-}" ]]; then
  echo "Error: set KUBECTL_CONTEXT explicitly. The current context is shared and" >&2
  echo "       gets switched by other work." >&2
  exit 1
fi
KCTX=(--context="${KUBECTL_CONTEXT}")

log() { echo -e "\033[1;36m[driver]: $*\033[0m"; }

# The golden snapshot's URI lives in ateapi's store, not the Kubernetes API, so
# resolve it here (once) rather than teaching the driver to speak to ateapi.
GOLDEN_SNAPSHOT_URI="${GOLDEN_SNAPSHOT_URI:-}"
if [[ -z "${GOLDEN_SNAPSHOT_URI}" && "${VIA}" == "atelet" ]]; then
  golden="$(kubectl "${KCTX[@]}" get actortemplate -n "${TEMPLATE_NS}" "${TEMPLATE_NAME}" \
    -o jsonpath='{.status.goldenSnapshot}')"
  if [[ -z "${golden}" ]]; then
    echo "Error: ActorTemplate ${TEMPLATE_NS}/${TEMPLATE_NAME} has no golden snapshot yet" >&2
    exit 1
  fi
  ATE_BIN="${ATE_BIN:-kubectl-ate}"
  ATE=("${ATE_BIN}" "${KCTX[@]}")
  [[ -n "${ATE_ENDPOINT:-}" ]] && ATE+=(--endpoint="${ATE_ENDPOINT}")
  [[ -n "${ATE_TOKEN_FILE:-}" ]] && ATE+=(--token-file="${ATE_TOKEN_FILE}")
  GOLDEN_SNAPSHOT_URI="$("${ATE[@]}" get actor-snapshot "${golden}" -a ate-golden -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["actorSnapshots"][0]["status"]["snapshotUri"])')"
fi
log "golden snapshot: ${GOLDEN_SNAPSHOT_URI}"

log "via=${VIA} mode=${MODE} count=${COUNT} parallel=${PARALLEL} prefix=${NAME_PREFIX} shards=${SHARDS}"

# Restart policy is Never, so previous runs leave Completed pods behind, and a
# pod's spec is immutable, so re-applying over one fails rather than replacing
# it. The bare name covers pods left by a run that predates the label.
kubectl "${KCTX[@]}" delete pod -n "${POOL_NS}" -l app=scaledriver --ignore-not-found --wait
kubectl "${KCTX[@]}" delete pod -n "${POOL_NS}" scaledriver --ignore-not-found --wait

# The RBAC objects are shared by every shard, so they are rendered once; only
# the Pod (the last document) repeats.
RBAC_DOC="$(awk '/^---$/{n++} n<3' hack/scaledriver/scaledriver.yaml.tmpl)"
POD_DOC="$(awk '/^---$/{n++} n>=3' hack/scaledriver/scaledriver.yaml.tmpl)"

subst_common() {
  sed -e "s|\${MODE}|${MODE}|g" \
      -e "s|\${VIA}|${VIA}|g" \
      -e "s|\${PARALLEL}|${PARALLEL}|g" \
      -e "s|\${POOL_NS}|${POOL_NS}|g" \
      -e "s|\${POOL_NAME}|${POOL_NAME}|g" \
      -e "s|\${TEMPLATE_NS}|${TEMPLATE_NS}|g" \
      -e "s|\${TEMPLATE_NAME}|${TEMPLATE_NAME}|g" \
      -e "s|\${ATESPACE}|${ATESPACE}|g" \
      -e "s|\${READYZ_TIMEOUT_SECONDS}|${READYZ_TIMEOUT_SECONDS}|g" \
      -e "s|\${GOLDEN_SNAPSHOT_URI}|${GOLDEN_SNAPSHOT_URI}|g" \
      -e "s|\${DRIVER_NODESELECTOR}|${DRIVER_NODESELECTOR}|g"
}

# Named up front: the render below runs in a subshell, so anything it appends
# to an array is lost.
PODS=()
for ((i = 0; i < SHARDS; i++)); do
  if (( SHARDS > 1 )); then PODS+=("scaledriver-${i}"); else PODS+=("scaledriver"); fi
done

{
  printf '%s\n' "${RBAC_DOC}" | subst_common
  for ((i = 0; i < SHARDS; i++)); do
    # One driver pod saturates well before the fleet does, so the count is split
    # across shards. Each needs its own name prefix to keep actor names unique.
    shard_count=$(( COUNT / SHARDS + (i < COUNT % SHARDS ? 1 : 0) ))
    if (( SHARDS > 1 )); then
      suffix="-${i}"; prefix="${NAME_PREFIX}s${i}"
    else
      suffix=""; prefix="${NAME_PREFIX}"
    fi
    printf '%s\n' "${POD_DOC}" | subst_common \
      | sed -e "s|\${SHARD_SUFFIX}|${suffix}|g" \
            -e "s|\${COUNT}|${shard_count}|g" \
            -e "s|\${NAME_PREFIX}|${prefix}|g"
  done
} | ./hack/run-tool.sh ko apply -f - -- --context="${KUBECTL_CONTEXT}"

# A run that reports failures exits non-zero, so the pod lands in Failed rather
# than Succeeded. Both are terminal, and the CSV is worth collecting either way.
wait_terminal() {
  local phase
  for _ in $(seq 1 720); do
    phase="$(kubectl "${KCTX[@]}" get pod "$1" -n "${POOL_NS}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" ]] && return 0
    sleep 10
  done
}

for p in "${PODS[@]}"; do
  kubectl "${KCTX[@]}" wait --for=condition=Ready "pod/${p}" -n "${POOL_NS}" --timeout=300s || wait_terminal "${p}"
done

# Follow one shard for progress, but do not trust the stream to survive the run:
# a long ramp outlives `logs -f` (the API server drops it, and the rows in
# flight go with it). The pods keep their whole logs, so refetch once every
# shard is done and let that be the CSV.
kubectl "${KCTX[@]}" logs -f "pod/${PODS[0]}" -n "${POOL_NS}" || true
for p in "${PODS[@]}"; do
  wait_terminal "${p}"
done
: > "${OUT}"
for p in "${PODS[@]}"; do
  kubectl "${KCTX[@]}" logs "pod/${p}" -n "${POOL_NS}" >> "${OUT}"
done
tail -3 "${OUT}"

log "done -- ${OUT}"
