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
#   MODE=terminate hack/scale-driver.sh <count>   tears the same actors back down

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

COUNT="${1:-500}"
PARALLEL="${2:-16}"
MODE="${MODE:-restore}"
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
if [[ -z "${GOLDEN_SNAPSHOT_URI}" ]]; then
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
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["snapshots"][0]["status"]["snapshotUri"])')"
fi
log "golden snapshot: ${GOLDEN_SNAPSHOT_URI}"

log "mode=${MODE} count=${COUNT} parallel=${PARALLEL} prefix=${NAME_PREFIX}"

# The pod is restartPolicy: Never, so a previous run's pod is still sitting
# there Completed and would block the apply.
kubectl "${KCTX[@]}" delete pod -n "${POOL_NS}" scaledriver --ignore-not-found --wait

sed -e "s|\${MODE}|${MODE}|g" \
    -e "s|\${COUNT}|${COUNT}|g" \
    -e "s|\${PARALLEL}|${PARALLEL}|g" \
    -e "s|\${NAME_PREFIX}|${NAME_PREFIX}|g" \
    -e "s|\${POOL_NS}|${POOL_NS}|g" \
    -e "s|\${POOL_NAME}|${POOL_NAME}|g" \
    -e "s|\${TEMPLATE_NS}|${TEMPLATE_NS}|g" \
    -e "s|\${TEMPLATE_NAME}|${TEMPLATE_NAME}|g" \
    -e "s|\${ATESPACE}|${ATESPACE}|g" \
    -e "s|\${READYZ_TIMEOUT_SECONDS}|${READYZ_TIMEOUT_SECONDS}|g" \
    -e "s|\${GOLDEN_SNAPSHOT_URI}|${GOLDEN_SNAPSHOT_URI}|g" \
    hack/scaledriver/scaledriver.yaml.tmpl \
  | ./hack/run-tool.sh ko apply -f - -- --context="${KUBECTL_CONTEXT}"

kubectl "${KCTX[@]}" wait --for=condition=Ready pod/scaledriver -n "${POOL_NS}" --timeout=300s ||
  kubectl "${KCTX[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/scaledriver -n "${POOL_NS}" --timeout=60s

# Follow for progress, but do not trust the stream to survive the run: a long
# ramp outlives `logs -f` (the API server drops it, and the rows in flight go
# with it). The pod keeps its whole log, so refetch it once the run is over and
# let that be the CSV.
kubectl "${KCTX[@]}" logs -f pod/scaledriver -n "${POOL_NS}" || true
kubectl "${KCTX[@]}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/scaledriver \
  -n "${POOL_NS}" --timeout=2h >/dev/null || true
kubectl "${KCTX[@]}" logs pod/scaledriver -n "${POOL_NS}" > "${OUT}"
tail -3 "${OUT}"

log "done -- ${OUT}"
