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

# Packs Actors onto ONE worker until something stops it, and records what.
#
# The question is not "does N work" but "which limit is reached first, and what
# does the Nth activation cost". So this ramps rather than jumping to a target,
# and after every batch it samples the things that could plausibly bind:
#
#   actors      how many are RUNNING (the answer)
#   resume_ms   what the most recent activation took (the cost curve)
#   pod_rss_mb  the worker pod's memory (the ceiling everyone expects)
#   veths       interfaces in the worker pod netns (one per Actor, plus two)
#   nft_elems   elements in the actor address map (should track actors exactly)
#
# Writes one CSV row per batch so the curve can be read afterwards, and stops on
# the first failed resume with the state that produced it.
#
# Usage: hack/scale-density.sh [max-actors] [batch-size]

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

MAX="${1:-4094}"
BATCH="${2:-25}"
# Activations run concurrently. Sequentially, 4000 Actors at ~4.7s each is over
# five hours; more to the point, serial activation would not exercise the thing
# the per-actor lock was split for -- concurrent activations of DIFFERENT actors
# are supposed to not wait on each other, and this is what shows whether they do.
PARALLEL="${PARALLEL:-12}"
ATESPACE="${ATESPACE:-scale}"
TEMPLATE_NS="${TEMPLATE_NS:-ate-scale-tiny}"
TEMPLATE_NAME="${TEMPLATE_NAME:-tiny}"
POOL_NS="${POOL_NS:-ate-scale-tiny}"
POOL_NAME="${POOL_NAME:-tiny}"
OUT="${OUT:-/tmp/scale-density.csv}"
# Actor names are scoped to the run. A previous run that died mid-ramp leaves
# its Actors behind, and reusing names turns that into an AlreadyExists failure
# at Actor 0 that looks exactly like a real ceiling.
RUN_ID="${RUN_ID:-$(date +%H%M%S)}"

if [[ -z "${KUBECTL_CONTEXT:-}" ]]; then
  echo "Error: set KUBECTL_CONTEXT explicitly. The current context is shared and" >&2
  echo "       gets switched by other work; a density test on the wrong cluster" >&2
  echo "       is expensive to notice." >&2
  exit 1
fi
KCTX=(--context="${KUBECTL_CONTEXT}")

log() { echo -e "\033[1;36m[scale]: $*\033[0m"; }

# BSD date (macOS) has no %3N, so shell out for a millisecond clock rather than
# silently producing a garbage timestamp.
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

worker_pod() {
  kubectl "${KCTX[@]}" get pods -n "${POOL_NS}" \
    -l "ate.dev/worker-pool=${POOL_NAME}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

# Counts interfaces in the worker pod's network namespace, from the NODE. The
# ateom image is distroless -- no shell, no nft, no ls -- so there is nothing to
# exec into the container itself. This costs a debug pod, so it runs at the end
# of a run (or on failure) rather than every batch.
#
# Three things here are less obvious than they look, and each of them silently
# reported the wrong number first:
#
#   * The netns is found by matching the ateom's own command line under hostPID,
#     not through crictl -- GKE's crictl is not on the PATH a chroot gets.
#   * Interfaces are counted with `ip`, not `ls /sys/class/net`. sysfs does not
#     follow `nsenter -n`, so the sysfs listing reports the HOST's interfaces
#     however deep into a namespace the probe has gone.
#   * Map elements are counted by their VALUE. nft renders an ifname key as ""
#     here, so matching the key finds nothing at all and reads as an empty map.
#
# netshoot rather than busybox because the count needs both ip and nft.
probe_pod_netns() {
  local pod="$1"
  local node
  node="$(kubectl "${KCTX[@]}" get pod -n "${POOL_NS}" "${pod}" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
  [[ -z "${node}" ]] && { echo "0 0"; return; }
  kubectl "${KCTX[@]}" debug "node/${node}" -q --image=nicolaka/netshoot --profile=sysadmin \
    -- bash -c "
      pid=''
      for p in /proc/[0-9]*; do
        # argv[0] first: this very shell was invoked with the pod UID in its
        # command line, so matching the UID alone finds the probe itself.
        case \"\$(tr '\\0' '\\n' < \$p/cmdline 2>/dev/null | head -1)\" in *ateom-*) ;; *) continue;; esac
        grep -qa -- '--pod-uid=${POD_UID}' \$p/cmdline 2>/dev/null || continue
        pid=\${p#/proc/}; break
      done
      [ -z \"\$pid\" ] && { echo '0 0'; exit 0; }
      veths=\$(nsenter -t \$pid -n ip -o link 2>/dev/null | grep -c 'ate[0-9]')
      elems=\$(nsenter -t \$pid -n nft list map ip ateom_actor actor_podside 2>/dev/null |
               grep -cE '\" : 169\\.254\\.')
      echo \"\$veths \$elems\"
    " 2>/dev/null | tr -d '\r' | tail -1 || echo "0 0"
}

pod_rss_mb() {
  local pod="$1"
  # The worker pod's whole cgroup: the ateom, every sandbox it supervises, and
  # the guests. This is the number that decides the ceiling.
  kubectl "${KCTX[@]}" get --raw \
    "/apis/metrics.k8s.io/v1beta1/namespaces/${POOL_NS}/pods/${pod}" 2>/dev/null \
    | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print(0); raise SystemExit
total = 0
for c in d.get("containers", []):
    mem = c.get("usage", {}).get("memory", "0")
    if mem.endswith("Ki"): total += int(mem[:-2]) // 1024
    elif mem.endswith("Mi"): total += int(mem[:-2])
    elif mem.endswith("Gi"): total += int(mem[:-2]) * 1024
print(total)
' 2>/dev/null || echo 0
}

running_actors() {
  "${ATE[@]}" get actors -a "${ATESPACE}" -o json 2>/dev/null \
    | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print(0); raise SystemExit
# kubectl-ate wraps the page in {"actors": [...]}; reading "items" (the
# Kubernetes shape) silently finds nothing and reports every run as zero.
items = d.get("actors") or (d if isinstance(d, list) else d.get("items", []))
print(sum(1 for a in items if a.get("status", {}).get("state") == "ACTOR_STATE_RUNNING"))
' 2>/dev/null || echo 0
}

log "context=${KUBECTL_CONTEXT} template=${TEMPLATE_NS}/${TEMPLATE_NAME} max=${MAX} batch=${BATCH} parallel=${PARALLEL} run=${RUN_ID}"

# One port-forward for the whole run. Without --endpoint, kubectl-ate stands up
# a fresh port-forward per invocation, and at two calls per Actor that setup
# would dominate the measurement it is supposed to be taking.
PF_PORT="${PF_PORT:-18443}"
kubectl "${KCTX[@]}" port-forward -n ate-system svc/api "${PF_PORT}:443" >/tmp/scale-pf.log 2>&1 &
PF_PID=$!
trap 'kill ${PF_PID} 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  nc -z localhost "${PF_PORT}" 2>/dev/null && break
  sleep 1
done
ATE_BIN="${ATE_BIN:-kubectl-ate}"
ATE=("${ATE_BIN}" "${KCTX[@]}" --endpoint="localhost:${PF_PORT}")
[[ -n "${ATE_TOKEN_FILE:-}" ]] && ATE+=(--token-file="${ATE_TOKEN_FILE}")
log "ateapi via localhost:${PF_PORT} (pid ${PF_PID})"

"${ATE[@]}" create atespace "${ATESPACE}" >/dev/null 2>&1 || true

POD="$(worker_pod)"
if [[ -z "${POD}" ]]; then
  echo "Error: no worker pod for ${POOL_NS}/${POOL_NAME}" >&2
  exit 1
fi
# The ateom carries its pod UID on its command line, which is what lets the
# netns probe pick this worker's ateom out of every process on the node.
POD_UID="$(kubectl "${KCTX[@]}" get pod -n "${POOL_NS}" "${POD}" -o jsonpath='{.metadata.uid}')"
log "worker pod: ${POD} (${POD_UID})"

echo "n_requested,n_running,resume_ms,pod_rss_mb,veths,nft_elems" | tee "${OUT}"

# activate creates and resumes one Actor, retrying a resume that fails in a way
# that looks transient.
#
# AssignWorker contends on the worker record, so under concurrency a resume can
# lose the race and time out with nothing actually wrong. Stopping the ramp on
# the first such error measures that contention rather than the ceiling, and
# reports a number well below the one the worker was already carrying. A hard
# refusal (no capacity, no free worker, a payload limit) is NOT retried: that is
# the answer we came for.
activate() {
  local name="$1" err attempt
  if ! err=$("${ATE[@]}" create actor "${name}" -a "${ATESPACE}" \
      --template "${TEMPLATE_NS}/${TEMPLATE_NAME}" 2>&1); then
    echo "FAIL create ${name}: ${err}" >> "${FAILLOG}"
    return 1
  fi
  for attempt in 1 2 3 4 5; do
    if err=$("${ATE[@]}" resume actor "${name}" -a "${ATESPACE}" 2>&1); then
      return 0
    fi
    case "${err}" in
      *"no free workers"*|*ResourceExhausted*|*exceeds*)
        echo "CEILING resume ${name}: ${err}" >> "${FAILLOG}"; return 1 ;;
    esac
    sleep "${attempt}"
  done
  echo "FAIL resume ${name} after 5 retries: ${err}" >> "${FAILLOG}"
  return 1
}

FAILLOG="$(mktemp)"
created=0
while (( created < MAX )); do
  batch_start=$(now_ms)
  pids=()
  for (( i = 0; i < BATCH && created < MAX; i++ )); do
    activate "t${RUN_ID}-$(printf '%05d' "${created}")" &
    pids+=($!)
    created=$(( created + 1 ))
    # Bound concurrency: the point is to keep the worker busy, not to open
    # thousands of gRPC streams through one port-forward.
    while (( $(jobs -rp | wc -l) >= PARALLEL )); do sleep 0.2; done
  done
  wait "${pids[@]}" 2>/dev/null || true

  batch_ms=$(( $(now_ms) - batch_start ))
  per_actor_ms=$(( BATCH > 0 ? batch_ms / BATCH : 0 ))

  if [[ -s "${FAILLOG}" ]]; then
    log "STOP at ~${created}: $(head -1 "${FAILLOG}")"
    echo "${created},$(running_actors),${per_actor_ms},$(pod_rss_mb "${POD}"),," | tee -a "${OUT}"
    break
  fi
  echo "${created},$(running_actors),${per_actor_ms},$(pod_rss_mb "${POD}"),," | tee -a "${OUT}"
done

# The expensive probe, once, on the state the run ended in: one veth per Actor
# plus lo and eth0, and a map element per Actor. A mismatch between these and
# the running count is a leak, and is the thing most likely to bite at scale.
log "probing worker pod netns (one debug pod)..."
read -r veths nft_elems < <(probe_pod_netns "${POD}")
log "final: actors=$(running_actors) veths=${veths} nft_map_elements=${nft_elems} rss_mb=$(pod_rss_mb "${POD}")"
echo "final,$(running_actors),,$(pod_rss_mb "${POD}"),${veths},${nft_elems}" | tee -a "${OUT}"

log "done -- ${OUT}"
