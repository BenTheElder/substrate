# PoC2: swap-based suspend & per-actor cgroup accounting — findings

> Status: **research spike (poc2-atelet branch).** The live-process mechanic in this
> branch is a deliberate dead-end that we used to *prove* a set of kernel
> constraints. The actionable recommendation is at the bottom. Do not ship the
> `swap_linux.go` mechanic as-is.

## Original idea

Like the `poc-atelet` branch, multiplex actors by suspending idle ones — but
instead of running the gVisor sandbox under `atelet` for its whole life, keep it
**running under `ateom`** (inside the worker pod) and only, *on suspend*,
"reparent" it to a node-level `atelet`-managed cgroup via cgroup manipulation,
page its memory out to swap/zswap, and free the worker. Resume would reparent it
back.

The goal behind it: **per-actor swap accounting** — when an actor is suspended,
its memory should move to swap and be charged to a node-level (per-actor) cgroup,
so suspended actors are individually accountable and the worker is freed.

## What was built (the mechanic in this branch)

- `internal/controllers/workerpool_controller.go` — `hostPID: true` on worker pods
  (so the sentry's pid-file PID is host-global and addressable by `atelet`).
- `manifests/ate-install/atelet.yaml` — `hostPID: true`, host `/sys` mount,
  tolerations on the atelet DaemonSet.
- `cmd/atelet/main.go` — `swap-out` / `swap-in` subcommand dispatch.
- `cmd/atelet/swap_linux.go` — the mechanic: `runsc pause`/`resume`,
  `process_madvise(MADV_PAGEOUT/WILLNEED)` page-out/recharge, cgroup-tree search,
  and moving the sandbox PIDs between cgroups.
- `cmd/atelet/swap_other.go` — non-Linux stub.
- `hack/create-kind-cluster.sh` — remount `/run/ateom-gvisor` exec on kind nodes.

It was exercised end-to-end on **kind** and on a **real GKE COS 1.36 + zswap**
cluster.

## What works

The **lifecycle** handoff works and preserves state: `runsc pause` → move the
sandbox's processes to another cgroup → `runsc resume` keeps the actor alive with
its in-memory state intact (the `counter` demo continued its count across the
cycle, both on kind and GKE).

## What does NOT work — and why (the core findings)

### 1. Moving processes between cgroups does not move memory/swap accounting

cgroup v2 **does not migrate a process's already-charged pages** when the process
is moved (`memory.move_charge_at_immigrate` is a cgroup v1 feature, deliberately
removed in v2). Pages stay charged to the cgroup where they were *first faulted*;
the charge only drops when the page is freed or reclaimed.
([cgroup v2 docs](https://docs.kernel.org/admin-guide/cgroup-v2.html))

Measured on GKE after `swap-out` (procs moved into `ate-suspended/<actor>`):

| cgroup | memory.current | memory.swap.current |
|---|---|---|
| `ate-suspended/<actor>` (procs live here) | ~8 KB | ~8 KB |
| `/pause` (where the sandbox was born) | ~33 MB | ~21 MB |

### 2. Swap charges are "sticky" to the original memcg

A swapped page's slot records its owning memcg; on swap-in the page is recharged
to the **recorded** cgroup, not the faulting task's current cgroup. Verified by
resuming the sandbox *in place* in `ate-suspended` and driving real load: the
guest faulted ~10 MB back in and **all of it re-attributed to `/pause`**, not to
`ate-suspended`. So a `PAGEOUT → WILLNEED → PAGEOUT` "recharge" cannot move the
charge either. ([memory controller docs](https://docs.kernel.org/admin-guide/cgroup-v1/memory.html))

### 3. The gVisor guest RAM is a shared memfd

The sentry's resident memory is dominated by `Shared_Dirty` (~26 MB). Shared /
page-cache pages are charged to their first-faulter independent of any process,
so process moves can't affect them at all.

### 4. The shared `/pause` cgroup

With no `Linux.CgroupsPath` set in the OCI spec, runsc puts *every* sandbox in a
single shared `/sys/fs/cgroup/pause` cgroup at the node root. So today there is no
per-actor accounting even while active. (This is also the root cause of the #50
`EBUSY` teardown bug — see [PR #161](https://github.com/agent-substrate/substrate/pull/161).)

### 5. Charge reparenting on `rmdir` goes to the *parent*, not a sibling

We tested the "create new child under parking → move PIDs → rmdir old child"
idea. On `rmdir`, the removed cgroup's residual charge **reparents one level up to
its own parent**, never to a sibling subtree under a different parent. Synthetic
test: 60 MB born in a child of `ateom_sim`, PIDs moved to a child of `parking`,
old child removed → the 60 MB landed on **`ateom_sim`**, not `parking`.

### 6. Live processes cannot be resumed in a *different* ateom

This is the decisive architectural constraint. A live (paused + swapped) sandbox
is pinned to:
- **its node** — swap/zswap is node-local memory; nothing exists on another node.
- **its worker's network namespace** — the per-worker interior netns is bound at
  sandbox creation; a different ateom can't rebind a live process's netns.

And **PID-namespace membership is immutable**: a process can't be moved between
PID namespaces, and pod teardown SIGKILLs everything in the pod's PID namespace.
So "run under the ateom pod while active, become node-level when parked" is
impossible for the live *processes* (it's only possible for the cgroup, which
doesn't carry the charge anyway).

**Therefore resuming an actor in a different ateom requires re-creating the
sandbox in that ateom's namespaces — i.e. `runsc checkpoint` → `runsc restore`
from a portable image. A live-process swap can only ever be a same-worker
fast-path.**

## What *does* control swapping cleanly: `memory.swap.max`

`memory.swap.max` gates swapping per-cgroup. Proven on GKE:

| state | memory.current | memory.swap.current |
|---|---|---|
| active, `swap.max=0` | 60 MB | 0 |
| forced `memory.reclaim`, `swap.max=0` | 60 MB (unmoved) | 0 |
| `swap.max=max` + `memory.reclaim` | 0.6 MB | 60 MB (zswap-compressed) |

So `swap.max=0` keeps an active actor **fully resident even under forced reclaim**
(no latency risk), and flipping to `max` + `memory.reclaim` deliberately swaps the
*whole* cgroup — including the shared memfd that `process_madvise` couldn't touch —
charged to that cgroup. This requires a **per-actor** cgroup (you can't gate the
shared `/pause` without affecting every sandbox).

## `hostPID` downsides (why dropping it matters)

`hostPID` was only needed to address the sentry by host-global PID for the
live-move mechanic. Its costs: full node-wide process visibility (cmdlines,
`/proc/<pid>/environ`), cross-pod signaling/DoS with privileges, loss of PID-1
reaping (breaks ateom's `go-reap` subreaper — orphaned runsc daemons reparent to
node init), orphaned-process leaks across pod deletion, and it's **forbidden by
the Pod Security baseline/restricted standards** (blocked on e.g. GKE Autopilot).
The recommended design below needs none of it.

## Recommended design (build on PR #161)

1. **Per-actor cgroup at birth** via [PR #161](https://github.com/agent-substrate/substrate/pull/161)
   — sets a *relative* `CgroupsPath` (`actors/<ns>/<tmpl>/<actor>/<container>`) so
   each sandbox is born in its own cgroup nested under the worker pod (rolls up
   into the pod's accounting; fixes #50). This is the "under ateom" property, at
   the cgroup level — which is the only level at which it's achievable. **No
   `hostPID`.** (Ensure the `memory` controller is delegated down to the leaf so
   `memory.swap.max` / `memory.reclaim` exist on it.)
2. **`memory.swap.max = 0` on active actors** → never swapped while running.
3. **Suspend = `runsc checkpoint`** to a portable image — this is what makes
   *different-ateom* resume work. Keep the image **local** on the node (zswap-
   compressed page cache) for fast same-node restore, and upload to object storage
   for cross-node restore.
4. **Resume** = `runsc restore` in whichever ateom is assigned (local image when
   same-node; object storage otherwise) → reset `swap.max=0`.

The live-process swap/reparent/`process_madvise`/`hostPID` machinery in this
branch should be **dropped** — it cannot migrate accounting and cannot satisfy
different-ateom resume.

## Reproduction notes

- kind: `make build && hack/create-kind-cluster.sh && hack/install-ate-kind.sh
  --deploy-ate-system && hack/install-ate-kind.sh --deploy-demo-counter`. kind's
  Linux VM may lack swap; add a loop-backed swapfile on the node to exercise swap
  (`losetup`/`swapon` — a plain swapfile fails on overlayfs).
- GKE COS 1.36 nodes already have an 8 GB swap partition + zswap enabled.
- Inspect host cgroup accounting from a privileged `hostPID` busybox pod with a
  hostPath `/sys` mount (atelet's image is distroless — no shell). Find a PID's
  real cgroup by searching the mounted cgroup tree for the `cgroup.procs` that
  contains it (`/proc/<pid>/cgroup` is rendered relative to the reader's cgroup
  namespace and won't resolve against a host `/sys` bind mount).
