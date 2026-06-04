# suspend-bench

Throwaway research spike answering, in order:

1. **Primary:** is **pause + suspend-to-swap (zswap)** better than **writing a
   snapshot to local SSD** — for gVisor and/or Kata/cloud-hypervisor?
2. **Only if swap wins — why?** Is the advantage the **"z" (compression)**, or the
   **swap/suspend mechanism itself** (no teardown/restore, lazy fault-back)? We
   answer this with diagnostic knobs, *not* by treating compression as a headline.

So the comparison is **swap vs snapshot first**; compression on both sides
(zswap on/off; snapshot `none|zstd|lz4`) is a **second-stage diagnostic** to
attribute a swap win — run it only after question 1 has an answer.

Runs on a single GCE VM, comparing **cloud-hypervisor** (microVM, container-image
rootfs) and **gVisor/runsc**.

Context:
[`../docs/dev/poc2-swap-suspend-findings.md`](../docs/dev/poc2-swap-suspend-findings.md).
This is a **standalone Go module** and does not touch the parent repo's build.

> **Results so far: [`FINDINGS.md`](FINDINGS.md).** TL;DR — on cloud-hypervisor,
> **sparse (memfd) + demand-paged checkpoint/restore to local SSD beats live swap
> on every axis** (suspend, resume, footprint, portability), and the gap widens
> with memory size. Answer to Q1: snapshot wins; swap does not.

## What it measures

For each `{runtime} × {mechanism} × {workload} × {working-set} × {rep}` it records,
with a monotonic clock and cgroup v2 / `/proc` accounting:

- **`resume_to_first_ping_ms`** — the headline UX metric (resume call → first vsock
  ping that returns the expected state token).
- `suspend_ms`, `resume_call_ms`, `steady_state_ping_ms` (post page-walk).
- `image_apparent_bytes` / `image_actual_bytes` (sparse vs real snapshot size).
- `host_rss_freed_bytes`, `swap_current_bytes`, `zswap_current_bytes`.
- `correctness_ok` (state preserved across suspend/resume).

Mechanisms (the **primary two** are `swap` vs `checkpoint_local`):

- **`swap`** — pause → cgroup v2 reclaim guest RAM to host swap/zswap → resume in
  place. The candidate.
- **`checkpoint_local`** — snapshot to local NVMe, tear down, restore. The foil.
  - cloud-hypervisor: `vm.snapshot` → restore `copy` (eager) or `ondemand`
    (userfaultfd, lazy); gVisor: `runsc checkpoint`/`restore`.
- **`coldstart`** — fresh boot baseline.

**Stage 1 (answer the question):** `swap` vs `checkpoint_local`, both in their plain
form, per runtime. **Stage 2 (only if swap wins — attribute it):** add the
diagnostic knobs — `--zswap on,off` isolates whether swap's win is the compression
or the suspend mechanism; `--compression none,zstd,lz4` and `--restore-modes
copy,ondemand` show how far the snapshot path can be pushed before the gap closes.

## Layout

| Path | What |
|---|---|
| `provision/` | One-time host setup for Debian 13 (KVM, CH, runsc, kernel, NVMe, swap+zswap, cgroup). |
| `rootfs/` | Build a virtio-blk ext4 rootfs from a container image + fetch the guest kernel. |
| `workloads/` | C and Node.js workloads (container images) + the static PID1 init shim. |
| `cmd/bench/` | The driver: runs the matrix, streams `results.jsonl` + `results.csv`. |
| `cmd/pinger/` | Standalone host-side vsock probe for debugging. |
| `internal/` | Runtime drivers, mechanisms, vsock, metrics, results schema. |

## Quickstart (on `bench-host-n2`)

```bash
# 0. Provision the host (idempotent; needs sudo): KVM, CH, runsc, NVMe, swap+zswap, cgroup.
sudo ./provision/provision-vm.sh

# 1. Fetch the guest kernel + build workload images, CH ext4 rootfs, and gVisor bundles.
./rootfs/fetch-kernel.sh
./rootfs/build-workloads.sh        # builds c + node images -> rootfs imgs + bundles

# 2. Build the harness (from a dev box, then scp, or build on the VM).
make build            # produces bin/bench, bin/pinger (linux/amd64)

# 3. Validate end-to-end before the full matrix (the gate; asserts state survives).
sudo bin/bench --smoke

# 4. Run a matrix.
sudo bin/bench \
  --runtimes ch,gvisor \
  --mechanisms checkpoint_local,swap,coldstart \
  --compression none,zstd \
  --restore-modes copy,ondemand \
  --zswap on,off \
  --workloads c \
  --working-sets 64MiB,256MiB,1GiB \
  --reps 10 \
  --out results/results.jsonl
```

Results stream to the **persistent boot disk** (`results/`), not the ephemeral NVMe.

## Status

Built milestone-first (see the plan): M1 cloud-hypervisor checkpoint(copy)+C is the
walking skeleton that proves vsock survives snapshot/restore; swap, ondemand, gVisor,
and the Node workload layer on top. Each `internal/runtime` and `internal/mechanism`
is a small plugin behind a stable interface.
