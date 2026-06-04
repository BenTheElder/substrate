# Suspend/Resume Benchmark — Findings (cloud-hypervisor)

Data collected with the `suspend-bench` harness on a single GCE VM. **Headline:
sparse + demand-paged checkpoint/restore to local SSD beats live swap on every
axis — suspend, resume, footprint, and portability — and the gap widens with
memory size.** This quantifies and strengthens the recommendation in
[`../docs/dev/poc2-swap-suspend-findings.md`](../docs/dev/poc2-swap-suspend-findings.md):
prefer checkpoint/restore over live swap; you do **not** need to schedule
suspended actors under a node-level component to leverage swap.

> Status: cloud-hypervisor evaluated thoroughly (C micro-workload + realistic
> Vite dev server + multi-GB). gVisor and the zswap diagnostic are still open
> (see [Open items](#open-items)).

## Test host & toolchain

- GCE `bench-host-n2`: **n2-standard-16** (16 vCPU, 62 GiB RAM), **nested virt on**,
  Debian 13 (kernel `6.12.90+deb13.1-cloud`), cgroup v2.
- **2× 375 GB local NVMe SSD, used as separate disks (NOT RAID0):** `nvme0n1` →
  `/mnt/nvme-images` (snapshots + rootfs images), `nvme0n2` → `/mnt/nvme-swap`
  (48 GiB swapfile). See `provision/setup-nvme.sh`.
- **cloud-hypervisor v52.0**, runsc `release-20260525.0`, guest kernel = Kata
  `vmlinux-6.12.42` (direct boot).
- Guest = **ext4 virtio-blk rootfs built from a container image**; control channel
  = **virtio-vsock** (C workload) or vsock→unix via socat (Node). No virtio-fs
  (breaks CH restore), no tap (avoids net-FD-on-restore).
- **zswap is NOT compiled into the Debian 13 cloud kernel** (`CONFIG_ZSWAP`
  unset) → "swap" here is **plain on-disk swap** (no compress-in-RAM tier).

### CH config details that matter (learned the hard way)
- **`image_type: "Raw"`** is required on v52 — it auto-detects raw images and
  disables sector-0 writes, which otherwise panics the ext4 rootfs (`Unable to
  mount root fs … ReadOnly`).
- **`memory.shared = true`** (memfd-backed guest RAM) enables **sparse snapshots**
  ([CH PR #8113](https://github.com/cloud-hypervisor/cloud-hypervisor/pull/8113)):
  CH walks `SEEK_DATA`/`SEEK_HOLE` and writes only touched pages. Anonymous mmap
  and plain file-backed RAM fall back to a dense (full-RAM) image.
- **`memory_restore_mode`**: `copy` (eager, default) vs `ondemand` (userfaultfd,
  lazy). CH **v44's CLI rejects** the option; **v52** accepts it.
- memfd does **not** compromise portability: the snapshot is an ordinary sparse
  file, restored into a brand-new CH process (verified: correctness holds across a
  fresh-process restore).

## Mechanisms compared
- **`swap`** — pause → cgroup v2 `memory.reclaim` pages guest RAM to host swap →
  resume in place. Node-pinned (swap is node-local; netns/PID-ns immutable).
- **`checkpoint_local`** — CH `vm.snapshot` to local NVMe → teardown → restore
  (`copy` or `ondemand`). Portable (cross-node capable).
- **`coldstart`** — fresh boot baseline.

Metric of record: **`resume → first-ping`** = restore/resume call → first
successful response (for Node, the Vite dev server serving HTTP again). All runs
verified state survives (seed/counter + region hash); n = medians as noted.

---

## 1. C micro-workload, 1 GiB guest, memfd sparse (n=3)

| mechanism | touched | **suspend** | **resume** | **footprint** | host RAM freed | portable |
|---|--:|--:|--:|--:|--:|:--:|
| checkpoint **ondemand** | 64 MiB | **83 ms** | **25.5 ms** | 127 MB img | 134 MB | ✅ |
| checkpoint copy | 64 MiB | 83 ms | 85.8 ms | 127 MB img | 134 MB | ✅ |
| swap | 64 MiB | 3139 ms | 78 ms | 128 MB swap | 130 MB | ❌ |
| checkpoint **ondemand** | 256 MiB | **206 ms** | **25.6 ms** | 328 MB img | 336 MB | ✅ |
| checkpoint copy | 256 MiB | 206 ms | 198 ms | 328 MB img | 336 MB | ✅ |
| swap | 256 MiB | 3641 ms | 83 ms | 329 MB swap | 331 MB | ❌ |

Checkpoint suspends **15–38× faster** than swap, resumes **~3× faster** (ondemand),
**same on-disk footprint** (sparse image == touched set == swap size), and is
portable. There is no axis on which swap wins.

## 2. Snapshot-size levers (1 GiB guest, 64 MiB touched)

| variant | image on disk | suspend | resume | notes |
|---|--:|--:|--:|--|
| dense (anonymous mmap), no compression | **1074 MB** | ~1000 ms | 871 ms | full guest RAM |
| dense + **zstd** | **77 MB** | 1495 ms | 1443 ms | compresses the zeros; +CPU |
| **memfd sparse** | **127 MB** | **83 ms** | 25–86 ms | only touched pages; ~12× faster suspend |

Two independent ways to make the image ≈ the touched set instead of full guest
RAM. **Sparse** is the big win (also makes suspend ~12× faster by not writing
zeros). **zstd** is the better *cross-node transfer* story (holes don't survive a
naïve copy/upload; compression squeezes the zeros for the GCS hop).

## 3. Multi-GB scaling — C workload, 8 GiB guest, memfd sparse (n=2)

| mechanism | touched | **resume** | **suspend** | image/footprint |
|---|--:|--:|--:|--:|
| checkpoint **ondemand** | 1 GiB | **25.8 ms** | 818 ms | 1341 MB |
| checkpoint copy | 1 GiB | 765 ms | 817 ms | 1341 MB |
| swap | 1 GiB | 103 ms | **7203 ms** | 1342 MB swap |
| checkpoint **ondemand** | 4 GiB | **24.5 ms** | 2776 ms | 4568 MB |
| checkpoint copy | 4 GiB | 2560 ms | 2909 ms | 4568 MB |
| swap | 4 GiB | ~170 ms | **17044 ms** | 4570 MB swap |

- **Sparse image == touched memory at every scale** (1 GiB→1.34 GB, 4 GiB→4.57 GB;
  the 8 GiB guest size is irrelevant). ~0.34 GB constant overhead = guest boot +
  page tables. Identical footprint to swap.
- **ondemand resume is FLAT ~25 ms even at 4 GiB** — first-response latency is
  independent of guest size (only first-touched pages fault in). copy scales with
  the touched set; swap ~100–170 ms.
- **Swap suspend is catastrophic at scale: 7 s (1 GiB) → 17 s (4 GiB)** —
  reclaiming GB page-by-page. The checkpoint-vs-swap suspend gap widens with size.

## 4. Realistic workload — agent-edited Vite + React dev server, 2 GiB guest, sparse (n=3)

A real Vite dev server (the app a Claude agent iterates on) with a background loop
that rewrites a component every 400 ms and triggers Vite to re-transform it. The
dev server **survives suspend/resume on all mechanisms and serves HTTP again**
(correctness 3/3). `resume` = time until it serves the app.

| mechanism | WS | resume→serves | suspend | footprint |
|---|--:|--:|--:|--:|
| checkpoint **copy** | 64 MiB | **275 ms** | 199 ms | 320 MB img |
| checkpoint ondemand | 64 MiB | 370 ms | 204 ms | 321 MB img |
| swap | 64 MiB | 946 ms | 3652 ms | 322 MB swap |
| checkpoint **copy** | 256 MiB | **372 ms** | 327 ms | 527 MB img |
| checkpoint ondemand | 256 MiB | 432 ms | 330 ms | 529 MB img |
| swap | 256 MiB | 811 ms | 4130 ms | 529 MB swap |
| (cold boot of the dev server) | — | ~2000 ms | — | — |

- Checkpoint beats swap on suspend (11–20×) and resume (~2×); waking a suspended
  dev server takes **275 ms vs ~2 s to cold-boot Vite**.
- **copy slightly beats ondemand here** (275 vs 370 ms) — the *opposite* of the C
  workload. Reason: serving the app immediately faults most of the module graph,
  so eager copy from a sparse image avoids userfaultfd per-page overhead.

### Restore-mode rule of thumb
**ondemand wins when wake touches a small fraction of memory** (C: flat 25 ms);
**sparse-copy wins when wake touches most of it** (Vite: serving faults the graph).
Sparse makes copy cheap in both cases (it only ever reads touched pages).

---

## Conclusion (for the architecture decision)
- **Use sparse (memfd) + checkpoint/restore to local SSD.** Pick restore mode by
  wake access pattern (ondemand default; copy when the workload touches most of
  its memory on wake). Upload a compressed image to object storage for cross-node.
- **Do not build live-swap-under-atelet.** Swap is slower to suspend (dramatically
  at multi-GB), slower to resume, the same footprint, and pins actors to a node.
  Its only theoretical edge (compress-in-RAM via zswap) is unproven here and
  cannot overcome a 7–17 s suspend.
- **memfd (needed for sparse) does not pin to a node** — snapshots restore into a
  fresh process tree, so portability is intact.

## Open items
- **zswap diagnostic**: the Debian 13 cloud kernel lacks `CONFIG_ZSWAP`; re-run
  swap (and snapshot-zstd) on a zswap-enabled kernel to isolate whether any swap
  advantage is the "z" (compression) vs the suspend mechanism. (A
  `c3-standard-22-lssd` host with a zswap kernel is being prepared.)
- **gVisor (partial — one blocking limitation found):**
  - **coldstart works** (~331 ms boot→serve, C workload) and **swap works**
    (resume ~59 ms, suspend ~3.0 s; note it freed only ~9 MB of a 64 MiB working
    set — gVisor's sentry holds the guest region in a way cgroup reclaim doesn't
    evict well; worth a closer look).
  - **checkpoint/restore is BLOCKED**: `runsc checkpoint` fails with
    **"Cannot save endpoint with bound host socket"**. Our control channel uses
    `runsc -host-uds=all` (the workload binds an AF_UNIX *host* socket so the
    harness can reach it), and gVisor's state encoder **cannot serialize a bound
    host socket** — it panics. This is a real difference vs cloud-hypervisor,
    which snapshots the whole VM (vsock state included) with no such restriction.
  - To benchmark gVisor checkpoint we'd need the workload to **quiesce** — close
    its host-uds listener (and any host FDs) before `runsc checkpoint` and
    re-bind after `runsc restore` — i.e. a control-plane "prepare-to-checkpoint"
    handshake. Worth doing since gVisor checkpoints serialize app state (not a
    full RAM dump) and could be much smaller.
  - Harness plumbing for gVisor is otherwise done (per-instance OCI bundle,
    `-host-uds=all`, cgroup placement, runsc create/start/checkpoint/restore/
    delete with the repo's flags).
- **Faster local SSD**: re-run on `c4`/`c3` (Titanium SSD) — copy-restore and
  suspend are SSD-write-bound, so absolute numbers should improve.
- **Correctness at scale**: the multi-GB `HASH` read deadline was raised to 180 s
  (lazy fault-in of many GB legitimately takes tens of seconds); earlier `0/2`
  flags were that timeout, not corruption (copy passed; resume succeeded).

## Reproducing
See `README.md`. Results JSONL/CSV are written under `results/`. Raw data for these
tables: `results/{memfd,multigb,node_real,v52}.jsonl` on the bench host.
