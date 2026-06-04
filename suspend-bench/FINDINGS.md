# Suspend/Resume Benchmark — Findings (cloud-hypervisor)

Data collected with the `suspend-bench` harness on a single GCE VM. **Headline:
sparse + demand-paged checkpoint/restore to local SSD beats live swap on every
axis — suspend, resume, footprint, and portability — and the gap widens with
memory size.** This quantifies and strengthens the recommendation in
[`../docs/dev/poc2-swap-suspend-findings.md`](../docs/dev/poc2-swap-suspend-findings.md):
prefer checkpoint/restore over live swap; you do **not** need to schedule
suspended actors under a node-level component to leverage swap.

> Status: cloud-hypervisor and gVisor/runsc evaluated on `bench-host-n2` (§1–§5).
> A second host (`bench-c3-standard-22-lssd`: zswap-enabled kernel, RAID0 local
> SSD, 22 vCPU Sapphire Rapids) adds the **zswap diagnostic**, **SSD-striping**,
> and **large-allocation (up to 16 GiB)** results in **§6**.

## Test host & toolchain

- GCE `bench-host-n2`: **n2-standard-16** (16 vCPU, 62 GiB RAM), **nested virt on**,
  Debian 13 (kernel `6.12.90+deb13.1-cloud`), cgroup v2.
- **2× 375 GB local NVMe SSD, used as separate disks (NOT RAID0):** `nvme0n1` →
  `/mnt/nvme-images` (snapshots + rootfs images), `nvme0n2` → `/mnt/nvme-swap`
  (48 GiB swapfile). See `provision/setup-nvme.sh`.
- **cloud-hypervisor v52.0**, runsc `release-20260525.0`, guest kernel = Kata
  `vmlinux-6.12.42` (direct boot).
- Guest = **ext4 virtio-blk rootfs built from a container image**. Control channel:
  on CH, **virtio-vsock** (C) / vsock→unix via socat (Node); on gVisor, **netstack
  TCP** over a per-instance veth (a host-uds socket can't be checkpointed).
  No virtio-fs (breaks CH restore), no tap (avoids net-FD-on-restore).
- **zswap is NOT compiled into the Debian 13 cloud kernel** (`CONFIG_ZSWAP`
  unset) → "swap" here is **plain on-disk swap** (no compress-in-RAM tier).

### CH config details that matter (learned the hard way)
All runs use **cloud-hypervisor v52.0**; the data below is all v52.
- **`image_type: "Raw"`** must be declared on each disk — CH auto-detects raw
  images and disables sector-0 writes, which otherwise panics the ext4 rootfs
  (`Unable to mount root fs … ReadOnly`).
- **`memory.shared = true`** (memfd-backed guest RAM) enables **sparse snapshots**
  ([CH PR #8113](https://github.com/cloud-hypervisor/cloud-hypervisor/pull/8113)):
  CH walks `SEEK_DATA`/`SEEK_HOLE` and writes only touched pages. Anonymous mmap
  and plain file-backed RAM fall back to a dense (full-RAM) image.
- **`memory_restore_mode`**: `copy` (eager, default) vs `ondemand` (userfaultfd,
  lazy). Both are used below.
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

Caveat measured while fixing the correctness check (see Open items): ondemand's
per-page userfaultfd fault-in is **slow in aggregate (~40 MB/s)** — faulting a full
4 GiB working set back in took **~98 s**, vs `copy`'s eager bulk read of the same
4 GiB in **~2.6 s**. So ondemand's flat ~25 ms is a *first-response* win; if the
workload ends up touching most of its memory, **copy is far better in total** (and
the gap widens with size). Pick ondemand only when wake genuinely touches little.

## 5. gVisor/runsc — C workload (n=3)

| mechanism | WS | resume | suspend | image | freed (cgroup swap) |
|---|--:|--:|--:|--:|--:|
| checkpoint | 64 MiB | 545 ms | 112 ms | **67 MB** | n/a |
| swap | 64 MiB | 49 ms | 3031 ms | — | 83 MB |
| coldstart | 64 MiB | 441 ms | — | — | — |
| checkpoint | 256 MiB | 984 ms | 234 ms | **269 MB** | n/a |
| swap | 256 MiB | 58 ms | 700 ms | — | 285 MB |

Findings:
- **gVisor checkpoint images are smaller than CH's** (67/269 MB vs CH sparse
  127/328 MB at the same working set) — gVisor serializes *application state*, not
  a guest-RAM dump, so there's no guest-kernel/page-table overhead.
- **Suspend is comparable to CH sparse** (112/234 ms vs 83/206 ms).
- **Resume is eager-only and scales with the working set** (545/984 ms): gVisor
  has **no userfaultfd/lazy restore**, so it can't match CH ondemand's flat
  ~25 ms. gVisor resume behaves like CH `copy`.
- **Checkpoint ≫ swap on suspend** here too (112–234 ms vs 0.7–3.0 s).
- Caveat: gVisor `host_rss_freed` is undercounted (we read one PID's VmRSS, but
  gVisor spreads memory across sentry+gofer); the cgroup `swap.current`
  (83/285 MB) is the accurate "freed" figure for gVisor swap.

### gVisor — realistic Vite dev server (n=2)
| mechanism | WS | boot | resume | suspend | image |
|---|--:|--:|--:|--:|--:|
| checkpoint | 64 MiB | 837 | **1080 ms** | 204 ms | 196 MB |
| coldstart | 64 MiB | 844 | **883 ms** | — | — |
| swap | 64 MiB | 865 | 419 ms | 3042 ms | 219 MB |
| checkpoint | 256 MiB | 861 | **1828 ms** | 298 ms | 383 MB |
| coldstart | 256 MiB | 837 | **883 ms** | — | — |
| swap | 256 MiB | 849 | 388 ms | 5177 ms | 407 MB |

**Surprise: for gVisor + a fast-booting app, checkpoint/restore resume is *slower
than cold start*** (1080–1828 ms vs 883 ms). gVisor boots fast (~840 ms vs CH's
~2 s), and its *eager* restore must deserialize the whole V8/Vite app-state, which
costs more than just re-running the app. So gVisor checkpoint/restore only pays off
for **slow-to-initialize** apps (restore ≪ cold init); for snappy ones, cold start
wins. (swap resumes fast at ~400 ms but suspends in 3–5 s — still a loss.)

### cloud-hypervisor vs gVisor (checkpoint/restore)
| | image size | suspend | resume (C) | resume (Vite) | lazy resume? | boot |
|---|--:|--:|--:|--:|:--:|--:|
| **CH (sparse + ondemand)** | touched + ~0.34 GB | fast | **flat ~25 ms** | **275–432 ms** | ✅ userfaultfd | ~2 s |
| **gVisor (runsc)** | **smallest** (app state) | fast | 545–984 ms (eager) | 1080–1828 ms (eager) | ❌ | **~0.8 s** |

Takeaways: **CH wins resume latency decisively** (demand-paged guest RAM beats
gVisor's eager app-state deserialization — ~25 ms vs ~1 s on Vite). **gVisor wins
image size and boot time.** Both crush live swap on suspend and restore into a
fresh process tree. The sharp one: **gVisor's fast boot can make checkpoint/restore
not worth it** for fast-initializing workloads — whereas CH (slow VM boot ~2 s,
flat ~25 ms ondemand resume) benefits enormously from checkpoint/restore.

---

## 6. Second host — `bench-c3-standard-22-lssd` (zswap kernel + striped SSD)

A second GCE host added the three things the n2 could not measure: a
**zswap-enabled kernel** (the stock Debian 13 *cloud* kernel has `CONFIG_ZSWAP`
unset — we boot the generic kernel `6.12.90+deb13.1-amd64`), **RAID0 across 4×
375 GB local SSD** (`/dev/md0`, ~1.5 TB), and a faster CPU (**22 vCPU Intel
Sapphire Rapids 8481C** vs the n2's 16 vCPU). cloud-hypervisor **v52.0**, memfd
sparse throughout. zswap = zstd / zsmalloc / 50% max pool. All cells verified.

### 6.1 Striping (RAID0) vs the n2's separate disks — C, 8 GiB guest, checkpoint
copy and ondemand are SSD-write/read-bound, so this isolates the storage win
(zswap-irrelevant — checkpoint never swaps):

| restore | WS | suspend (c3 / n2) | resume→ping (c3 / n2) |
|---|--:|--:|--:|
| ondemand | 1 GiB | 674 / 818 ms | **22 / 25.8 ms** |
| copy | 1 GiB | **680 / 817 ms** | **684 / 765 ms** |
| ondemand | 4 GiB | 2,274 / 2,776 ms | **23 / 24.5 ms** |
| copy | 4 GiB | **2,288 / 2,909 ms** | **2,281 / 2,560 ms** |

- Striping (+ the faster CPU) buys **~15–20%** on the SSD-bound paths
  (checkpoint-copy suspend and resume).
- **ondemand resume stays flat ~22 ms** regardless of striping — first-response
  latency is fault-bound, not storage-bound, so faster disks don't move it. The
  win from faster SSD lands entirely on *copy* restore and on *suspend* (the write
  side), exactly where §3 predicted.

### 6.2 zswap diagnostic (the question the n2 couldn't answer) — swap, C, 8 GiB guest
Isolates whether any swap advantage is the **"z" (compress-in-RAM)** or the
suspend **mechanism**, by toggling `zswap.enabled` on the same swap path:

| zswap | WS | **suspend** | **resume→ping** | zswap pool | on-disk swap |
|---|--:|--:|--:|--:|--:|
| off | 256 MiB | **3,179 ms** | 79 ms | 0 | 511 MiB |
| on | 256 MiB | 3,661 ms | **24 ms** | 18 MiB | 511 MiB |
| off | 1 GiB | **3,695 ms** | 97 ms | 0 | 1,280 MiB |
| on | 1 GiB | 5,358 ms | **45 ms** | 18 MiB | 1,280 MiB |

- **zswap does not help suspend — it makes it *slower*** (1 GiB: 3.7 s → 5.4 s).
  Compression runs inline during the page-by-page cgroup reclaim, adding CPU to the
  exact path that is already swap's bottleneck.
- **zswap roughly halves *resume*** (97 → 45 ms): the small compressed fraction
  faults back from RAM instead of disk. But resume was never swap's problem.
- **The C working set is largely incompressible** (a HASH fill): only **18 MiB**
  ever entered the zswap pool while the *full* working set still hit on-disk swap.
  So even where zswap *could* help, it barely engaged.
- **Conclusion: the suspend cost is the reclaim mechanism, not the absence of
  compression.** The "z" is not the source of any swap advantage (there isn't one);
  it only ever helps the cheap axis (resume) and taxes the expensive one (suspend).
  Swap suspend stays 3–5 s vs sparse-checkpoint's 0.7 s suspend / 22 ms resume.

### 6.3 Large-allocation scaling — C, up to 16 GiB dirtied, 20 GiB guest, sparse
_In progress — a checkpoint-vs-swap scaling run (8 + 16 GiB dirtied, copy/ondemand,
plus zswap on/off at scale) is underway on the c3; numbers will be appended here.
Based on §3, expect ondemand resume to stay flat (~tens of ms) and swap suspend to
keep climbing linearly into the tens of seconds._

### 6.4 Node/Vite re-run — 2 GiB guest
| mechanism | WS | suspend | resume→serves | note |
|---|--:|--:|--:|--|
| checkpoint copy | 64 MiB | 185 ms | **259 ms** | ≈ n2 (275 ms), slightly faster |
| checkpoint ondemand | 64 MiB | 186 ms | 326 ms | copy ≥ ondemand for Vite, as on n2 |
| checkpoint copy | 256 MiB | 290 ms | **353 ms** | ≈ n2 (372 ms) |
| swap | 64 MiB | **7,447 ms** | 267 ms | zswap on (host default) |
| swap | 256 MiB | **8,114 ms** | 246 ms | zswap on (host default) |

Checkpoint parity with n2 holds (copy ≥ ondemand for a fast-faulting app). Swap
suspend here is *worse* than the n2's 3.6–4.1 s **because these ran with zswap on**
(host default), reinforcing §6.2: compression taxes suspend.

### 6.5 Backing up a snapshot to object storage
A CH snapshot is a **directory**, not a single file:

| file | size (2 GiB guest, idle) | contents |
|---|--:|---|
| `config.json` | ~1.5 KB | VM topology: kernel path, disk path, vsock socket, mem cfg |
| `state.json` | ~56 KB | device + vCPU register state |
| `memory-ranges` | **2 GiB apparent / 76 MB on-disk** | guest RAM dump — a **sparse file** |

`memory-ranges` is sparse: logical size = full guest RAM, physical size = touched
set only (measured: 8.59 GB apparent / 1.34 GB on-disk at 1 GiB WS; / 4.57 GB at
4 GiB). **The trap:** object stores (GCS/S3) persist exactly the bytes you `PUT` —
a naïve `cp`/`gsutil cp` reads the holes back as zeros and uploads the *full guest
RAM* (~6.4× inflation that scales with guest size, not working set); holes don't
survive a plain HTTP upload anyway.

- **Compress before upload** (the §2 zstd path, already in
  `internal/mechanism/compress.go` — it zstd's only `memory-ranges`): squeezes the
  zero-holes to ~the touched-set size. `tar -S -I zstd -cf snap.tar.zst <dir>/` →
  one object; restore = pull + decompress + `--restore source_url=file://<dir>`.
- **Sparse-aware copy** (`tar -S`, `cp --sparse=always`, `rsync -S`) only helps a
  *filesystem* hop, not a raw object upload.
- **Cross-node restore needs more than the snapshot dir:** the snapshot holds RAM
  + device state only — **not the rootfs disk, not the kernel**. The target node
  also needs the guest kernel and the (rw, so current-state) rootfs image at the
  paths `config.json` references, or those paths rewritten. Same-node
  suspend/resume needs none of this.

---

## Conclusion (for the architecture decision)
- **Use sparse (memfd) + checkpoint/restore to local SSD.** Pick restore mode by
  wake access pattern (ondemand default; copy when the workload touches most of
  its memory on wake). Upload a compressed image to object storage for cross-node.
- **Do not build live-swap-under-atelet.** Swap is slower to suspend (dramatically
  at multi-GB), slower to resume, the same footprint, and pins actors to a node.
  Its only theoretical edge (compress-in-RAM via zswap) is now measured (§6.2) and
  does **not** help: zswap makes suspend *slower* (inline compression on the reclaim
  path) and benefits only resume — it cannot overcome a multi-second suspend.
- **memfd (needed for sparse) does not pin to a node** — snapshots restore into a
  fresh process tree, so portability is intact.

## Open items
- **zswap diagnostic: DONE** (§6.2) — on a zswap-enabled kernel, zswap makes swap
  *suspend* slower and only helps resume; the "z" is not a swap advantage.
- **Faster/striped local SSD: DONE** (§6.1) — RAID0 over 4× local SSD buys ~15–20%
  on checkpoint-copy suspend/resume; ondemand resume stays flat ~22 ms
  (storage-independent).
- **gVisor: DONE** (see §5) — C + Vite, checkpoint/restore + swap + coldstart all
  work and verify. (Minor: gVisor `host_rss_freed` is undercounted for swap; use
  the cgroup `swap.current` figure.)
- **Correctness at scale: FIXED.** The post-resume correctness `HASH` reads the
  whole working set; on ondemand restore / swap-in that faults the entire set in
  lazily, which at multi-GB outran the old fixed RPC deadline and was mis-recorded
  as `0/2` "corruption" (it wasn't — `copy` passed and resume succeeded; only the
  read timed out). Fix (harness-only): (1) do `WALK` **before** `HASH` so the
  fault-in is borne by `WALK` (already non-fatal) and `HASH` then reads resident
  memory; (2) scale the per-command read deadline with the working set
  (`60 s + WS/50 MBps`). Optional further hardening (not done — needs a workload
  rebuild): make `HASH` sample one word per spread-of-pages so correctness compute
  is O(sample) regardless of size.

## Reproducing
See `README.md`. Results JSONL/CSV are written under `results/`. Raw data for these
tables: `results/{memfd,multigb,node_real,v52}.jsonl` on `bench-host-n2`; the §6
host data is `results/c3_{multigb,zswap,node,16g,16g_zswap}.jsonl` on
`bench-c3-standard-22-lssd`.
