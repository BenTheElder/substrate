// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command bench drives the suspend/resume benchmark matrix and streams results.
//
// Per cell it boots a sandbox, drives the workload to a target dirty working set,
// suspends it via the chosen mechanism, resumes it, checks state was preserved,
// and records timings + memory accounting. See README.md for the question this
// answers and the staged methodology (swap vs checkpoint first; compression /
// restore-mode / zswap as Stage-2 diagnostics).
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/suspend-bench/internal/mechanism"
	"github.com/agent-substrate/substrate/suspend-bench/internal/metrics"
	"github.com/agent-substrate/substrate/suspend-bench/internal/results"
	"github.com/agent-substrate/substrate/suspend-bench/internal/runtime"
	"github.com/agent-substrate/substrate/suspend-bench/internal/runtime/ch"
	"github.com/agent-substrate/substrate/suspend-bench/internal/runtime/gvisor"
)

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

type config struct {
	runtimes    []string
	mechanisms  []string
	workloads   []string
	workingSets []int64
	reps        int
	compression []string
	restoreMode []string
	zswap       []string // "on"/"off"; empty => leave host as-is

	out         string
	smoke       bool
	dropCaches  bool
	guestMem    int64
	vcpus       int
	backingFile bool
	sharedMem   bool

	kernel    string
	rootfs    map[string]string // workload -> ext4 image (ch)
	bundle    map[string]string // workload -> OCI bundle (gvisor)
	imageBase string
	workBase  string
	hostMeta  string
	chBin     string
	runscBin  string

	resumeDeadline time.Duration
	cellTimeout    time.Duration
}

func parseFlags() config {
	var c config
	var runtimes, mechs, workloads, wss, comp, rmode, zswap string
	var rootfsC, rootfsNode, bundleC, bundleNode string
	flag.StringVar(&runtimes, "runtimes", "ch", "comma list: ch,gvisor")
	flag.StringVar(&mechs, "mechanisms", "checkpoint_local,swap,coldstart", "comma list")
	flag.StringVar(&workloads, "workloads", "c", "comma list: c,node")
	flag.StringVar(&wss, "working-sets", "64MiB", "comma list of sizes, e.g. 64MiB,256MiB,1GiB")
	flag.IntVar(&c.reps, "reps", 5, "repetitions per cell")
	flag.StringVar(&comp, "compression", "none", "checkpoint_local: none,zstd,lz4")
	flag.StringVar(&rmode, "restore-modes", "copy", "ch checkpoint_local: copy,ondemand")
	flag.StringVar(&zswap, "zswap", "", "swap diagnostic: on,off (empty=leave host as-is)")
	flag.StringVar(&c.out, "out", "results/results.jsonl", "output JSONL path (CSV sibling auto-created)")
	flag.BoolVar(&c.smoke, "smoke", false, "run a small smoke matrix and assert correctness, then exit")
	flag.BoolVar(&c.dropCaches, "drop-caches", false, "drop page cache before each cell")
	flag.Int64Var(&c.guestMem, "guest-mem", 2<<30, "guest RAM bytes")
	flag.IntVar(&c.vcpus, "vcpus", 2, "guest vCPUs")
	flag.BoolVar(&c.backingFile, "backing-file", false, "back guest RAM with a file")
	flag.BoolVar(&c.sharedMem, "shared-mem", false, "memfd-backed guest RAM (CH shared=true) to enable sparse snapshots")
	flag.StringVar(&c.kernel, "kernel", "rootfs/kernel/vmlinux", "guest vmlinux (ch)")
	flag.StringVar(&rootfsC, "rootfs-c", "/mnt/nvme-images/rootfs/cworkload.img", "ch ext4 rootfs for c workload")
	flag.StringVar(&rootfsNode, "rootfs-node", "/mnt/nvme-images/rootfs/nodeworkload.img", "ch ext4 rootfs for node workload")
	flag.StringVar(&bundleC, "bundle-c", "/mnt/nvme-images/bundles/cworkload", "gvisor OCI bundle for c workload")
	flag.StringVar(&bundleNode, "bundle-node", "/mnt/nvme-images/bundles/nodeworkload", "gvisor OCI bundle for node workload")
	flag.StringVar(&c.imageBase, "image-dir", "/mnt/nvme-images/snapshots", "base dir for snapshots (local NVMe)")
	flag.StringVar(&c.workBase, "work-base", "/run/suspend-bench", "per-instance scratch base")
	flag.StringVar(&c.hostMeta, "host-meta", "/var/lib/suspend-bench/host-metadata.json", "host metadata json")
	flag.StringVar(&c.chBin, "ch-bin", "cloud-hypervisor", "cloud-hypervisor binary")
	flag.StringVar(&c.runscBin, "runsc-bin", "runsc", "runsc binary")
	flag.DurationVar(&c.resumeDeadline, "resume-deadline", 60*time.Second, "max wait for first ping after resume")
	flag.DurationVar(&c.cellTimeout, "cell-timeout", 5*time.Minute, "hard per-cell timeout backstop")
	flag.Parse()

	c.runtimes = splitCSV(runtimes)
	c.mechanisms = splitCSV(mechs)
	c.workloads = splitCSV(workloads)
	c.compression = splitCSV(comp)
	c.restoreMode = splitCSV(rmode)
	c.zswap = splitCSV(zswap)
	for _, s := range splitCSV(wss) {
		n, err := parseSize(s)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad --working-sets:", err)
			os.Exit(2)
		}
		c.workingSets = append(c.workingSets, n)
	}
	c.rootfs = map[string]string{"c": rootfsC, "node": rootfsNode}
	c.bundle = map[string]string{"c": bundleC, "node": bundleNode}
	return c
}

// cell is one unit of work in the matrix.
type cell struct {
	runtime, mech, workload  string
	wsBytes                  int64
	rep                      int
	compression, restoreMode string
	zswap                    *bool
}

func run(cfg config) error {
	if cfg.smoke {
		return runSmoke(cfg)
	}
	cells := buildMatrix(cfg)
	rand.New(rand.NewSource(time.Now().UnixNano())).Shuffle(len(cells), func(i, j int) {
		cells[i], cells[j] = cells[j], cells[i]
	})

	if err := os.MkdirAll(filepath.Dir(cfg.out), 0o755); err != nil {
		return err
	}
	w, err := results.NewWriter(cfg.out)
	if err != nil {
		return err
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	host, err := results.LoadHostMeta(cfg.hostMeta)
	if err != nil {
		fmt.Fprintln(os.Stderr, "warn: host meta:", err)
	}

	fmt.Printf("running %d cells -> %s\n", len(cells), cfg.out)
	for i, cl := range cells {
		if ctx.Err() != nil {
			fmt.Println("interrupted; stopping")
			break
		}
		cctx, ccancel := context.WithTimeout(ctx, cfg.cellTimeout)
		res := runCell(cctx, cfg, host, cl)
		ccancel()
		if err := w.Append(res); err != nil {
			return fmt.Errorf("writing result: %w", err)
		}
		status := "ok"
		if res.Error != "" {
			status = "ERR: " + res.Error
		} else if cl.mech != "coldstart" && !res.CorrectnessOK {
			status = "CORRUPT"
		}
		fmt.Printf("[%d/%d] %s/%s/%s ws=%s rep=%d resume=%.1fms %s\n",
			i+1, len(cells), cl.runtime, cl.mech, cl.workload,
			humanSize(cl.wsBytes), cl.rep, res.ResumeToFirstPingMs, status)
	}
	return nil
}

// buildMatrix expands the requested dimensions, gating diagnostic knobs to the
// mechanisms/runtimes where they apply so we don't waste runs.
func buildMatrix(cfg config) []cell {
	var cells []cell
	zswapVals := func(mech string) []*bool {
		if mech != "swap" || len(cfg.zswap) == 0 {
			return []*bool{nil}
		}
		var out []*bool
		for _, z := range cfg.zswap {
			b := z == "on"
			out = append(out, &b)
		}
		return out
	}
	for _, rt := range cfg.runtimes {
		for _, mech := range cfg.mechanisms {
			comps := []string{""}
			rmodes := []string{""}
			if mech == "checkpoint_local" {
				comps = cfg.compression
				if rt == "ch" {
					rmodes = cfg.restoreMode
				}
			}
			for _, wl := range cfg.workloads {
				for _, ws := range cfg.workingSets {
					for _, comp := range comps {
						for _, rm := range rmodes {
							for _, z := range zswapVals(mech) {
								for rep := 0; rep < cfg.reps; rep++ {
									cells = append(cells, cell{
										runtime: rt, mech: mech, workload: wl,
										wsBytes: ws, rep: rep,
										compression: comp, restoreMode: rm, zswap: z,
									})
								}
							}
						}
					}
				}
			}
		}
	}
	return cells
}

var cidCounter uint32 = 3

func runCell(ctx context.Context, cfg config, host results.HostMeta, cl cell) results.Result {
	res := results.Result{
		TimestampUnixNano: time.Now().UnixNano(),
		Runtime:           cl.runtime,
		Mechanism:         cl.mech,
		Workload:          cl.workload,
		WorkingSetBytes:   cl.wsBytes,
		Rep:               cl.rep,
		RestoreMode:       cl.restoreMode,
		Compression:       cl.compression,
		ZswapEnabled:      cl.zswap,
		GuestRAMBytes:     cfg.guestMem,
		Host:              host,
	}
	fail := func(err error) results.Result {
		res.Error = err.Error()
		return res
	}

	if cl.zswap != nil {
		if err := setZswap(*cl.zswap); err != nil {
			return fail(fmt.Errorf("set zswap: %w", err))
		}
	}
	if cfg.dropCaches {
		dropCaches()
	}

	rt, err := runtimeByName(cfg, cl.runtime)
	if err != nil {
		return fail(err)
	}
	mech, err := mechByName(cl.mech)
	if err != nil {
		return fail(err)
	}

	id := fmt.Sprintf("%s-%s-%s-%d-%d", cl.runtime, cl.mech, cl.workload, cl.wsBytes, time.Now().UnixNano())
	cg, err := metrics.NewCgroup(id)
	if err != nil {
		return fail(fmt.Errorf("cgroup: %w", err))
	}
	defer cg.Remove()
	workDir := filepath.Join(cfg.workBase, id)
	defer os.RemoveAll(workDir)

	spec := runtime.BootSpec{
		ID:          id,
		KernelPath:  cfg.kernel,
		RootfsPath:  cfg.rootfs[cl.workload],
		BundlePath:  cfg.bundle[cl.workload],
		MemoryBytes: cfg.guestMem,
		VCPUs:       cfg.vcpus,
		VsockCID:    nextCID(),
		// ondemand restore (userfaultfd) works from an anonymous-memory snapshot;
		// it does not require a file-backed guest, so only honor an explicit flag.
		BackingFile: cfg.backingFile,
		SharedMem:   cfg.sharedMem,
		Cgroup:      cg,
		WorkDir:     workDir,
	}

	// Boot + ready (first ping) = BootMs.
	bootStart := time.Now()
	h, err := rt.Boot(ctx, spec)
	if err != nil {
		return fail(fmt.Errorf("boot: %w", err))
	}
	defer rt.Teardown(context.Background(), h)
	bc, _, err := runtime.FirstPing(h, cfg.resumeDeadline)
	if err != nil {
		return fail(fmt.Errorf("boot ready ping: %w", err))
	}
	// WALK/HASH read the whole working set; on ondemand restore / swap-in that
	// faults the entire set in via userfaultfd, which is slow (~40 MB/s measured:
	// 4 GiB took ~98 s). Scale the per-command read deadline with the working set
	// (budget ~25 MB/s for margin) so it never times out and gets misread as
	// corruption. (e.g. 4 GiB → ~224 s, 16 GiB → ~12 min.)
	cmdTimeout := 60*time.Second + time.Duration(cl.wsBytes/(25<<20))*time.Second
	bc.SetCmdTimeout(cmdTimeout)
	res.BootMs = metrics.Ms(bootStart)

	// Drive to target working set and capture the pre-suspend state token + hash.
	if _, err := bc.Cmd(fmt.Sprintf("SETWS %d", cl.wsBytes)); err != nil {
		bc.Close()
		return fail(fmt.Errorf("setws: %w", err))
	}
	if _, err := bc.Cmd(fmt.Sprintf("DIRTY %d", cl.wsBytes)); err != nil {
		bc.Close()
		return fail(fmt.Errorf("dirty: %w", err))
	}
	preTok, err := bc.Ping()
	if err != nil {
		bc.Close()
		return fail(fmt.Errorf("pre ping: %w", err))
	}
	preHash, err := bc.Hash()
	if err != nil {
		bc.Close()
		return fail(fmt.Errorf("pre hash: %w", err))
	}
	bc.Close()

	// Suspend.
	sr, err := mech.Suspend(ctx, rt, h, mechanism.SuspendInput{
		ImageDir: imageDir(cfg, id), Compression: cl.compression,
	})
	if err != nil {
		return fail(fmt.Errorf("suspend: %w", err))
	}
	res.SuspendMs = sr.SuspendMs
	res.ImageApparentBytes = sr.ImageApparentBytes
	res.ImageActualBytes = sr.ImageActualBytes
	res.HostRSSFreedBytes = sr.HostRSSFreedBytes
	res.SwapCurrentBytes = sr.SwapCurrentBytes
	res.ZswapCurrentBytes = sr.ZswapCurrentBytes

	// Resume.
	rr, err := mech.Resume(ctx, rt, h, mechanism.ResumeInput{
		ImageDir: imageDir(cfg, id), Compression: cl.compression,
		RestoreMode: cl.restoreMode, ResumeDeadline: cfg.resumeDeadline,
	})
	if err != nil {
		return fail(fmt.Errorf("resume: %w", err))
	}
	res.ResumeCallMs = rr.ResumeCallMs
	res.ResumeToFirstPingMs = rr.ResumeToFirstPingMs

	// Post-resume page walk THEN correctness. WALK faults the whole working set
	// back in (its cost is exactly post_resume_walk_ms, and a timeout here is
	// non-fatal); doing it first means the subsequent HASH reads resident memory,
	// so the correctness check can't be misread as corruption just because a
	// multi-GB lazy fault-in outran a deadline.
	rc := rr.Client
	rc.SetCmdTimeout(cmdTimeout)
	walkStart := time.Now()
	if _, err := rc.Cmd("WALK"); err == nil {
		res.PostResumeWalkMs = metrics.Ms(walkStart)
	}
	if mech.PreservesState() {
		postHash, herr := rc.Hash()
		res.CorrectnessOK = herr == nil &&
			rr.Token.Seed == preTok.Seed &&
			rr.Token.Counter == preTok.Counter &&
			postHash == preHash
	} else {
		res.CorrectnessOK = true // baseline: state intentionally not preserved
	}
	rc.Close()

	// Clean up the snapshot dir for checkpoint runs.
	if mech.NeedsImageDir() {
		os.RemoveAll(imageDir(cfg, id))
	}
	return res
}

func imageDir(cfg config, id string) string { return filepath.Join(cfg.imageBase, id) }

func nextCID() uint32 {
	c := cidCounter
	cidCounter++
	return c
}

func runtimeByName(cfg config, name string) (runtime.Runtime, error) {
	switch name {
	case "ch":
		return ch.New(cfg.chBin), nil
	case "gvisor":
		return gvisor.New(cfg.runscBin), nil
	default:
		return nil, fmt.Errorf("unknown runtime %q", name)
	}
}

func mechByName(name string) (mechanism.Mechanism, error) {
	switch name {
	case "checkpoint_local":
		return mechanism.CheckpointLocal{}, nil
	case "swap":
		return mechanism.Swap{}, nil
	case "coldstart":
		return mechanism.ColdStart{}, nil
	default:
		return nil, fmt.Errorf("unknown mechanism %q", name)
	}
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
