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

package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// CgroupParent is where setup-cgroup.sh delegated the memory controller.
const CgroupParent = "/sys/fs/cgroup/suspend-bench"

// Cgroup is one per-instance leaf cgroup. The VMM/sandbox is launched directly
// into it so cgroup v2's "charges don't migrate on move" rule never bites — we
// drive memory.reclaim / memory.swap.max on the leaf where pages were faulted.
type Cgroup struct {
	Path string // absolute path under /sys/fs/cgroup
}

// NewCgroup creates (mkdir) a leaf named id under CgroupParent.
func NewCgroup(id string) (*Cgroup, error) {
	p := filepath.Join(CgroupParent, id)
	if err := os.Mkdir(p, 0o755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("creating cgroup %s: %w", p, err)
	}
	return &Cgroup{Path: p}, nil
}

// AddPID moves a process into this leaf (one PID per write, per cgroup v2).
func (c *Cgroup) AddPID(pid int) error {
	return os.WriteFile(filepath.Join(c.Path, "cgroup.procs"),
		[]byte(strconv.Itoa(pid)), 0o644)
}

// write is a helper for the small control files.
func (c *Cgroup) write(file, val string) error {
	return os.WriteFile(filepath.Join(c.Path, file), []byte(val), 0o644)
}

// SetSwapMax sets memory.swap.max ("0" to forbid swap on active actors, "max" to
// allow the whole cgroup to be swapped).
func (c *Cgroup) SetSwapMax(val string) error { return c.write("memory.swap.max", val) }

// Reclaim asks the kernel to reclaim n bytes from this cgroup (cgroup v2
// memory.reclaim). With swap.max>0 this is what pages the guest RAM out to
// swap/zswap, charged to this leaf. Returns how long the kernel took via the
// caller's timer.
func (c *Cgroup) Reclaim(n int64) error {
	return c.write("memory.reclaim", strconv.FormatInt(n, 10))
}

// ReadBytes reads a single-value memory.* file; "max" maps to -1.
func (c *Cgroup) ReadBytes(file string) int64 {
	b, err := os.ReadFile(filepath.Join(c.Path, file))
	if err != nil {
		return -1
	}
	s := strings.TrimSpace(string(b))
	if s == "max" {
		return -1
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1
	}
	return v
}

func (c *Cgroup) Current() int64      { return c.ReadBytes("memory.current") }
func (c *Cgroup) SwapCurrent() int64  { return c.ReadBytes("memory.swap.current") }
func (c *Cgroup) ZswapCurrent() int64 { return c.ReadBytes("memory.zswap.current") }

// Remove deletes the leaf cgroup (must be empty of processes).
func (c *Cgroup) Remove() error { return os.Remove(c.Path) }
