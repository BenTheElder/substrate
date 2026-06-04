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

//go:build linux

package gvisor

import (
	"context"
	"fmt"
	"hash/fnv"
	"os/exec"
	"path/filepath"
)

// netConf is the per-instance network: a veth pair with one end in a dedicated
// netns (which gVisor's netstack adopts) so the host can reach the sandbox's TCP
// listener. We use TCP (a netstack socket) instead of a host-uds socket because
// runsc cannot checkpoint a bound host socket.
type netConf struct {
	nsPath    string // /var/run/netns/<name>
	nsName    string
	vethHost  string
	guestAddr string // "<ip>:<port>" the harness dials
}

// setupNet creates netns + veth and returns how to reach the guest. Each instance
// gets its own /30 (10.222.N.0/30): host .1, guest .2. The subnet/netns name is
// DETERMINISTIC in id so it can be re-created identically on restore (the netns is
// torn down by the post-checkpoint Teardown, but config.json still references it).
// It cleans any stale leftovers first, so re-creating is safe.
func setupNet(ctx context.Context, id string, port int) (*netConf, error) {
	h := fnv.New32a()
	h.Write([]byte(id))
	n := h.Sum32()%250 + 1
	ns := fmt.Sprintf("sb%d", n)
	vh := fmt.Sprintf("vh%d", n)
	vg := fmt.Sprintf("vg%d", n)
	host := fmt.Sprintf("10.222.%d.1", n)
	guest := fmt.Sprintf("10.222.%d.2", n)

	// Best-effort clean any stale leftovers from a crashed prior run.
	exec.Command("ip", "netns", "del", ns).Run()
	exec.Command("ip", "link", "del", vh).Run()

	run := func(args ...string) error {
		if out, err := exec.CommandContext(ctx, "ip", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("ip %v: %w: %s", args, err, out)
		}
		return nil
	}
	steps := [][]string{
		{"netns", "add", ns},
		{"link", "add", vh, "type", "veth", "peer", "name", vg},
		{"link", "set", vg, "netns", ns},
		{"addr", "add", host + "/30", "dev", vh},
		{"link", "set", vh, "up"},
		{"-n", ns, "addr", "add", guest + "/30", "dev", vg},
		{"-n", ns, "link", "set", vg, "up"},
		{"-n", ns, "link", "set", "lo", "up"},
	}
	for _, s := range steps {
		if err := run(s...); err != nil {
			teardownNet(&netConf{nsName: ns, vethHost: vh})
			return nil, err
		}
	}
	return &netConf{
		nsPath:    filepath.Join("/var/run/netns", ns),
		nsName:    ns,
		vethHost:  vh,
		guestAddr: fmt.Sprintf("%s:%d", guest, port),
	}, nil
}

// teardownNet removes the netns (which also deletes the moved veth peer) and the
// host-side veth.
func teardownNet(n *netConf) {
	if n == nil {
		return
	}
	if n.nsName != "" {
		exec.Command("ip", "netns", "del", n.nsName).Run()
	}
	if n.vethHost != "" {
		exec.Command("ip", "link", "del", n.vethHost).Run()
	}
}
