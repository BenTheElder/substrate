//go:build linux

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

package kata

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata/agentpb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// This file holds the host-side sandbox bring-up ateom does over the kata-agent
// ttrpc API when it OWNS the cold boot (no kata shim): the sandbox-level
// storages, the guest network configuration, and the "carrier" container. The
// values mirror what kata's runtime (kata_agent.go startSandbox / fs_share) sends
// for a cloud-hypervisor + virtio-fs sandbox.

const (
	// FsTag is the virtio-fs tag kata uses for the shared filesystem. The CH fs
	// device Tag and the agent mount Source must both be this value.
	FsTag = "kataShared"
	// typeVirtioFS / virtioFSDriver are the agent fstype + driver for it.
	typeVirtioFS   = "virtiofs"
	virtioFSDriver = "virtio-fs"
	// guestSharedDir is where the agent mounts the kataShared tag in the guest.
	// Per-container rootfs then lives at <guestSharedDir>/<cid>/rootfs.
	guestSharedDir = "/run/kata-containers/shared/containers/"
)

// OverlayUpperBase is the in-guest mount point for the host-backed scratch disk
// (/dev/vdb) that holds the overlay upper/work. It is keyed on the FROZEN base id
// (the golden cold-run id), NOT the per-actor id, because the mount + overlay are
// frozen in the snapshot and survive restore unchanged across the actor's lineage;
// the checkpoint reset-to-golden wipe must target this same frozen path.
func OverlayUpperBase(baseID string) string {
	return "/run/ateom-upper/" + baseID
}

// GuestSharedRootfs is the in-guest path the kataShared mount exposes a
// container's rootfs at. A container created with this as its OCI Root.Path makes
// the agent's setup_bundle bind it (eagerly, stable on arm64) to
// /run/kata-containers/<cid>/rootfs, which a later overlay can use as lowerdir.
func GuestSharedRootfs(containerID string) string {
	return guestSharedDir + containerID + "/rootfs"
}

// CreateSandboxForActor brings up the guest sandbox for ateom-owned cold boot:
// it mounts the kataShared virtio-fs filesystem (which backs the container rootfs
// base) and marks the sandbox running. Must be called after the network RPCs and
// before any CreateContainer (mirrors kata startSandbox ordering).
func (a *AgentClient) CreateSandboxForActor(ctx context.Context, sandboxID string) error {
	req := &agentpb.CreateSandboxRequest{
		SandboxId: sandboxID,
		Storages: []*agentpb.Storage{
			{
				Driver:     virtioFSDriver,
				Source:     FsTag,
				Fstype:     typeVirtioFS,
				MountPoint: guestSharedDir,
			},
		},
	}
	return a.CreateSandbox(ctx, req)
}

// GuestNetwork is the static guest eth0 configuration ateom applies on cold boot.
// The address is stable across the actor's lineage (see net.go), so restore needs
// no re-IP — the guest's frozen config survives.
type GuestNetwork struct {
	// Iface is the guest device name (e.g. "eth0").
	Iface string
	// Address / MaskPrefix are the actor IP and CIDR prefix length (e.g.
	// "169.254.17.2" / "30").
	Address    string
	MaskPrefix string
	// MAC is the guest eth0 hardware address (must equal the CH virtio-net device
	// MAC so the agent's apply is a no-op rather than a change).
	MAC string
	// MTU is the guest eth0 MTU (must equal the host veth/tap MTU for the TC
	// mirror to pass packets without fragmentation).
	MTU uint64
	// Gateway is the default-route next hop (e.g. "169.254.17.1").
	Gateway string
	// GatewayMAC is the fixed host veth MAC; pinned as a static ARP entry so the
	// guest's frozen ARP cache stays valid on every pod across restore.
	GatewayMAC string
}

// rtScopeLink is RT_SCOPE_LINK — the scope of the directly-connected route the
// kernel derives from the interface address.
const rtScopeLink = 253

// ConfigureNetwork applies the guest interface address/MTU/MAC, installs the
// connected + default routes, and pins the gateway's ARP entry — the work kata's
// runtime does via setupNetworks before CreateSandbox.
func (a *AgentClient) ConfigureNetwork(ctx context.Context, n GuestNetwork) error {
	iface := &agentpb.Interface{
		Device: n.Iface,
		Name:   n.Iface,
		Mtu:    n.MTU,
		HwAddr: n.MAC,
		Type:   "tap",
		IPAddresses: []*agentpb.IPAddress{
			{Family: agentpb.IPFamily_v4, Address: n.Address, Mask: n.MaskPrefix},
		},
	}
	if err := a.UpdateInterface(ctx, iface); err != nil {
		return err
	}

	// Connected route (derived from the /30) + default route via the gateway.
	connected := fmt.Sprintf("%s/%s", networkAddress(n.Address, n.MaskPrefix), n.MaskPrefix)
	routes := []*agentpb.Route{
		{Dest: connected, Device: n.Iface, Scope: rtScopeLink, Source: n.Address, Family: agentpb.IPFamily_v4},
		{Dest: "", Gateway: n.Gateway, Device: n.Iface, Family: agentpb.IPFamily_v4},
	}
	if err := a.UpdateRoutes(ctx, routes); err != nil {
		return err
	}

	if n.GatewayMAC != "" {
		// NUD_PERMANENT (0x80) static entry pinning the gateway MAC.
		const nudPermanent = 0x80
		neighbors := []*agentpb.ARPNeighbor{{
			Device:      n.Iface,
			Lladdr:      n.GatewayMAC,
			State:       nudPermanent,
			ToIPAddress: &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: n.Gateway},
		}}
		// Best-effort: a stale ARP entry only matters across restore, and the guest
		// would re-ARP the (fixed-MAC) gateway anyway.
		if err := a.AddARPNeighbors(ctx, neighbors); err != nil {
			return err
		}
	}
	return nil
}

// SetupScratchUpper formats the boot-time scratch disk (/dev/vdb) and mounts it
// at upperBase in the guest, via the debug console (vsock 1026) — the same channel
// reset-to-golden uses. The kata guest provides mkfs.ext4 + mount, so no host-side
// e2fsprogs is needed. Run after the guest is up and before StartOverlayWorkload
// (whose overlay upper/work land under upperBase). Returns an error if the console
// does not confirm success.
func SetupScratchUpper(ctx context.Context, vsockPath, upperBase string) error {
	cmd := "mkfs.ext4 -F -q /dev/vdb && mkdir -p " + upperBase +
		" && mount /dev/vdb " + upperBase + " && echo ATE_SETUP_OK"
	out := DebugConsoleDump(ctx, vsockPath, cmd)
	if !strings.Contains(out, "ATE_SETUP_OK") {
		return fmt.Errorf("scratch-disk setup did not confirm (console: %s)", strings.TrimSpace(out))
	}
	return nil
}

// CreateCarrier creates the "carrier" container (id == sandboxID): a container
// whose rootfs is the kataShared virtio-fs base, created but NOT started. Its
// CreateContainer makes the agent's setup_bundle bind the base to
// /run/kata-containers/<id>/rootfs (eager + stable on arm64), which
// StartOverlayWorkload then uses as the overlay lowerdir. The kata shim created
// this implicitly as the first container; ateom creates it explicitly.
func (a *AgentClient) CreateCarrier(ctx context.Context, sandboxID string, spec *specs.Spec) error {
	pbSpec := SpecToAgentPB(spec)
	pbSpec.Root = &agentpb.Root{Path: GuestSharedRootfs(sandboxID), Readonly: false}
	// Distinct cgroup from the overlay workload (which reuses the same spec's
	// CgroupsPath) so the two containers don't collide in the guest.
	if pbSpec.Linux != nil {
		pbSpec.Linux.CgroupsPath = "/ateomchv/" + sandboxID + "-carrier"
	}
	if err := a.CreateContainer(ctx, &agentpb.CreateContainerRequest{
		ContainerId: sandboxID,
		ExecId:      sandboxID,
		OCI:         pbSpec,
	}); err != nil {
		return fmt.Errorf("creating carrier container %q: %w", sandboxID, err)
	}
	return nil
}

// networkAddress masks an IPv4 dotted address to its network address given a CIDR
// prefix length (e.g. 169.254.17.2/30 -> 169.254.17.0).
func networkAddress(addr, prefix string) string {
	ip := net.ParseIP(addr).To4()
	if ip == nil {
		return addr
	}
	var plen int
	if _, err := fmt.Sscanf(prefix, "%d", &plen); err != nil || plen < 0 || plen > 32 {
		return addr
	}
	mask := net.CIDRMask(plen, 32)
	return ip.Mask(mask).String()
}
