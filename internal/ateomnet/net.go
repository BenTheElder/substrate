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

// Package ateomnet provides shared networking configuration logic for Substrate runtime agents.
package ateomnet

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"

	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	HostVethName     = "ateom0"
	ActorVethName    = "eth0"
	HostVethCIDR     = "169.254.17.1/30"
	ActorVethCIDR    = "169.254.17.2/30"
	ActorVethGateway = "169.254.17.1"
	ActorVethIP      = "169.254.17.2"

	// ActorVethSubnet is the point-to-point /30 the actor veth lives on.
	ActorVethSubnet = "169.254.17.0/30"
)

var (
	HostVethAddr  = MustParseAddr(HostVethCIDR)
	ActorVethAddr = MustParseAddr(ActorVethCIDR)
	ActorVethGwIP = MustParseIP(ActorVethGateway)
)

// MustParseAddr parses a CIDR string into a netlink.Addr, panicking on error.
func MustParseAddr(cidr string) *netlink.Addr {
	a, err := netlink.ParseAddr(cidr)
	if err != nil {
		panic(fmt.Sprintf("parsing constant CIDR %q: %v", cidr, err))
	}
	return a
}

// MustParseIP parses an IPv4 string into a net.IP, panicking on error.
func MustParseIP(s string) net.IP {
	ip := net.ParseIP(s).To4()
	if ip == nil {
		panic(fmt.Sprintf("parsing constant IPv4 %q", s))
	}
	return ip
}

// MustParseMAC parses a MAC address string into a net.HardwareAddr, panicking on error.
func MustParseMAC(s string) net.HardwareAddr {
	m, err := net.ParseMAC(s)
	if err != nil {
		panic(fmt.Sprintf("parsing constant MAC %q: %v", s, err))
	}
	return m
}

// EnableIPv4Forwarding enables IPv4 forwarding in the current network namespace.
//
// Forwarding is required because actor packets enter the worker pod via the
// host-side veth and then leave through the pod's eth0. Without this, the kernel
// would not route traffic between those interfaces even though both live in the
// worker pod network namespace.
func EnableIPv4Forwarding() error {
	return setNetSysctl("net/ipv4/ip_forward", "1")
}

// DisableReversePathFilter turns off reverse-path validation for the actor veths
// in the current (worker pod) network namespace.
//
// Reverse-path validation cannot work here, by construction. Every actor holds
// the SAME frozen address, so every host-side veth carries the same /30 and the
// kernel has one connected route per actor for it. Asked which interface
// 169.254.17.2 lives behind, it can only answer "the first one" -- so with
// rp_filter on, every actor but that one is a martian source. What actually
// tells the actors apart is the iifname map in nftables, which runs at raw
// prerouting, before the routing decision.
//
// For IP that rewrite lands first and the packet passes. ARP is what suffers:
// it is not IP, so no nftables ip rule sees it, and arp_process runs the same
// reverse-path check on the sender address. The check fails, the kernel drops
// the request WITHOUT replying, and logs "martian source 169.254.17.1 from
// 169.254.17.2". An actor that has to resolve its gateway itself -- rather than
// learning the MAC from an inbound request, or restoring a snapshot with the
// entry already in it -- cannot.
//
// Scoped deliberately. Setting only conf.all leaves each interface's own value
// in force (the effective setting is the max of the two), so eth0 keeps the
// validation it was created with and only the veths created afterwards, which
// inherit conf.default, give it up.
func DisableReversePathFilter() error {
	for _, key := range []string{"net/ipv4/conf/all/rp_filter", "net/ipv4/conf/default/rp_filter"} {
		if err := setNetSysctl(key, "0"); err != nil {
			return err
		}
	}
	return nil
}

// setNetSysctl writes value to the named sysctl in the current network
// namespace, and is a no-op when it already reads that way.
//
// Without privileged, the container runtime bind-mounts /proc/sys read-only.
// The worker holds CAP_SYS_ADMIN and uses no user namespace, so the ro flag is
// not locked: clear it, write the sysctl, restore ro.
func setNetSysctl(key, value string) error {
	path := "/proc/sys/" + key
	if b, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(b)) == value {
		return nil
	}
	if err := os.WriteFile(path, []byte(value+"\n"), 0o644); err == nil {
		return nil
	}
	if err := unix.Mount("none", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT, ""); err != nil {
		return fmt.Errorf("while remounting /proc/sys read-write to set %s: %w", key, err)
	}
	defer func() {
		_ = unix.Mount("none", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, "")
	}()
	if err := os.WriteFile(path, []byte(value+"\n"), 0o644); err != nil {
		return fmt.Errorf("while setting %s in worker pod netns: %w", key, err)
	}
	return nil
}

func TCPProtocol() []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{unix.IPPROTO_TCP},
		},
	}
}

// CreateNetNSWithoutSwitching creates a named netns and returns its handle,
// restoring the caller's current netns before returning.
func CreateNetNSWithoutSwitching(name string) (netns.NsHandle, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return -1, fmt.Errorf("while getting current netns: %w", err)
	}
	// Registered before the restoring defer below since deferred calls are LIFO.
	defer curNetNS.Close()
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	interiorNetNS, err := netns.NewNamed(name)
	if err != nil {
		return -1, fmt.Errorf("while creating interior network namespace: %w", err)
	}
	return interiorNetNS, nil
}

// NetNSDo runs do() with the OS thread switched into targetNS, then restores it.
func NetNSDo(ctx context.Context, targetNS netns.NsHandle, do func(context.Context) error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting current netns: %w", err)
	}
	// Registered before the restoring defer below since deferred calls are LIFO.
	defer curNetNS.Close()
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	if err := netns.Set(targetNS); err != nil {
		return fmt.Errorf("setting target netns: %w", err)
	}
	if err := do(ctx); err != nil {
		return fmt.Errorf("while executing function in target netns: %w", err)
	}
	return nil
}
