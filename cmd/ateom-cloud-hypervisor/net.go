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

package main

// Networking mirrors cmd/ateom-gvisor: the pod's eth0 is moved into a named
// "interior" network namespace (its addresses/routes restored after the move),
// and the sandbox is pointed at that netns. gVisor reads the link via AF_PACKET;
// kata instead finds eth0 in the netns and builds a tap + TC mirror for the VM's
// virtio-net. (Copied with light adaptation; expected to be de-duplicated into a
// shared package on a later rebase.)

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"sort"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// SaveLinkInfo captures a link's addresses and routes so they can be restored
// after the link is moved into another network namespace (which drops them).
type SaveLinkInfo struct {
	Addresses []SaveAddr
	Routes    []SaveRoute
	MTU       int
}

type SaveAddr struct {
	Addr      net.IPNet
	Scope     int
	Broadcast net.IP
}

type SaveRoute struct {
	Scope    uint8
	Dst      net.IPNet
	Src      net.IP
	Gateway  net.IP
	Protocol int
	Type     int
}

func scrapeLink(link netlink.Link) (*SaveLinkInfo, error) {
	rawAddrs, err := netlink.AddrList(link, netlink.FAMILY_ALL)
	if err != nil {
		return nil, fmt.Errorf("while scraping addresses: %w", err)
	}
	var addrs []SaveAddr
	for _, rawAddr := range rawAddrs {
		addrs = append(addrs, SaveAddr{
			Addr:      *rawAddr.IPNet,
			Scope:     rawAddr.Scope,
			Broadcast: rawAddr.Broadcast,
		})
	}

	rawRoutes, err := netlink.RouteList(link, netlink.FAMILY_ALL)
	if err != nil {
		return nil, fmt.Errorf("while scraping routes: %w", err)
	}
	var routes []SaveRoute
	for _, rawRoute := range rawRoutes {
		dst := net.IPNet{}
		if rawRoute.Dst != nil {
			dst = *rawRoute.Dst
		}
		routes = append(routes, SaveRoute{
			Scope:    uint8(rawRoute.Scope),
			Dst:      dst,
			Src:      rawRoute.Src,
			Gateway:  rawRoute.Gw,
			Protocol: int(rawRoute.Protocol),
			Type:     rawRoute.Type,
		})
	}

	return &SaveLinkInfo{Addresses: addrs, Routes: routes, MTU: link.Attrs().MTU}, nil
}

func restoreLink(ctx context.Context, link netlink.Link, info *SaveLinkInfo) error {
	for i, saveAddr := range info.Addresses {
		addr := &netlink.Addr{
			IPNet:     &saveAddr.Addr,
			Scope:     saveAddr.Scope,
			Broadcast: saveAddr.Broadcast,
		}
		if err := netlink.AddrReplace(link, addr); err != nil {
			return fmt.Errorf("while restoring addr %d onto link: %w", i, err)
		}
	}
	// Link-scope routes must be installed before gateway routes so the kernel
	// can resolve each gateway's nexthop (fib_check_nh_v4_gw).
	routes := append([]SaveRoute(nil), info.Routes...)
	sort.SliceStable(routes, func(i, j int) bool {
		return routes[i].Gateway == nil && routes[j].Gateway != nil
	})
	for i, saveRoute := range routes {
		dst := saveRoute.Dst
		route := &netlink.Route{
			LinkIndex: link.Attrs().Index,
			Scope:     netlink.Scope(saveRoute.Scope),
			Dst:       &dst,
			Src:       saveRoute.Src,
			Gw:        saveRoute.Gateway,
			Protocol:  netlink.RouteProtocol(saveRoute.Protocol),
			Type:      saveRoute.Type,
		}
		slog.InfoContext(ctx, "Restoring route", slog.String("dst", dst.String()), slog.Any("gateway", saveRoute.Gateway))
		if err := netlink.RouteReplace(route); err != nil {
			return fmt.Errorf("while restoring route %d: %w", i, err)
		}
	}
	return nil
}

// createNetNSWithoutSwitching creates a named netns and returns its handle,
// restoring the caller's current netns before returning.
func createNetNSWithoutSwitching(name string) (netns.NsHandle, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	curNetNS, err := netns.Get()
	if err != nil {
		return -1, fmt.Errorf("while getting current netns: %w", err)
	}
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	interiorNetNS, err := netns.NewNamed(name)
	if err != nil {
		return -1, fmt.Errorf("while creating interior network namespace: %w", err)
	}
	return interiorNetNS, nil
}

// netNSDo runs do() with the OS thread switched into targetNS, then restores it.
func netNSDo(ctx context.Context, targetNS netns.NsHandle, do func(context.Context) error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	curNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting current netns: %w", err)
	}
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
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

// moveEth0IntoInteriorNetns moves the pod's eth0 into the interior netns and
// restores its addresses/routes there. After this, the interior netns holds a
// fully configured eth0 the sandbox runtime can consume.
func (s *AteomService) moveEth0IntoInteriorNetns(ctx context.Context) error {
	eth0Link, err := netlink.LinkByName("eth0")
	if err != nil {
		return fmt.Errorf("while getting netlink link for eth0: %w", err)
	}
	if err := netlink.LinkSetNsFd(eth0Link, int(s.interiorNetNS)); err != nil {
		return fmt.Errorf("while moving eth0 into interior network namespace: %w", err)
	}
	return netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		// The interior netns is persistent (created once at boot). kata's tcfilter
		// model creates a tap (and may leave it behind when a failed run is torn
		// down by driving the shim directly). A leftover tap with the same name/
		// index makes the next run's qdisc add fail "Failed to add qdisc ... file
		// exists". Delete every link that isn't lo or the eth0 we're about to move
		// in, so kata always builds its network on a clean slate.
		if links, lerr := netlink.LinkList(); lerr == nil {
			for _, l := range links {
				name := l.Attrs().Name
				if name == "lo" || name == "eth0" {
					continue
				}
				if delErr := netlink.LinkDel(l); delErr != nil {
					slog.WarnContext(ctx, "Failed to delete leftover link in interior netns", slog.String("link", name), slog.Any("err", delErr))
				} else {
					slog.InfoContext(ctx, "Deleted leftover link in interior netns", slog.String("link", name))
				}
			}
		}
		loLink, err := netlink.LinkByName("lo")
		if err != nil {
			return fmt.Errorf("while acquiring lo in interior netns: %w", err)
		}
		if err := netlink.LinkSetUp(loLink); err != nil {
			return fmt.Errorf("while bringing up lo in interior netns: %w", err)
		}
		link, err := netlink.LinkByName("eth0")
		if err != nil {
			return fmt.Errorf("while acquiring eth0 in interior netns: %w", err)
		}
		if err := netlink.LinkSetUp(link); err != nil {
			return fmt.Errorf("while bringing up eth0 in interior netns: %w", err)
		}
		if err := restoreLink(ctx, link, s.eth0LinkInfo); err != nil {
			return fmt.Errorf("while restoring eth0 routes/addresses in interior netns: %w", err)
		}
		// kata's tcfilter network model adds a clsact qdisc to eth0; a stale one
		// (left by a prior failed attempt, carried across the netns move) makes
		// kata's add fail "Failed to add qdisc ... file exists". Remove any
		// clsact/ingress qdisc so kata can add its own.
		if qdiscs, qerr := netlink.QdiscList(link); qerr == nil {
			for _, q := range qdiscs {
				if t := q.Type(); t == "clsact" || t == "ingress" {
					if delErr := netlink.QdiscDel(q); delErr != nil {
						slog.WarnContext(ctx, "Failed to delete stale qdisc on eth0", slog.String("type", t), slog.Any("err", delErr))
					} else {
						slog.InfoContext(ctx, "Removed stale qdisc on eth0", slog.String("type", t))
					}
				}
			}
		}
		return nil
	})
}

// setupRestoreTap recreates, in the interior netns, the tap + TC-mirror wiring
// kata's tcfilter network model builds at boot: a tap device cross-connected to
// eth0 with mirred-redirect ingress filters in both directions. Returns the
// open tap FDs (one per queue pair) for cloud-hypervisor to adopt via
// vm.restore net_fds (the snapshot's virtio-net device is fd-backed, so CH
// requires fresh FDs on restore). Call after moveEth0IntoInteriorNetns.
func (s *AteomService) setupRestoreTap(ctx context.Context, name string, queuePairs int) ([]*os.File, error) {
	var fds []*os.File
	err := netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		eth0, err := netlink.LinkByName("eth0")
		if err != nil {
			return fmt.Errorf("acquiring eth0 in interior netns: %w", err)
		}
		if old, lerr := netlink.LinkByName(name); lerr == nil {
			_ = netlink.LinkDel(old)
		}
		flags := netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR
		if queuePairs > 1 {
			flags |= netlink.TUNTAP_MULTI_QUEUE
		}
		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: name, MTU: eth0.Attrs().MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     flags,
			Queues:    queuePairs,
		}
		if err := netlink.LinkAdd(tap); err != nil {
			return fmt.Errorf("creating tap %q: %w", name, err)
		}
		fds = tap.Fds
		if err := netlink.LinkSetUp(tap); err != nil {
			return fmt.Errorf("bringing up tap %q: %w", name, err)
		}
		// Cross-connect: everything arriving on eth0 redirects out the tap and
		// vice versa (kata's TCFilterModel: ingress qdisc + match-all u32 with a
		// mirred egress-redirect action, here via U32.RedirIndex).
		for _, pair := range [][2]netlink.Link{{eth0, tap}, {tap, eth0}} {
			qdisc := &netlink.Ingress{QdiscAttrs: netlink.QdiscAttrs{
				LinkIndex: pair[0].Attrs().Index,
				Parent:    netlink.HANDLE_INGRESS,
				Handle:    netlink.MakeHandle(0xffff, 0),
			}}
			if err := netlink.QdiscReplace(qdisc); err != nil {
				return fmt.Errorf("adding ingress qdisc to %q: %w", pair[0].Attrs().Name, err)
			}
			filter := &netlink.U32{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: pair[0].Attrs().Index,
					Parent:    netlink.MakeHandle(0xffff, 0),
					Priority:  1,
					Protocol:  unix.ETH_P_ALL,
				},
				ClassId:    netlink.MakeHandle(1, 1),
				RedirIndex: pair[1].Attrs().Index,
			}
			if err := netlink.FilterAdd(filter); err != nil {
				return fmt.Errorf("adding mirred filter %s -> %s: %w", pair[0].Attrs().Name, pair[1].Attrs().Name, err)
			}
		}
		return nil
	})
	if err != nil {
		for _, f := range fds {
			_ = f.Close()
		}
		return nil, err
	}
	return fds, nil
}

// ensureEth0InPodNetns moves eth0 back to the pod netns if a prior Run/Restore
// left it in the interior netns. Idempotent.
func (s *AteomService) ensureEth0InPodNetns(ctx context.Context) error {
	if _, err := netlink.LinkByName("eth0"); err == nil {
		return nil
	}
	podNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting pod netns: %w", err)
	}
	var moved bool
	err = netNSDo(ctx, s.interiorNetNS, func(_ context.Context) error {
		link, lookupErr := netlink.LinkByName("eth0")
		if lookupErr != nil {
			return nil
		}
		if mvErr := netlink.LinkSetNsFd(link, int(podNetNS)); mvErr != nil {
			return fmt.Errorf("while moving eth0 to pod netns: %w", mvErr)
		}
		moved = true
		return nil
	})
	if moved {
		slog.WarnContext(ctx, "Recovered eth0 from interior netns to pod netns")
	}
	return err
}

// returnEth0ToPodNetns moves eth0 back from the interior netns to the pod netns
// (called after a workload is torn down).
func (s *AteomService) returnEth0ToPodNetns(ctx context.Context) error {
	podNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting pod netns: %w", err)
	}
	return netNSDo(ctx, s.interiorNetNS, func(_ context.Context) error {
		link, lookupErr := netlink.LinkByName("eth0")
		if lookupErr != nil {
			return nil // already gone
		}
		if err := netlink.LinkSetNsFd(link, int(podNetNS)); err != nil {
			return fmt.Errorf("while sending eth0 back to pod netns: %w", err)
		}
		return nil
	})
}
