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

package ch

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// This file is the COLD-boot half of ateom owning the cloud-hypervisor lifecycle:
// ateom builds the same VmConfig the kata shim would (kernel + guest-OS image +
// virtio-fs + vsock + memfd-shared RAM, plus optionally a writable scratch disk
// for the overlay upper) and drives vm.create -> vm.add-net (tap fds via
// SCM_RIGHTS) -> vm.boot itself, instead of letting the kata shim do it. ateom
// then drives the stock kata-agent over ttrpc (CreateSandbox/UpdateInterface/
// CreateContainer) to bring the sandbox up. The restore half (LaunchVMM +
// RestoreWithNetFDs) is in restorefds.go; this mirrors its fd-passing shape so a
// cold-booted guest snapshots into a restore-compatible (fd-backed net) image.
//
// The struct shapes match a real kata-produced CH snapshot config.json (a
// snapshot's config.json IS the VmConfig kata built); fields kata leaves at CH's
// defaults are omitted and CH fills them.

// VmConfig is the body of /api/v1/vm.create. Only the fields ateom sets are
// modeled; CH defaults the rest. Net devices are NOT included here — they are
// added after create via AddNet so their tap fds can be passed over SCM_RIGHTS.
type VmConfig struct {
	Cpus     CpusConfig      `json:"cpus"`
	Memory   MemoryConfig    `json:"memory"`
	Payload  PayloadConfig   `json:"payload"`
	Disks    []DiskConfig    `json:"disks,omitempty"`
	Fs       []FsConfig      `json:"fs,omitempty"`
	Vsock    *VsockConfig    `json:"vsock,omitempty"`
	Rng      *RngConfig      `json:"rng,omitempty"`
	Serial   *ConsoleConfig  `json:"serial,omitempty"`
	Console  *ConsoleConfig  `json:"console,omitempty"`
	Balloon  *BalloonConfig  `json:"balloon,omitempty"`
	Platform *PlatformConfig `json:"platform,omitempty"`
}

type CpusConfig struct {
	BootVcpus int `json:"boot_vcpus"`
	MaxVcpus  int `json:"max_vcpus"`
}

type MemoryConfig struct {
	Size int64 `json:"size"`
	// Shared=true backs guest RAM with a memfd, which CH v52 snapshots sparsely
	// (SEEK_DATA/SEEK_HOLE) and which virtio-fs requires. Always true for ateom.
	Shared bool `json:"shared"`
	Thp    bool `json:"thp,omitempty"`
}

type PayloadConfig struct {
	Kernel  string `json:"kernel"`
	Cmdline string `json:"cmdline"`
}

// DiskConfig is one virtio-blk device. Order matters: the first disk is /dev/vda
// (the kata guest-OS image, readonly), the second /dev/vdb (the scratch upper).
type DiskConfig struct {
	Path string `json:"path"`
	// ImageType "Raw" stops CH v52 auto-detecting the image and disabling sector-0
	// writes (which would panic an ext4 mount).
	ImageType string `json:"image_type,omitempty"`
	Readonly  bool   `json:"readonly,omitempty"`
}

// FsConfig is a virtio-fs device backed by a vhost-user (virtiofsd) socket.
type FsConfig struct {
	Tag        string `json:"tag"`
	Socket     string `json:"socket"`
	NumQueues  int    `json:"num_queues,omitempty"`
	QueueSize  int    `json:"queue_size,omitempty"`
	PciSegment int    `json:"pci_segment,omitempty"`
}

type VsockConfig struct {
	Cid    uint32 `json:"cid"`
	Socket string `json:"socket"`
}

type RngConfig struct {
	Src string `json:"src"`
}

type ConsoleConfig struct {
	Mode string `json:"mode"`           // Off | Null | Tty | File
	File string `json:"file,omitempty"` // when Mode==File
}

type BalloonConfig struct {
	Size              int64 `json:"size"`
	DeflateOnOOM      bool  `json:"deflate_on_oom,omitempty"`
	FreePageReporting bool  `json:"free_page_reporting,omitempty"`
}

type PlatformConfig struct {
	NumPciSegments int `json:"num_pci_segments,omitempty"`
}

// NetConfig is the body of /api/v1/vm.add-net. The tap fds are NOT in the JSON —
// they are passed as SCM_RIGHTS ancillary data; CH records the received fd
// numbers into the device. ip/mask are left to CH's defaults (the guest gets its
// address from the kata-agent UpdateInterface, not from CH).
type NetConfig struct {
	Mac       string `json:"mac,omitempty"`
	NumQueues int    `json:"num_queues,omitempty"`
	QueueSize int    `json:"queue_size,omitempty"`
}

// KataVMOptions are the per-actor inputs for building a kata-guest VmConfig.
type KataVMOptions struct {
	// KernelPath is the guest vmlinux (kata kernel asset).
	KernelPath string
	// ImagePath is the kata guest-OS rootfs image (becomes /dev/vda, readonly).
	ImagePath string
	// Cmdline is the guest kernel command line (static per kata version + arch;
	// see kata.KataKernelCmdline).
	Cmdline string
	// MemoryBytes is guest RAM.
	MemoryBytes int64
	// VCPUs is the guest vCPU count.
	VCPUs int
	// VsockCID is the guest vsock context id (hybrid vsock; >=3).
	VsockCID uint32
	// VsockSocket is the host hybrid-vsock unix socket path (kata.VsockSocketPath).
	VsockSocket string
	// VirtiofsdSocket is the vhost-user-fs socket virtiofsd listens on
	// (kata.VirtiofsdSocketPath).
	VirtiofsdSocket string
	// FsTag is the virtio-fs tag the guest mounts (kata uses "kataShared").
	FsTag string
	// EntropySource backs the guest RNG (e.g. "/dev/urandom"); empty omits the rng.
	EntropySource string
	// Balloon adds a virtio-balloon (size 0, free-page-reporting) so the host can
	// drive a pre-snapshot inflate. Dropped once the disk-backed upper lands.
	Balloon bool
	// ScratchDiskPath, if set, adds a second writable virtio-blk disk (/dev/vdb)
	// for the overlay upper.
	ScratchDiskPath string
	// ConsoleLog, if set, points the guest serial console at this file (mode File);
	// otherwise the serial console is a tty tied to the VMM stdout. A file is
	// safer in a headless pod and captures the boot log for diagnosis.
	ConsoleLog string
}

// KataVMConfig assembles a VmConfig equivalent to what the kata shim's clh.go
// builds for a virtio-fs sandbox, from the resolved per-actor asset paths. Net
// devices are added separately (AddNet) so their tap fds pass over SCM_RIGHTS.
func KataVMConfig(o KataVMOptions) VmConfig {
	cfg := VmConfig{
		Cpus:    CpusConfig{BootVcpus: o.VCPUs, MaxVcpus: o.VCPUs},
		Memory:  MemoryConfig{Size: o.MemoryBytes, Shared: true, Thp: true},
		Payload: PayloadConfig{Kernel: o.KernelPath, Cmdline: o.Cmdline},
		Disks: []DiskConfig{
			// _disk0 = the kata guest-OS image -> /dev/vda (boot disk, RO).
			{Path: o.ImagePath, ImageType: "Raw", Readonly: true},
		},
		// virtio-fs on its own pci segment (segment 1), mirroring kata; that needs
		// the platform to declare 2 segments.
		Fs: []FsConfig{
			{Tag: o.FsTag, Socket: o.VirtiofsdSocket, NumQueues: 1, QueueSize: 1024, PciSegment: 1},
		},
		Vsock:    &VsockConfig{Cid: o.VsockCID, Socket: o.VsockSocket},
		Serial:   &ConsoleConfig{Mode: "Tty"},
		Console:  &ConsoleConfig{Mode: "Off"},
		Platform: &PlatformConfig{NumPciSegments: 2},
	}
	if o.ConsoleLog != "" {
		cfg.Serial = &ConsoleConfig{Mode: "File", File: o.ConsoleLog}
	}
	if o.EntropySource != "" {
		cfg.Rng = &RngConfig{Src: o.EntropySource}
	}
	if o.ScratchDiskPath != "" {
		// _disk1 = writable scratch ext4 -> /dev/vdb (the overlay upper).
		cfg.Disks = append(cfg.Disks, DiskConfig{Path: o.ScratchDiskPath, ImageType: "Raw"})
	}
	if o.Balloon {
		cfg.Balloon = &BalloonConfig{Size: 0, FreePageReporting: true}
	}
	return cfg
}

// CreateVM issues /api/v1/vm.create with the given config. The VM is created but
// not booted; add nets (AddNet) then call BootVM.
func (c *Client) CreateVM(ctx context.Context, cfg VmConfig) error {
	if err := c.api.put(ctx, "/api/v1/vm.create", cfg); err != nil {
		return fmt.Errorf("vm.create: %w", err)
	}
	return nil
}

// BootVM issues /api/v1/vm.boot (start the created VM running).
func (c *Client) BootVM(ctx context.Context) error {
	if err := c.api.put(ctx, "/api/v1/vm.boot", nil); err != nil {
		return fmt.Errorf("vm.boot: %w", err)
	}
	return nil
}

// AddNet attaches a virtio-net device to a created (not-yet-booted) VM, passing
// the tap fds via SCM_RIGHTS on the api-socket — the only way CH adopts host tap
// fds (mirrors kata clh.go vmAddNetPut and our RestoreWithNetFDs). CH records the
// received fd numbers into the device, so the resulting snapshot is fd-backed and
// restore-compatible (RestoreWithNetFDs supplies fresh fds).
func (c *Client) AddNet(ctx context.Context, cfg NetConfig, fds []int) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	raddr, err := net.ResolveUnixAddr("unix", c.apiSocket)
	if err != nil {
		return err
	}
	conn, err := net.DialUnix("unix", nil, raddr)
	if err != nil {
		return fmt.Errorf("dialing api-socket: %w", err)
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	}

	// Raw HTTP/1.1 over the unix socket: net/http cannot attach SCM_RIGHTS, and
	// CH's micro_http collects fds from the recvmsg ancillary data of the request
	// that carries them.
	req := fmt.Sprintf("PUT /api/v1/vm.add-net HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	var oob []byte
	if len(fds) > 0 {
		oob = unix.UnixRights(fds...)
	}
	if _, _, err := conn.WriteMsgUnix([]byte(req), oob, nil); err != nil {
		return fmt.Errorf("sending vm.add-net with fds: %w", err)
	}

	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading vm.add-net response: %w", err)
	}
	parts := strings.SplitN(strings.TrimSpace(status), " ", 3)
	if len(parts) < 2 || !strings.HasPrefix(parts[1], "2") {
		return fmt.Errorf("vm.add-net failed: %s", strings.TrimSpace(status))
	}
	return nil
}
