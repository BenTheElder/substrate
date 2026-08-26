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

// Package dra is atelet's dynamic resource allocation driver. It offers the
// host devices a sandbox runtime needs (for example /dev/kvm) so a worker pod
// can be granted just those devices instead of running privileged.
//
// Device access is gated by the cgroup v2 device controller, which denies by
// default before DAC is consulted: no capability, hostPath mount, or
// supplemental group grants it. The driver reports CDI device IDs, which
// kubelet passes to the runtime, and the runtime creates the device node with a
// matching cgroup allow.
package dra

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
)

const (
	// DriverName is what this driver publishes ResourceSlices under and what a
	// DeviceClass selects on. A DNS subdomain in a domain we own, per the DRA
	// convention.
	DriverName = "devices.ate.dev"

	// cdiClass is the CDI class of every device we offer, making a CDI device ID
	// "devices.ate.dev/device=<name>".
	cdiClass = "device"

	// cdiSpecVersion is the CDI schema the written spec declares.
	cdiSpecVersion = "0.6.0"

	// DefaultPluginDir is where the driver serves its kubelet socket, the kubelet
	// convention of one directory per driver.
	DefaultPluginDir = "/var/lib/kubelet/plugins/" + DriverName

	// devicePermissions is what the runtime grants on each device node: read and
	// write, but not mknod.
	devicePermissions = "rw"

	// devicesPerType is how many copies of each device the node publishes.
	//
	// The devices are shareable — any number of sandboxes can use /dev/kvm at
	// once — but DRA only models that through consumable capacity, beta since
	// 1.36 and alpha before it, so an older API server silently drops
	// AllowMultipleAllocations and allocates each device exclusively. Publishing
	// one copy per potential claim is how to express a device that is not really
	// consumed to that server. 128 is the most a single ResourceSlice holds and
	// comfortably exceeds the usual max-pods-per-node.
	devicesPerType = 128

	// deviceTypeAttribute names each device in ResourceSlice attributes so a
	// claim can ask for one kind without knowing the pool layout.
	deviceTypeAttribute = "type"
)

// Device names, also used as the CDI device name and the ResourceSlice device
// name. Worker pods reach these through a DeviceClass, not by name.
const (
	DeviceKVM = "kvm"
	DeviceTUN = "tun"
)

// HostDevice is a device node this driver can offer.
type HostDevice struct {
	// Name identifies the device in the ResourceSlice and the CDI spec.
	Name string
	// Path is the device node, e.g. "/dev/kvm", exposed at the same path.
	Path string
}

// SandboxDevices are the host devices a sandbox runtime may need. atelet offers
// whichever of these exist on its node.
var SandboxDevices = []HostDevice{
	{Name: DeviceKVM, Path: "/dev/kvm"},
	{Name: DeviceTUN, Path: "/dev/net/tun"},
}

// cdiDeviceID is the fully-qualified CDI name the runtime resolves against the
// spec written by WriteCDISpec.
func cdiDeviceID(name string) string {
	return DriverName + "/" + cdiClass + "=" + name
}

// Present reports whether the device node exists on the node as a character
// device. devRoot is where the node's /dev is mounted for inspection, our own
// container having a minimal /dev of its own.
func (d HostDevice) Present(devRoot string) bool {
	fi, err := os.Stat(filepath.Join(devRoot, strings.TrimPrefix(d.Path, "/dev/")))
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// Available returns the subset of devs present on this node, looking under
// devRoot. atelet runs on every node, so offering a device only where it exists
// keeps pods claiming it off nodes that cannot run them.
func Available(devs []HostDevice, devRoot string) []HostDevice {
	out := make([]HostDevice, 0, len(devs))
	for _, d := range devs {
		if d.Present(devRoot) {
			out = append(out, d)
		}
	}
	return out
}

// cdiSpec is the subset of the CDI schema we emit.
type cdiSpec struct {
	CDIVersion string      `json:"cdiVersion"`
	Kind       string      `json:"kind"`
	Devices    []cdiDevice `json:"devices"`
}

type cdiDevice struct {
	Name           string         `json:"name"`
	ContainerEdits cdiContainerEd `json:"containerEdits"`
}

type cdiContainerEd struct {
	DeviceNodes []cdiDeviceNode `json:"deviceNodes"`
}

type cdiDeviceNode struct {
	Path string `json:"path"`
	// Permissions withholds mknod, which CDI would otherwise grant by default:
	// the container is handed this device node, not the ability to mint others.
	Permissions string `json:"permissions"`
}

// WriteCDISpec publishes the CDI spec naming every device this driver offers,
// so the runtime can resolve the IDs PrepareResourceClaims returns. Written
// atomically because the runtime may read it at any time.
func WriteCDISpec(dir string, devs []HostDevice) error {
	spec := cdiSpec{
		CDIVersion: cdiSpecVersion,
		Kind:       DriverName + "/" + cdiClass,
		Devices:    make([]cdiDevice, 0, len(devs)),
	}
	for _, d := range devs {
		spec.Devices = append(spec.Devices, cdiDevice{
			Name:           d.Name,
			ContainerEdits: cdiContainerEd{DeviceNodes: []cdiDeviceNode{{Path: d.Path, Permissions: devicePermissions}}},
		})
	}
	body, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return fmt.Errorf("while marshaling CDI spec: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("while creating CDI directory %q: %w", dir, err)
	}
	final := filepath.Join(dir, DriverName+".json")
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return fmt.Errorf("while writing %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("while renaming %q: %w", final, err)
	}
	return nil
}

// replicaName is the published name of one copy of a device type, e.g. "kvm-7".
func replicaName(deviceType string, i int) string {
	return fmt.Sprintf("%s-%d", deviceType, i)
}

// deviceTypeOf recovers the device type from a published name, so an allocation
// result maps back to the single CDI device for that type.
func deviceTypeOf(name string) string {
	base, _, found := strings.Cut(name, "-")
	if !found {
		return name
	}
	return base
}

// resourceSlice describes devs as this node's pool.
//
// These devices are shareable: any number of sandboxes can use /dev/kvm at once,
// which AllowMultipleAllocations states directly. Where the API server does not
// support it the field is dropped and every device would be allocated to one
// claim, so fall back to publishing a copy per potential claim — a device that
// is not really consumed, expressed the only way that server understands. One
// slice per type keeps each within the maximum device count.
func resourceSlice(nodeName string, devs []HostDevice, shareable bool) resourceslice.DriverResources {
	slices := make([]resourceslice.Slice, 0, len(devs))
	for _, d := range devs {
		copies := devicesPerType
		if shareable {
			copies = 1
		}
		devices := make([]resourceapi.Device, 0, copies)
		for i := range copies {
			name := d.Name
			if !shareable {
				name = replicaName(d.Name, i)
			}
			devices = append(devices, resourceapi.Device{
				Name: name,
				Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					deviceTypeAttribute: {StringValue: ptr(d.Name)},
				},
				AllowMultipleAllocations: ptr(shareable),
			})
		}
		slices = append(slices, resourceslice.Slice{Devices: devices})
	}
	return resourceslice.DriverResources{
		Pools: map[string]resourceslice.Pool{nodeName: {Slices: slices}},
	}
}

// supportsSharedDevices reports whether the API server keeps
// AllowMultipleAllocations. It is dropped silently when DRAConsumableCapacity is
// disabled, and the symptom is remote — pods stuck Pending on "cannot allocate
// all claims" — so probe for it up front with a dry-run create, which prunes
// exactly as a real write would while persisting nothing.
func supportsSharedDevices(ctx context.Context, kube kubernetes.Interface, nodeName string) (bool, error) {
	probe := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "ate-capability-probe-"},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   DriverName,
			Pool:     resourceapi.ResourcePool{Name: nodeName, ResourceSliceCount: 1},
			NodeName: ptr(nodeName),
			Devices: []resourceapi.Device{{
				Name:                     "probe",
				AllowMultipleAllocations: ptr(true),
			}},
		},
	}
	got, err := kube.ResourceV1().ResourceSlices().Create(ctx, probe, metav1.CreateOptions{
		DryRun: []string{metav1.DryRunAll},
	})
	if err != nil {
		return false, fmt.Errorf("while probing for shareable device support: %w", err)
	}
	kept := got.Spec.Devices[0].AllowMultipleAllocations
	return kept != nil && *kept, nil
}

func ptr[T any](v T) *T { return &v }

// driver implements kubeletplugin.DRAPlugin.
type driver struct{}

// PrepareResourceClaims maps each allocated device to its CDI ID. The runtime
// does the work; there is nothing to set up per claim.
func (driver) PrepareResourceClaims(_ context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	out := make(map[types.UID]kubeletplugin.PrepareResult, len(claims))
	for _, claim := range claims {
		var devices []kubeletplugin.Device
		for _, result := range claim.Status.Allocation.Devices.Results {
			if result.Driver != DriverName {
				continue
			}
			devices = append(devices, kubeletplugin.Device{
				Requests:     []string{result.Request},
				PoolName:     result.Pool,
				DeviceName:   result.Device,
				CDIDeviceIDs: []string{cdiDeviceID(deviceTypeOf(result.Device))},
			})
		}
		out[claim.UID] = kubeletplugin.PrepareResult{Devices: devices}
	}
	return out, nil
}

// HandleError reports a background failure from the helper. Publishing errors
// are not retried into correctness by the helper alone, so surface them.
func (driver) HandleError(ctx context.Context, err error, msg string) {
	slog.ErrorContext(ctx, "DRA driver error", slog.String("msg", msg), slog.Any("err", err))
}

// WatchHealthStatus reports no health. These are static host device nodes, only
// published where they already exist, so there is no health to observe beyond
// their presence.
func (driver) WatchHealthStatus(context.Context, chan<- kubeletplugin.DeviceHealthReport) error {
	return kubeletplugin.ErrHealthNotSupported
}

// UnprepareResourceClaims has nothing to undo: the devices are shareable and the
// runtime removes the injected nodes with the container.
func (driver) UnprepareResourceClaims(_ context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	out := make(map[types.UID]error, len(claims))
	for _, claim := range claims {
		out[claim.UID] = nil
	}
	return out, nil
}

// Options configures Start.
type Options struct {
	// KubeClient publishes ResourceSlices and reads ResourceClaims.
	KubeClient kubernetes.Interface
	// NodeName is the node whose devices are offered; it also names the pool.
	NodeName string
	// NodeUID owns the published ResourceSlices so they are garbage collected
	// with the node.
	NodeUID types.UID
	// Devices are the devices to offer, already filtered to those present.
	Devices []HostDevice
	// CDIDir is where the CDI spec is written for the runtime to read.
	CDIDir string
	// PluginDir is where the driver serves its kubelet socket. Empty uses the
	// kubelet default for this driver.
	PluginDir string
}

// Start writes the CDI spec, publishes this node's ResourceSlice, and serves the
// kubelet plugin until ctx is cancelled.
func Start(ctx context.Context, opts Options) (*kubeletplugin.Helper, error) {
	if err := WriteCDISpec(opts.CDIDir, opts.Devices); err != nil {
		return nil, err
	}

	pluginDir := opts.PluginDir
	if pluginDir == "" {
		pluginDir = DefaultPluginDir
	}
	// The helper binds its socket here but does not create the directory.
	if err := os.MkdirAll(pluginDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating plugin directory %q: %w", pluginDir, err)
	}

	helper, err := kubeletplugin.Start(ctx, driver{},
		kubeletplugin.DriverName(DriverName),
		kubeletplugin.KubeClient(opts.KubeClient),
		kubeletplugin.NodeName(opts.NodeName),
		kubeletplugin.NodeUID(opts.NodeUID),
		kubeletplugin.PluginDataDirectoryPath(pluginDir),
	)
	if err != nil {
		return nil, fmt.Errorf("while starting DRA kubelet plugin: %w", err)
	}

	shareable, err := supportsSharedDevices(ctx, opts.KubeClient, opts.NodeName)
	if err != nil {
		helper.Stop()
		return nil, err
	}
	if !shareable {
		slog.WarnContext(ctx, "API server does not support shareable devices; publishing one device per potential claim",
			slog.String("driver", DriverName), slog.Int("copiesPerDevice", devicesPerType))
	}
	if err := helper.PublishResources(ctx, resourceSlice(opts.NodeName, opts.Devices, shareable)); err != nil {
		helper.Stop()
		return nil, fmt.Errorf("while publishing ResourceSlice: %w", err)
	}
	return helper, nil
}
