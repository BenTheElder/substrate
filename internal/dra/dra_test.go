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

package dra

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
)

// A node without the device must not offer it, so the scheduler keeps pods
// claiming it off that node.
func TestAvailableFiltersToPresentDevices(t *testing.T) {
	devs := []HostDevice{
		// /dev/null is a character device everywhere the tests run.
		{Name: "null", Path: "/dev/null"},
		{Name: "absent", Path: "/dev/definitely-not-a-device"},
	}
	got := Available(devs, "/dev")
	if len(got) != 1 || got[0].Name != "null" {
		t.Fatalf("Available() = %+v, want only null", got)
	}
}

// A regular file at the device path is not a device; offering it would grant a
// device the runtime cannot inject.
func TestPresentRejectsNonDevice(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "kvm"), nil, 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if (HostDevice{Name: "kvm", Path: "/dev/kvm"}).Present(dir) {
		t.Errorf("Present() = true for a regular file")
	}
}

// The CDI spec must name every offered device, because the IDs
// PrepareResourceClaims returns are resolved against it.
func TestWriteCDISpecCoversEveryDevice(t *testing.T) {
	dir := t.TempDir()
	devs := []HostDevice{{Name: DeviceKVM, Path: "/dev/kvm"}, {Name: DeviceTUN, Path: "/dev/net/tun"}}
	if err := WriteCDISpec(dir, devs); err != nil {
		t.Fatalf("WriteCDISpec() error = %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, DriverName+".json"))
	if err != nil {
		t.Fatalf("reading spec: %v", err)
	}
	var spec cdiSpec
	if err := json.Unmarshal(body, &spec); err != nil {
		t.Fatalf("spec is not valid JSON: %v", err)
	}
	if want := DriverName + "/" + cdiClass; spec.Kind != want {
		t.Errorf("kind = %q, want %q", spec.Kind, want)
	}
	if len(spec.Devices) != len(devs) {
		t.Fatalf("spec has %d devices, want %d", len(spec.Devices), len(devs))
	}
	for i, d := range devs {
		got := spec.Devices[i]
		if got.Name != d.Name {
			t.Errorf("device %d name = %q, want %q", i, got.Name, d.Name)
		}
		if len(got.ContainerEdits.DeviceNodes) != 1 || got.ContainerEdits.DeviceNodes[0].Path != d.Path {
			t.Errorf("device %q nodes = %+v, want just %q", d.Name, got.ContainerEdits.DeviceNodes, d.Path)
		}
		// mknod must not be granted: the container gets this node, not the
		// ability to create others.
		if perms := got.ContainerEdits.DeviceNodes[0].Permissions; perms != "rw" {
			t.Errorf("device %q permissions = %q, want rw", d.Name, perms)
		}
	}

	// No leftover temp file: the runtime scans the whole directory.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading dir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("directory has %d entries, want only the spec", len(entries))
	}
}

// Writing twice must leave a single valid spec, since atelet rewrites it on
// every start.
func TestWriteCDISpecIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	devs := []HostDevice{{Name: DeviceKVM, Path: "/dev/kvm"}}
	for range 2 {
		if err := WriteCDISpec(dir, devs); err != nil {
			t.Fatalf("WriteCDISpec() error = %v", err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("directory has %d entries after rewrite, want 1", len(entries))
	}
}

// Where the API server keeps AllowMultipleAllocations, one device per type is
// enough: it is shareable, so every pod on the node can claim the same one.
func TestResourceSliceSharesOneDevicePerType(t *testing.T) {
	devs := []HostDevice{{Name: DeviceKVM, Path: "/dev/kvm"}, {Name: DeviceTUN, Path: "/dev/net/tun"}}
	pool := resourceSlice("node-a", devs, true).Pools["node-a"]

	if len(pool.Slices) != len(devs) {
		t.Fatalf("got %d slices, want one per device type", len(pool.Slices))
	}
	for i, slice := range pool.Slices {
		if len(slice.Devices) != 1 {
			t.Fatalf("slice %d has %d devices, want 1 shareable device", i, len(slice.Devices))
		}
		d := slice.Devices[0]
		if d.Name != devs[i].Name {
			t.Errorf("device name = %q, want %q", d.Name, devs[i].Name)
		}
		if d.AllowMultipleAllocations == nil || !*d.AllowMultipleAllocations {
			t.Errorf("device %q must be shareable", d.Name)
		}
	}
}

// Where it is dropped, each device is allocated exclusively, so publish a copy
// per potential claim instead or only one pod per node could ever run.
func TestResourceSliceFallsBackToReplicas(t *testing.T) {
	devs := []HostDevice{{Name: DeviceKVM, Path: "/dev/kvm"}}
	pool := resourceSlice("node-a", devs, false).Pools["node-a"]

	if len(pool.Slices) != 1 {
		t.Fatalf("got %d slices, want 1", len(pool.Slices))
	}
	got := pool.Slices[0].Devices
	if len(got) != devicesPerType {
		t.Fatalf("got %d devices, want %d copies", len(got), devicesPerType)
	}
	// A slice may not exceed the API's per-slice device limit.
	if len(got) > 128 {
		t.Errorf("slice exceeds the ResourceSlice device limit")
	}
	seen := map[string]bool{}
	for _, d := range got {
		if seen[d.Name] {
			t.Fatalf("duplicate device name %q", d.Name)
		}
		seen[d.Name] = true
		if deviceTypeOf(d.Name) != DeviceKVM {
			t.Errorf("device %q does not map back to type %q", d.Name, DeviceKVM)
		}
		attr, ok := d.Attributes[deviceTypeAttribute]
		if !ok || attr.StringValue == nil || *attr.StringValue != DeviceKVM {
			t.Errorf("device %q missing %q=%q", d.Name, deviceTypeAttribute, DeviceKVM)
		}
	}
}

// Prepare maps each allocated device to the CDI ID the runtime resolves, and
// ignores results belonging to other drivers sharing the claim.
func TestPrepareResourceClaimsReturnsCDIIDs(t *testing.T) {
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "kvm", Namespace: "ns", UID: types.UID("uid-1")},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{Driver: DriverName, Pool: "node-a", Device: replicaName(DeviceKVM, 7), Request: "kvm"},
						{Driver: "someone.else", Pool: "node-a", Device: "gpu0", Request: "gpu"},
					},
				},
			},
		},
	}

	got, err := driver{}.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
	if err != nil {
		t.Fatalf("PrepareResourceClaims() error = %v", err)
	}
	res, ok := got[types.UID("uid-1")]
	if !ok {
		t.Fatalf("result missing the claim UID; got %v", got)
	}
	if res.Err != nil {
		t.Fatalf("result error = %v", res.Err)
	}
	if len(res.Devices) != 1 {
		t.Fatalf("got %d devices, want only ours: %+v", len(res.Devices), res.Devices)
	}
	d := res.Devices[0]
	if want := cdiDeviceID(DeviceKVM); len(d.CDIDeviceIDs) != 1 || d.CDIDeviceIDs[0] != want {
		t.Errorf("CDIDeviceIDs = %v, want [%s]", d.CDIDeviceIDs, want)
	}
	if d.PoolName != "node-a" || d.DeviceName != replicaName(DeviceKVM, 7) {
		t.Errorf("device = %s/%s, want node-a/%s", d.PoolName, d.DeviceName, replicaName(DeviceKVM, 7))
	}
}

// Unprepare reports success per claim; there is nothing to tear down.
func TestUnprepareResourceClaimsSucceedsPerClaim(t *testing.T) {
	claims := []kubeletplugin.NamespacedObject{
		{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "kvm"}, UID: types.UID("uid-1")},
	}
	got, err := driver{}.UnprepareResourceClaims(context.Background(), claims)
	if err != nil {
		t.Fatalf("UnprepareResourceClaims() error = %v", err)
	}
	if err, ok := got[types.UID("uid-1")]; !ok || err != nil {
		t.Errorf("got %v, want a nil error for the claim", got)
	}
}
