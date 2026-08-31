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

package resources

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// shareablePrefix marks the extended resources substrate advertises itself.
//
// They are pseudo-devices: /dev/kvm is opened by every micro-VM on a node, so a
// grant is not a reservation and counting them would cap a Worker at one Actor.
// Anything else with a vendor prefix is a real device an Actor takes for itself.
const shareablePrefix = "ate.dev/"

// ExclusiveDevices returns the devices in a resource list that an Actor takes
// exclusively, keyed by extended resource name. Core resources (cpu, memory,
// storage) are not devices, and substrate's own shareable pseudo-devices are
// excluded; what is left is a device with a vendor prefix, which nothing can
// subdivide.
//
// Returns nil rather than an empty map so an absent dimension stays absent on
// the wire.
func ExclusiveDevices(list corev1.ResourceList) map[string]int64 {
	var devices map[string]int64
	for name, q := range list {
		n := string(name)
		if !IsExclusiveDevice(n) {
			continue
		}
		count := q.Value()
		if count <= 0 {
			continue
		}
		if devices == nil {
			devices = make(map[string]int64, 1)
		}
		devices[n] = count
	}
	return devices
}

// IsExclusiveDevice reports whether an extended resource names a device an
// Actor takes for itself.
func IsExclusiveDevice(name string) bool {
	if !strings.Contains(name, "/") {
		return false // cpu, memory, ephemeral-storage, pods
	}
	if strings.HasPrefix(name, corev1.ResourceHugePagesPrefix) {
		return false // memory, not a device
	}
	return !strings.HasPrefix(name, shareablePrefix)
}
