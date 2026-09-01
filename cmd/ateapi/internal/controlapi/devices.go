//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package controlapi

import (
	"fmt"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"
)

// devicesFromLimits picks the devices out of a declared limit list, dropping cpu,
// memory, and the shareable ate.dev/* grants. Returns nil rather than an empty
// map so an absent dimension stays absent on the wire.
//
// A count that is not a whole number above zero is an error rather than a
// dropped entry: half a GPU is not a smaller request, and silently discarding it
// would place the actor somewhere without one.
func devicesFromLimits(limits []*ateapipb.Limits, what string) (map[string]int64, error) {
	var devices map[string]int64
	for _, limit := range limits {
		if !resources.IsExclusiveDevice(limit.GetName()) {
			continue
		}
		q, err := resource.ParseQuantity(limit.GetQuantity())
		if err != nil {
			return nil, fmt.Errorf("invalid %s device limit %s=%q: %w", what, limit.GetName(), limit.GetQuantity(), err)
		}
		count := q.Value()
		if count <= 0 || q.MilliValue() != count*1000 {
			return nil, fmt.Errorf("%s device limit %s=%q must be a whole number greater than zero", what, limit.GetName(), limit.GetQuantity())
		}
		if devices == nil {
			devices = make(map[string]int64, 1)
		}
		devices[limit.GetName()] = count
	}
	return devices, nil
}

// actorDevices is what the template asks for that only places the actor: unlike
// cpu and memory, a device is not passed to the sandbox to size it.
func actorDevices(tmpl *ateapipb.ActorTemplate) (map[string]int64, error) {
	return devicesFromLimits(tmpl.GetResources().GetLimits(), "template")
}

// containerDevices is which of the actor's devices each container gets, keyed by
// container name. Containers asking for none are absent.
func containerDevices(tmpl *ateapipb.ActorTemplate) (map[string]map[string]int64, error) {
	var byContainer map[string]map[string]int64
	for _, ctr := range tmpl.GetContainers() {
		devices, err := devicesFromLimits(ctr.GetResources().GetLimits(), "container "+ctr.GetName())
		if err != nil {
			return nil, err
		}
		if len(devices) == 0 {
			continue
		}
		if byContainer == nil {
			byContainer = make(map[string]map[string]int64, 1)
		}
		byContainer[ctr.GetName()] = devices
	}
	return byContainer, nil
}

// validateTemplateDevices rejects a template whose containers do not divide up
// the devices the actor asks for. The actor's own request is what schedules, so
// a container claim that exceeds it would be admitted and then have nothing to
// bind to.
func validateTemplateDevices(tmpl *ateapipb.ActorTemplate) error {
	actor, err := actorDevices(tmpl)
	if err != nil {
		return err
	}
	byContainer, err := containerDevices(tmpl)
	if err != nil {
		return err
	}
	return resources.ValidateDeviceSubdivision(actor, byContainer)
}
