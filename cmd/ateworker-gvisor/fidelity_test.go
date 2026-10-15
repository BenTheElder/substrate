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

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// TestValidateFidelity pins what this runtime serves. gVisor keeps rootfs
// changes inside its memory checkpoint, so ROOTFS is refused until it can
// capture them on their own; the error names the levels it does serve.
func TestValidateFidelity(t *testing.T) {
	validateFidelity := func(f ateworkerpb.SnapshotFidelity) error {
		if errs := resources.ValidateSnapshotFidelity(f, config.Fidelities, field.NewPath("fidelity")); len(errs) > 0 {
			return apierror.InvalidArgument("%v", errs.ToAggregate())
		}
		return nil
	}
	for _, tc := range []struct {
		name     string
		fidelity ateworkerpb.SnapshotFidelity
		wantErr  bool
	}{
		{"volumes", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, false},
		{"memory", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, false},
		{"rootfs is not served", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, true},
		{"unspecified", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, true},
		{"outside the enum", ateworkerpb.SnapshotFidelity(99), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFidelity(tc.fidelity)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("validateFidelity(%v) = %v, wantErr %t", tc.fidelity, err, tc.wantErr)
			}
			if err != nil && apierror.Code(err) != codes.InvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", apierror.Code(err))
			}
		})
	}
	err := validateFidelity(ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS)
	for _, want := range []string{"SNAPSHOT_FIDELITY_VOLUMES", "SNAPSHOT_FIDELITY_MEMORY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("rootfs rejection %q does not list %s as supported", err, want)
		}
	}
}
