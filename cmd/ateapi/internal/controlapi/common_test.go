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

package controlapi

import (
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Helpers shared by the unit tests in this package.
const (
	testAtespace = "test-atespace"
	testActorID  = "id1"
)

var (
	ignoreUID        = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid")
	ignoreTimestamps = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "create_time", "update_time")
)

func selectorLabelsOfSize(n int) map[string]string {
	labels := make(map[string]string, n)
	for i := 0; i < n; i++ {
		labels[fmt.Sprintf("k%d", i)] = "v"
	}
	return labels
}

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, want, got)
}

// firstAssignment returns the single Actor a Worker is hosting, or nil when it
// is hosting none. These tests place one Actor per Worker, so "the assignment"
// is still a meaningful thing to assert on even though a Worker now holds a
// list; asserting through this keeps them readable and would fail loudly (by
// looking at the wrong entry) if a test ever placed two.
func firstAssignment(w *ateapipb.Worker) *ateapipb.ActorAssignment {
	assignments := w.GetStatus().GetAssignments()
	if len(assignments) == 0 {
		return nil
	}
	return assignments[0]
}

// assignmentsOf wraps a single optional assignment as a Worker's assignment
// list, mapping "no assignment" to an empty list rather than a list holding a
// nil entry — which would look to everything downstream like a Worker hosting a
// nameless Actor.
func assignmentsOf(assignment *ateapipb.ActorAssignment) []*ateapipb.ActorAssignment {
	if assignment == nil {
		return nil
	}
	return []*ateapipb.ActorAssignment{assignment}
}
