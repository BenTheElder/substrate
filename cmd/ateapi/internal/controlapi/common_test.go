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

	"github.com/agent-substrate/substrate/internal/resources"
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

// soleAssignment is the one Actor a Worker is hosting, or nil when it hosts
// none. A Worker admits one at a time, so these tests can name it without
// searching for it.
func soleAssignment(worker *ateapipb.Worker) *ateapipb.ActorAssignment {
	assignments := worker.GetStatus().GetAssignments()
	if len(assignments) == 0 {
		return nil
	}
	return assignments[0]
}

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

// hostingStatus is the status of a Worker hosting these Actors, built the way
// every path that binds one builds it, so the allocation total matches the list
// rather than being a second thing for a fixture to get wrong. A nil assignment
// is how a table case says "hosting nobody".
func hostingStatus(state ateapipb.WorkerState, assignments ...*ateapipb.ActorAssignment) *ateapipb.WorkerStatus {
	worker := &ateapipb.Worker{Status: &ateapipb.WorkerStatus{State: state}}
	for _, assignment := range assignments {
		if assignment != nil {
			resources.BindAssignment(worker, assignment)
		}
	}
	return worker.GetStatus()
}
