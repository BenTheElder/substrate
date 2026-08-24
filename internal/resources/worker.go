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

import "github.com/agent-substrate/substrate/pkg/proto/ateapipb"

// WorkerMaxActors is how many Actors a Worker admits, reading unset capacity as
// one rather than as unconstrained.
//
// It lives here, rather than in the scheduler that enforces it, because the
// scheduler is not the only reader: the CLI reports occupancy against the same
// limit, and if the two disagreed about what an unset capacity means, workers
// would be shown as having room the scheduler would never use, or the reverse.
//
// Unset means one because that is the safe reading. Unlike an unknown cpu or
// memory limit -- where the envelope could not be determined and refusing to
// place would be worse than allowing it -- a Worker that has not said it can
// host more than one Actor almost certainly cannot, and treating silence as
// "no limit" would let any Worker built without capacity accept Actors without
// bound.
func WorkerMaxActors(capacity *ateapipb.WorkerCapacity) int32 {
	if capacity.GetActors() < 1 {
		return 1
	}
	return capacity.GetActors()
}
