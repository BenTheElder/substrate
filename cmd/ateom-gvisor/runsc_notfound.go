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
	"fmt"
	"strings"
)

// parseContainerExists classifies the result of `runsc state <container>`.
//
// It returns (true, nil) when the command succeeded (the container exists),
// (false, nil) when the command failed because the container is absent from the
// runsc root, and (false, err) for any other failure.
//
// gVisor reports a missing container while loading its on-disk metadata, e.g.
// "loading container: file does not exist", so we treat a "does not exist"
// marker in the combined output as the not-found signal.
//
// It is kept free of any OS-specific dependencies so it can be unit tested on
// any platform (the rest of this package is linux-only).
func parseContainerExists(output string, runErr error) (bool, error) {
	if runErr == nil {
		return true, nil
	}
	if strings.Contains(output, "does not exist") {
		return false, nil
	}
	return false, fmt.Errorf("while running `runsc state`: %w (output: %s)", runErr, output)
}
