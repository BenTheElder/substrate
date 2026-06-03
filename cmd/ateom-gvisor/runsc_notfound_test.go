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
	"errors"
	"testing"
)

func TestParseContainerExists(t *testing.T) {
	runErr := errors.New("exit status 128")

	for _, tc := range []struct {
		name       string
		output     string
		runErr     error
		wantExists bool
		wantErr    bool
	}{
		{
			name:       "success means exists",
			output:     `{"id":"pause","status":"paused"}`,
			runErr:     nil,
			wantExists: true,
			wantErr:    false,
		},
		{
			name:       "missing container is not an error",
			output:     "FetchSpec failed: loading container: file does not exist",
			runErr:     runErr,
			wantExists: false,
			wantErr:    false,
		},
		{
			name:       "other failure propagates",
			output:     "some unexpected runsc failure",
			runErr:     runErr,
			wantExists: false,
			wantErr:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotExists, gotErr := parseContainerExists(tc.output, tc.runErr)
			if gotExists != tc.wantExists {
				t.Errorf("parseContainerExists exists = %v, want %v", gotExists, tc.wantExists)
			}
			if (gotErr != nil) != tc.wantErr {
				t.Errorf("parseContainerExists err = %v, wantErr %v", gotErr, tc.wantErr)
			}
		})
	}
}
