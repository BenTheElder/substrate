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

package vsock

import "testing"

func TestParsePong(t *testing.T) {
	tok, err := parsePong("PONG seed=0123456789abcdef counter=42")
	if err != nil {
		t.Fatalf("parsePong error: %v", err)
	}
	if tok.Seed != "0123456789abcdef" {
		t.Errorf("seed = %q, want 0123456789abcdef", tok.Seed)
	}
	if tok.Counter != 42 {
		t.Errorf("counter = %d, want 42", tok.Counter)
	}

	for _, bad := range []string{"", "PONG", "NOPE seed=x counter=1", "PONG seed=x counter=notnum"} {
		if _, err := parsePong(bad); err == nil {
			t.Errorf("parsePong(%q) should error", bad)
		}
	}
}
