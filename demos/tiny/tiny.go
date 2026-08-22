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

// Command tiny is the smallest actor that can still prove it is alive: it
// answers /readyz so activation completes, and / so a caller can tell it apart
// from its neighbors.
//
// It exists for density testing. The counter demo is the fixture for behavior,
// but it carries a durable-dir volume, a second listener, and a snapshot of its
// own data, so packing thousands of counters onto one worker measures storage
// and volume plumbing as much as it measures how many actors fit. This carries
// none of that, so what it measures is the floor: the sandbox, the network
// namespace, the veth, and one process.
//
// Deliberately no dependencies beyond the standard library, no background
// goroutines, and no allocation after startup. Anything this holds is paid for
// once per actor, and at a few thousand per node that multiplies.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
)

func main() {
	port := flag.Int("port", 80, "Port to serve on.")
	flag.Parse()

	// The identity a caller reads back. Substrate gives every actor the same
	// address, so the response has to carry something else for a density test to
	// tell one from another; the hostname is per actor and free.
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "tiny actor %s\n", host)
	})

	addr := net.JoinHostPort("", fmt.Sprintf("%d", *port))
	slog.Info("tiny actor serving", "addr", addr, "host", host)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("serving", "err", err)
		os.Exit(1)
	}
}
