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

// Command pinger is a standalone debug probe for the workload control channel.
// It dials either a cloud-hypervisor vsock unix socket (with the CONNECT
// handshake) or a plain AF_UNIX socket (gVisor), sends one command, and prints
// the reply — handy for poking a guest by hand outside the harness.
//
//	pinger --ch-socket /run/suspend-bench/<id>/vsock.sock --cmd PING
//	pinger --unix /run/suspend-bench/<id>/ping/ping.sock --cmd "SETWS 67108864"
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/agent-substrate/substrate/suspend-bench/internal/vsock"
)

func main() {
	chSocket := flag.String("ch-socket", "", "cloud-hypervisor vsock unix socket")
	unixSocket := flag.String("unix", "", "plain AF_UNIX socket (gVisor)")
	port := flag.Int("port", 1234, "guest vsock port (ch only)")
	cmd := flag.String("cmd", "PING", "command to send")
	timeout := flag.Duration("timeout", 5*time.Second, "dial/read timeout")
	flag.Parse()

	var (
		c   *vsock.Client
		err error
	)
	switch {
	case *chSocket != "":
		c, err = vsock.DialCH(*chSocket, *port, *timeout)
	case *unixSocket != "":
		c, err = vsock.DialUnix(*unixSocket, *timeout)
	default:
		fmt.Fprintln(os.Stderr, "pinger: provide --ch-socket or --unix")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pinger: dial:", err)
		os.Exit(1)
	}
	defer c.Close()

	reply, err := c.Cmd(*cmd)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pinger: cmd:", err)
		os.Exit(1)
	}
	fmt.Println(reply)
}
