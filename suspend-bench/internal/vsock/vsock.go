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

// Package vsock is the host side of the workload control channel.
//
// For cloud-hypervisor, the guest's vsock is surfaced on the host as a unix
// socket; to reach a guest-listening port you connect to that socket and send a
// "CONNECT <port>\n" handshake, after which it is a transparent byte stream
// (https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/vsock.md).
//
// For gVisor there is no vsock, so the workload binds an AF_UNIX socket inside a
// bind-mounted dir and we dial it directly. Both cases yield the same line
// protocol the C/Node workloads speak.
package vsock

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Client is a control-channel connection speaking the newline-delimited workload
// protocol (PING/SETWS/DIRTY/WALK/HASH/READY/QUIT).
type Client struct {
	conn       net.Conn
	r          *bufio.Reader
	cmdTimeout time.Duration // read deadline per Cmd; 0 => default
}

// SetCmdTimeout sets the per-command read deadline. WALK/HASH over a large region
// can take a long time on ondemand restore / swap-in (the whole working set faults
// in), so callers scale this with the working-set size.
func (c *Client) SetCmdTimeout(d time.Duration) { c.cmdTimeout = d }

// StateToken is the cheap liveness/identity reply to PING. Seed and Counter live
// in guest memory and must be unchanged across suspend/resume.
type StateToken struct {
	Seed    string
	Counter uint64
}

// DialCH connects to a cloud-hypervisor vsock unix socket and performs the
// CONNECT handshake to reach the guest port.
func DialCH(socketPath string, port int, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("unix", socketPath, timeout)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReader(conn)
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", port); err != nil {
		conn.Close()
		return nil, err
	}
	// CH replies "OK <host_port>\n" on success.
	conn.SetReadDeadline(time.Now().Add(timeout))
	line, err := r.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("vsock CONNECT handshake: %w", err)
	}
	if !strings.HasPrefix(line, "OK") {
		conn.Close()
		return nil, fmt.Errorf("vsock CONNECT refused: %q", strings.TrimSpace(line))
	}
	conn.SetReadDeadline(time.Time{})
	return &Client{conn: conn, r: r}, nil
}

// DialUnix connects directly to an AF_UNIX socket (the gVisor host-uds path —
// note runsc cannot checkpoint a bound host socket, so prefer DialTCP for gVisor).
func DialUnix(path string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("unix", path, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, r: bufio.NewReader(conn)}, nil
}

// DialTCP connects to host:port (the gVisor netstack path — a netstack TCP socket
// IS checkpointable by runsc, unlike a bound host unix socket). addr is "ip:port".
func DialTCP(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, r: bufio.NewReader(conn)}, nil
}

// DialFunc abstracts "open a fresh control channel" so mechanisms can reconnect
// after a restore (a brand-new VMM) without knowing the transport.
type DialFunc func(timeout time.Duration) (*Client, error)

// DialUntil retries dial until it succeeds or the deadline passes. Used for
// resume-to-first-ping: after restore the socket may not be ready instantly.
func DialUntil(dial DialFunc, perTry, deadline time.Duration) (*Client, error) {
	end := time.Now().Add(deadline)
	var last error
	for time.Now().Before(end) {
		c, err := dial(perTry)
		if err == nil {
			return c, nil
		}
		last = err
		time.Sleep(5 * time.Millisecond)
	}
	return nil, fmt.Errorf("dial timed out after %s: %w", deadline, last)
}

// Cmd sends one command line and returns the single-line reply (trimmed).
// The read deadline is generous because correctness commands (HASH/WALK) read
// the entire working set, which on ondemand restore / swap-in lazily faults in
// many GB — at multi-GB that legitimately takes tens of seconds.
func (c *Client) Cmd(cmd string) (string, error) {
	if _, err := fmt.Fprintf(c.conn, "%s\n", cmd); err != nil {
		return "", err
	}
	d := c.cmdTimeout
	if d <= 0 {
		d = 180 * time.Second
	}
	c.conn.SetReadDeadline(time.Now().Add(d))
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	c.conn.SetReadDeadline(time.Time{})
	return strings.TrimSpace(line), nil
}

// Ping issues PING and parses the state token. This is the timed liveness probe.
func (c *Client) Ping() (StateToken, error) {
	reply, err := c.Cmd("PING")
	if err != nil {
		return StateToken{}, err
	}
	return parsePong(reply)
}

// Hash issues HASH and returns the region fingerprint (correctness check).
func (c *Client) Hash() (string, error) {
	reply, err := c.Cmd("HASH")
	if err != nil {
		return "", err
	}
	f := strings.Fields(reply)
	if len(f) != 2 || f[0] != "HASH" {
		return "", fmt.Errorf("bad HASH reply: %q", reply)
	}
	return f[1], nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

func parsePong(reply string) (StateToken, error) {
	// "PONG seed=<hex16> counter=<dec>"
	f := strings.Fields(reply)
	if len(f) != 3 || f[0] != "PONG" {
		return StateToken{}, fmt.Errorf("bad PONG reply: %q", reply)
	}
	t := StateToken{Seed: strings.TrimPrefix(f[1], "seed=")}
	cnt, err := strconv.ParseUint(strings.TrimPrefix(f[2], "counter="), 10, 64)
	if err != nil {
		return StateToken{}, fmt.Errorf("bad counter in %q: %w", reply, err)
	}
	t.Counter = cnt
	return t, nil
}
