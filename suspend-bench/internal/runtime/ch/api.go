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

package ch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// apiClient speaks the cloud-hypervisor REST API over its unix api-socket.
type apiClient struct {
	http *http.Client
}

func newAPIClient(socketPath string) *apiClient {
	return &apiClient{
		http: &http.Client{
			Transport: &http.Transport{
				// CH's API server closes idle connections (and gets heavily
				// swapped out during the swap mechanism's reclaim). Reusing a
				// kept-alive connection then blocks forever on the next request
				// (observed: vm.resume hangs while ch-remote resume works
				// instantly). Force a fresh connection per request.
				DisableKeepAlives: true,
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", socketPath)
				},
			},
		},
	}
}

// waitReady blocks until the api socket answers (or the deadline passes).
func (c *apiClient) waitReady(ctx context.Context, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if err := c.get(ctx, "/api/v1/vmm.ping"); err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("cloud-hypervisor api socket not ready after %s", deadline)
}

const apiBase = "http://localhost"

func (c *apiClient) get(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}
	return nil
}

// put issues a PUT with an optional JSON body and checks for a 2xx status.
func (c *apiClient) put(ctx context.Context, path string, body any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, apiBase+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s: status %d: %s", path, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// --- Request bodies (subset of the CH OpenAPI we use). ----------------------

type vmConfig struct {
	Cpus    cpusConfig   `json:"cpus"`
	Memory  memoryConfig `json:"memory"`
	Payload payload      `json:"payload"`
	Disks   []diskConfig `json:"disks"`
	Vsock   vsockConfig  `json:"vsock"`
	Serial  consoleCfg   `json:"serial,omitempty"`
	Console consoleCfg   `json:"console,omitempty"`
}

type cpusConfig struct {
	BootVcpus int `json:"boot_vcpus"`
	MaxVcpus  int `json:"max_vcpus"`
}

type memoryConfig struct {
	Size   int64  `json:"size"`
	Shared bool   `json:"shared,omitempty"`
	File   string `json:"file,omitempty"`
}

type payload struct {
	Kernel  string `json:"kernel"`
	Cmdline string `json:"cmdline"`
}

type diskConfig struct {
	Path string `json:"path"`
	// CH v52+ auto-detects raw images and disables sector-0 writes, which panics
	// an ext4 rootfs ("Unable to mount root fs ... ReadOnly"). Declare it Raw so
	// normal writes (incl. the ext4 journal at sector 0) are allowed.
	ImageType string `json:"image_type,omitempty"`
}

type vsockConfig struct {
	Cid    uint32 `json:"cid"`
	Socket string `json:"socket"`
}

type consoleCfg struct {
	Mode string `json:"mode"`           // Off | Null | Tty | File
	File string `json:"file,omitempty"` // when Mode==File
}

type snapshotConfig struct {
	DestinationURL string `json:"destination_url"`
}

// NOTE: restore is driven via the cloud-hypervisor CLI `--restore
// source_url=...,memory_restore_mode=...` form (see ch.go Restore), which
// reliably accepts memory_restore_mode, rather than the REST /vm.restore body.
