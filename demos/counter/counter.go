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

// Command counter is a simple server that will be used as a worker pod. It listens on ports 80
// and returns a greeting with the IP of the pod where it is running.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
)

// requestCount is the in-RAM counter. It lives in the actor's memory and so is
// carried across suspend/resume by the guest memory snapshot.
var requestCount uint64

// diskCounterPath is the on-disk counter. It lives in the actor's rootfs
const diskCounterPath = "/disk-counter"

func main() {
	pflag.Parse()
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	defaultMux := http.NewServeMux()
	defaultMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		count := atomic.AddUint64(&requestCount, 1)
		currentIP := getCurrentIP()
		response := fmt.Sprintf("hello from: %s | preserved memory count: %d | on-disk count: %s\n",
			currentIP, count, diskCounterString())
		slog.InfoContext(ctx, "Handled request", slog.String("response", response))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	})

	// /disk/incr increments the ON-DISK counter: read the file, add one, write
	// it back. This is a rootfs write made after the golden snapshot, so it shows
	// whether the runtime preserves the rootfs across suspend/resume (contrast
	// with the in-RAM counter above).
	defaultMux.HandleFunc("/disk/incr", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		n, _ := readDiskCounter()
		n++
		if err := writeDiskCounter(n); err != nil {
			slog.ErrorContext(ctx, "Error writing on-disk counter", slog.Any("err", err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		slog.InfoContext(ctx, "Incremented on-disk counter", slog.Uint64("disk_count", n))
		fmt.Fprintf(w, "on-disk count: %d\n", n)
	})

	// /disk reads the ON-DISK counter without modifying it. Reports "<absent>"
	// when the file does not exist (e.g. the rootfs was reset on resume, dropping
	// the post-golden write).
	defaultMux.HandleFunc("/disk", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "on-disk count: %s\n", diskCounterString())
	})

	go func() {
		slog.InfoContext(ctx, "Starting counter server on port 80")
		if err := http.ListenAndServe(":80", defaultMux); err != nil {
			slog.ErrorContext(ctx, "Error starting server", slog.Any("err", err))
			os.Exit(1)
		}
	}()

	// Write some random data to a file in the root filesystem, to test
	// filesystem checkpoint/restore.
	if err := writeRandomFile(); err != nil {
		slog.InfoContext(ctx, "Error writing random file", slog.Any("err", err))
	} else {
		slog.InfoContext(ctx, "Wrote content to random file", slog.String("fshash", hashRandomFile()))
	}

	count := 0
	slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
	count++

	for range time.Tick(10 * time.Second) {
		// TODO: Test outbound connectivity by pinging google.com
		slog.InfoContext(ctx, "Count", slog.Int("count", count), slog.String("fshash", hashRandomFile()))
		count++
	}
}

func writeRandomFile() error {
	rf, err := os.Create("/random-content-file")
	if err != nil {
		return fmt.Errorf("while opening file: %w", err)
	}
	defer rf.Close()

	_, err = io.CopyN(rf, rand.Reader, 1*1024*1024)
	if err != nil {
		return fmt.Errorf("while copying rand data: %w", err)
	}

	return nil
}

func hashRandomFile() string {
	rfBytes, err := os.ReadFile("/random-content-file")
	if err != nil {
		panic(err)
	}

	hash := sha256.Sum256(rfBytes)
	return base64.RawStdEncoding.EncodeToString(hash[:])
}

// readDiskCounter returns the on-disk counter and whether the file was present.
// A missing or unparseable file reads as (0, false).
func readDiskCounter() (uint64, bool) {
	b, err := os.ReadFile(diskCounterPath)
	if err != nil {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// writeDiskCounter persists the on-disk counter value to the rootfs.
func writeDiskCounter(n uint64) error {
	return os.WriteFile(diskCounterPath, []byte(strconv.FormatUint(n, 10)), 0o644)
}

// diskCounterString renders the on-disk counter for a response, or "<absent>"
// when the file does not exist.
func diskCounterString() string {
	n, ok := readDiskCounter()
	if !ok {
		return "<absent>"
	}
	return strconv.FormatUint(n, 10)
}

func getCurrentIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		slog.Error("Error getting interface addresses", slog.Any("err", err))
		return "x.x.x.x"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "y.y.y.y"
}
