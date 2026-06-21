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

package ategcs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("ategcs")

type ObjectStorage interface {
	GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

func FetchFromGCS(ctx context.Context, client ObjectStorage, gsURL string) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "fetchFromGCS")
	defer span.End()

	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return nil, fmt.Errorf("while parsing url: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("while reading all content: %w", err)
	}

	return content, nil
}

// SendBytesToGCS uploads the given bytes (uncompressed) to gsURL. Intended for
// small objects such as the snapshot manifest.
func SendBytesToGCS(ctx context.Context, client ObjectStorage, gsURL string, content []byte) error {
	ctx, span := tracer.Start(ctx, "sendBytesToGCS")
	defer span.End()

	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}
	if err := client.PutObject(ctx, bucket, object, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("while putting object bucket=%q object=%q: %w", bucket, object, err)
	}
	return nil
}

func SendLocalFileToGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "sendLocalFileToGCSWithZstd")
	defer span.End()

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := sendToGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("in sendToGCSWithZstd: %w", err)
	}

	return nil
}

// sendToGCSWithZstd zstd-compresses content to a temp file and uploads it to gsURL.
//
// The snapshot memory-ranges is the large object here (the whole guest RAM image,
// mostly zero) on the SUSPEND critical path, so we compress with SpeedFastest across
// all CPUs — high-ratio levels scan the multi-GiB image far slower for little size
// gain on near-zero data, and the decoder auto-detects the level so restore + older
// snapshots are unaffected. That level/concurrency change is the dominant win.
//
// We compress to a SEEKABLE temp file (not a streaming io.Pipe) on purpose: the
// S3/rustfs PutObject hands the body to the AWS SDK, which needs a seekable body to
// sign + set Content-Length on the payload — a non-seekable pipe hangs there (GCS
// tolerates streaming, S3/rustfs does not). The overlap a pipe would buy is small
// vs a local object store anyway. Logs compress wall-clock vs total.
func sendToGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, content io.Reader) (err error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}
	tStart := time.Now()

	tmpFile, err := os.CreateTemp("", "substrate-upload-compress-")
	if err != nil {
		return fmt.Errorf("while creating temp compress file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	zw, err := zstd.NewWriter(tmpFile,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(runtime.GOMAXPROCS(0)))
	if err != nil {
		return fmt.Errorf("while creating zstd writer: %w", err)
	}
	t0 := time.Now()
	inBytes, err := io.Copy(zw, content)
	if err != nil {
		zw.Close()
		return fmt.Errorf("while compressing data to temp file: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("while closing zstd writer: %w", err)
	}
	dCompress := time.Since(t0)

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("while seeking temp file: %w", err)
	}
	if err := client.PutObject(ctx, bucket, object, tmpFile); err != nil {
		return fmt.Errorf("while putting object %q: %w", object, err)
	}
	slog.InfoContext(ctx, "Compressed zstd upload",
		slog.String("object", object), slog.Int64("in_bytes", inBytes),
		slog.Duration("compress", dCompress), slog.Duration("total", time.Since(tStart)))
	return nil
}

func FetchLocalFileFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	ctx, span := tracer.Start(ctx, "fetchLocalFileFromGCSWithZstd")
	defer span.End()

	localFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := localFile.Chmod(0o600); err != nil {
		return fmt.Errorf("in localFile.Chmod(0o600): %w", err)
	}

	if err := fetchFromGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("while fetching %q from GCS: %w", gsURL, err)
	}

	return nil
}

func fetchFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, out io.Writer) (err error) {
	bucket, object, err := parseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return fmt.Errorf("while getting object: %w", err)
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			if err != nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from rc.Close", slog.Any("err", closeErr))
			}
		}
	}()

	zrc, err := zstd.NewReader(rc, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("in zstd.NewReader: %w", err)
	}
	defer zrc.Close()

	// Write SPARSE when the destination is a regular file. A guest memory-ranges
	// image is mostly zero (free / un-faulted RAM), so a plain io.Copy materializes a
	// DENSE multi-GiB file — and writing those zeros to disk is the dominant cost of
	// resume (the mirror of the upload bug fixed in the compress->upload streaming).
	// Skipping zero blocks (leaving holes) cuts the write to ~the resident set, makes
	// the on-disk file sparse like the snapshot itself (so OnDemand mmap/restore and
	// the blk path's MergeSparseOverlay base-copy only touch real data), and is
	// independent of guest RAM size — what matters when actors legitimately use lots
	// of ephemeral memory. Falls back to io.Copy for non-file writers; the decoder
	// auto-detects the zstd level (backward compatible, no format change).
	if f, ok := out.(*os.File); ok {
		t0 := time.Now()
		size, written, derr := copyZstdSparse(f, zrc)
		if derr != nil {
			return fmt.Errorf("in sparse decompress: %w", derr)
		}
		slog.InfoContext(ctx, "Sparse zstd download",
			slog.Int64("size", size), slog.Int64("written", written), slog.Duration("took", time.Since(t0)))
		return nil
	}
	if _, err = io.Copy(out, zrc); err != nil {
		return fmt.Errorf("in io.Copy: %w", err)
	}

	return nil
}

// copyZstdSparse copies src into dst skipping all-zero blocks, so dst becomes a
// sparse file (the skipped regions are holes). Returns the logical size (total bytes
// consumed from src) and the bytes actually written (non-zero). dst is truncated to
// the logical size at the end so trailing zero regions become a hole and the file
// size is exact. dst must be a fresh/truncated regular file opened for writing.
func copyZstdSparse(dst *os.File, src io.Reader) (size int64, written int64, err error) {
	// 64KiB blocks: a multiple of the 4KiB fs block (so skipped runs align to whole
	// hole-able blocks) while keeping the zero-scan + WriteAt syscall count modest.
	const block = 64 << 10
	buf := make([]byte, block)
	var pos int64
	for {
		n, rerr := io.ReadFull(src, buf)
		if n > 0 {
			chunk := buf[:n]
			if !allZero(chunk) {
				if _, werr := dst.WriteAt(chunk, pos); werr != nil {
					return 0, 0, werr
				}
				written += int64(n)
			}
			pos += int64(n)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return 0, 0, rerr
		}
	}
	// Materialize the exact logical size: extends past the last written byte with a
	// hole when the tail was zero (skipped), and is a no-op otherwise.
	if terr := dst.Truncate(pos); terr != nil {
		return 0, 0, terr
	}
	return pos, written, nil
}

// allZero reports whether b is all zero bytes, checking 8 bytes at a time.
func allZero(b []byte) bool {
	i := 0
	for ; i+8 <= len(b); i += 8 {
		if binary.LittleEndian.Uint64(b[i:]) != 0 {
			return false
		}
	}
	for ; i < len(b); i++ {
		if b[i] != 0 {
			return false
		}
	}
	return true
}

func parseGCSURL(gsURL string) (string, string, error) {
	parsed, err := url.Parse(gsURL)
	if err != nil {
		return "", "", fmt.Errorf("while parsing %q: %w", gsURL, err)
	}

	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}
