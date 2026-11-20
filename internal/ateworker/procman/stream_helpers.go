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

package procman

import (
	"context"
	"errors"
	"io"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LookupManager resolves the Manager for actorUID.
type LookupManager func(actorUID string) (*Manager, error)

// ServeWriteProcessInput handles the Worker.WriteProcessInput client stream.
func ServeWriteProcessInput(stream ateworkerpb.Worker_WriteProcessInputServer, lookup LookupManager) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "empty WriteProcessInput stream")
	}
	if err != nil {
		return err
	}
	mgr, err := lookup(first.GetActorUid())
	if err != nil {
		return err
	}
	processID := first.GetProcessId()
	if processID == "" {
		return status.Error(codes.InvalidArgument, "process_id is required")
	}

	var total int64
	msg := first
	for {
		n, err := mgr.WriteProcessInput(stream.Context(), processID, msg.GetData(), msg.GetClose())
		total += n
		if err != nil {
			return err
		}
		if msg.GetClose() {
			return stream.SendAndClose(&ateworkerpb.WriteProcessInputResponse{BytesWritten: total})
		}
		msg, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&ateworkerpb.WriteProcessInputResponse{BytesWritten: total})
		}
		if err != nil {
			return err
		}
	}
}

// ServeWriteFile handles the Worker.WriteFile client stream, streaming chunks
// directly into Manager.WriteFile via an io.Pipe without buffering the whole file.
func ServeWriteFile(stream ateworkerpb.Worker_WriteFileServer, lookup LookupManager) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "empty WriteFile stream")
	}
	if err != nil {
		return err
	}
	mgr, err := lookup(first.GetActorUid())
	if err != nil {
		return err
	}

	pr, pw := io.Pipe()
	go func() {
		if len(first.GetChunk()) > 0 {
			if _, werr := pw.Write(first.GetChunk()); werr != nil {
				_ = pw.CloseWithError(werr)
				return
			}
		}
		for {
			msg, rerr := stream.Recv()
			if errors.Is(rerr, io.EOF) {
				_ = pw.Close()
				return
			}
			if rerr != nil {
				_ = pw.CloseWithError(rerr)
				return
			}
			if len(msg.GetChunk()) > 0 {
				if _, werr := pw.Write(msg.GetChunk()); werr != nil {
					_ = pw.CloseWithError(werr)
					return
				}
			}
		}
	}()

	written, err := mgr.WriteFile(
		stream.Context(),
		first.GetContainerName(),
		first.GetPath(),
		first.GetMode(),
		first.GetSeekOffset(),
		pr,
	)
	_ = pr.Close()
	if err != nil {
		return err
	}
	return stream.SendAndClose(&ateworkerpb.WriteFileResponse{BytesWritten: written})
}

// StreamReaderToEmit copies r in <= chunkSize chunks to emit until EOF.
func StreamReaderToEmit(ctx context.Context, r io.Reader, chunkSize int, emit func([]byte) error) error {
	if chunkSize <= 0 {
		chunkSize = DefaultMaxChunkBytes
	}
	buf := make([]byte, chunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if emitErr := emit(chunk); emitErr != nil {
				return emitErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
