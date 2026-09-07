/*
Copyright 2026 The InftyAI Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package modal reads a sandbox's logs with an exact cursor.
//
// The SDK already streams the same RPC, but sb.Stdout bottoms out in a byte pipe: the entry ID,
// the per-item timestamp and the file descriptor are all dropped before a caller sees a byte, and
// the entry ID is what makes a resume exact. Hence the direct RPC — not for the call, but to keep
// what the call already returns.
//
// It deliberately knows nothing about sinks: no epoch-millis conversion, no size limits, no line
// splitting. Those are one sink's rules, and this outlives any one sink.
package modal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	pb "github.com/modal-labs/modal-client/go/proto/modal_proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CursorStart asks for a sandbox's whole retained history.
//
// Any UNRECOGNIZED cursor means the same thing to the server — "1-0" replays everything too, with
// no error — so a corrupted cursor is a silent full replay, not a failure. Nothing here can
// detect that; the duplicate is absorbed downstream. See design.md at the component root.
const CursorStart = "0-0"

// streamTimeout is how long the server holds one stream open. It is the server's budget, not
// ours: Recv returns io.EOF at the end of it with the sandbox still running and more logs to
// come, which is the whole reason Follow re-opens instead of returning. The SDK's value.
const streamTimeout = 55 * time.Second

// maxRetries bounds consecutive transient failures. Unlike the SDK's, the budget is restored on
// every batch received — a stream followed for a training run's whole life would otherwise
// exhaust ten isolated blips spread over hours and give up on a healthy sandbox.
const maxRetries = 10

// Entry is one log item with the metadata the SDK's byte pipe discards.
type Entry struct {
	// Data is a raw chunk, NOT a line: it may hold several newlines, or half of one. Assembling
	// lines is the caller's, because whether a line is even the unit depends on the sink.
	Data string
	// At is the item's own timestamp, zero if the server sent neither form. A caller that needs a
	// timestamp regardless decides what to substitute; guessing read time here would smear every
	// replay and make a restart look like a burst.
	At time.Time
	FD pb.FileDescriptor
}

// Batch is one server batch, with the cursor that resumes strictly after it.
type Batch struct {
	Entries []Entry
	Cursor  string
}

// Follow calls fn once per batch of output, from cursor forward, and returns nil when the sandbox
// has no more logs to give. Pass CursorStart, or the empty string, for the whole history.
//
// Reconnection is internal, so the caller sees one continuous sequence across the server's
// 55-second windows and any transient failure. fn is called on the calling goroutine and must not
// block for long: a stalled fn stalls the cursor, and a stalled cursor risks Modal aging out logs
// that were never copied. Returning an error from fn aborts and propagates.
//
// At-least-once, by construction. The cursor advances in memory as batches arrive, so a caller
// that crashes re-reads from wherever its own durable cursor was.
func Follow(ctx context.Context, c LogsClient, sandboxID string, fd pb.FileDescriptor, cursor string, fn func(Batch) error) error {
	if cursor == "" {
		cursor = CursorStart
	}
	retries := maxRetries
	for {
		stream, err := c.SandboxGetLogs(ctx, pb.SandboxGetLogsRequest_builder{
			SandboxId:      sandboxID,
			FileDescriptor: fd,
			Timeout:        float32(streamTimeout.Seconds()),
			LastEntryId:    cursor,
		}.Build())
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if retryable(err) && retries > 0 {
				retries--
				continue
			}
			return fmt.Errorf("opening log stream for sandbox %s: %w", sandboxID, err)
		}

		for {
			batch, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// io.EOF is the server's window closing, NOT the end of the logs — that is
				// batch.Eof below. Re-open from the cursor either way; the only difference is
				// whether it costs a retry.
				if !errors.Is(err, io.EOF) {
					if !retryable(err) || retries == 0 {
						return fmt.Errorf("reading log stream for sandbox %s: %w", sandboxID, err)
					}
					retries--
				}
				break
			}
			retries = maxRetries

			// Guarded, not ordered: the final batch carries eof with an EMPTY entry ID, and
			// letting that reach the cursor would make the next resume replay the whole sandbox.
			// Checking eof first would work today and break the moment a batch arrives without
			// one for any other reason.
			if id := batch.GetEntryId(); id != "" {
				cursor = id
			}
			// A batch can be all state markers, so an empty one is normal and still advances the
			// cursor above. Only real output is worth waking the caller for.
			if entries := entries(batch, fd); len(entries) > 0 {
				if err := fn(Batch{Entries: entries, Cursor: cursor}); err != nil {
					return err
				}
			}
			if batch.GetEof() {
				return nil
			}
		}
	}
}

// entries keeps the items that are output. want is the descriptor the stream was opened for, used
// only when an item does not name its own.
func entries(batch *pb.TaskLogsBatch, want pb.FileDescriptor) []Entry {
	items := batch.GetItems()
	out := make([]Entry, 0, len(items))
	for _, item := range items {
		// Task state and progress markers ride the same stream carrying no data. They are not
		// output, and a sink like CloudWatch rejects an empty message outright.
		if item.GetData() == "" {
			continue
		}
		fd := item.GetFileDescriptor()
		if fd == pb.FileDescriptor_FILE_DESCRIPTOR_UNSPECIFIED {
			fd = want
		}
		out = append(out, Entry{Data: item.GetData(), At: itemTime(item), FD: fd})
	}
	return out
}

// itemTime prefers timestamp_ns. The float64 seconds field cannot represent every millisecond
// exactly, and a sink with millisecond resolution can therefore land two lines from the same
// millisecond in the wrong order.
func itemTime(item *pb.TaskLogs) time.Time {
	if ns := item.GetTimestampNs(); ns > 0 {
		return time.Unix(0, int64(ns))
	}
	if sec := item.GetTimestamp(); sec > 0 {
		return time.Unix(0, int64(sec*float64(time.Second)))
	}
	return time.Now()
}

// retryable mirrors the SDK's classification. Canceled is in it there and stays here: a server
// that cancels a log stream is not the same event as our own ctx being cancelled, which Follow
// checks first.
func retryable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.DeadlineExceeded, codes.Unavailable, codes.Canceled, codes.Internal, codes.Unknown:
		return true
	default:
		return false
	}
}
