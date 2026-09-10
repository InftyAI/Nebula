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

package modal

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/modal-labs/modal-client/go/proto/modal_proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The four cursor behaviours below are what a live sandbox actually did (see the component's
// design.md); fakeServer reproduces them so the loop can be tested against them without a
// Modal account.

func TestFollow_CursorSemantics(t *testing.T) {
	history := []batch{
		{id: "100-0", lines: []string{"first\n"}},
		{id: "200-0", lines: []string{"second\n"}},
		{id: "300-0", lines: []string{"third\n"}},
	}
	cases := []struct {
		name   string
		cursor string
		want   []string
	}{
		{"whole history from the start cursor", CursorStart, []string{"first\n", "second\n", "third\n"}},
		{"whole history from an empty cursor", "", []string{"first\n", "second\n", "third\n"}},
		{"only what followed the first entry", "100-0", []string{"second\n", "third\n"}},
		{"nothing but eof from the last entry", "300-0", nil},
		// The server does not reject a cursor it does not know, so this replays instead of
		// failing. Silent by design on Modal's side, and nothing here can tell the difference.
		{"whole history from an unrecognized cursor", "1-0", []string{"first\n", "second\n", "third\n"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := startFakeServer(t, history)
			got := collect(t, client, tc.cursor)
			if strings.Join(got, "") != strings.Join(tc.want, "") {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFollow_CursorSurvivesTheEmptyEntryIDOnEOF(t *testing.T) {
	// The eof batch carries a final line and no entry ID at all. Taking that ID would leave the
	// caller resuming from "" — a full replay of everything already shipped.
	history := []batch{
		{id: "100-0", lines: []string{"first\n"}},
		{id: "", lines: []string{"last\n"}, eof: true},
	}
	client, _ := startFakeServer(t, history)

	var last string
	if err := Follow(t.Context(), client, "sb-1", pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT, CursorStart,
		func(b Batch) error {
			last = b.Cursor
			return nil
		}); err != nil {
		t.Fatalf("Follow: %v", err)
	}
	if last != "100-0" {
		t.Fatalf("cursor %q after the eof batch, want the last real entry ID", last)
	}
}

func TestFollow_ReopensAcrossTheServerWindow(t *testing.T) {
	// Two streams' worth of history: the first ends without eof, exactly as a 55-second server
	// window does. A loop that mistook that io.EOF for the end would stop after "first".
	client, srv := startFakeServer(t, []batch{
		{id: "100-0", lines: []string{"first\n"}, closeWindow: true},
		{id: "200-0", lines: []string{"second\n"}, eof: true},
	})
	if got := collect(t, client, CursorStart); strings.Join(got, "") != "first\nsecond\n" {
		t.Fatalf("got %q across the window boundary", got)
	}
	if srv.opened != 2 {
		t.Fatalf("%d streams opened, want 2 — the window close must reopen", srv.opened)
	}
	if srv.cursors[1] != "100-0" {
		t.Fatalf("reopened at %q, want the last delivered entry ID", srv.cursors[1])
	}
}

func TestFollow_SkipsItemsWithNoData(t *testing.T) {
	// State and progress markers ride the same stream. They must not reach a sink, and a batch
	// made only of them must still advance the cursor.
	client, srv := startFakeServer(t, []batch{
		{id: "100-0", markers: 2},
		{id: "200-0", lines: []string{"real\n"}, closeWindow: true},
		{id: "300-0", eof: true},
	})
	if got := collect(t, client, CursorStart); strings.Join(got, "") != "real\n" {
		t.Fatalf("got %q, want only the item carrying data", got)
	}
	if srv.cursors[1] != "200-0" {
		t.Fatalf("reopened at %q, want a marker-only batch to have advanced the cursor", srv.cursors[1])
	}
}

func TestFollow_RetriesTransientAndResumes(t *testing.T) {
	client, srv := startFakeServer(t, []batch{
		{id: "100-0", lines: []string{"first\n"}},
		{fail: status.Error(codes.Unavailable, "server going away")},
		{id: "200-0", lines: []string{"second\n"}, eof: true},
	})
	if got := collect(t, client, CursorStart); strings.Join(got, "") != "first\nsecond\n" {
		t.Fatalf("got %q, want the stream to resume after a retryable error", got)
	}
	if srv.cursors[1] != "100-0" {
		t.Fatalf("retried at %q, want no re-delivery of what already arrived", srv.cursors[1])
	}
}

func TestFollow_StopsOnPermanentError(t *testing.T) {
	client, _ := startFakeServer(t, []batch{
		{fail: status.Error(codes.PermissionDenied, "bad token")},
	})
	err := Follow(t.Context(), client, "sb-1", pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT, CursorStart,
		func(Batch) error { return nil })
	if err == nil {
		t.Fatal("Follow returned nil on a non-retryable error")
	}
	if !strings.Contains(err.Error(), "sb-1") {
		t.Fatalf("error %q does not name the sandbox", err)
	}
}

func TestFollow_KeepsTheItemTimestamp(t *testing.T) {
	ns := uint64(1788477379282_000_000)
	client, _ := startFakeServer(t, []batch{
		{id: "100-0", lines: []string{"stamped\n"}, timestampNs: ns, eof: true},
	})
	var got []Entry
	if err := Follow(t.Context(), client, "sb-1", pb.FileDescriptor_FILE_DESCRIPTOR_STDERR, CursorStart,
		func(b Batch) error {
			got = append(got, b.Entries...)
			return nil
		}); err != nil {
		t.Fatalf("Follow: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("%d entries, want 1", len(got))
	}
	if want := time.Unix(0, int64(ns)); !got[0].At.Equal(want) {
		t.Fatalf("At = %v, want %v", got[0].At, want)
	}
	// The item did not name a descriptor, so it inherits the one the stream was opened for —
	// otherwise every entry would land as UNSPECIFIED and the two streams would merge.
	if got[0].FD != pb.FileDescriptor_FILE_DESCRIPTOR_STDERR {
		t.Fatalf("FD = %v, want the requested descriptor", got[0].FD)
	}
}

func TestFollow_PropagatesCallbackError(t *testing.T) {
	client, _ := startFakeServer(t, []batch{
		{id: "100-0", lines: []string{"first\n"}},
		{id: "200-0", lines: []string{"second\n"}, eof: true},
	})
	sentinel := status.Error(codes.ResourceExhausted, "sink is full")
	calls := 0
	err := Follow(t.Context(), client, "sb-1", pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT, CursorStart,
		func(Batch) error {
			calls++
			return sentinel
		})
	if err == nil || !strings.Contains(err.Error(), "sink is full") {
		t.Fatalf("err = %v, want the callback's error", err)
	}
	if calls != 1 {
		t.Fatalf("%d callbacks, want the first failure to abort", calls)
	}
}

func TestFollow_HonoursContextCancellation(t *testing.T) {
	client, _ := startFakeServer(t, []batch{
		{id: "100-0", lines: []string{"first\n"}},
		// The stream then stays open with no eof, which is what a running sandbox looks like.
		// Without it the fake would close and the cancellation would race the final batch.
		{hold: true},
	})
	ctx, cancel := context.WithCancel(t.Context())
	err := Follow(ctx, client, "sb-1", pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT, CursorStart,
		func(Batch) error {
			cancel()
			return nil
		})
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func collect(t *testing.T, c LogsClient, cursor string) []string {
	t.Helper()
	var got []string
	if err := Follow(t.Context(), c, "sb-1", pb.FileDescriptor_FILE_DESCRIPTOR_STDOUT, cursor,
		func(b Batch) error {
			for _, e := range b.Entries {
				got = append(got, e.Data)
			}
			return nil
		}); err != nil {
		t.Fatalf("Follow: %v", err)
	}
	return got
}
