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
	"net"
	"testing"

	pb "github.com/modal-labs/modal-client/go/proto/modal_proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// A real gRPC server over bufconn rather than a hand-rolled LogsClient, because the generated
// server stub is right there — and a fake client would skip the dial path and the metadata
// headers, which is exactly where a wrong header shows up as an auth failure in production.

// batch is one scripted server response. Exactly one of lines/markers/fail/hold is usual.
type batch struct {
	id      string
	lines   []string
	markers int // items carrying a task state and no data
	eof     bool
	// closeWindow ends the stream cleanly after this batch, without eof — what the server's
	// 55-second timeout looks like to a client.
	closeWindow bool
	// fail aborts the stream with this error, once. A retry gets the batches after it.
	fail error
	// hold keeps the stream open indefinitely, like a sandbox that is still running.
	hold        bool
	timestampNs uint64
}

type fakeServer struct {
	pb.UnimplementedModalClientServer
	history []batch
	// opened counts streams, and cursors records the LastEntryId each was opened with, so a test
	// can assert what a resume actually asked for.
	opened  int
	cursors []string
	fired   map[int]bool
}

func (f *fakeServer) SandboxGetLogs(req *pb.SandboxGetLogsRequest, stream grpc.ServerStreamingServer[pb.TaskLogsBatch]) error {
	f.opened++
	f.cursors = append(f.cursors, req.GetLastEntryId())

	for i := f.startIndex(req.GetLastEntryId()); i < len(f.history); i++ {
		b := f.history[i]
		if b.fail != nil {
			if f.fired[i] {
				continue
			}
			f.fired[i] = true
			return b.fail
		}
		if b.hold {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		if err := stream.Send(toBatch(b, req.GetFileDescriptor())); err != nil {
			return err
		}
		if b.eof || b.closeWindow {
			return nil
		}
	}
	// Past the end of the script: the sandbox is finished, which the server says with an eof batch
	// carrying no entry ID of its own.
	return stream.Send(pb.TaskLogsBatch_builder{Eof: true}.Build())
}

// startIndex resolves a cursor to the batch after it. An unrecognized one replays from the
// beginning, with no error, which is what Modal really does.
func (f *fakeServer) startIndex(cursor string) int {
	if cursor == CursorStart {
		return 0
	}
	for i, b := range f.history {
		if b.id != "" && b.id == cursor {
			return i + 1
		}
	}
	return 0
}

func toBatch(b batch, fd pb.FileDescriptor) *pb.TaskLogsBatch {
	items := make([]*pb.TaskLogs, 0, len(b.lines)+b.markers)
	for _, line := range b.lines {
		items = append(items, pb.TaskLogs_builder{
			Data:        line,
			TimestampNs: b.timestampNs,
			// Left unspecified on purpose: the live server scopes a stream by descriptor and does
			// not repeat it per item, so inheriting the request's is the normal path.
			FileDescriptor: pb.FileDescriptor_FILE_DESCRIPTOR_UNSPECIFIED,
		}.Build())
	}
	for range b.markers {
		items = append(items, pb.TaskLogs_builder{TaskState: pb.TaskState_TASK_STATE_LOADING_IMAGE}.Build())
	}
	return pb.TaskLogsBatch_builder{Items: items, EntryId: b.id, Eof: b.eof}.Build()
}

func startFakeServer(t *testing.T, history []batch) (LogsClient, *fakeServer) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	fake := &fakeServer{history: history, fired: map[int]bool{}}
	pb.RegisterModalClientServer(srv, fake)
	go func() {
		// Errors here are the listener closing at cleanup, which is not a test failure.
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(headerInjectorStream(Credentials{TokenID: "ak-test", TokenSecret: "as-test"})),
	)
	if err != nil {
		t.Fatalf("dialing the fake server: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return pb.NewModalClientClient(conn), fake
}
