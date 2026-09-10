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

package emit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/InftyAI/Nebula/components/logship/internal/ship"
)

func events(msgs ...string) []ship.Event {
	out := make([]ship.Event, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, ship.Event{Message: m, At: time.Unix(0, 0)})
	}
	return out
}

func put(t *testing.T, msgs ...string) string {
	t.Helper()
	var buf bytes.Buffer
	if err := New(&buf).Put(t.Context(), events(msgs...)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	return buf.String()
}

func TestPutWritesOneNewlineTerminatedLinePerEvent(t *testing.T) {
	if got := put(t, `{"a":1}`, `{"b":2}`); got != `{"a":1}`+"\n"+`{"b":2}`+"\n" {
		t.Fatalf("stdout = %q", got)
	}
}

// A record has to reach the agent byte for byte: the agent re-emits the line as it found it, so
// anything this layer adds or escapes lands in the consumer's parser.
func TestPutDoesNotRewriteTheRecord(t *testing.T) {
	const msg = `{"time":"t","log":"{\"category\":\"user\"}","kubernetes":{"pod_name":"p"}}`
	if got := strings.TrimSuffix(put(t, msg), "\n"); got != msg {
		t.Fatalf("record changed in transit:\n got %s\nwant %s", got, msg)
	}
}

func TestPutOfNothingWritesNothing(t *testing.T) {
	var buf bytes.Buffer
	if err := New(&buf).Put(t.Context(), nil); err != nil {
		t.Fatalf("Put(nil): %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("empty Put wrote %q", buf.String())
	}
}

// Cancellation is how Shutdown reaches the pipelines, and Pipeline.Run drains what it has already read
// afterwards. A sink that honoured ctx would discard exactly the tail of a finishing sandbox.
func TestPutIgnoresACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var buf bytes.Buffer
	if err := New(&buf).Put(ctx, events("last line")); err != nil {
		t.Fatalf("Put on a cancelled ctx: %v", err)
	}
	if buf.String() != "last line\n" {
		t.Fatalf("stdout = %q", buf.String())
	}
}

// Every stream in the process shares one sink and one file descriptor. A write that interleaved would
// not lose a record, it would splice two together — an unparseable line the consumer drops in silence.
func TestConcurrentPutsDoNotInterleave(t *testing.T) {
	const streams, batches = 16, 32

	// A writer that keeps each Write separate, so "the batch arrived in one piece" is checkable at all
	// — a plain buffer would hide it.
	w := &chunkWriter{}
	s := New(w)

	var wg sync.WaitGroup
	for stream := range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range batches {
				// Two events per Put: a batch is the unit that must not be broken up, not a line.
				msgs := []string{
					fmt.Sprintf(`{"stream":%d,"n":%d,"half":"a"}`, stream, i),
					fmt.Sprintf(`{"stream":%d,"n":%d,"half":"b"}`, stream, i),
				}
				if err := s.Put(t.Context(), events(msgs...)); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(w.chunks) != streams*batches {
		t.Fatalf("%d writes, want one per batch (%d)", len(w.chunks), streams*batches)
	}
	seen := map[string]bool{}
	for _, chunk := range w.chunks {
		lines := strings.Split(strings.TrimSuffix(chunk, "\n"), "\n")
		if len(lines) != 2 {
			t.Fatalf("a batch reached the writer as %d lines: %q", len(lines), chunk)
		}
		for _, l := range lines {
			if !strings.HasPrefix(l, `{"stream":`) || !strings.HasSuffix(l, `"}`) {
				t.Fatalf("spliced line: %q", l)
			}
			if seen[l] {
				t.Fatalf("duplicated line: %q", l)
			}
			seen[l] = true
		}
	}
	if len(seen) != streams*batches*2 {
		t.Fatalf("%d distinct lines, want %d", len(seen), streams*batches*2)
	}
}

// The batcher splits a line by MaxEventBytes and nothing downstream splits again, so this cap is the
// only thing standing between a long line and the multiline rejoin the agent would otherwise have to
// get right. See maxEventBytes.
func TestLimitsKeepARecordInsideOneCRIChunk(t *testing.T) {
	got := Limits(0)
	if got.MaxEventBytes != maxEventBytes || got.MaxEventBytes > 16<<10 {
		t.Fatalf("MaxEventBytes = %d, want at most one 16 KiB chunk", got.MaxEventBytes)
	}
	if got.PerEventOverhead != perEventOverhead {
		t.Fatalf("PerEventOverhead = %d, want CloudWatch's per-event charge reserved", got.PerEventOverhead)
	}
	if got.MaxBytes != DefaultBatchBytes {
		t.Fatalf("MaxBytes = %d, want the default write bound", got.MaxBytes)
	}
	if got.MaxEvents != 0 {
		t.Fatalf("MaxEvents = %d, want unset", got.MaxEvents)
	}
	if n := Limits(1234).MaxBytes; n != 1234 {
		t.Fatalf("Limits(1234).MaxBytes = %d", n)
	}
}

// One line per record is the contract, and a record's own text is arbitrary bytes. The escaping that
// keeps the two compatible lives in ship, so this is the check that the two packages still agree.
func TestARecordWithAnEmbeddedNewlineStaysOneLine(t *testing.T) {
	format := ship.Record{Pod: "p"}.Formatter()
	msg := format(ship.Line{Data: "first\nsecond\r\ttab", At: time.Unix(0, 0), Cursor: "1-0"})

	body := put(t, msg)
	if n := strings.Count(body, "\n"); n != 1 {
		t.Fatalf("%d newlines in one record's output, want the terminator only:\n%s", n, body)
	}
}

func TestSinkIsAShipSink(t *testing.T) {
	var _ ship.Sink = (*Sink)(nil)

	// And it must NOT be a Closer. Pipeline used to close a sink at the end of its stream; one stream
	// ending must not close stdout for the thousand still running.
	if _, ok := any(New(&bytes.Buffer{})).(interface{ Close() error }); ok {
		t.Fatal("*Sink implements Close, which a shared stdout must not")
	}
}

func TestPutReportsAWriteFailure(t *testing.T) {
	want := errors.New("broken pipe")
	if err := New(failWriter{want}).Put(t.Context(), events("x")); !errors.Is(err, want) {
		t.Fatalf("Put = %v, want %v", err, want)
	}
}

// chunkWriter records each Write as it arrived, without a lock of its own.
type chunkWriter struct{ chunks []string }

func (w *chunkWriter) Write(p []byte) (int, error) {
	w.chunks = append(w.chunks, string(p))
	return len(p), nil
}

type failWriter struct{ err error }

func (w failWriter) Write([]byte) (int, error) { return 0, w.err }
