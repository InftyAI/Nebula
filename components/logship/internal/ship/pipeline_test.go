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

package ship

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPipeline_ShipsEveryLineAndResumesFromTheCursor(t *testing.T) {
	src := &fakeSource{batches: []Batch{
		{Entries: []Entry{{Data: "a\nb\n", At: t1}}, Cursor: "1-0"},
		{Entries: []Entry{{Data: "c\n", At: t2}}, Cursor: "2-0"},
	}}
	sink := &fakeSink{}
	var shipped []string
	p := New(Config{
		Source: src, Sink: sink, Format: FormatCompact,
		Cursor:  "5-0",
		Shipped: func(c string) { shipped = append(shipped, c) },
	})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if src.cursor != "5-0" {
		t.Fatalf("followed from %q, want the configured cursor", src.cursor)
	}
	want := []string{"1-0 a", "1-0 b", "2-0 c"}
	if got := sink.messages(); !equal(got, want) {
		t.Fatalf("shipped %q, want %q", got, want)
	}
	// The record half of read -> put -> record: the highest cursor of the batch, once, after it
	// landed. Recording per line would checkpoint output that has not been accepted yet.
	if !equal(shipped, []string{"2-0"}) {
		t.Fatalf("recorded %q, want [2-0]", shipped)
	}
	if s := p.Stats(); s.Lines != 3 || s.Events != 3 || s.Dropped != 0 || s.Failed != 0 {
		t.Fatalf("Stats = %+v", s)
	}
}

func TestPipeline_FlushesTheUnterminatedTail(t *testing.T) {
	// A sandbox that exits mid-line has still printed that text, and it is usually the panic.
	src := &fakeSource{batches: []Batch{
		{Entries: []Entry{{Data: "done\nfatal: no newline", At: t1}}, Cursor: "1-0"},
	}}
	sink := &fakeSink{}

	if err := New(Config{Source: src, Sink: sink, Format: FormatCompact}).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []string{"1-0 done", "1-0 fatal: no newline"}
	if got := sink.messages(); !equal(got, want) {
		t.Fatalf("shipped %q, want %q", got, want)
	}
}

func TestPipeline_ReturnsTheSourcesErrorAfterShippingWhatItRead(t *testing.T) {
	// The lines before the failure are already durable-worthy; losing them because the stream broke
	// afterwards would defeat the point.
	boom := errors.New("stream broke")
	src := &fakeSource{
		batches: []Batch{{Entries: []Entry{{Data: "before\n", At: t1}}, Cursor: "1-0"}},
		err:     boom,
	}
	sink := &fakeSink{}

	if err := New(Config{Source: src, Sink: sink, Format: FormatCompact}).Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want %v", err, boom)
	}
	if got := sink.messages(); !equal(got, []string{"1-0 before"}) {
		t.Fatalf("shipped %q, want the line read before the failure", got)
	}
}

func TestPipeline_FlushesAQuietStreamOnTheInterval(t *testing.T) {
	// The batch is nowhere near full and the stream stays open, so nothing but the clock will ship
	// this. Without it a low-volume workload's logs sit in memory for the length of the run.
	src := &holdSource{
		batches: []Batch{{Entries: []Entry{{Data: "one quiet line\n", At: t1}}, Cursor: "1-0"}},
		release: make(chan struct{}),
	}
	sink := &fakeSink{}
	p := New(Config{Source: src, Sink: sink, Format: FormatCompact, Interval: 10 * time.Millisecond})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()

	if !waitFor(func() bool { return len(sink.messages()) == 1 }) {
		t.Fatal("nothing shipped while the stream stayed open")
	}
	close(src.release)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestPipeline_DropsRatherThanStallingTheReader(t *testing.T) {
	// The trade the whole component turns on: a reader blocked on CloudWatch is a reader not
	// draining Modal, and Modal is the copy on a retention clock. So the buffer overflows into a
	// counter and the reader keeps going.
	lines := make([]Entry, 200)
	for i := range lines {
		lines[i] = Entry{Data: strings.Repeat("x", 100) + "\n", At: t1}
	}
	sink := &fakeSink{gate: make(chan struct{})}
	src := &fakeSource{batches: []Batch{{Entries: lines, Cursor: "1-0"}}}
	p := New(Config{Source: src, Sink: sink, Format: FormatCompact, MaxPendingBytes: 500})

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()

	// The reader finishes all 200 lines with the sink held shut.
	if !waitFor(func() bool { return p.Stats().Lines == 200 }) {
		t.Fatalf("reader stalled at %d lines", p.Stats().Lines)
	}
	close(sink.gate)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	s := p.Stats()
	if s.Dropped == 0 {
		t.Fatal("nothing dropped, so the bound did not apply")
	}
	if s.DroppedBytes < s.Dropped {
		t.Fatalf("Dropped = %d but DroppedBytes = %d", s.Dropped, s.DroppedBytes)
	}
	if s.Events+s.Dropped != s.Lines {
		t.Fatalf("%d shipped + %d dropped != %d read", s.Events, s.Dropped, s.Lines)
	}
}

func TestPipeline_CountsWhatAQueuedLineCostsBesidesItsBytes(t *testing.T) {
	// A blank line is real output with no payload, so a payload-only bound does not bound it at all:
	// 2,000 of them would sit inside a 500-byte buffer, which is the unbounded line count the byte
	// bound replaced.
	src := &fakeSource{batches: []Batch{
		{Entries: []Entry{{Data: strings.Repeat("\n", 2000), At: t1}}, Cursor: "1-0"},
	}}
	p := New(Config{Source: src, Sink: &fakeSink{}, Format: FormatCompact, MaxPendingBytes: 500})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	s := p.Stats()
	if s.Lines != 2000 {
		t.Fatalf("Lines = %d, want one per newline", s.Lines)
	}
	if s.Dropped == 0 {
		t.Fatal("2,000 blank lines fit a 500-byte buffer, so the bound is not a memory bound")
	}
	if s.Events+s.Dropped != s.Lines {
		t.Fatalf("%d shipped + %d dropped != %d read", s.Events, s.Dropped, s.Lines)
	}
}

func TestPipeline_AdmitsALineBiggerThanTheWholeBuffer(t *testing.T) {
	// Otherwise a line over the bound is unshippable forever rather than merely awkward, and the
	// assembler emits exactly such a line at maxFragment.
	src := &fakeSource{batches: []Batch{
		{Entries: []Entry{{Data: strings.Repeat("y", 4096) + "\n", At: t1}}, Cursor: "1-0"},
	}}
	sink := &fakeSink{}
	p := New(Config{Source: src, Sink: sink, Format: FormatCompact, MaxPendingBytes: 64})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if s := p.Stats(); s.Events != 1 || s.Dropped != 0 {
		t.Fatalf("Stats = %+v, want the oversized line shipped", s)
	}
}

func TestPipeline_RetriesAThrottleAndOnlyAThrottle(t *testing.T) {
	throttle := errors.New("slow down")
	src := &fakeSource{batches: []Batch{{Entries: []Entry{{Data: "a\n", At: t1}}, Cursor: "1-0"}}}
	sink := &fakeSink{failures: 1, err: throttle}
	p := New(Config{
		Source: src, Sink: sink, Format: FormatCompact,
		Throttled: func(err error) bool { return errors.Is(err, throttle) },
	})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sink.attempts() != 2 {
		t.Fatalf("%d attempts, want the throttled one then the retry", sink.attempts())
	}
	if s := p.Stats(); s.Events != 1 || s.Failed != 0 {
		t.Fatalf("Stats = %+v, want the retry to have landed", s)
	}
}

func TestPipeline_DropsWhatTheSinkRefusesWithoutRetrying(t *testing.T) {
	// A malformed batch retried five times is five times the damage and the same outcome. Counted
	// and abandoned, so the cursor keeps moving.
	src := &fakeSource{batches: []Batch{{Entries: []Entry{{Data: "a\n", At: t1}}, Cursor: "1-0"}}}
	sink := &fakeSink{failures: 10, err: errors.New("invalid")}
	var shipped []string
	p := New(Config{
		Source: src, Sink: sink, Format: FormatCompact,
		Throttled: func(error) bool { return false },
		Shipped:   func(c string) { shipped = append(shipped, c) },
	})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sink.attempts() != 1 {
		t.Fatalf("%d attempts, want no retry for a non-throttle", sink.attempts())
	}
	if s := p.Stats(); s.Failed != 1 || s.Events != 0 {
		t.Fatalf("Stats = %+v, want one failed event", s)
	}
	// The cursor must not advance past output that was never stored, or the loss becomes permanent
	// at the next restart instead of recoverable.
	if len(shipped) != 0 {
		t.Fatalf("recorded %q for a batch that never landed", shipped)
	}
}

func TestPipeline_SaysSoOnceWhenTheSinkRefuses(t *testing.T) {
	// The sink is shared by every stream, so a broken one loses the whole fleet's logs with the
	// process still up. One line rather than one per batch, or the flood buries what it announces.
	src := &fakeSource{batches: []Batch{
		{Entries: []Entry{{Data: "a\nb\nc\n", At: t1}}, Cursor: "1-0"},
	}}
	sink := &fakeSink{failures: 10, err: errors.New("invalid")}
	var logged int
	p := New(Config{
		Source: src, Sink: sink, Format: FormatCompact,
		// One event per put, so the dedup is actually exercised rather than hidden by batching.
		Limits: Limits{MaxEvents: 1},
		Log:    func(string, ...any) { logged++ },
	})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sink.attempts() != 3 {
		t.Fatalf("%d puts, want one per line", sink.attempts())
	}
	if s := p.Stats(); s.Failed != 3 {
		t.Fatalf("Stats = %+v, want every lost line counted", s)
	}
	if logged != 1 {
		t.Fatalf("logged %d times for 3 refused batches, want 1", logged)
	}
}

func TestPipeline_CancellationStopsTheReaderAndTheShipper(t *testing.T) {
	src := &holdSource{release: make(chan struct{})}
	p := New(Config{Source: src, Sink: &fakeSink{}, Format: FormatCompact})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

type fakeSource struct {
	batches []Batch
	err     error
	cursor  string
}

func (f *fakeSource) Follow(_ context.Context, cursor string, fn func(Batch) error) error {
	f.cursor = cursor
	for _, b := range f.batches {
		if err := fn(b); err != nil {
			return err
		}
	}
	return f.err
}

// holdSource emits its batches and then stays open, which is what a live sandbox does and what makes
// the interval flush and cancellation observable.
type holdSource struct {
	batches []Batch
	release chan struct{}
}

func (h *holdSource) Follow(ctx context.Context, _ string, fn func(Batch) error) error {
	for _, b := range h.batches {
		if err := fn(b); err != nil {
			return err
		}
	}
	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type fakeSink struct {
	mu sync.Mutex
	// failures is how many leading Puts return err; gate, when set, holds the first Put until it is
	// closed, which is how the reader gets observed running ahead of the sink.
	failures int
	err      error
	gate     chan struct{}

	tries int
	got   []Event
}

func (f *fakeSink) Put(_ context.Context, events []Event) error {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tries++
	if f.failures > 0 {
		f.failures--
		return f.err
	}
	f.got = append(f.got, events...)
	return nil
}

func (f *fakeSink) messages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.got))
	for _, e := range f.got {
		out = append(out, e.Message)
	}
	return out
}

func (f *fakeSink) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tries
}

func waitFor(cond func() bool) bool {
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func equal(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
