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

package vnode

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
)

// testTiming drives the heuristics in milliseconds. ceiling >> idle, so a test aiming
// at one cannot hit the other.
var testTiming = logTiming{idle: 30 * time.Millisecond, ceiling: 5 * time.Second}

// fakeLogSource is a provider log stream under test control. Like a real following
// stream, it blocks when there is nothing to give instead of reporting EOF.
type fakeLogSource struct {
	mu     sync.Mutex
	buf    []byte
	closed bool
	ready  chan struct{} // signalled on every write, so Read wakes without polling
}

func newFakeLogSource(initial string) *fakeLogSource {
	return &fakeLogSource{buf: []byte(initial), ready: make(chan struct{}, 1)}
}

func (f *fakeLogSource) write(s string) {
	f.mu.Lock()
	f.buf = append(f.buf, s...)
	f.mu.Unlock()
	select {
	case f.ready <- struct{}{}:
	default:
	}
}

func (f *fakeLogSource) Read(p []byte) (int, error) {
	for {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return 0, io.EOF
		}
		if len(f.buf) > 0 {
			n := copy(p, f.buf)
			f.buf = f.buf[n:]
			f.mu.Unlock()
			return n, nil
		}
		f.mu.Unlock()
		// The point: silence is NOT the end.
		select {
		case <-f.ready:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (f *fakeLogSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	select {
	case f.ready <- struct{}{}: // wake a blocked Read so it observes closed
	default:
	}
	return nil
}

func (f *fakeLogSource) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// `kubectl logs` with no --follow must terminate, but a provider stream never EOFs
// while the instance lives. The idle gap ends it — after the whole backlog, not
// truncated at the first pause.
func TestKubeletLogStream_OneShotEndsAtIdleGap(t *testing.T) {
	src := newFakeLogSource("line one\nline two\n")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{}, testTiming)
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "line one\nline two\n" {
		t.Fatalf("logs = %q, want the whole backlog", got)
	}
}

// The bug this guards: a chunk queued when the idle timer fires used to be dropped at
// random, because select picks between two ready cases at random. The symptom was a
// `kubectl logs` that printed a different amount of the SAME log on each read.
//
// idle=0 makes the timer ready at the same instant as the queued chunks, i.e. the race
// in the wild, every iteration.
func TestCopyLogs_IdleDoesNotDropQueuedChunks(t *testing.T) {
	const want = "first\nsecond\nthird\n"
	for i := range 50 {
		chunks := make(chan []byte, 8)
		chunks <- []byte("first\n")
		chunks <- []byte("second\n")
		chunks <- []byte("third\n")

		var got bytes.Buffer
		copyLogs(context.Background(), &got, chunks, make(chan struct{}), vkapi.ContainerLogOpts{},
			logTiming{idle: 0, ceiling: time.Second})
		if got.String() != want {
			t.Fatalf("iteration %d: logs = %q, want %q — a queued chunk was dropped at the idle gap",
				i, got.String(), want)
		}
	}
}

// With --follow, silence must NOT end it: a `kubectl logs -f` on an idle server would
// otherwise exit at once and look like the Pod had died.
func TestKubeletLogStream_FollowDoesNotEndAtIdleGap(t *testing.T) {
	src := newFakeLogSource("first\n")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Follow: true}, testTiming)
	defer func() { _ = rc.Close() }()

	if got := readN(t, rc, len("first\n")); got != "first\n" {
		t.Fatalf("first read = %q", got)
	}
	// Well past the idle gap, so a one-shot read would already have EOF'd here.
	time.Sleep(4 * testTiming.idle)
	src.write("later\n")
	if got := readN(t, rc, len("later\n")); got != "later\n" {
		t.Fatalf("second read = %q, want output written after the idle gap", got)
	}
}

// --tail=N applies to the backlog: buffer it, emit only the last N lines.
func TestKubeletLogStream_TailKeepsLastLines(t *testing.T) {
	src := newFakeLogSource("a\nb\nc\nd\n")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Tail: 2}, testTiming)
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "c\nd\n" {
		t.Fatalf("logs = %q, want the last 2 lines", got)
	}
}

// A last write with no trailing newline (a crash message, a prompt) is exactly what
// --tail is asked for, so the remainder counts as a line instead of being dropped.
func TestKubeletLogStream_TailKeepsUnterminatedLine(t *testing.T) {
	src := newFakeLogSource("a\nb\npanic: no newline")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Tail: 2}, testTiming)
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "b\npanic: no newline" {
		t.Fatalf("logs = %q, want the last line plus the unterminated tail", got)
	}
}

// With both, print the last N lines and then keep streaming; the tail must not
// swallow what follows.
func TestKubeletLogStream_TailThenFollow(t *testing.T) {
	src := newFakeLogSource("a\nb\nc\n")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Tail: 1, Follow: true}, testTiming)
	defer func() { _ = rc.Close() }()

	if got := readN(t, rc, len("c\n")); got != "c\n" {
		t.Fatalf("backlog tail = %q, want the last line only", got)
	}
	src.write("d\n")
	if got := readN(t, rc, len("d\n")); got != "d\n" {
		t.Fatalf("followed output = %q", got)
	}
}

// LimitBytes caps what reaches the client, applied outermost so it bounds the result
// of the other options.
func TestKubeletLogStream_LimitBytes(t *testing.T) {
	src := newFakeLogSource("0123456789\n")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{LimitBytes: 4}, testTiming)
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "0123" {
		t.Fatalf("logs = %q, want 4 bytes", got)
	}
}

// When the instance exits the stream really ends, so the tail buffer must be flushed
// — a short-lived Pod's output is the whole reason to run `kubectl logs`.
func TestKubeletLogStream_SourceEOFFlushesTail(t *testing.T) {
	src := newFakeLogSource("only line\n")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Tail: 5, Follow: true}, testTiming)
	defer func() { _ = rc.Close() }()

	// EOF under --follow: without the flush this read would block forever.
	go func() {
		time.Sleep(2 * testTiming.idle)
		_ = src.Close()
	}()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "only line\n" {
		t.Fatalf("logs = %q, want the buffered line flushed on EOF", got)
	}
}

// The leak test: a Ctrl-C'd `kubectl logs -f` must release the provider stream. On
// Modal that is a long-polling gRPC call, so one per abandoned client is unbounded.
func TestKubeletLogStream_CloseReleasesSource(t *testing.T) {
	src := newFakeLogSource("streaming\n")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Follow: true}, testTiming)

	if got := readN(t, rc, len("streaming\n")); got != "streaming\n" {
		t.Fatalf("read = %q", got)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitFor(t, func() bool { return src.isClosed() }, "provider stream closed after Close")

	// Idempotent: the route may close alongside a cancelled request.
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// A dead request context (client disconnected) must end the stream, not just stop the
// reader.
func TestKubeletLogStream_ContextCancelEndsFollow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := newFakeLogSource("x\n")
	rc := newLogStream(ctx, src, vkapi.ContainerLogOpts{Follow: true}, testTiming)
	defer func() { _ = rc.Close() }()

	if got := readN(t, rc, 2); got != "x\n" {
		t.Fatalf("read = %q", got)
	}
	cancel()
	// The pipe closes, so the read terminates instead of hanging.
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll after cancel: %v", err)
	}
}

// The tail buffer must not grow forever on a workload that writes megabytes with no
// newline, so at the cap the fragment becomes a line.
func TestLineRing_BoundsUnterminatedLine(t *testing.T) {
	r := newLineRing(1)
	r.write([]byte(strings.Repeat("x", logsMaxLineBytes)))
	if len(r.partial) != 0 {
		t.Fatalf("partial = %d bytes, want it flushed into a line at the cap", len(r.partial))
	}
	if got := len(r.bytes()); got != logsMaxLineBytes {
		t.Fatalf("retained %d bytes, want %d", got, logsMaxLineBytes)
	}
	// Still a ring: one more capped fragment evicts the first.
	r.write([]byte(strings.Repeat("y", logsMaxLineBytes)))
	if got := len(r.bytes()); got != logsMaxLineBytes {
		t.Fatalf("retained %d bytes after a second line, want the ring to hold 1", got)
	}
}

// --tail=N is the client's number, so N must not be what bounds the manager's heap:
// past logsTailMaxBytes the oldest lines go, and the newest survive.
func TestLineRing_BoundsTotalBytes(t *testing.T) {
	const line = 64 * 1024
	r := newLineRing(1000) // 1000 x 64KiB = 64MiB if the byte cap did not hold
	for i := range 1000 {
		r.write(append(bytes.Repeat([]byte{byte('a' + i%26)}, line-1), '\n'))
	}
	if got := len(r.bytes()); got > logsTailMaxBytes {
		t.Fatalf("retained %d bytes, want <= %d", got, logsTailMaxBytes)
	}
	// The retained lines must be the LAST ones written, not the first.
	if want := bytes.Repeat([]byte{byte('a' + 999%26)}, line-1); !bytes.Contains(r.bytes(), want) {
		t.Fatal("last line written was evicted, want the newest output retained")
	}
}

// loggingProvider is a fakeProvider that also implements provider.LogStreamer.
// fakeProvider deliberately does not, so the two cover both sides of the assertion.
type loggingProvider struct {
	*fakeProvider
	logs      string
	logsErr   error
	askedFor  string
	askedOnce chan struct{}
}

func newLoggingProvider(fp *fakeProvider, logs string) *loggingProvider {
	return &loggingProvider{fakeProvider: fp, logs: logs, askedOnce: make(chan struct{}, 1)}
}

func (p *loggingProvider) Logs(_ context.Context, instanceID string) (io.ReadCloser, error) {
	p.askedFor = instanceID
	select {
	case p.askedOnce <- struct{}{}:
	default:
	}
	if p.logsErr != nil {
		return nil, p.logsErr
	}
	return io.NopCloser(strings.NewReader(p.logs)), nil
}

// The whole path: Pod → the instance id it provisioned → that instance's output. The
// id is the assertion that matters; a wrong one would show another tenant's logs.
func TestGetContainerLogs_StreamsTrackedPod(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	lp := newLoggingProvider(fp, "hello from the sandbox\n")
	h := NewHandler(lp, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	rc, err := h.GetContainerLogs(context.Background(), "default", "p1", "main", vkapi.ContainerLogOpts{})
	if err != nil {
		t.Fatalf("GetContainerLogs: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello from the sandbox\n" {
		t.Fatalf("logs = %q", got)
	}
	if lp.askedFor != "inst-1" {
		t.Fatalf("provider asked for instance %q, want inst-1", lp.askedFor)
	}
}

// One instance, one console: `-c whatever` must not 404 on a name with no separate
// stream behind it.
func TestGetContainerLogs_IgnoresContainerName(t *testing.T) {
	lp := newLoggingProvider(&fakeProvider{provisionID: "inst-1"}, "out\n")
	h := NewHandler(lp, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	for _, container := range []string{"", "main", "not-a-container"} {
		rc, err := h.GetContainerLogs(context.Background(), "default", "p1", container, vkapi.ContainerLogOpts{})
		if err != nil {
			t.Fatalf("GetContainerLogs(container=%q): %v", container, err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("ReadAll(container=%q): %v", container, err)
		}
		if string(got) != "out\n" {
			t.Fatalf("container=%q logs = %q", container, got)
		}
	}
}

// Every miss must read as NotFound, which kubectl reports as such instead of a 500
// the user is invited to retry.
func TestGetContainerLogs_NotFoundCases(t *testing.T) {
	// No log support at all — a legitimate configuration, not an internal error.
	t.Run("provider does not stream logs", func(t *testing.T) {
		h := NewHandler(&fakeProvider{provisionID: "inst-1"}, nil, nil, openCluster())
		if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
			t.Fatalf("CreatePod: %v", err)
		}
		_, err := h.GetContainerLogs(context.Background(), "default", "p1", "main", vkapi.ContainerLogOpts{})
		assertNotFound(t, err)
	})

	// Another node's pod, or one this process never adopted.
	t.Run("pod not tracked", func(t *testing.T) {
		h := NewHandler(newLoggingProvider(&fakeProvider{}, "x\n"), nil, nil, openCluster())
		_, err := h.GetContainerLogs(context.Background(), "default", "ghost", "main", vkapi.ContainerLogOpts{})
		assertNotFound(t, err)
	})

	// Tracked with no instance id: what a rejected Provision leaves behind. Nothing to
	// read, and asking the provider for instance "" is meaningless.
	t.Run("tracked without an instance", func(t *testing.T) {
		fp := &fakeProvider{provisionErr: errors.New("no capacity")}
		lp := newLoggingProvider(fp, "x\n")
		h := NewHandler(lp, nil, nil, openCluster())
		if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
			t.Fatal("CreatePod: expected the provision rejection to surface")
		}
		_, err := h.GetContainerLogs(context.Background(), "default", "p1", "main", vkapi.ContainerLogOpts{})
		assertNotFound(t, err)
		if lp.askedFor != "" {
			t.Fatalf("provider was asked for instance %q; it must not be called without an id", lp.askedFor)
		}
	})
}

// A provider that cannot open the stream (auth expired, API down) is a real error.
// NotFound would claim the pod has no logs when Nebula simply could not ask.
func TestGetContainerLogs_ProviderErrorIsNotNotFound(t *testing.T) {
	lp := newLoggingProvider(&fakeProvider{provisionID: "inst-1"}, "")
	lp.logsErr = errors.New("provider API unreachable")
	h := NewHandler(lp, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	_, err := h.GetContainerLogs(context.Background(), "default", "p1", "main", vkapi.ContainerLogOpts{})
	if err == nil {
		t.Fatal("expected an error when the provider cannot open the stream")
	}
	if errdefs.IsNotFound(err) {
		t.Fatalf("err = %v, want a plain error rather than NotFound", err)
	}
	if !strings.Contains(err.Error(), "inst-1") {
		t.Fatalf("err = %v, want the instance id in the message", err)
	}
}

// The handler must route through kubeletLogStream, not return the raw stream —
// otherwise --tail/--limit-bytes would silently do nothing.
func TestGetContainerLogs_AppliesOpts(t *testing.T) {
	lp := newLoggingProvider(&fakeProvider{provisionID: "inst-1"}, "a\nb\nc\n")
	h := NewHandler(lp, nil, nil, openCluster())
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	rc, err := h.GetContainerLogs(context.Background(), "default", "p1", "main", vkapi.ContainerLogOpts{Tail: 1})
	if err != nil {
		t.Fatalf("GetContainerLogs: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "c\n" {
		t.Fatalf("logs = %q, want --tail=1 honoured", got)
	}
}

func assertNotFound(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errdefs.IsNotFound(err) {
		t.Fatalf("err = %v, want NotFound so kubectl reports it as such", err)
	}
}

// readN reads exactly n bytes, failing rather than hanging if the stream stalls.
func readN(t *testing.T, r io.Reader, n int) string {
	t.Helper()
	buf := make([]byte, n)
	type result struct {
		s   string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_, err := io.ReadFull(r, buf)
		ch <- result{string(buf), err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("read %d bytes: %v (got %q)", n, res.err, res.s)
		}
		return res.s
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out reading %d bytes", n)
		return ""
	}
}

// waitFor polls cond, so a test need not sleep for a fixed guess.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// --- stream failures --------------------------------------------------------

// failingLogSource yields s, then fails — a provider stream that died mid-poll.
type failingLogSource struct {
	s      string
	err    error
	n      int
	closed bool
}

func (f *failingLogSource) Read(p []byte) (int, error) {
	if f.n < len(f.s) {
		n := copy(p, f.s[f.n:])
		f.n += n
		return n, nil
	}
	return 0, f.err
}

func (f *failingLogSource) Close() error { f.closed = true; return nil }

// A stream that BROKE must not read as the end of the log. Providers serve only recent
// output, so an empty result is ordinary — swallowing the error left an outage and a quiet
// workload looking identical, which is how a working `kubectl logs` was read as broken.
func TestKubeletLogStream_SourceFailureSurfaces(t *testing.T) {
	src := &failingLogSource{s: "before the failure\n", err: errors.New("provider stream unavailable")}
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Follow: true}, testTiming)
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err == nil {
		t.Fatal("ReadAll: expected the stream failure to surface")
	}
	if !strings.Contains(err.Error(), "provider stream unavailable") {
		t.Fatalf("err = %v, want the provider's own message", err)
	}
	// What arrived is still delivered: the error ends the log, it does not discard it.
	if string(got) != "before the failure\n" {
		t.Fatalf("logs = %q, want the output that preceded the failure", got)
	}
}

// EOF is the log ENDING, not a failure: an instance that exited must not make
// `kubectl logs` report an error over its final output.
func TestKubeletLogStream_EOFIsNotAnError(t *testing.T) {
	src := &failingLogSource{s: "all of it\n", err: io.EOF}
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Follow: true}, testTiming)
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v, want a clean EOF", err)
	}
	if string(got) != "all of it\n" {
		t.Fatalf("logs = %q", got)
	}
}

// Closing is OUR teardown — a `kubectl logs -f` client hanging up. The read errors it
// causes must not be reported as a provider failure.
func TestKubeletLogStream_CloseIsNotAFailure(t *testing.T) {
	src := newFakeLogSource("first\n")
	rc := newLogStream(context.Background(), src, vkapi.ContainerLogOpts{Follow: true}, testTiming)

	if got := readN(t, rc, len("first\n")); got != "first\n" {
		t.Fatalf("first read = %q", got)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The reader is closed either way; what matters is that nothing reports the closed
	// source as an outage.
	if _, err := io.ReadAll(rc); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("err = %v, want teardown to end the stream cleanly", err)
	}
}
