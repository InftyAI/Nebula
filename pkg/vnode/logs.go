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
	"sync"
	"time"

	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
)

// A real kubelet reads logs from a file, so it knows where the backlog ends. A
// provider stream does not: it replays from the instance's first byte and then
// long-polls forever, with nothing marking "you have caught up". The constants below
// are how we guess that boundary.
const (
	// logsIdleGap is how long the stream must go silent before a one-shot read (`kubectl
	// logs`, no --follow) calls the backlog finished. A heuristic, but the alternative is
	// hanging until the workload exits. Well above a provider's polling granularity, so a
	// pause mid-backlog is not read as the end of it.
	logsIdleGap = time.Second

	// logsBacklogCeiling bounds a one-shot read on a workload too chatty to ever go
	// idle. Hitting it truncates rather than fails; --follow has no ceiling.
	logsBacklogCeiling = 30 * time.Second

	// logsChunkSize is the read size off the provider stream.
	logsChunkSize = 32 * 1024

	// logsMaxLineBytes caps the unterminated tail held while splitting lines for
	// --tail, so a workload that never writes a newline cannot grow it without bound.
	// At this size the fragment counts as a line.
	logsMaxLineBytes = 1 << 20

	// logsTailMaxBytes caps what one --tail read buffers. N comes from the client and a
	// line may reach logsMaxLineBytes, so N alone does not bound the manager's heap.
	// Past the cap the oldest lines are dropped: --tail returns fewer lines than asked
	// instead of growing without limit.
	logsTailMaxBytes = 4 << 20
)

// kubeletLogStream turns a provider stream into what `kubectl logs` expects. The provider
// seam is option-free (see provider.LogStreamer), so every option is applied here, once,
// for all providers.
//
// Honoured:
//
//   - Follow: without it the read ends at the first silent gap or the ceiling; with it, at
//     instance exit, client disconnect, or Close.
//   - Tail: the backlog is buffered into a ring of the last N lines, then --follow
//     continues from there.
//   - LimitBytes: a hard cap, applied last so it bounds the above.
//
// Ignored, because this seam cannot serve them: Timestamps (raw bytes, no per-line time),
// Previous, SinceSeconds, SinceTime (no restart history, no time index). Ignored rather
// than rejected, so a habitual --since still prints the log. See docs/status.md.
//
// Close tears down src and every goroutine below, and the VK log route always calls it,
// including when a --follow client disconnects.
func kubeletLogStream(ctx context.Context, src io.ReadCloser, opts vkapi.ContainerLogOpts) io.ReadCloser {
	return newLogStream(ctx, src, opts, logTiming{idle: logsIdleGap, ceiling: logsBacklogCeiling})
}

// logTiming is a seam for tests: the same state machine, driven in milliseconds.
type logTiming struct {
	idle    time.Duration
	ceiling time.Duration
}

func newLogStream(
	ctx context.Context, src io.ReadCloser, opts vkapi.ContainerLogOpts, t logTiming,
) io.ReadCloser {
	pr, pw := io.Pipe()
	done := make(chan struct{})
	chunks := make(chan []byte, 8)

	// A provider stream that BROKE, kept for the close below. Ending on it instead of at
	// EOF is what stops an outage from printing as an empty log — the VK route turns a
	// non-nil error into an HTTP error while no bytes have gone out yet.
	var (
		srcMu  sync.Mutex
		srcErr error
	)

	// The only goroutine touching src. It cannot block forever on a full channel:
	// teardown closes done, and src.Close unblocks the Read.
	go func() {
		defer close(chunks)
		buf := make([]byte, logsChunkSize)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				c := make([]byte, n)
				copy(c, buf[:n])
				select {
				case chunks <- c:
				case <-done:
					return
				}
			}
			if err != nil {
				// EOF is the instance's log ending. Anything else is a failure worth
				// reporting, unless we caused it by closing src in teardown.
				if !errors.Is(err, io.EOF) {
					select {
					case <-done:
					default:
						srcMu.Lock()
						srcErr = err
						srcMu.Unlock()
					}
				}
				return
			}
		}
	}()

	go func() {
		defer func() {
			srcMu.Lock()
			err := srcErr
			srcMu.Unlock()
			// CloseWithError(nil) is a plain EOF, so this covers both endings.
			_ = pw.CloseWithError(err)
		}()
		copyLogs(ctx, pw, chunks, done, opts, t)
	}()

	// Outermost, so the cap is on bytes leaving for the client rather than on the raw
	// stream. At the cap the reader sees EOF and the route closes, tearing down the
	// goroutines above.
	var out io.ReadCloser = pr
	if opts.LimitBytes > 0 {
		out = readCloser{Reader: io.LimitReader(pr, int64(opts.LimitBytes)), Closer: pr}
	}

	return &logStream{
		ReadCloser: out,
		close: func() {
			// Order matters: signal first so the reader stops queueing, then close src to
			// unblock a Read parked on the provider's long poll.
			closeOnce(done)
			_ = src.Close()
		},
	}
}

// copyLogs is the option state machine. One goroutine, so "still draining the
// backlog?" needs no lock.
func copyLogs(
	ctx context.Context, pw io.Writer, chunks <-chan []byte, done <-chan struct{},
	opts vkapi.ContainerLogOpts, t logTiming,
) {
	var ring *lineRing
	if opts.Tail > 0 {
		ring = newLineRing(opts.Tail)
	}

	// Only bound the backlog when something needs its end: --tail (emit the last N
	// lines) or a one-shot read (stop there). A plain --follow needs neither, so the
	// timers stay nil — a nil channel blocks forever, i.e. no deadline.
	bounded := ring != nil || !opts.Follow
	var idleC, ceilingC <-chan time.Time
	var idle *time.Timer
	if bounded {
		idle = time.NewTimer(t.idle)
		defer idle.Stop()
		ceiling := time.NewTimer(t.ceiling)
		defer ceiling.Stop()
		idleC, ceilingC = idle.C, ceiling.C
	}

	// flushRing hands over what --tail buffered, once.
	flushRing := func() {
		if ring != nil {
			_, _ = pw.Write(ring.bytes())
			ring = nil
		}
	}

	// endBacklog emits whatever the ring held and reports whether to keep going.
	endBacklog := func() bool {
		flushRing()
		if !opts.Follow {
			return false
		}
		// Past the backlog: write straight through, and never let the timers fire again
		// or a quiet --follow would end itself.
		idleC, ceilingC = nil, nil
		return true
	}

	// take routes one receive off chunks and reports whether to keep looping.
	take := func(c []byte, ok bool) bool {
		if !ok {
			// The instance exited, or the provider hung up. Flush the tail buffer so a
			// short-lived Pod's output is not swallowed.
			flushRing()
			return false
		}
		if ring != nil {
			ring.write(c)
		} else if _, err := pw.Write(c); err != nil {
			return false // reader gone (client disconnected)
		}
		if idle != nil {
			// Only silence counts, so restart on every byte.
			idle.Reset(t.idle)
		}
		return true
	}

	for {
		select {
		case c, ok := <-chunks:
			if !take(c, ok) {
				return
			}
		case <-idleC:
			// A chunk that landed as the timer fired must not be lost: with both cases
			// ready, select picks at random, so ending here would drop the last batch of a
			// `kubectl logs` — at random, which is how the same log printed a different
			// amount on each read. Take the queued chunk and re-measure the gap from it;
			// the backlog is only over once the queue is genuinely empty.
			select {
			case c, ok := <-chunks:
				if !take(c, ok) {
					return
				}
				continue
			default:
			}
			if !endBacklog() {
				return
			}
		case <-ceilingC:
			if !endBacklog() {
				return
			}
		case <-ctx.Done():
			return
		case <-done:
			return
		}
	}
}

// logStream is the reader the VK log route gets: the pipe plus the teardown that
// releases the provider stream. Close is idempotent — the route may close it
// alongside a cancelled request.
type logStream struct {
	io.ReadCloser
	close  func()
	closed bool
}

func (s *logStream) Close() error {
	if !s.closed {
		s.closed = true
		s.close()
	}
	return s.ReadCloser.Close()
}

// readCloser re-attaches a Close to an io.LimitReader, which drops it.
type readCloser struct {
	io.Reader
	io.Closer
}

// closeOnce guards against a double teardown panicking a request goroutine.
func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// lineRing keeps the last n lines, for --tail. A provider stream has no line framing, so
// it splits arbitrary chunks itself and holds the unterminated remainder apart — that
// remainder is the newest output, exactly what --tail is for.
type lineRing struct {
	n       int
	lines   [][]byte
	partial []byte
	size    int // bytes held in lines, for the byte cap
}

func newLineRing(n int) *lineRing { return &lineRing{n: n, lines: make([][]byte, 0, n)} }

func (r *lineRing) write(p []byte) {
	r.partial = append(r.partial, p...)
	for {
		i := bytes.IndexByte(r.partial, '\n')
		if i < 0 {
			break
		}
		// The newline is kept with its line: these bytes go to the client verbatim, and
		// stripping it would join every tailed line into one.
		r.push(r.partial[:i+1])
		r.partial = r.partial[i+1:]
	}
	if len(r.partial) >= logsMaxLineBytes {
		r.push(r.partial)
		r.partial = nil
	}
}

// push copies: the argument aliases partial, which the next write re-slices.
func (r *lineRing) push(line []byte) {
	if len(r.lines) == r.n {
		r.drop()
	}
	r.lines = append(r.lines, bytes.Clone(line))
	r.size += len(line)
	// Keep one line even if it alone is over the cap — it is still the newest output.
	for r.size > logsTailMaxBytes && len(r.lines) > 1 {
		r.drop()
	}
}

// drop evicts the oldest retained line.
func (r *lineRing) drop() {
	r.size -= len(r.lines[0])
	r.lines = r.lines[1:]
}

// bytes flattens the retained lines plus any remainder. The remainder counts as one
// of the n: --tail=2 ending mid-line means one complete line plus the fragment.
func (r *lineRing) bytes() []byte {
	lines := r.lines
	if len(r.partial) > 0 && len(lines) == r.n {
		lines = lines[1:]
	}
	var out []byte
	for _, l := range lines {
		out = append(out, l...)
	}
	return append(out, r.partial...)
}
