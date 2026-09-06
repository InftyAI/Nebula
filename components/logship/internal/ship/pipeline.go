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
	"sync"
	"time"
)

// Defaults sized for the fleet rather than for one stream: what matters is the per-stream bound
// times 1,000 streams, so these are 64 MiB of buffers and 200 requests per second, not 1 GiB and
// 1,000. See design.md's Scale section.
const (
	DefaultInterval        = 5 * time.Second
	DefaultMaxPendingBytes = 64 << 10

	// Only throttling is retried, and not for long: a stream that retries forever is a stream whose
	// logs age out of Modal while it waits.
	maxPutAttempts = 5
	baseBackoff    = 200 * time.Millisecond
)

// Config describes one stream's copy. Source, Sink and Format are required.
type Config struct {
	Source Source
	Sink   Sink
	Format Formatter
	Limits Limits

	// Cursor is where to resume. Empty means the source's beginning, which is a full replay of
	// whatever it still holds.
	Cursor string
	// CollapseFrames discards a progress bar's superseded frames. See Assembler.
	CollapseFrames bool

	// Interval flushes a stream too quiet to fill a batch. Zero means DefaultInterval.
	Interval time.Duration
	// MaxPendingBytes bounds lines read but not yet shipped. Zero means DefaultMaxPendingBytes;
	// negative means unbounded, which trades the memory bound for never dropping.
	MaxPendingBytes int

	// Throttled tells "slow down" apart from "this batch is lost". Nil retries nothing. It is a
	// function rather than an error check here because the answer belongs to the sink's API, and
	// this package must not import an adapter.
	Throttled func(error) bool
	// Shipped is called with the cursor of the last event of every accepted batch, in order. It is
	// the record half of read -> put -> record: without it a restart replays from the beginning,
	// which the consumer shows as duplicated lines.
	Shipped func(cursor string)
}

// Stats is one stream's counters. Dropped and Failed are the two ways a line does not arrive, and
// they are separate because one is our backpressure and the other is the sink refusing.
type Stats struct {
	Lines  int
	Events int
	// Dropped counts lines the reader threw away because the buffer was full.
	Dropped      int
	DroppedBytes int
	// Failed counts events the sink would not take, after retries.
	Failed int
	// Clamped counts lines whose timestamp was raised to keep a batch ordered. See Batcher.
	Clamped int
}

// Pipeline copies one stream: source -> assembler -> batcher -> sink.
//
// Two goroutines, and it has to be two. The source calls back on its own goroutine and a stalled
// callback stalls the cursor, so shipping cannot happen inline; and a stream quiet enough to need
// the interval flush needs a select the synchronous callback cannot provide. They are joined by a
// byte-bounded queue that drops rather than blocks — a reader waiting on CloudWatch is a reader not
// draining Modal, and Modal's retention is the clock this whole component exists to beat.
type Pipeline struct {
	cfg   Config
	queue *queue

	mu    sync.Mutex
	stats Stats
}

func New(cfg Config) *Pipeline {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.MaxPendingBytes == 0 {
		cfg.MaxPendingBytes = DefaultMaxPendingBytes
	}
	return &Pipeline{cfg: cfg, queue: newQueue(cfg.MaxPendingBytes)}
}

// Run copies until the stream ends, ctx is cancelled, or the source fails, and returns the source's
// error. It always drains what it has already read first, including the unterminated fragment a
// sandbox that exits mid-line leaves behind — on cancellation those puts will fail with ctx, so a
// caller that wants the tail shipped gives the drain its own deadline rather than the cancelled one.
func (p *Pipeline) Run(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.deliver(ctx)
	}()

	asm := NewAssembler(p.cfg.CollapseFrames)
	err := p.cfg.Source.Follow(ctx, p.cfg.Cursor, func(b Batch) error {
		p.push(asm.Add(b))
		return nil
	})
	p.push(asm.Flush())
	p.queue.close()
	<-done
	return err
}

// Stats reports this stream's counters. Safe to call while Run is in flight.
func (p *Pipeline) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.stats
	out.Dropped, out.DroppedBytes = p.queue.drops()
	return out
}

func (p *Pipeline) push(lines []Line) {
	if len(lines) == 0 {
		return
	}
	p.record(func(s *Stats) { s.Lines += len(lines) })
	p.queue.push(lines)
}

func (p *Pipeline) deliver(ctx context.Context) {
	b := NewBatcher(p.cfg.Limits, p.cfg.Format)
	tick := time.NewTicker(p.cfg.Interval)
	defer tick.Stop()

	for {
		select {
		case _, open := <-p.queue.wake:
			// Taken before the open check, because close happens after the last push: a closed
			// queue still has the final lines in it.
			for _, full := range b.Add(p.queue.take()...) {
				p.put(ctx, full)
			}
			if !open {
				p.put(ctx, b.Flush())
				p.record(func(s *Stats) { s.Clamped = b.Clamped() })
				return
			}
		case <-tick.C:
			p.put(ctx, b.Flush())
			p.record(func(s *Stats) { s.Clamped = b.Clamped() })
		}
	}
}

// put ships one batch, retrying only what the sink says is a rate problem. Anything else is counted
// and dropped: the alternative is a stream that stops advancing, and a cursor that stops advancing
// loses logs permanently rather than partially.
func (p *Pipeline) put(ctx context.Context, events []Event) {
	if len(events) == 0 {
		return
	}
	for attempt := range maxPutAttempts {
		err := p.cfg.Sink.Put(ctx, events)
		if err == nil {
			p.record(func(s *Stats) { s.Events += len(events) })
			if p.cfg.Shipped != nil {
				p.cfg.Shipped(events[len(events)-1].Cursor)
			}
			return
		}
		if p.cfg.Throttled == nil || !p.cfg.Throttled(err) {
			break
		}
		if !sleep(ctx, baseBackoff<<attempt) {
			break
		}
	}
	p.record(func(s *Stats) { s.Failed += len(events) })
}

func (p *Pipeline) record(f func(*Stats)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(&p.stats)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// queue holds lines between the reader and the shipper.
//
// Bounded in bytes rather than in lines, because a line's size is unbounded and 1,000 streams each
// holding "a few thousand lines" is not a number anyone can size a Deployment from. wake is a
// one-slot signal rather than the queue itself, so a full buffer never makes the reader wait for a
// receiver.
type queue struct {
	mu    sync.Mutex
	lines []Line
	bytes int
	max   int

	dropped      int
	droppedBytes int

	wake chan struct{}
}

func newQueue(max int) *queue {
	return &queue{max: max, wake: make(chan struct{}, 1)}
}

// push never blocks. Over the bound it drops the newest line and keeps what is already queued:
// the older lines are the ones a reader has been waiting for, and dropping them to make room would
// turn a slow sink into a reordered log.
func (q *queue) push(lines []Line) {
	q.mu.Lock()
	for _, l := range lines {
		if q.max > 0 && q.bytes+len(l.Data) > q.max && len(q.lines) > 0 {
			q.dropped++
			q.droppedBytes += len(l.Data)
			continue
		}
		q.lines = append(q.lines, l)
		q.bytes += len(l.Data)
	}
	q.mu.Unlock()

	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *queue) take() []Line {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.lines
	q.lines, q.bytes = nil, 0
	return out
}

func (q *queue) drops() (int, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped, q.droppedBytes
}

func (q *queue) close() { close(q.wake) }
