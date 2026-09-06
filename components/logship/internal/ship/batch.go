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
	"strings"
	"time"
	"unicode/utf8"
)

// Event is one message as a Sink accepts it.
type Event struct {
	Message string
	At      time.Time
	// Cursor is the source cursor of the line this came from, repeated across every piece of a
	// split line, so the pieces stay attributable to each other.
	Cursor string
}

// Limits are one sink's per-request caps. A zero field means that limit does not apply.
//
// PerEventOverhead is the part that gets forgotten: CloudWatch charges 26 bytes per event against
// MaxBytes on top of the message, so a sum of message lengths passes every local test and then
// fails under a flood of short lines. It is counted against MaxEventBytes too — AWS documents the
// 26 bytes plainly for the batch and vaguely for the event, and over-reserving costs one short
// split at the boundary while under-reserving costs a rejected request.
type Limits struct {
	MaxEvents        int
	MaxBytes         int
	MaxEventBytes    int
	PerEventOverhead int
}

// Formatter renders a line as the message a sink stores. The whole wire format lives in one of
// these, because it is the one decision that is permanent per stream: already-shipped events cannot
// be reformatted, so changing it puts a discontinuity in the middle of a log group.
//
// The real one is Record.Formatter, which matches the consumer's schema. Every formatter carries the
// cursor, which is not decoration: it is what lets a reader collapse the duplicates a replay
// produces, and it is also why a blank line never becomes an empty message — which every log API
// rejects.
type Formatter func(Line) string

// FormatCompact renders `<cursor> <data>`. For tests and for reading a stream raw under `aws logs
// tail`; it carries no identity, so nothing that queries by tenant can use it.
func FormatCompact(l Line) string {
	return l.Cursor + " " + l.Data
}

// encodeJSONString writes s as a JSON string. encoding/json would allocate a map and an
// intermediate []byte per line; at 1,000 streams this runs on every line of the fleet.
func encodeJSONString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for i := range len(s) {
		switch c := s[i]; {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c == '\t':
			b.WriteString(`\t`)
		case c < 0x20:
			// Control bytes are legal in a log line and illegal raw in JSON. \u00XX rather than
			// dropping them: a terminal escape sequence in the output is sometimes the evidence.
			b.WriteString(`\u00`)
			const hex = "0123456789abcdef"
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xf])
		default:
			// Written bytewise, so invalid UTF-8 passes through as it arrived rather than becoming
			// U+FFFD. A split line is reassembled by concatenation, and that has to survive it.
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
}

// Batcher groups lines into requests a Sink will accept.
//
// It has no clock: a batch is returned when a limit would be exceeded, and the caller flushes on
// whatever interval it wants. Owning a timer here would mean owning a goroutine, and the shipping
// loop already has one.
//
// Not safe for concurrent use — one batcher per stream, like Assembler.
type Batcher struct {
	limits Limits
	format Formatter

	pending []Event
	bytes   int
	// last is the highest timestamp emitted so far. PutLogEvents rejects an entire request if one
	// event is out of order, so a batch has to be non-decreasing.
	last    time.Time
	clamped int
}

func NewBatcher(limits Limits, format Formatter) *Batcher {
	if format == nil {
		format = FormatCompact
	}
	return &Batcher{limits: limits, format: format}
}

// Add returns the requests these lines filled, in order. Anything short of a limit is held for a
// later Add or for Flush — including a batch sitting exactly on a limit, since "full" is discovered
// by the event that does not fit.
func (b *Batcher) Add(lines ...Line) [][]Event {
	var out [][]Event
	for _, l := range lines {
		for _, e := range b.events(l) {
			if full := b.push(e); full != nil {
				out = append(out, full)
			}
		}
	}
	return out
}

// Flush returns the pending request, or nil if there is nothing pending.
func (b *Batcher) Flush() []Event {
	if len(b.pending) == 0 {
		return nil
	}
	out := b.pending
	b.pending, b.bytes = nil, 0
	return out
}

// Clamped counts lines whose timestamp went backwards and was raised to its predecessor's. Nonzero
// means the durable copy is dated slightly wrong; it is a metric rather than an error, because the
// alternative is a rejected request or a reordered log.
func (b *Batcher) Clamped() int { return b.clamped }

func (b *Batcher) events(l Line) []Event {
	at := l.At
	if at.Before(b.last) {
		at = b.last
		b.clamped++
	}
	b.last = at

	msgs := b.messages(l)
	out := make([]Event, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, Event{Message: m, At: at, Cursor: l.Cursor})
	}
	return out
}

// messages formats a line, splitting it if the result exceeds the per-event cap.
//
// The split is of the line's DATA and each piece is formatted separately, so every event is
// independently well-formed — splitting the formatted message instead would cut a JSON envelope in
// half and leave two events that no query can parse. Pieces share the line's id, which is what marks
// them as one line: concatenating the `msg` of same-id events reassembles it.
func (b *Batcher) messages(l Line) []string {
	limit := b.limits.MaxEventBytes - b.limits.PerEventOverhead
	msg := b.format(l)
	if limit <= 0 || len(msg) <= limit {
		return []string{msg}
	}

	var out []string
	for data := l.Data; ; {
		piece := b.fit(l, data, limit)
		out = append(out, b.format(withData(l, piece)))
		data = data[len(piece):]
		if data == "" {
			return out
		}
	}
}

// fit returns the longest prefix of data whose formatted message fits in limit.
//
// It measures by formatting rather than by arithmetic on the envelope, because a Formatter may
// expand what it is given — escaping does — and only the formatter knows by how much. Costly, and
// deliberately only on this path: an ordinary line never reaches it.
func (b *Batcher) fit(l Line, data string, limit int) string {
	n := min(len(data), limit)
	for {
		n = runeBoundary(data, n)
		if n == 0 {
			// The envelope alone exceeds the cap, which needs a cursor of absurd length. Emit one
			// rune rather than spinning; the sink's error will name the real problem.
			return data[:runeLen(data)]
		}
		over := len(b.format(withData(l, data[:n]))) - limit
		if over <= 0 {
			return data[:n]
		}
		n -= max(over, 1)
		if n < 0 {
			n = 0
		}
	}
}

// runeBoundary rounds n down to a rune boundary. A cut inside a multi-byte rune reaches the sink as
// U+FFFD, which corrupts the durable copy rather than merely splitting it.
func runeBoundary(s string, n int) int {
	for n > 0 && n < len(s) && !utf8.RuneStart(s[n]) {
		n--
	}
	return n
}

func runeLen(s string) int {
	_, n := utf8.DecodeRuneInString(s)
	return n
}

func withData(l Line, data string) Line {
	l.Data = data
	return l
}

func (b *Batcher) push(e Event) []Event {
	size := len(e.Message) + b.limits.PerEventOverhead

	var full []Event
	if len(b.pending) > 0 && b.exceeds(len(b.pending)+1, b.bytes+size) {
		full = b.Flush()
	}
	b.pending = append(b.pending, e)
	b.bytes += size
	return full
}

func (b *Batcher) exceeds(events, bytes int) bool {
	return (b.limits.MaxEvents > 0 && events > b.limits.MaxEvents) ||
		(b.limits.MaxBytes > 0 && bytes > b.limits.MaxBytes)
}
