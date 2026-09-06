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
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestBatcher_CountsPerEventOverhead(t *testing.T) {
	// The whole point of the field: by message length these four fit in one request (28 bytes of
	// 100), and the request would be rejected at 132.
	b := NewBatcher(Limits{MaxBytes: 100, PerEventOverhead: 26}, FormatCompact)
	lines := make([]Line, 4)
	for i := range lines {
		lines[i] = Line{Data: "abc", At: t1, Cursor: "1-0"} // "1-0 abc" = 7 + 26 = 33 bytes
	}

	got := b.Add(lines...)
	if len(got) != 1 || len(got[0]) != 3 {
		t.Fatalf("got %v, want one request of 3 events", sizes(got))
	}
	if rest := b.Flush(); len(rest) != 1 {
		t.Fatalf("%d events pending, want the fourth", len(rest))
	}
}

func TestBatcher_CapsTheEventCount(t *testing.T) {
	b := NewBatcher(Limits{MaxEvents: 2}, FormatCompact)
	lines := make([]Line, 5)
	for i := range lines {
		lines[i] = Line{Data: "x", At: t1, Cursor: "1-0"}
	}

	if got := b.Add(lines...); len(got) != 2 || len(got[0]) != 2 || len(got[1]) != 2 {
		t.Fatalf("got %v, want two requests of 2", sizes(got))
	}
	if rest := b.Flush(); len(rest) != 1 {
		t.Fatalf("%d events pending, want the fifth", len(rest))
	}
}

func TestBatcher_SplitsAnOversizedLine(t *testing.T) {
	// Split rather than dropped: an over-long line is usually a stack trace or a serialized tensor
	// shape, which is exactly the thing someone is reading the durable copy for.
	b := NewBatcher(Limits{MaxEventBytes: 40}, FormatCompact)
	line := Line{Data: strings.Repeat("x", 100), At: t1, Cursor: "100-0"}

	b.Add(line)
	events := b.Flush()
	if len(events) < 3 {
		t.Fatalf("%d events, want 100 bytes of data cut into at least 3", len(events))
	}
	var joined strings.Builder
	for _, e := range events {
		if len(e.Message) > 40 {
			t.Fatalf("event of %d bytes exceeds the cap", len(e.Message))
		}
		// Every piece keeps the line's cursor and time, so the pieces stay attributable to each
		// other and a split cannot reorder a line against itself.
		if e.Cursor != "100-0" || !e.At.Equal(t1) {
			t.Fatalf("piece carries %q at %v, want the line's own", e.Cursor, e.At)
		}
		// Each piece is a whole message in its own right — the envelope is re-emitted per piece,
		// not cut in half — so reassembly strips it rather than concatenating raw messages.
		prefix := "100-0 "
		if !strings.HasPrefix(e.Message, prefix) {
			t.Fatalf("piece %q is not independently well-formed", e.Message)
		}
		joined.WriteString(strings.TrimPrefix(e.Message, prefix))
	}
	if joined.String() != line.Data {
		t.Fatalf("reassembled %q, want %q", joined.String(), line.Data)
	}
}

func TestBatcher_SplitKeepsEachPieceValidJSON(t *testing.T) {
	// The reason the split is of the data and not of the formatted message: cutting a JSON envelope
	// in half leaves two events that no Insights query can parse, which is worse than not splitting.
	// The record format nests one envelope inside another, so there are two ways to get this wrong.
	format := Record{Pod: "p", Labels: map[string]string{"app": "sandbox"}}.Formatter()
	b := NewBatcher(Limits{MaxEventBytes: 160}, format)
	line := Line{Data: strings.Repeat(`a"b\c`, 40), At: t1, Cursor: "100-0"}

	b.Add(line)
	events := b.Flush()
	if len(events) < 2 {
		t.Fatalf("%d events, want a split", len(events))
	}
	var joined strings.Builder
	for _, e := range events {
		rec := decodeRecord(t, e.Message)
		if rec.inner.ID != "100-0" {
			t.Fatalf("piece carries id %q, want the line's", rec.inner.ID)
		}
		joined.WriteString(rec.inner.Message)
	}
	if joined.String() != line.Data {
		t.Fatalf("reassembled %q, want %q", joined.String(), line.Data)
	}
}

func TestRecordFormatter_MatchesTheConsumersSchema(t *testing.T) {
	// Every field here is read by the consumer's log client: labels by the tenant filter, pod_name by
	// the worker-index lookup, and level/category from INSIDE log — a plain string there is treated
	// as private, which hides the line from anyone without the developer-view role.
	labels := map[string]string{
		"app":                       "sandbox",
		"example.com/org-id":        "org-1",
		"example.com/team-id":       "team-1",
		"example.com/experiment-id": "exp-1",
	}
	format := Record{Pod: "worker-3", Labels: labels}.Formatter()

	rec := decodeRecord(t, format(Line{Data: "training step 1", At: t1, Cursor: "100-0"}))
	if rec.Kubernetes.PodName != "worker-3" {
		t.Fatalf("pod_name = %q", rec.Kubernetes.PodName)
	}
	for k, v := range labels {
		if rec.Kubernetes.Labels[k] != v {
			t.Fatalf("labels[%q] = %q, want %q", k, rec.Kubernetes.Labels[k], v)
		}
	}
	if rec.Time != t1.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("time = %q, want %q", rec.Time, t1.UTC().Format(time.RFC3339Nano))
	}
	if rec.inner.Message != "training step 1" {
		t.Fatalf("message = %q", rec.inner.Message)
	}
	if rec.inner.Level != LevelInfo || rec.inner.Category != CategoryUser {
		t.Fatalf("level/category = %q/%q, want %q/%q", rec.inner.Level, rec.inner.Category, LevelInfo, CategoryUser)
	}
}

func TestRecordFormatter_EscapesWhatWouldBreakAQuery(t *testing.T) {
	// Twice over: the text is escaped into log, and log is escaped into the record. A quote in the
	// output reaches CloudWatch as `\\\"`, and getting either layer wrong loses the whole line.
	want := "say \"hi\"\tC:\\path\x1b[0m"
	format := Record{Pod: "p"}.Formatter()

	if got := decodeRecord(t, format(Line{Cursor: "100-0", Data: want})).inner.Message; got != want {
		t.Fatalf("round-tripped to %q, want %q", got, want)
	}
}

func TestRecordFormatter_IsByteStableAcrossLabelOrder(t *testing.T) {
	// Map iteration order is random, and an unstable encoding would make every size calculation in
	// fit() — and every diff of a shipped stream — depend on it.
	labels := map[string]string{"app": "a", "b": "2", "c": "3", "d": "4", "e": "5"}
	line := Line{Data: "x", At: t1, Cursor: "1-0"}

	want := Record{Pod: "p", Labels: labels}.Formatter()(line)
	for range 20 {
		if got := (Record{Pod: "p", Labels: labels}).Formatter()(line); got != want {
			t.Fatalf("encoding varies with map order:\n%q\n%q", got, want)
		}
	}
}

// decodeRecord parses the two nested envelopes a Record produces, failing the test if either layer
// is malformed.
func decodeRecord(t *testing.T, msg string) recordShape {
	t.Helper()
	var rec recordShape
	if err := json.Unmarshal([]byte(msg), &rec); err != nil {
		t.Fatalf("record %q is not valid JSON: %v", msg, err)
	}
	if err := json.Unmarshal([]byte(rec.Log), &rec.inner); err != nil {
		t.Fatalf("log field %q is not valid JSON: %v", rec.Log, err)
	}
	return rec
}

// recordShape mirrors the consumer's log record, plus the inner envelope its buildLine looks for.
type recordShape struct {
	Time       string `json:"time"`
	Log        string `json:"log"`
	Kubernetes struct {
		Labels  map[string]string `json:"labels"`
		PodName string            `json:"pod_name"`
	} `json:"kubernetes"`

	inner struct {
		Level    string `json:"level"`
		Category string `json:"category"`
		Message  string `json:"message"`
		ID       string `json:"id"`
	}
}

func TestBatcher_SplitFallsOnRuneBoundaries(t *testing.T) {
	// A cut inside a multi-byte rune reaches the sink as U+FFFD, which is a corrupted durable copy
	// rather than a split one. The cap is swept because whether a naive cut lands mid-rune depends
	// on the prefix length as much as on the data.
	line := Line{Data: strings.Repeat("日本語", 8), At: t1, Cursor: "1-0"}
	want := line.Data

	for limit := 8; limit <= 20; limit++ {
		b := NewBatcher(Limits{MaxEventBytes: limit}, FormatCompact)
		b.Add(line)
		events := b.Flush()
		if len(events) < 2 {
			t.Fatalf("limit %d: %d events, want a split", limit, len(events))
		}
		var joined strings.Builder
		for _, e := range events {
			if !utf8.ValidString(e.Message) {
				t.Fatalf("limit %d: piece %q is not valid UTF-8", limit, e.Message)
			}
			joined.WriteString(strings.TrimPrefix(e.Message, "1-0 "))
		}
		if joined.String() != want {
			t.Fatalf("limit %d: reassembled %q, want %q", limit, joined.String(), want)
		}
	}
}

func TestBatcher_ClampsATimestampThatGoesBackwards(t *testing.T) {
	// One out-of-order event rejects the whole request, so this cannot be left to the sink. Clamping
	// misdates a line by milliseconds; sorting would reorder the log itself.
	b := NewBatcher(Limits{}, FormatCompact)
	b.Add(
		Line{Data: "later", At: t2, Cursor: "200-0"},
		Line{Data: "earlier", At: t1, Cursor: "100-0"},
	)

	events := b.Flush()
	if len(events) != 2 {
		t.Fatalf("%d events, want 2", len(events))
	}
	if !events[1].At.Equal(t2) {
		t.Fatalf("At = %v, want it raised to %v", events[1].At, t2)
	}
	if b.Clamped() != 1 {
		t.Fatalf("Clamped() = %d, want 1", b.Clamped())
	}
}

func TestBatcher_FormatKeepsABlankLineNonEmpty(t *testing.T) {
	// An empty message is rejected outright by PutLogEvents, and a blank line is real output. The
	// prefix is what stops those two facts from colliding.
	if got := FormatCompact(Line{Data: "", Cursor: "100-0"}); got == "" {
		t.Fatal("a blank line formatted to an empty message")
	}
}

func TestBatcher_ZeroLimitsHoldEverything(t *testing.T) {
	b := NewBatcher(Limits{}, FormatCompact)
	lines := make([]Line, 1000)
	for i := range lines {
		lines[i] = Line{Data: strings.Repeat("x", 1000), At: t1, Cursor: "1-0"}
	}

	if got := b.Add(lines...); got != nil {
		t.Fatalf("got %v, want no request when no limit applies", sizes(got))
	}
	if rest := b.Flush(); len(rest) != 1000 {
		t.Fatalf("%d events, want all of them", len(rest))
	}
	if again := b.Flush(); again != nil {
		t.Fatalf("second Flush gave %d events, want nothing", len(again))
	}
}

func sizes(batches [][]Event) []int {
	out := make([]int, 0, len(batches))
	for _, b := range batches {
		out = append(out, len(b))
	}
	return out
}
