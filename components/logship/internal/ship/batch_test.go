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

func TestBatcher_DropsAnOversizedLine(t *testing.T) {
	b := NewBatcher(Limits{MaxEventBytes: 100}, FormatCompact)
	line := Line{Data: strings.Repeat("x", 500), At: t1, Cursor: "100-0"}

	b.Add(line)
	events := b.Flush()
	if len(events) != 1 {
		t.Fatalf("%d events, want one notice", len(events))
	}
	e := events[0]
	if len(e.Message) > 100 {
		t.Fatalf("the notice itself is %d bytes, over the cap", len(e.Message))
	}
	// The notice stands in for the line, so it has to be findable where the line was.
	if e.Cursor != "100-0" || !e.At.Equal(t1) {
		t.Fatalf("notice carries %q at %v, want the line's own", e.Cursor, e.At)
	}
	if !strings.Contains(e.Message, "dropped") || !strings.Contains(e.Message, "500") {
		t.Fatalf("notice %q does not say what was dropped", e.Message)
	}
	if b.Oversized() != 1 {
		t.Fatalf("Oversized() = %d, want 1", b.Oversized())
	}
}

func TestBatcher_DropDoesNotRelabelAnEnvelope(t *testing.T) {
	// The whole reason an oversized line is dropped rather than split. A cut envelope has no decodable
	// prefix, so adopt would reject every piece and Record.Formatter would wrap each with its own
	// fallbacks — INFO/user. The consumer shows category user to everyone and private only to the
	// developer-view role, so a split would publish the text of a private line.
	format := Record{Pod: "p", Labels: map[string]string{"app": "sandbox"}}.Formatter()
	b := NewBatcher(Limits{MaxEventBytes: 400}, format)
	const secret = "PRIVATE-TRACEBACK-"
	data := `{"level":"ERROR","category":"private","message":"` + strings.Repeat(secret, 100) + `"}`

	b.Add(Line{Data: data, At: t1, Cursor: "100-0"})
	events := b.Flush()
	if len(events) != 1 {
		t.Fatalf("%d events, want one notice", len(events))
	}
	if strings.Contains(events[0].Message, secret) {
		t.Fatalf("the dropped line's own text was shipped: %q", events[0].Message)
	}
	if len(events[0].Message) > 400 {
		t.Fatalf("the notice itself is %d bytes, over the cap", len(events[0].Message))
	}
	rec := decodeRecord(t, events[0].Message)
	if rec.inner.ID != "100-0" {
		t.Fatalf("notice carries id %q, want the line's", rec.inner.ID)
	}
}

func TestBatcher_ShipsNothingWhenEvenTheNoticeCannotFit(t *testing.T) {
	// A cap below the record envelope is a misconfiguration, and the invariant still holds: whatever
	// else happens, no event over the cap is emitted. The counter is the only trace left.
	b := NewBatcher(Limits{MaxEventBytes: 20}, FormatCompact)

	b.Add(Line{Data: strings.Repeat("x", 100), At: t1, Cursor: "100-0"})
	if events := b.Flush(); events != nil {
		t.Fatalf("shipped %v, want nothing", events)
	}
	if b.Oversized() != 1 {
		t.Fatalf("Oversized() = %d, want 1", b.Oversized())
	}
}

func TestBatcher_TreatsOnlyAnUnsetCapAsUnlimited(t *testing.T) {
	// A cap at or below PerEventOverhead leaves no room for a payload, which is a tighter version of the
	// case above and must not come out the other side as "no cap at all". Reading it that way made the
	// guard fail open on exactly the misconfiguration it exists to catch — and inconsistently, since one
	// byte looser already shipped nothing.
	line := Line{Data: strings.Repeat("x", 100), At: t1, Cursor: "100-0"}

	for _, mb := range []int{1, 26, 27} {
		b := NewBatcher(Limits{MaxEventBytes: mb, PerEventOverhead: 26}, FormatCompact)
		b.Add(line)
		if events := b.Flush(); events != nil {
			t.Fatalf("MaxEventBytes=%d shipped %v, want nothing", mb, events)
		}
		if b.Oversized() != 1 {
			t.Fatalf("MaxEventBytes=%d: Oversized() = %d, want 1", mb, b.Oversized())
		}
	}

	// Unset is still the documented way to disable it — see Limits.
	b := NewBatcher(Limits{PerEventOverhead: 26}, FormatCompact)
	b.Add(line)
	if events := b.Flush(); len(events) != 1 || events[0].Message != "100-0 "+line.Data {
		t.Fatalf("got %v, want the line unchanged", events)
	}
}

func TestBatcher_KeepsALineThatFits(t *testing.T) {
	b := NewBatcher(Limits{MaxEventBytes: 40}, FormatCompact)
	line := Line{Data: strings.Repeat("x", 10), At: t1, Cursor: "1-0"}

	b.Add(line)
	events := b.Flush()
	if len(events) != 1 || events[0].Message != "1-0 "+line.Data {
		t.Fatalf("got %v, want the line unchanged", events)
	}
	if b.Oversized() != 0 {
		t.Fatalf("Oversized() = %d, want 0", b.Oversized())
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

func TestRecordFormatter_KeepsTheWorkloadsOwnLevelAndCategory(t *testing.T) {
	// The consumer unwraps log exactly once, so an envelope nested inside ours is never read: every
	// sandbox line arrived as INFO/user with the real envelope stranded in message as text. A workload
	// that logs `system` is then absent from a system query while its user query doubles.
	format := Record{Pod: "p"}.Formatter()
	data := `{"level":"WARN","category":"system","message":"loading checkpoint"}`

	rec := decodeRecord(t, format(Line{Data: data, At: t1, Cursor: "100-0"}))
	if rec.inner.Level != "WARN" || rec.inner.Category != "system" {
		t.Fatalf("level/category = %q/%q, want WARN/system", rec.inner.Level, rec.inner.Category)
	}
	if rec.inner.Message != "loading checkpoint" {
		t.Fatalf("message = %q, want the text, not the envelope", rec.inner.Message)
	}
	if rec.inner.ID != "100-0" {
		t.Fatalf("id = %q, want the cursor", rec.inner.ID)
	}
}

func TestRecordFormatter_SuppliesOnlyTheCategoryAnEnvelopeLacks(t *testing.T) {
	// Category is the visibility gate — an envelope without one is read as private and hidden from
	// every caller lacking the developer-view role. Level needs no such fallback: the consumer
	// defaults it to INFO itself, so adding one would only overwrite what the workload meant.
	format := Record{Pod: "p"}.Formatter()

	rec := decodeRecord(t, format(Line{Data: `{"level":"DEBUG","message":"x"}`, At: t1, Cursor: "1-0"}))
	if rec.inner.Category != CategoryUser {
		t.Fatalf("category = %q, want %q", rec.inner.Category, CategoryUser)
	}
	if rec.inner.Level != "DEBUG" {
		t.Fatalf("level = %q, want the workload's", rec.inner.Level)
	}
}

func TestRecordFormatter_WrapsWhatTheConsumerCouldNotRead(t *testing.T) {
	// Adopting an object the consumer cannot decode into {level, category, message} strings would be
	// worse than wrapping it: its decode fails, the category falls back to private, and the line
	// disappears. So the bar is not "valid JSON" but "an envelope that survives that decode".
	for _, data := range []string{
		"training step 1",  // the ordinary case: not JSON at all
		`  {"message":"x"`, // truncated, e.g. a line the sandbox was killed mid-write
		`{"message":42}`,   // decodes to nothing the consumer can render
		`{"level":{},"message":"x"}`,
		`{"message":null}`,
		`{"level":"WARN"}`, // an object, but no text to show
		`[{"message":"x"}]`,
		`{"message":"x"} trailing`,
	} {
		rec := decodeRecord(t, Record{Pod: "p"}.Formatter()(Line{Data: data, At: t1, Cursor: "1-0"}))
		if rec.inner.Message != data {
			t.Fatalf("%q: message = %q, want the line verbatim", data, rec.inner.Message)
		}
		if rec.inner.Category != CategoryUser || rec.inner.Level != LevelInfo {
			t.Fatalf("%q: level/category = %q/%q, want the fallbacks", data, rec.inner.Level, rec.inner.Category)
		}
	}
}

func TestRecordFormatter_CursorOutranksAWorkloadsOwnID(t *testing.T) {
	// Duplicate keys are legal and every decoder keeps the last, which is why the cursor is appended
	// rather than prepended: a workload logging its own id would otherwise shadow the only thing that
	// can resume a stream.
	format := Record{Pod: "p"}.Formatter()

	rec := decodeRecord(t, format(Line{Data: `{"id":"theirs","message":"x"}`, At: t1, Cursor: "100-0"}))
	if rec.inner.ID != "100-0" {
		t.Fatalf("id = %q, want the cursor", rec.inner.ID)
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
