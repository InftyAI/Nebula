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
	"testing"
	"time"
	"unicode/utf8"
)

var (
	t1 = time.Unix(1788477379, 229_000_000)
	t2 = time.Unix(1788477379, 282_000_000)
)

func TestAssembler_SplitsChunksIntoLines(t *testing.T) {
	a := NewAssembler(false)
	got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: "one\ntwo\nthree", At: t1}}})

	if want := []string{"one", "two"}; !equalData(got, want) {
		t.Fatalf("got %v, want %v — the unterminated tail must be held", data(got), want)
	}
	// Held, not emitted: "three" may still be half a line.
	if lines := a.Flush(); len(lines) != 1 || lines[0].Data != "three" {
		t.Fatalf("Flush gave %v, want the pending fragment", data(lines))
	}
}

func TestAssembler_LineSpanningBatchesTakesTheCompletingCursor(t *testing.T) {
	// The reason this matters: resuming from the cursor a line STARTED in re-delivers the rest of
	// that batch. Resuming from where it completed re-delivers nothing it needed.
	a := NewAssembler(false)
	if lines := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: "half", At: t1}}}); len(lines) != 0 {
		t.Fatalf("got %v, want nothing until the line completes", data(lines))
	}
	got := a.Add(Batch{Cursor: "200-0", Entries: []Entry{{Data: " and half\n", At: t2}}})

	if len(got) != 1 || got[0].Data != "half and half" {
		t.Fatalf("got %v, want the joined line", data(got))
	}
	if got[0].Cursor != "200-0" {
		t.Fatalf("cursor %q, want the batch that completed the line", got[0].Cursor)
	}
	// Dated from its first byte, so a line is stamped when it was printed rather than when the
	// rest of it happened to arrive.
	if !got[0].At.Equal(t1) {
		t.Fatalf("At = %v, want the chunk that started the line (%v)", got[0].At, t1)
	}
}

func TestAssembler_KeepsBlankLines(t *testing.T) {
	// A blank line is real output, and it still has to carry a timestamp: the sink stamps events
	// from this, and a zero time would land it in 1970.
	a := NewAssembler(false)
	got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: "\n", At: t1}}})

	if len(got) != 1 || got[0].Data != "" {
		t.Fatalf("got %v, want one empty line", data(got))
	}
	if !got[0].At.Equal(t1) {
		t.Fatalf("At = %v, want %v", got[0].At, t1)
	}
}

func TestAssembler_StripsTheCarriageReturnOfACRLF(t *testing.T) {
	for _, collapse := range []bool{false, true} {
		a := NewAssembler(collapse)
		got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: "windows\r\n", At: t1}}})

		// In collapse mode this is the case that would otherwise vanish entirely: the CR looks
		// exactly like a progress frame separator unless it is taken off first.
		if len(got) != 1 || got[0].Data != "windows" {
			t.Fatalf("collapseFrames=%v: got %v, want the line without its CR", collapse, data(got))
		}
	}
}

func TestAssembler_StripsACRLFSplitBetweenChunks(t *testing.T) {
	// The two bytes need not arrive together. Resolved without its LF in view, the CR is either left in
	// the line or — once frames collapse — read as a frame separator, which discards the line's text and
	// ships an empty line in its place.
	for _, collapse := range []bool{false, true} {
		a := NewAssembler(collapse)
		got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{
			{Data: "windows\r", At: t1},
			{Data: "\n", At: t2},
		}})

		if !equalData(got, []string{"windows"}) {
			t.Fatalf("collapseFrames=%v: got %v, want what an unsplit CRLF gives", collapse, data(got))
		}
	}
}

func TestAssembler_HoldsATrailingCRAcrossBatches(t *testing.T) {
	// The likelier of the two boundaries, since a batch ends wherever the source's page does — and the
	// held CR has to survive between Add calls, not merely between the entries of one.
	for _, collapse := range []bool{false, true} {
		a := NewAssembler(collapse)
		if got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: "windows\r", At: t1}}}); len(got) != 0 {
			t.Fatalf("collapseFrames=%v: %v completed on the CR alone", collapse, data(got))
		}

		got := a.Add(Batch{Cursor: "200-0", Entries: []Entry{{Data: "\nnext\n", At: t2}}})
		if !equalData(got, []string{"windows", "next"}) {
			t.Fatalf("collapseFrames=%v: got %v, want both lines", collapse, data(got))
		}
		// Completed in the second batch, so that is the cursor a resume must not pass — see Line.
		if got[0].Cursor != "200-0" {
			t.Fatalf("collapseFrames=%v: cursor = %q, want the completing batch's", collapse, got[0].Cursor)
		}
	}
}

func TestAssembler_ATrailingCRIsStillAFrameSeparator(t *testing.T) {
	// The other half of holding a CR back: one that turns out NOT to precede an LF has to behave exactly
	// as it would have arriving in one piece. Otherwise fixing CRLF would break every progress bar whose
	// chunk happens to end on the separator, which is where tqdm's writes land.
	entries := []Entry{
		{Data: " 10%|## \r", At: t1},
		{Data: " 50%|##### \r", At: t1},
		{Data: "100%|##########\n", At: t2},
	}

	a := NewAssembler(true)
	if got := a.Add(Batch{Cursor: "100-0", Entries: entries}); !equalData(got, []string{"100%|##########"}) {
		t.Fatalf("collapsed: got %v, want only the final frame", data(got))
	}
	b := NewAssembler(false)
	want := " 10%|## \r 50%|##### \r100%|##########"
	if got := b.Add(Batch{Cursor: "100-0", Entries: entries}); !equalData(got, []string{want}) {
		t.Fatalf("verbatim: got %q, want every frame", data(got))
	}
}

func TestAssembler_FlushDropsAHeldCR(t *testing.T) {
	// Nothing follows to overwrite what the CR separated, so collapsing on it here would erase text that
	// was still on the terminal when the sandbox exited.
	for _, collapse := range []bool{false, true} {
		a := NewAssembler(collapse)
		a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: "half a line\r", At: t1}}})

		if got := a.Flush(); !equalData(got, []string{"half a line"}) {
			t.Fatalf("collapseFrames=%v: got %v, want the text without its CR", collapse, data(got))
		}
	}
}

func TestAssembler_ProgressFrames(t *testing.T) {
	// What tqdm actually emits: a frame per update, carriage-returned, with no newline until the
	// bar is done.
	frames := Batch{Cursor: "100-0", Entries: []Entry{
		{Data: "\r 10%|## ", At: t1},
		{Data: "\r 50%|##### ", At: t1},
		{Data: "\r100%|##########\n", At: t2},
	}}

	t.Run("collapsed keeps only the last", func(t *testing.T) {
		a := NewAssembler(true)
		got := a.Add(frames)
		if len(got) != 1 || got[0].Data != "100%|##########" {
			t.Fatalf("got %v, want only the final frame", data(got))
		}
		// The frame that survived is the one that set the timestamp, not the first update.
		if !got[0].At.Equal(t2) {
			t.Fatalf("At = %v, want the surviving frame's time", got[0].At)
		}
	})

	t.Run("kept verbatim otherwise", func(t *testing.T) {
		a := NewAssembler(false)
		got := a.Add(frames)
		if len(got) != 1 || got[0].Data != "\r 10%|## \r 50%|##### \r100%|##########" {
			t.Fatalf("got %q, want every frame", data(got))
		}
	})
}

func TestAssembler_CapsAnUnterminatedLine(t *testing.T) {
	// A source that never sends a newline must not be able to grow the pending line without
	// bound; a progress bar in the un-collapsed mode is the ordinary way that happens.
	a := NewAssembler(false)
	chunk := strings.Repeat("x", maxFragment/2)

	if lines := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: chunk, At: t1}}}); len(lines) != 0 {
		t.Fatalf("%d lines below the cap, want none", len(lines))
	}
	// Held at exactly the cap rather than emitted, because a fragment that fills it is not over it —
	// see fill. One more byte takes it, so the bound this test exists for is unaffected.
	if got := a.Add(Batch{Cursor: "200-0", Entries: []Entry{{Data: chunk, At: t2}}}); len(got) != 0 {
		t.Fatalf("%d lines at exactly the cap, want it held", len(got))
	}
	if lines := a.Flush(); len(lines) != 1 || len(lines[0].Data) != maxFragment {
		t.Fatalf("Flush gave %v, want one line of %d bytes", data(lines), maxFragment)
	}
}

func TestAssembler_DoesNotFollowAnExactCapWithABlankLine(t *testing.T) {
	// Emitting at exactly the cap left the buffer empty, and the newline path takes unconditionally —
	// so a line whose length was a multiple of the cap shipped a spurious blank record after it. Not the
	// 1-in-65536 accident it looks like: maxFragment is 64 KiB, so every power-of-two-sized dump lands
	// on the boundary exactly.
	exact := strings.Repeat("x", maxFragment)

	for _, collapse := range []bool{false, true} {
		a := NewAssembler(collapse)
		got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: exact + "\n", At: t1}}})
		if !equalData(got, []string{exact}) {
			t.Fatalf("collapseFrames=%v: %d lines of %v bytes, want one of %d",
				collapse, len(got), lengths(got), maxFragment)
		}

		// The likelier arrival, since a batch ends wherever the source's page does.
		b := NewAssembler(collapse)
		b.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: exact, At: t1}}})
		if got := b.Add(Batch{Cursor: "200-0", Entries: []Entry{{Data: "\n", At: t2}}}); !equalData(got, []string{exact}) {
			t.Fatalf("collapseFrames=%v: a newline in a later batch gave %v bytes, want one line of %d",
				collapse, lengths(got), maxFragment)
		}
	}

	// And no blank between the pieces of a longer multiple, nor after the last one.
	a := NewAssembler(false)
	got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: strings.Repeat("x", 3*maxFragment) + "\n", At: t1}}})
	if len(got) != 3 {
		t.Fatalf("%d lines for 3x the cap plus its newline, want 3: %v", len(got), lengths(got))
	}
	if rest := a.Flush(); rest != nil {
		t.Fatalf("Flush gave %v, want nothing", data(rest))
	}
}

func TestAssembler_SplitsAChunkThatIsItselfOverTheCap(t *testing.T) {
	// The cap has to bound the line, not merely flush after it: one Modal item can be megabytes, and a
	// Line that size rides the queue's oversized-line escape only to be dropped by the batcher.
	a := NewAssembler(false)
	got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{
		{Data: strings.Repeat("x", 5*maxFragment/2), At: t1},
	}})

	if len(got) != 2 {
		t.Fatalf("%d lines for 2.5x the cap, want 2", len(got))
	}
	for i, l := range got {
		if len(l.Data) != maxFragment {
			t.Fatalf("line %d is %d bytes, want the cap %d", i, len(l.Data), maxFragment)
		}
	}
	if rest := a.Flush(); len(rest) != 1 || len(rest[0].Data) != maxFragment/2 {
		t.Fatalf("Flush gave %v, want the remaining half-cap fragment", data(rest))
	}
}

func TestAssembler_CapsALineItsOwnNewlineTerminated(t *testing.T) {
	// The terminated case used to skip the cap entirely, because the newline path wrote its whole prefix
	// before the bounded loop ever saw it. That matters at the queue rather than here: it admits any one
	// line into an empty buffer, so a Line over the cap turns a 64 KiB byte bound into "one longest
	// line", and 1,000 streams of those is a memory figure nobody can size a Deployment from.
	a := NewAssembler(false)
	got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{
		{Data: strings.Repeat("x", 5*maxFragment/2) + "\n", At: t1},
	}})

	if len(got) != 3 {
		t.Fatalf("%d lines for 2.5x the cap plus its newline, want 3", len(got))
	}
	var total int
	for i, l := range got {
		if len(l.Data) > maxFragment {
			t.Fatalf("line %d is %d bytes, over the cap %d", i, len(l.Data), maxFragment)
		}
		total += len(l.Data)
	}
	// Split, not truncated: the newline is the only byte that should be missing.
	if total != 5*maxFragment/2 {
		t.Fatalf("the pieces hold %d bytes, want the line's %d", total, 5*maxFragment/2)
	}
	if rest := a.Flush(); rest != nil {
		t.Fatalf("Flush gave %v, want nothing — the newline took the last piece", data(rest))
	}
}

func TestAssembler_AdvancesWhenNoRuneBoundaryExists(t *testing.T) {
	// runeBoundary walks down to zero when the window holds no rune start, and taking the pending line
	// cannot conjure one — so this used to spin, emitting empty lines until the process died. A stream
	// of continuation bytes is invalid UTF-8 either way; the cut is the lesser failure.
	a := NewAssembler(false)
	flood := strings.Repeat("\x80", 2*maxFragment)

	got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: flood, At: t1}}})
	got = append(got, a.Flush()...)

	var joined strings.Builder
	for i, l := range got {
		if len(l.Data) > maxFragment {
			t.Fatalf("piece %d is %d bytes, over the cap %d", i, len(l.Data), maxFragment)
		}
		joined.WriteString(l.Data)
	}
	// Passed through as it arrived rather than replaced by U+FFFD: the durable copy is the evidence.
	if joined.String() != flood {
		t.Fatalf("reassembled %d bytes, want the %d that arrived", joined.Len(), len(flood))
	}
}

func TestAssembler_CutsAFragmentOnRuneBoundaries(t *testing.T) {
	// A cut inside a multi-byte rune reaches the sink as U+FFFD, which corrupts the durable copy
	// rather than merely splitting it. The leading chunk is swept because whether the cap lands
	// mid-rune depends on what is already pending as much as on the data.
	body := strings.Repeat("日本語", maxFragment/9+16) // 9 bytes a repeat, so comfortably over the cap
	for pad := 0; pad < 9; pad++ {
		a := NewAssembler(false)
		want := strings.Repeat("a", pad) + body
		got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: want, At: t1}}})
		got = append(got, a.Flush()...)

		var joined strings.Builder
		for i, l := range got {
			if !utf8.ValidString(l.Data) {
				t.Fatalf("pad %d: piece %d is not valid UTF-8", pad, i)
			}
			joined.WriteString(l.Data)
		}
		if joined.String() != want {
			t.Fatalf("pad %d: reassembled %d bytes, want %d", pad, joined.Len(), len(want))
		}
	}
}

func TestAssembler_ACarriageReturnCannotReachPastAnEmittedPiece(t *testing.T) {
	// The one thing splitting at the cap costs, asserted so it is a decision rather than a surprise:
	// collapsing still discards what is pending, but a piece already emitted cannot be un-emitted, so
	// a CR reaches back maxFragment rather than without limit. Honouring it further is the unbounded
	// buffer the cap exists to prevent.
	a := NewAssembler(true)
	got := a.Add(Batch{Cursor: "100-0", Entries: []Entry{
		{Data: strings.Repeat("x", 2*maxFragment) + "\rdone", At: t1},
	}})

	if len(got) != 2 {
		t.Fatalf("%d lines, want the two full pieces emitted before the CR arrived", len(got))
	}
	if rest := a.Flush(); len(rest) != 1 || rest[0].Data != "done" {
		t.Fatalf("Flush gave %v, want the CR to have collapsed what was still pending", data(rest))
	}
}

func TestAssembler_CollapsingKeepsAProgressBarBounded(t *testing.T) {
	// The other half of the cap's argument: with frames collapsed, a bar that never terminates
	// holds one frame rather than accumulating toward maxFragment at all.
	a := NewAssembler(true)
	for range 1000 {
		if lines := a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: "\r" + strings.Repeat("#", 200), At: t1}}}); len(lines) != 0 {
			t.Fatalf("got %v, want nothing until the bar terminates", data(lines))
		}
	}
	if a.buf.Len() != 200 {
		t.Fatalf("pending fragment is %d bytes after 1000 frames, want one frame's worth", a.buf.Len())
	}
}

func TestAssembler_FlushIsIdempotent(t *testing.T) {
	a := NewAssembler(false)
	a.Add(Batch{Cursor: "100-0", Entries: []Entry{{Data: "tail", At: t1}}})
	if lines := a.Flush(); len(lines) != 1 {
		t.Fatalf("%d lines, want the fragment", len(lines))
	}
	// A drain can be attempted twice — once at eof, once by the caller shutting down — and the
	// second must not re-emit what the first already shipped.
	if lines := a.Flush(); lines != nil {
		t.Fatalf("second Flush gave %v, want nothing", data(lines))
	}
}

func data(lines []Line) []string {
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		out = append(out, l.Data)
	}
	return out
}

// lengths is for failures where the byte counts are the whole story and the data is 64 KiB of "x".
func lengths(lines []Line) []int {
	out := make([]int, 0, len(lines))
	for _, l := range lines {
		out = append(out, len(l.Data))
	}
	return out
}

func equalData(got []Line, want []string) bool {
	return strings.Join(data(got), "\x00") == strings.Join(want, "\x00")
}
