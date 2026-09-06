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
	got := a.Add(Batch{Cursor: "200-0", Entries: []Entry{{Data: chunk, At: t2}}})
	if len(got) != 1 || len(got[0].Data) != maxFragment {
		t.Fatalf("%d lines of %d bytes, want one of %d", len(got), len(got[0].Data), maxFragment)
	}
	if lines := a.Flush(); len(lines) != 0 {
		t.Fatalf("Flush gave %v, want nothing left behind", data(lines))
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

func equalData(got []Line, want []string) bool {
	return strings.Join(data(got), "\x00") == strings.Join(want, "\x00")
}
