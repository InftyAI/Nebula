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

// Package ship holds the ports and the mechanism, and imports neither of its adapters: a provider
// satisfies Source, a log service satisfies Sink, and cmd is where the two meet. Everything here
// is scoped to ONE stream — one sandbox, one file descriptor — because that is what makes a cursor
// meaningful and a CloudWatch stream monotonic.
package ship

import (
	"strings"
	"time"
)

// Entry is one chunk of output as a source produced it. Data is not a line: it may hold several
// newlines, or half of one. See Assembler.
type Entry struct {
	Data string
	At   time.Time
}

// Batch is a source's batch, with the cursor that resumes strictly after it.
type Batch struct {
	Entries []Entry
	Cursor  string
}

// Line is one assembled line, with the cursor of the batch it was COMPLETED in.
//
// Completed, not started: a resume from that cursor re-delivers nothing this line needed, whereas a
// resume from where the line began would re-deliver the rest of that batch. Duplicates over losses
// is the whole trade, but there is no reason to pay for a duplicate that is avoidable.
//
// Data can be empty — a blank line is real output. It is the sink's job not to turn that into an
// empty message, which the entry-ID prefix already prevents.
type Line struct {
	Data   string
	At     time.Time
	Cursor string
}

// maxFragment bounds one Line, terminated or not.
//
// Not defensive for the unterminated case: a `tqdm` progress bar emits a carriage-returned frame per
// update and no newline until the run ends, so an assembler that simply waited for one would hold a
// whole training run's progress output as a single pending line. A terminated line needs the same cap
// for a different reason, which is that the queue's byte bound assumes it — see fill.
const maxFragment = 64 * 1024

// Assembler turns one stream's chunks into lines.
//
// Not safe for concurrent use, and not meant to be: one assembler belongs to one stream, which is
// read by one goroutine.
type Assembler struct {
	// collapseFrames discards a line's superseded carriage-returned frames, keeping only the
	// newest. It trades the timing of a run's progress for its size — see docs on the ingest cost
	// in design.md — and it is also what keeps a progress bar from reaching maxFragment at
	// all, since each frame replaces the last instead of extending it.
	collapseFrames bool

	buf strings.Builder
	// at is the timestamp of the chunk that contributed the pending line's FIRST byte, so a line
	// is dated when it was emitted rather than when it happened to finish.
	at     time.Time
	cursor string
	// pendingCR holds a chunk's trailing CR, which cannot be classified until the next byte arrives:
	// see Add. Kept out of buf so that whichever it turns out to be, nothing has been committed yet.
	pendingCR string
}

func NewAssembler(collapseFrames bool) *Assembler {
	return &Assembler{collapseFrames: collapseFrames}
}

// Add returns the lines this batch completed. A trailing fragment is held for a later batch, or for
// Flush; anything that would exceed maxFragment is split instead, whether a newline terminated it or
// not — see fill.
func (a *Assembler) Add(b Batch) []Line {
	a.cursor = b.Cursor

	var out []Line
	for _, e := range b.Entries {
		// A trailing CR is ambiguous until the next byte decides it: the first half of a CRLF
		// terminator, or a progress frame's separator. Holding it back and putting it in front of the
		// next chunk is what lets the loops below see it together with the byte that decides — a CR
		// resolved against the wrong one either leaves a stray CR in the line or, once frames collapse,
		// takes the whole line's text as overwritten.
		data := a.pendingCR + e.Data
		a.pendingCR = ""
		if strings.HasSuffix(data, "\r") {
			data, a.pendingCR = data[:len(data)-1], "\r"
		}
		for {
			i := strings.IndexByte(data, '\n')
			if i < 0 {
				break
			}
			// A trailing CR here is a Windows line ending, not a progress frame, and dropping it
			// before the frame logic is what stops "line\r\n" from collapsing to nothing.
			out = a.fill(out, e.At, strings.TrimSuffix(data[:i], "\r"))
			out = append(out, a.take())
			data = data[i+1:]
		}
		out = a.fill(out, e.At, data)
	}
	return out
}

// fill writes s into the pending line, taking it whenever it would exceed maxFragment.
//
// Both of Add's paths go through here, because whether a chunk ends in a newline only decides WHICH
// bound a Line the size of that chunk defeats: the queue admits any single line into an empty buffer —
// it has to, or a long line could never ship at all — so it is a Line over maxFragment that turns the
// queue's byte bound into "one largest line". Splitting rather than writing and checking after is what
// keeps a megabyte-long item from ever being one Line.
//
// A CR in a later piece still discards what an earlier one left pending, so collapsing is unaffected
// within the buffer; what it cannot do any more is reach back past a piece already emitted.
func (a *Assembler) fill(out []Line, at time.Time, s string) []Line {
	for {
		room := maxFragment - a.buf.Len()
		// Not `<`: an exact fit is not over the cap, and emitting it would leave Add's newline path
		// taking an empty buffer as a blank line. A dump of any power-of-two size lands exactly here.
		if len(s) <= room {
			break
		}
		// Rounded down so a cut never lands inside a rune. Zero means the pending fragment left room
		// for less than one rune, and taking it is what makes room.
		n := runeBoundary(s, room)
		if n == 0 && a.buf.Len() == 0 {
			// Except when there was no boundary to find in a whole fragment's worth of bytes, where
			// taking the pending line makes none either and the loop would spin without advancing.
			// Cut at the cap: this is already invalid UTF-8, and a hang is the worse of the two.
			n = room
		}
		a.write(at, s[:n])
		out = append(out, a.take())
		s = s[n:]
	}
	a.write(at, s)
	return out
}

// Flush returns the pending fragment, if any, as a line with no terminator of its own. For the end
// of a stream: a sandbox that exits mid-line has still printed that text.
//
// A CR left pending is dropped rather than resolved. Nothing follows it to overwrite what it separated,
// so collapsing on it here would erase text that was still on the terminal when the sandbox exited —
// and the other reading, a CRLF whose LF never came, wants it gone too.
func (a *Assembler) Flush() []Line {
	a.pendingCR = ""
	if a.buf.Len() == 0 {
		return nil
	}
	return []Line{a.take()}
}

func (a *Assembler) write(at time.Time, s string) {
	if a.collapseFrames {
		// Everything before the last CR has been overwritten on the terminal this was meant for,
		// including whatever is already pending.
		if i := strings.LastIndexByte(s, '\r'); i >= 0 {
			a.buf.Reset()
			s = s[i+1:]
		}
	}
	// Before the write rather than after, and unguarded by s != "": a blank line is taken
	// immediately by the caller, and it still has to carry the time it was printed.
	if a.buf.Len() == 0 {
		a.at = at
	}
	a.buf.WriteString(s)
}

func (a *Assembler) take() Line {
	line := Line{Data: a.buf.String(), At: a.at, Cursor: a.cursor}
	a.buf.Reset()
	a.at = time.Time{}
	return line
}
