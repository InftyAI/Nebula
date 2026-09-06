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

// maxFragment bounds a line that never terminates. Not defensive: a `tqdm` progress bar emits a
// carriage-returned frame per update and no newline until the run ends, so an assembler that simply
// waited for one would hold a whole training run's progress output as a single pending line.
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
}

func NewAssembler(collapseFrames bool) *Assembler {
	return &Assembler{collapseFrames: collapseFrames}
}

// Add returns the lines this batch completed. A trailing fragment is held for a later batch, or for
// Flush, unless it grows past maxFragment — in which case it is emitted as its own line, split.
func (a *Assembler) Add(b Batch) []Line {
	a.cursor = b.Cursor

	var out []Line
	for _, e := range b.Entries {
		data := e.Data
		for {
			i := strings.IndexByte(data, '\n')
			if i < 0 {
				break
			}
			// A trailing CR here is a Windows line ending, not a progress frame, and dropping it
			// before the frame logic is what stops "line\r\n" from collapsing to nothing.
			a.write(e.At, strings.TrimSuffix(data[:i], "\r"))
			out = append(out, a.take())
			data = data[i+1:]
		}
		a.write(e.At, data)
		if a.buf.Len() >= maxFragment {
			out = append(out, a.take())
		}
	}
	return out
}

// Flush returns the pending fragment, if any, as a line with no terminator of its own. For the end
// of a stream: a sandbox that exits mid-line has still printed that text.
func (a *Assembler) Flush() []Line {
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
