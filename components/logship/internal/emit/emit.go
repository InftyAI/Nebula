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

// Package emit ships a record by printing it: one line on the process's own stdout, and nothing else.
//
// The destination is then whatever already collects container logs — here, the cluster's shared
// Fluent Bit DaemonSet, which this repo neither configures nor may change. It tails
// /var/log/containers/*.log, so logship is collected by virtue of being a pod. No spool, no glob to
// keep in sync with a config we do not own, no AWS credentials, and kubelet's rotation of that file
// is the buffer a restart reads back.
//
// What it costs is the envelope. The agent's `kubernetes` filter wraps our record in one of its own,
// stamped with logship's pod rather than the sandbox's, and `Merge_Log On` parses ours into a nested
// object under `log_processed`. So the identity the consumer filters on sits one level down, at
// $.log_processed.kubernetes.labels — see design.md and ship.Record.
package emit

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/InftyAI/Nebula/components/logship/internal/ship"
)

const (
	// maxEventBytes keeps a record inside a single CRI chunk. The container runtime reads a
	// container's stdout in 16 KiB pieces and writes each as its own line in the container log,
	// flagged partial, leaving Fluent Bit's cri multiline parser to glue them back together. Staying
	// under the chunk means that rejoin never runs: a rejoin that failed would deliver two halves of
	// a JSON object, and the consumer drops an unparseable record without reporting it.
	maxEventBytes = 16 << 10

	// perEventOverhead is CloudWatch's 26 bytes per event, and it is still ours to reserve even
	// though we no longer call PutLogEvents — the agent turns one line into one event and has no way
	// to split what we hand it. Here it doubles as headroom under maxEventBytes for the timestamp and
	// stream prefix the runtime puts in front of every line.
	perEventOverhead = 26

	// DefaultBatchBytes bounds one Write, not one request; there is no request. Batching survives
	// only to keep the syscall off the per-line path.
	DefaultBatchBytes = 64 << 10
)

// Limits are the caps a ship.Batcher must respect for a record to survive the trip. batchBytes <= 0
// means DefaultBatchBytes.
//
// MaxEvents stays unset: the 10,000-per-request cap belonged to the API call we no longer make.
func Limits(batchBytes int) ship.Limits {
	if batchBytes <= 0 {
		batchBytes = DefaultBatchBytes
	}
	return ship.Limits{
		MaxBytes:         batchBytes,
		MaxEventBytes:    maxEventBytes,
		PerEventOverhead: perEventOverhead,
	}
}

// Sink writes each event as one line on w.
//
// One Sink serves every stream in the process, which is the difference from the per-stream sinks that
// came before it: stdout is one file descriptor, and two batches interleaved on it corrupt both. The
// mutex is what makes that sharing safe, and it is why a batch is assembled into a single buffer and
// written once rather than a line at a time.
type Sink struct {
	w io.Writer

	mu sync.Mutex
}

func New(w io.Writer) *Sink { return &Sink{w: w} }

// Put writes the batch. Every message must be newline-free or one record becomes two lines and
// neither parses — which ship.Assembler guarantees, since it splits on newlines to produce a Line at
// all, and Record.Formatter escapes any that a line's data still holds.
//
// ctx is ignored on purpose. Cancellation is how shutdown reaches the pipelines, and the last batch
// of a stopping process is precisely the one that still has to be printed; there is also nothing to
// cancel, since a write to stdout either completes or fails.
func (s *Sink) Put(_ context.Context, events []ship.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Built outside the lock: at fleet scale a thousand streams contend for it, and formatting is the
	// part that does not have to be serialised.
	n := len(events)
	for _, e := range events {
		n += len(e.Message)
	}
	var b bytes.Buffer
	b.Grow(n)
	for _, e := range events {
		b.WriteString(e.Message)
		b.WriteByte('\n')
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.w.Write(b.Bytes())
	return err
}
