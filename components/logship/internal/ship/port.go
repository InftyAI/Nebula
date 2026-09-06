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

import "context"

// Source follows one stream, delivering batches strictly after cursor and returning when the stream
// ends. An adapter binds whatever identifies the stream — for Modal, a sandbox and a file descriptor
// — so nothing here has to know what a stream is named.
//
// fn's error aborts the follow, and a source must not lose the batch it was called with: the caller
// decides what a rejected batch means, and the only cursor is the one it has accepted.
type Source interface {
	Follow(ctx context.Context, cursor string, fn func(Batch) error) error
}

// Sink appends events somewhere durable.
//
// Append-only, deliberately: the shipper never reads its destination, so there is no Tail and the
// cursor is never recovered from what landed. See design.md — that decision is what makes a
// restart a replay.
//
// Put's events are already sized and ordered for the sink by Batcher; a sink that has limits states
// them as Limits rather than enforcing them again here.
//
// One Sink may be shared by every stream in the process, and the real one is: Put must be safe for
// concurrent use, and must deliver a batch whole rather than event by event, because a sink with one
// destination has no way to tell two interleaved batches apart afterwards. Nothing closes a Sink —
// a stream ending says nothing about a destination the other thousand are still writing to.
type Sink interface {
	Put(ctx context.Context, events []Event) error
}
