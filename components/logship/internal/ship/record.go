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
	"maps"
	"slices"
	"strings"
	"time"
)

// Fallbacks for a line that carries no envelope of its own. INFO because bare sandbox output has no
// level; "user" because the alternative is invisible — see Record.Category.
const (
	LevelInfo    = "INFO"
	CategoryUser = "user"
)

// Record is the identity stamped into every event, and the reason it is per-event rather than in the
// stream name: the consumer pages the whole log group with FilterLogEvents and a
// `{$.…kubernetes.labels…}` pattern, never reading a stream name. Which also means a shipped event
// stays attributable after its Pod is deleted, since nothing has to be joined against the cluster.
//
// The shape is Fluent Bit's Kubernetes-filter output, copied rather than invented — but a record now
// travels through that filter instead of past it, so it arrives nested one level down under
// `log_processed` while a node's own logs stay at the top level. Same field names, two depths: the
// consumer needs a clause for each, and neither may be dropped. See emit and design.md.
type Record struct {
	// Pod lands in kubernetes.pod_name, which the consumer maps a worker index onto. An empty one
	// ships fine and is unreachable through that path.
	Pod string
	// Labels lands in kubernetes.labels. The consumer's tenant filter reads its org, team and
	// experiment IDs from here, and the source name from `app` — a missing one is not an error here
	// but a line no query will match.
	Labels map[string]string

	// Level and Category go INSIDE the log field, not beside it: the consumer parses log as JSON and
	// defaults a plain string to category "private", which is hidden from every caller without the
	// developer-view role. Shipping raw stdout bare would ship it invisibly.
	//
	// Both are fallbacks, applied only to a line that is not already an envelope — see adopt. A
	// workload that logs its own level and category keeps them, and overriding them here is what made
	// every sandbox line arrive as INFO/user.
	Level    string
	Category string
}

// Formatter renders lines as this record.
//
// Built once per stream because the identity is constant for one: pod name and labels are encoded
// here and only the timestamp, cursor and text are rebuilt per line.
func (r Record) Formatter() Formatter {
	category := nonEmpty(r.Category, CategoryUser)
	// Doubly encoded, unavoidably: log is a JSON string whose contents are themselves JSON, so a
	// quote in the output reaches CloudWatch as `\\\"`. That is the consumer's existing format.
	head := `{"level":` + quoteJSON(nonEmpty(r.Level, LevelInfo)) +
		`,"category":` + quoteJSON(category) + `,"id":`
	tail := r.metadata()

	return func(l Line) string {
		log, ok := adopt(l.Data, l.Cursor, category)
		if !ok {
			var inner strings.Builder
			inner.WriteString(head)
			encodeJSONString(&inner, l.Cursor)
			inner.WriteString(`,"message":`)
			encodeJSONString(&inner, l.Data)
			inner.WriteByte('}')
			log = inner.String()
		}

		var b strings.Builder
		b.WriteString(`{"time":"`)
		// RFC3339Nano is what the CRI parser leaves in Fluent Bit's time field, and the consumer
		// passes it through to its client verbatim. So the nanoseconds the sink has to truncate for
		// PutLogEvents survive here for free.
		b.WriteString(l.At.UTC().Format(time.RFC3339Nano))
		b.WriteString(`","log":`)
		encodeJSONString(&b, log)
		b.WriteString(tail)
		return b.String()
	}
}

// adopt returns a line that is ALREADY an envelope, with the cursor added, plus a category if it had
// none. Not ok for anything else, which the caller wraps instead.
//
// This is the whole reason a workload's level and category survive: the consumer unwraps `log` exactly
// once, so nesting an envelope inside ours makes ours the only one it reads and buries theirs in
// `message` as text.
//
// The test is deliberately the consumer's own decode rather than "is this JSON". An object it cannot
// decode into {level, category, message} strings, or one with no message at all, falls back to
// category private and renders the raw JSON as the text — so adopting one would hide a line the wrap
// shows. Cost is a parse per line that starts with `{`, which is why the cheap byte test comes first.
func adopt(data, cursor, category string) (string, bool) {
	s := strings.TrimSpace(data)
	if len(s) == 0 || s[0] != '{' {
		return "", false
	}
	// Pointers to tell an absent category from an empty one, and to reject a null message the way the
	// consumer's plain strings would not. Level is read by nothing and still has to be declared: it is
	// what makes a non-string level fail this decode, as it would fail the consumer's.
	var env struct {
		Category *string `json:"category"`
		Message  *string `json:"message"`
		Level    *string `json:"level"`
	}
	if json.Unmarshal([]byte(s), &env) != nil || env.Message == nil {
		return "", false
	}

	// Spliced in before the closing brace rather than re-marshalled, so the workload's field order,
	// number formatting and unknown fields survive untouched. Last also settles a collision: every
	// JSON decoder keeps the last of a duplicate key, so a workload logging its own `id` cannot
	// shadow the cursor. No level is added — the consumer already defaults that to INFO.
	var b strings.Builder
	b.WriteString(s[:len(s)-1])
	b.WriteByte(',')
	if env.Category == nil || *env.Category == "" {
		b.WriteString(`"category":`)
		encodeJSONString(&b, category)
		b.WriteByte(',')
	}
	b.WriteString(`"id":`)
	encodeJSONString(&b, cursor)
	b.WriteByte('}')
	return b.String(), true
}

// metadata encodes the kubernetes block and the closing braces of the whole record.
//
// Labels are sorted so a stream's events are byte-identical run to run, which map order would
// otherwise make random.
func (r Record) metadata() string {
	var b strings.Builder
	b.WriteString(`,"kubernetes":{"pod_name":`)
	encodeJSONString(&b, r.Pod)
	b.WriteString(`,"labels":{`)
	for i, k := range slices.Sorted(maps.Keys(r.Labels)) {
		if i > 0 {
			b.WriteByte(',')
		}
		encodeJSONString(&b, k)
		b.WriteByte(':')
		encodeJSONString(&b, r.Labels[k])
	}
	b.WriteString(`}}}`)
	return b.String()
}

func quoteJSON(s string) string {
	var b strings.Builder
	encodeJSONString(&b, s)
	return b.String()
}

func nonEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
