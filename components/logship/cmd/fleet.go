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

package main

import (
	"github.com/InftyAI/Nebula/components/logship/internal/emit"
	"github.com/InftyAI/Nebula/components/logship/internal/modal"
	"github.com/InftyAI/Nebula/components/logship/internal/provider"
	"github.com/InftyAI/Nebula/components/logship/internal/ship"
	"github.com/InftyAI/Nebula/components/logship/internal/supervise"
)

// register wires the log providers this build has. Adding one is this line plus its package: nothing
// else here names a provider, and nothing has to be told which one a cluster uses.
func register(set *provider.Set) {
	set.Register(provider.ProviderModal, func() (provider.Provider, error) {
		p, err := modal.Open()
		if err != nil {
			return nil, err
		}
		return p, nil
	})
}

// fleet routes each instance to the provider that can read it, and is both halves of what the
// supervisor needs — the stream list and the pipeline — plus the watch's Fleet.
//
// One type because all three are the same lookup: the Pod said which provider it is on, and
// everything else follows from resolving that once.
type fleet struct {
	set *provider.Set
	sup *supervise.Supervisor

	// sink is shared by every stream: there is one stdout. See emit.Sink.
	sink *emit.Sink

	// cursor resolves where a stream resumes. Nil means the beginning; phase 3 replaces it with the
	// checkpoint, which is why it is a function rather than a string.
	cursor func(inst supervise.Instance, stream string) string

	// Only failures are reported from here, hence errf rather than a general logger: every one of
	// them means an instance's logs are not being shipped.
	errf func(msg string, keysAndValues ...any)
}

// Ensure starts copying an instance, after making room for it.
//
// Ordering is the point: a provider has to reserve capacity BEFORE the streams it accounts for open.
// For Modal that is a connection, and a connection added afterwards cannot take over streams already
// queued behind HTTP/2's concurrency cap — which gRPC-go does silently rather than failing, the worst
// available failure for a log shipper. Reserving here rather than at a configured ceiling is why
// there is no fleet size to guess.
//
// Either failure skips the instance rather than shipping it: with no backend there is nothing to read
// it, and with no capacity its streams would wait forever with nothing said.
func (f *fleet) Ensure(inst supervise.Instance) {
	if !f.reserve(inst.Provider, f.sup.InstanceCount()+1) {
		f.errf("NOT shipping this instance", "provider", inst.Provider, "instance", inst.ID, "pod", inst.Pod)
		return
	}
	f.sup.Ensure(inst)
}

func (f *fleet) Forget(id string) { f.sup.Forget(id) }

// streams is supervise.Config.Streams: the provider's own stream names, because they are the names it
// will have to recognise again in Source.
func (f *fleet) streams(inst supervise.Instance) []string {
	p, err := f.set.Get(inst.Provider)
	if err != nil {
		// Unreachable via Ensure, which resolved the same provider first. Nothing is logged here
		// because an instance with no streams is not silent — the supervisor says so.
		return nil
	}
	return p.Streams()
}

// build is supervise.Builder: one instance and one stream into a running pipeline. Called again on
// every restart, so it holds no per-run state.
func (f *fleet) build(inst supervise.Instance, stream string) (*ship.Pipeline, error) {
	p, err := f.set.Get(inst.Provider)
	if err != nil {
		return nil, err
	}
	src, err := p.Source(inst.ID, stream)
	if err != nil {
		return nil, err
	}

	// The Pod's labels wholesale, which is both less code than picking the tenant keys out and a
	// closer match to what the agent writes for every other pod in the group. Nothing here has to
	// know a key: the consumer reads them from the record.
	record := ship.Record{Pod: inst.Pod, Labels: inst.Labels}

	cursor := ""
	if f.cursor != nil {
		cursor = f.cursor(inst, stream)
	}

	return ship.New(ship.Config{
		Source: src,
		Sink:   f.sink,
		Format: record.Formatter(),
		Limits: emit.Limits(0),
		Cursor: cursor,
		Log:    f.errf,
		// On because off loses the bar entirely: an un-collapsed progress bar never sends a newline,
		// so the assembler holds every frame until maxFragment and emits one 64 KiB line, which the
		// batcher then drops for exceeding the per-event cap. Collapsed, each frame replaces the last,
		// so the line stays small and arrives — as the bar's final state rather than its history.
		CollapseFrames: true,
		// No Throttled: a write to stdout has no rate to back off from. The agent owns retrying the
		// one hop that does, which is the point of shipping this way.
	}), nil
}

// reserve resolves the instance's provider and makes room for instances of them, reporting whether
// the fleet now has somewhere to put them.
func (f *fleet) reserve(name string, instances int) bool {
	p, err := f.set.Get(name)
	if err != nil {
		f.errf("no log provider for this instance", "provider", name, "err", err)
		return false
	}
	if err := p.Reserve(instances); err != nil {
		f.errf("cannot make room for this instance", "provider", name, "instances", instances, "err", err)
		return false
	}
	return true
}
