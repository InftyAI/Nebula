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

// Package supervise owns the goroutines: it turns a changing set of instances into a running set of
// pipelines. It knows nothing about Kubernetes, Modal or CloudWatch — a watch calls Ensure and
// Forget, and a Builder supplied by cmd decides what a stream actually connects to.
package supervise

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/InftyAI/Nebula/components/logship/internal/ship"
)

// Defaults for restart pacing. Bounded rather than infinite because every restart resumes from the
// last durable cursor, so an instance that can never be read would otherwise re-ship its history
// on a loop and bill us for it.
const (
	DefaultMaxRestarts = 5
	DefaultMinBackoff  = time.Second
	DefaultMaxBackoff  = 30 * time.Second
)

// Instance is one external instance whose output is being copied, with everything needed to name
// and label its events. Sourced from the Pod, which carries the provider, the provider's instance id
// and the tenant labels on the same object.
//
// No namespace: the consumer identifies a record by pod name and labels alone, so carrying one
// would only be a field that looks stamped and is not.
type Instance struct {
	// Provider is which backend reads this instance, and travels with ID because ID is only
	// meaningful to that one — the same field holds a Modal sandbox id or an EC2 instance id. Carried
	// and never interpreted here; Config.Streams and Build are what route on it.
	Provider string

	ID     string
	Pod    string
	Labels map[string]string
}

// Builder makes the pipeline for one stream of one instance.
//
// Called again on every restart, and it has to be: a Pipeline is single-use — Run closes its queue —
// so a retry needs a fresh one, with whatever cursor the checkpoint holds by then.
type Builder func(inst Instance, stream string) (*ship.Pipeline, error)

// Config describes the fleet. Streams and Build are required.
type Config struct {
	// Streams names one instance's streams, one pipeline each. Two of them for a Modal sandbox:
	// stdout and stderr have separate cursors, so they are separate all the way down rather than
	// merged and re-split.
	//
	// A function of the instance rather than one list for the fleet, because the streams belong to
	// the provider that reads the instance, and a cluster can be split across two providers whose
	// streams are named differently.
	Streams func(inst Instance) []string
	Build   Builder

	MaxRestarts int
	MinBackoff  time.Duration
	MaxBackoff  time.Duration

	// Log matches logr.Logger.Info's signature so cmd can pass it straight in. Nil is silent, which
	// is fine for tests and wrong in production: abandoning a stream is the one event here that
	// nothing else will tell anyone about.
	Log func(msg string, keysAndValues ...any)
}

// Stats is the fleet view. Shipping is summed across every stream this supervisor has ever run, so
// the counters survive a restart of the stream that produced them.
type Stats struct {
	// Instances is how many are tracked right now; Started is how many ever were.
	Instances int
	Started   int
	// Streams is how many pipelines are live right now.
	Streams int

	Restarts int
	// Abandoned counts streams that used up their restart budget. Each one is an instance whose
	// remaining logs will not be copied, which is the alert worth having.
	Abandoned int
	// Completed counts streams that reached their source's end, which is the normal way to finish.
	Completed int

	Shipping ship.Stats
}

// Supervisor runs one set of pipelines per instance.
//
// It holds the context it was built with, which is the one place that is the right design rather
// than a smell: its entire job is owning goroutine lifetimes, and the alternative — a context per
// Ensure call — would tie a stream's life to a reconcile that has already returned.
//
// Safe for concurrent use. Ensure and Forget are cheap and non-blocking, because the caller is a
// watch loop.
type Supervisor struct {
	cfg Config
	ctx context.Context

	mu      sync.Mutex
	known   map[string]*tracked
	stats   Stats
	stopped bool

	wg sync.WaitGroup
}

// tracked is one instance's running state. live holds the current pipeline per stream, so Stats can
// read counters in flight; a retired pipeline's counters are folded into Supervisor.stats first, so
// nothing accumulates per restart.
type tracked struct {
	inst   Instance
	cancel context.CancelFunc
	live   map[string]*ship.Pipeline
}

// New builds a supervisor. It panics on a Config missing Streams or Build, which is a wiring mistake
// worth having at startup: the alternative surfaces as the process dying on the first Pod the watch
// delivers, in production, hours later.
func New(ctx context.Context, cfg Config) *Supervisor {
	if cfg.Streams == nil || cfg.Build == nil {
		panic("supervise: Config.Streams and Config.Build are required")
	}
	if cfg.MaxRestarts <= 0 {
		cfg.MaxRestarts = DefaultMaxRestarts
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = DefaultMinBackoff
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = max(DefaultMaxBackoff, cfg.MinBackoff)
	}
	return &Supervisor{ctx: ctx, cfg: cfg, known: map[string]*tracked{}}
}

// Ensure starts copying inst, and does nothing if it is already known.
//
// Idempotent on the id, deliberately: a watch re-delivers the same Pod on every unrelated update,
// and treating one of those as new would start a second set of pipelines that replay the instance
// from the beginning. An instance whose streams have all finished stays known for the same reason —
// only Forget takes it out.
func (s *Supervisor) Ensure(inst Instance) {
	// Said out loud because it is otherwise the quietest way to ship nothing: the instance is tracked,
	// Forget works, Stats counts it, and no pipeline ever runs.
	streams := s.cfg.Streams(inst)
	if len(streams) == 0 {
		s.log("instance has no streams to follow", "instance", inst.ID, "provider", inst.Provider)
		return
	}

	s.mu.Lock()
	if s.stopped || inst.ID == "" || s.known[inst.ID] != nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	t := &tracked{inst: inst, cancel: cancel, live: map[string]*ship.Pipeline{}}
	s.known[inst.ID] = t
	s.stats.Instances++
	s.stats.Started++
	s.mu.Unlock()

	for _, name := range streams {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.follow(ctx, t, name)
		}()
	}
}

// Forget stops copying an instance and drops it.
//
// Returns immediately; the goroutines wind down on their own. In-flight puts are cut off, which is
// why the drain finalizer exists — it is what keeps a Pod's deletion from reaching here before the
// tail of the log has shipped.
func (s *Supervisor) Forget(id string) {
	s.mu.Lock()
	t := s.known[id]
	if t != nil {
		delete(s.known, id)
		s.stats.Instances--
	}
	s.mu.Unlock()

	if t != nil {
		t.cancel()
	}
}

// Sync makes the tracked set exactly want: it starts what is missing and forgets what is gone.
//
// For a resync and for startup, where a per-object event has no chance of firing for an instance
// that disappeared while the process was down.
func (s *Supervisor) Sync(want []Instance) {
	keep := make(map[string]bool, len(want))
	for _, inst := range want {
		keep[inst.ID] = true
		s.Ensure(inst)
	}

	s.mu.Lock()
	var stale []string
	for id := range s.known {
		if !keep[id] {
			stale = append(stale, id)
		}
	}
	s.mu.Unlock()

	for _, id := range stale {
		s.Forget(id)
	}
}

// Shutdown cancels every stream and waits. The pipelines will fail their final puts against the
// cancelled context, so a caller that wants the tails shipped drains before calling this.
func (s *Supervisor) Shutdown() {
	s.mu.Lock()
	s.stopped = true
	cancels := make([]context.CancelFunc, 0, len(s.known))
	for id, t := range s.known {
		cancels = append(cancels, t.cancel)
		delete(s.known, id)
	}
	s.stats.Instances = 0
	s.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	s.wg.Wait()
}

// Stats reports the fleet's counters, folding in every live pipeline's own.
func (s *Supervisor) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := s.stats
	for _, t := range s.known {
		for _, p := range t.live {
			out.Streams++
			add(&out.Shipping, p.Stats())
		}
	}
	return out
}

// follow runs one stream, restarting it on failure until the budget is spent.
func (s *Supervisor) follow(ctx context.Context, t *tracked, name string) {
	backoff := s.cfg.MinBackoff
	for attempt := 0; ; attempt++ {
		ended, err := s.runOnce(ctx, t, name)
		if ctx.Err() != nil {
			return
		}
		if ended {
			// The source reached its end. Restarting would replay the whole instance and then end
			// again, forever — this is the one outcome that must not be retried.
			s.record(func(st *Stats) { st.Completed++ })
			return
		}
		if attempt >= s.cfg.MaxRestarts {
			s.record(func(st *Stats) { st.Abandoned++ })
			s.log("abandoning stream after exhausting its restart budget",
				"instance", t.inst.ID, "stream", name, "restarts", attempt, "err", err)
			return
		}
		s.record(func(st *Stats) { st.Restarts++ })
		if !sleep(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, s.cfg.MaxBackoff)
	}
}

// runOnce builds and runs one pipeline. A build failure counts against the same restart budget as a
// run failure: both mean this stream is not copying, and the cause is usually the same outage.
func (s *Supervisor) runOnce(ctx context.Context, t *tracked, name string) (bool, error) {
	p, err := s.cfg.Build(t.inst, name)
	if err != nil {
		return false, fmt.Errorf("build stream %s/%s: %w", t.inst.ID, name, err)
	}
	s.setLive(t, name, p)
	defer s.retire(t, name, p)

	if err := p.Run(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Supervisor) setLive(t *tracked, name string, p *ship.Pipeline) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t.live[name] = p
}

// retire folds a finished pipeline's counters into the fleet total and drops the pointer, so a
// stream that has restarted five times is still five sets of counters and one live object.
func (s *Supervisor) retire(t *tracked, name string, p *ship.Pipeline) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.live[name] == p {
		delete(t.live, name)
	}
	add(&s.stats.Shipping, p.Stats())
}

func (s *Supervisor) log(msg string, kv ...any) {
	if s.cfg.Log != nil {
		s.cfg.Log(msg, kv...)
	}
}

func (s *Supervisor) record(f func(*Stats)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.stats)
}

func add(dst *ship.Stats, src ship.Stats) {
	dst.Lines += src.Lines
	dst.Events += src.Events
	dst.Dropped += src.Dropped
	dst.DroppedBytes += src.DroppedBytes
	dst.Failed += src.Failed
	dst.Clamped += src.Clamped
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
