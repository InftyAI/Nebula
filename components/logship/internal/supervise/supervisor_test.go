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

package supervise

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/InftyAI/Nebula/components/logship/internal/ship"
)

// only is Config.Streams for a fleet whose instances all have the same streams, which is every fleet
// here: what varies per provider in production is not what these tests are about.
func only(names ...string) func(Instance) []string {
	return func(Instance) []string { return names }
}

func TestEnsure_StartsOnePipelinePerStream(t *testing.T) {
	b := &builder{block: true}
	s := New(context.Background(), Config{Streams: only("stdout", "stderr"), Build: b.build})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1", Pod: "p"})

	waitFor(t, "both streams built", func() bool { return b.count() == 2 })
	if got := b.streams(); got["stdout"] != 1 || got["stderr"] != 1 {
		t.Fatalf("built %v, want one of each", got)
	}
	if st := s.Stats(); st.Instances != 1 || st.Started != 1 || st.Streams != 2 {
		t.Fatalf("Stats() = %+v", st)
	}
}

func TestInstanceCount_AgreesWithStats(t *testing.T) {
	// The fleet reserves capacity from InstanceCount on every watch event and reports Stats once at
	// shutdown. Two counters of the same thing, so the cheap one has to answer what the fold would.
	b := &builder{block: true}
	s := New(context.Background(), Config{Streams: only("stdout"), Build: b.build})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1", Pod: "p"})
	s.Ensure(Instance{ID: "sb-2", Pod: "p"})
	waitFor(t, "both instances running", func() bool { return b.running() == 2 })
	if got, want := s.InstanceCount(), s.Stats().Instances; got != want || got != 2 {
		t.Fatalf("InstanceCount() = %d, Stats().Instances = %d, want 2", got, want)
	}

	s.Forget(Ref{ID: "sb-1"})

	if got, want := s.InstanceCount(), s.Stats().Instances; got != want || got != 1 {
		t.Fatalf("after Forget: InstanceCount() = %d, Stats().Instances = %d, want 1", got, want)
	}
}

func TestEnsure_IsIdempotentOnTheInstanceID(t *testing.T) {
	// A watch re-delivers the same Pod on every unrelated update; a second set of pipelines would
	// replay the instance from the beginning.
	b := &builder{block: true}
	s := New(context.Background(), Config{Streams: only("stdout"), Build: b.build})
	defer s.Shutdown()

	for range 5 {
		s.Ensure(Instance{ID: "sb-1", Pod: "p"})
	}

	waitFor(t, "one stream built", func() bool { return b.count() == 1 })
	time.Sleep(20 * time.Millisecond) // give a duplicate a chance to show up
	if got := b.count(); got != 1 {
		t.Fatalf("%d pipelines for one instance, want 1", got)
	}
	if st := s.Stats(); st.Started != 1 {
		t.Fatalf("Started = %d, want 1", st.Started)
	}
}

func TestSupervisor_TellsTwoProvidersApartOnTheSameID(t *testing.T) {
	// An id is minted by the provider that owns it, so nothing stops two backends from using the same
	// string — see Ref. Keyed by the id alone, the second Ensure was a silent no-op that shipped none of
	// that instance's logs, and either Forget cancelled the other one's streams.
	one := Instance{Provider: "modal", ID: "sb-1", Pod: "p"}
	two := Instance{Provider: "aws", ID: "sb-1", Pod: "q"}

	b := &builder{block: true}
	s := New(context.Background(), Config{Streams: only("stdout"), Build: b.build})
	defer s.Shutdown()

	s.Ensure(one)
	s.Ensure(two)

	waitFor(t, "both instances running", func() bool { return b.running() == 2 })
	if st := s.Stats(); st.Instances != 2 || st.Started != 2 {
		t.Fatalf("Stats() = %+v, want both instances tracked", st)
	}

	s.Forget(one.Ref())

	waitFor(t, "only the forgotten instance stopped", func() bool { return b.runningFor(one.Ref()) == 0 })
	if got := b.runningFor(two.Ref()); got != 1 {
		t.Fatalf("%d streams running for the other provider's instance, want 1", got)
	}
	if st := s.Stats(); st.Instances != 1 {
		t.Fatalf("Instances = %d, want the other provider's still tracked", st.Instances)
	}
}

func TestEnsure_IgnoresAnInstanceWithNoID(t *testing.T) {
	// The provider half of a Ref is no identity on its own. A Pod whose instance-id annotation has not
	// landed yet is not an instance called "" — it is one we cannot track, and the next resync gets it.
	b := &builder{block: true}
	s := New(context.Background(), Config{Streams: only("stdout"), Build: b.build})
	defer s.Shutdown()

	s.Ensure(Instance{Pod: "p"})

	time.Sleep(20 * time.Millisecond)
	if got := b.count(); got != 0 {
		t.Fatalf("%d pipelines for an instance with no id, want 0", got)
	}
}

func TestFollow_DoesNotRestartAStreamThatEnded(t *testing.T) {
	// The sandbox finished. Restarting would replay the whole instance and end again, forever.
	b := &builder{}
	s := New(context.Background(), Config{
		Streams: only("stdout"), Build: b.build, MinBackoff: time.Millisecond,
	})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1"})

	waitFor(t, "stream completed", func() bool { return s.Stats().Completed == 1 })
	time.Sleep(20 * time.Millisecond)
	if got := b.count(); got != 1 {
		t.Fatalf("%d pipelines after a clean end, want 1", got)
	}
	if st := s.Stats(); st.Restarts != 0 {
		t.Fatalf("Restarts = %d after a clean end, want 0", st.Restarts)
	}
}

func TestFollow_RestartsAFailedStreamUntilItSucceeds(t *testing.T) {
	b := &builder{failures: 2}
	s := New(context.Background(), Config{
		Streams: only("stdout"), Build: b.build, MinBackoff: time.Millisecond,
	})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1"})

	waitFor(t, "stream completed after retrying", func() bool { return s.Stats().Completed == 1 })
	if st := s.Stats(); st.Restarts != 2 || st.Abandoned != 0 {
		t.Fatalf("Stats() = %+v, want 2 restarts and nothing abandoned", st)
	}
	if got := b.count(); got != 3 {
		t.Fatalf("%d pipelines built, want 3 — a Pipeline is single-use, so a retry needs a fresh one", got)
	}
}

func TestFollow_GivesUpWhenTheRestartBudgetIsSpent(t *testing.T) {
	// Bounded on purpose: every restart resumes from the last durable cursor, so a stream that can
	// never be read would otherwise re-ship its history on a loop and bill us for it.
	b := &builder{failures: 1000}
	var logged int
	var mu sync.Mutex
	s := New(context.Background(), Config{
		Streams: only("stdout"), Build: b.build, MaxRestarts: 3, MinBackoff: time.Millisecond,
		Log: func(string, ...any) { mu.Lock(); logged++; mu.Unlock() },
	})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1"})

	// Both halves, because abandoning silently is the failure worth catching and nothing else reports
	// it — and waiting on the counter alone would race the log it is published alongside.
	waitFor(t, "stream abandoned and said so", func() bool {
		if s.Stats().Abandoned != 1 {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		return logged > 0
	})
	if st := s.Stats(); st.Restarts != 3 || st.Completed != 0 {
		t.Fatalf("Stats() = %+v, want 3 restarts and nothing completed", st)
	}
	// 4 attempts for 3 restarts: the budget counts retries, not tries.
	if got := b.count(); got != 4 {
		t.Fatalf("%d attempts, want 4", got)
	}
}

func TestFollow_CountsABuildFailureAgainstTheSameBudget(t *testing.T) {
	// Both mean the stream is not copying, and the cause is usually the same outage.
	b := &builder{buildErr: errors.New("no credentials")}
	s := New(context.Background(), Config{
		Streams: only("stdout"), Build: b.build, MaxRestarts: 2, MinBackoff: time.Millisecond,
	})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1"})

	waitFor(t, "stream abandoned", func() bool { return s.Stats().Abandoned == 1 })
	if got := b.count(); got != 3 {
		t.Fatalf("%d build attempts, want 3", got)
	}
}

func TestForget_CancelsTheStreamsAndUntracksTheInstance(t *testing.T) {
	b := &builder{block: true}
	s := New(context.Background(), Config{
		Streams: only("stdout", "stderr"), Build: b.build, MinBackoff: time.Millisecond,
	})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1"})
	waitFor(t, "both streams running", func() bool { return b.running() == 2 })

	s.Forget(Ref{ID: "sb-1"})

	waitFor(t, "both streams stopped", func() bool { return b.running() == 0 })
	if st := s.Stats(); st.Instances != 0 || st.Streams != 0 {
		t.Fatalf("Stats() = %+v, want nothing tracked", st)
	}
	// A cancelled stream is neither finished nor broken, so it must not spend the abandon budget.
	if st := s.Stats(); st.Abandoned != 0 || st.Restarts != 0 {
		t.Fatalf("Stats() = %+v, want a cancellation to be neither a restart nor an abandonment", st)
	}
}

func TestForget_IsHarmlessForAnInstanceItNeverKnew(t *testing.T) {
	s := New(context.Background(), Config{Streams: only("stdout"), Build: (&builder{}).build})
	defer s.Shutdown()

	s.Forget(Ref{ID: "sb-unknown"})

	if st := s.Stats(); st.Instances != 0 {
		t.Fatalf("Instances = %d after forgetting an unknown id", st.Instances)
	}
}

func TestStats_SurviveTheStreamThatProducedThem(t *testing.T) {
	// A restart replaces the Pipeline that holds the counters, so retire has to fold them into the
	// fleet total — otherwise a restart resets the numbers a metric is built on.
	b := &builder{failures: 1, lines: 3}
	s := New(context.Background(), Config{
		Streams: only("stdout"), Build: b.build, MinBackoff: time.Millisecond,
	})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1"})

	waitFor(t, "stream completed after retrying", func() bool { return s.Stats().Completed == 1 })
	// Both attempts shipped their lines, and the failed one's counters are still counted.
	if got := s.Stats().Shipping.Lines; got != 6 {
		t.Fatalf("Shipping.Lines = %d across two attempts of 3 lines, want 6", got)
	}
}

func TestStats_CountLiveAndRetiredPipelinesExactlyOnce(t *testing.T) {
	// The live pointer is dropped as the counters are folded in, under one lock. Getting that wrong
	// double-counts every finished stream.
	b := &builder{lines: 2}
	s := New(context.Background(), Config{
		Streams: only("stdout", "stderr"), Build: b.build, MinBackoff: time.Millisecond,
	})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1"})

	waitFor(t, "both streams completed", func() bool { return s.Stats().Completed == 2 })
	if got := s.Stats().Shipping.Lines; got != 4 {
		t.Fatalf("Shipping.Lines = %d for two streams of 2 lines, want 4", got)
	}
	if got := s.Stats().Streams; got != 0 {
		t.Fatalf("Streams = %d after both ended, want 0", got)
	}
}

func TestShutdown_StopsEverythingAndWaits(t *testing.T) {
	b := &builder{block: true}
	s := New(context.Background(), Config{Streams: only("stdout", "stderr"), Build: b.build})

	s.Ensure(Instance{ID: "sb-1"})
	s.Ensure(Instance{ID: "sb-2"})
	waitFor(t, "four streams running", func() bool { return b.running() == 4 })

	s.Shutdown()

	// Shutdown waits, so this needs no polling: a goroutine still running here is a leak.
	if got := b.running(); got != 0 {
		t.Fatalf("%d streams still running after Shutdown returned", got)
	}
	if st := s.Stats(); st.Instances != 0 {
		t.Fatalf("Instances = %d after Shutdown", st.Instances)
	}
}

func TestEnsure_IsRefusedAfterShutdown(t *testing.T) {
	// Ensure adds to the WaitGroup that Shutdown already waited on, so admitting one here would both
	// leak a goroutine and race the wg.
	b := &builder{block: true}
	s := New(context.Background(), Config{Streams: only("stdout"), Build: b.build})
	s.Shutdown()

	s.Ensure(Instance{ID: "sb-1"})

	time.Sleep(20 * time.Millisecond)
	if got := b.count(); got != 0 {
		t.Fatalf("%d pipelines started after Shutdown, want 0", got)
	}
}

func TestNew_FixesUpAnUnusableBackoffRange(t *testing.T) {
	wired := Config{Streams: only("stdout"), Build: (&builder{}).build}

	cfg := wired
	cfg.MinBackoff, cfg.MaxBackoff = time.Minute, time.Millisecond
	s := New(context.Background(), cfg)
	if s.cfg.MaxBackoff < s.cfg.MinBackoff {
		t.Fatalf("MaxBackoff %v < MinBackoff %v", s.cfg.MaxBackoff, s.cfg.MinBackoff)
	}
	d := New(context.Background(), wired)
	if d.cfg.MaxRestarts != DefaultMaxRestarts || d.cfg.MinBackoff != DefaultMinBackoff {
		t.Fatalf("defaults = %d/%v", d.cfg.MaxRestarts, d.cfg.MinBackoff)
	}
}

func TestNew_RefusesAConfigWithNoStreams(t *testing.T) {
	// At startup rather than on the first Pod: an instance whose streams are unknown is one whose logs
	// are simply absent, and this is the one moment that can still be a loud failure.
	defer func() {
		if recover() == nil {
			t.Error("New accepted a Config with no Streams")
		}
	}()
	New(context.Background(), Config{Build: (&builder{}).build})
}

func TestEnsure_SaysSoWhenAnInstanceHasNoStreams(t *testing.T) {
	// The quietest way to ship nothing: tracked, counted, and no pipeline ever built. A provider whose
	// stream list came back empty has to be as loud as one that failed outright.
	b := &builder{}
	var logged int
	s := New(context.Background(), Config{
		Streams: func(Instance) []string { return nil },
		Build:   b.build,
		Log:     func(string, ...any) { logged++ },
	})
	defer s.Shutdown()

	s.Ensure(Instance{ID: "sb-1", Provider: "nowhere"})

	if logged == 0 {
		t.Error("an instance with no streams was skipped without a word")
	}
	if st := s.Stats(); st.Instances != 0 || st.Started != 0 {
		t.Errorf("Stats counts an instance nothing is following: %+v", st)
	}
	if got := b.count(); got != 0 {
		t.Errorf("%d pipelines built, want 0", got)
	}
}

// builder is a Config.Build that hands out real Pipelines over fake ports, and records what it was
// asked for. Real pipelines because Builder returns a concrete *ship.Pipeline, which is also what
// makes the Stats folding worth testing here rather than mocking away.
type builder struct {
	// failures is how many of the first attempts fail before one succeeds; buildErr fails the build
	// itself; block makes a stream run until its context is cancelled; lines is how many lines each
	// attempt ships.
	failures int
	buildErr error
	block    bool
	lines    int

	mu       sync.Mutex
	built    int
	byStream map[string]int
	live     int
	// liveByRef is live broken down by the instance the stream belongs to, for the tests where which
	// instance is still running is the whole question.
	liveByRef map[Ref]int
}

func (b *builder) build(inst Instance, stream string) (*ship.Pipeline, error) {
	b.mu.Lock()
	b.built++
	attempt := b.built
	if b.byStream == nil {
		b.byStream = map[string]int{}
	}
	b.byStream[stream]++
	b.mu.Unlock()

	if b.buildErr != nil {
		return nil, b.buildErr
	}
	return ship.New(ship.Config{
		Source:   &fakeSource{owner: b, ref: inst.Ref(), block: b.block, fail: attempt <= b.failures, lines: b.lines},
		Sink:     fakeSink{},
		Limits:   ship.Limits{MaxEvents: 100, MaxBytes: 1 << 20, MaxEventBytes: 1 << 10},
		Interval: time.Millisecond,
	}), nil
}

func (b *builder) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.built
}

func (b *builder) running() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.live
}

func (b *builder) streams() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]int{}
	for k, v := range b.byStream {
		out[k] = v
	}
	return out
}

func (b *builder) runningFor(ref Ref) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.liveByRef[ref]
}

func (b *builder) enter(ref Ref, delta int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.live += delta
	if b.liveByRef == nil {
		b.liveByRef = map[Ref]int{}
	}
	b.liveByRef[ref] += delta
}

type fakeSource struct {
	owner *builder
	ref   Ref
	block bool
	fail  bool
	lines int
}

func (s *fakeSource) Follow(ctx context.Context, cursor string, fn func(ship.Batch) error) error {
	s.owner.enter(s.ref, 1)
	defer s.owner.enter(s.ref, -1)

	for i := range s.lines {
		err := fn(ship.Batch{
			Entries: []ship.Entry{{Data: fmt.Sprintf("line %d\n", i), At: time.Now()}},
			Cursor:  fmt.Sprintf("%d-0", i),
		})
		if err != nil {
			return err
		}
	}
	if s.fail {
		return errors.New("stream broke")
	}
	if s.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

type fakeSink struct{}

func (fakeSink) Put(context.Context, []ship.Event) error { return nil }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
