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

// Package failover holds the in-memory, TTL-bounded blocklist that turns a provision
// failure into a temporary exclusion, so placement fails over to the next candidate
// (zone → region → tier) instead of hot-looping against a provider that just said no.
//
// It follows SkyPilot's blocklist-granularity rule: record at the exact granularity
// that failed (a provider.BlockScope), and block a candidate only if some live
// entry's scope MATCHES it. So "no Spot H100 in us-east-1" (DenyCandidate) never
// disqualifies an OnDemand request, another accelerator, or another region — while an
// auth failure (DenyAll) blocks the whole provider until its TTL lapses.
//
// A lapsed block is HALF-OPEN rather than gone: the first placement to ask is admitted as
// a probe (see Admit), and the candidate stays blocked for everyone else for ProbeWindow.
// A refused probe is recorded like any failure and blocks again; a ProbeWindow with no
// failure means it was not refused, and the candidate opens. So a candidate that is still
// out costs one attempt per expiry, not one per waiting Pod.
//
// In-memory and per-process on purpose: this is a hint that bounds churn, not durable
// state. After a restart it is empty and the next attempt re-probes the provider,
// costing at worst one rejected attempt that is then re-recorded. No blocklist CRD, no
// API round-trip on the reconcile hot path.
package failover

import (
	"sync"
	"time"

	"k8s.io/utils/clock"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// ProbeWindow is how long a half-open candidate's one probe has to be refused before the
// candidate opens for everyone. Long enough for most refusals: a synchronous provider's
// (AWS, Modal) fails Provision itself, and Karpenter's arrives within ~3-8s, poll included,
// while it still remembers the shortage. One that rechecks EC2 can take ~30s; such a late
// refusal only lets the waiting Pods in as before, never worse.
const ProbeWindow = 15 * time.Second

// entry is one recorded block: the provider it applies to, the scope that failed, when the
// block lapses, and, once half-open, until when its admitted probe holds the others back.
type entry struct {
	provider   string
	scope      provider.BlockScope
	expiresAt  time.Time
	probeUntil time.Time // zero until a probe is admitted
}

// heldUntil is when e stops holding candidates back: its expiry while blocked, its probe
// window while probing, zero once half-open with no probe admitted.
func (e entry) heldUntil(now time.Time) time.Time {
	switch {
	case now.Before(e.expiresAt):
		return e.expiresAt
	case now.Before(e.probeUntil):
		return e.probeUntil
	}
	return time.Time{}
}

// Candidate is a placement being considered — the (provider, accelerator, tier,
// region) tuple the blocklist is queried against. It is the query counterpart to a
// recorded BlockScope: Blocked reports whether any live entry's scope covers it.
// Accelerator is the request's POOL identity (type:count, e.g. "H100:8"), not the
// provider's SKU id, so a block recorded against one (type, count) pool matches
// only candidates that share it — and stays stable even when a launch spans
// several interchangeable provider instance types.
type Candidate struct {
	Provider     string
	Accelerator  string
	CapacityType nebulav1alpha1.CapacityType
	Region       string
}

// Blocklist is a concurrency-safe, TTL-bounded set of provider blocks. The zero
// value is not usable; construct with New. It is safe for concurrent use by the
// vnode handlers (which Record failures) and the placement controller (which
// queries Blocked) sharing one instance.
type Blocklist struct {
	mu      sync.Mutex
	entries []entry
	clock   clock.Clock
}

// New returns an empty Blocklist backed by the real clock.
func New() *Blocklist {
	return &Blocklist{clock: clock.RealClock{}}
}

// newWithClock is the test seam: an injectable clock so TTL expiry is exercised
// without sleeping.
func newWithClock(c clock.Clock) *Blocklist {
	return &Blocklist{clock: c}
}

// Record adds a block for prov at scope, lapsing after ttl; scope.Deny says how much of the
// provider it covers (see provider.DenyPolicy). A non-positive ttl, an empty provider or a
// scope with no known policy is a no-op — defensive, so a misfire cannot install a
// permanent, provider-less or unmatchable block. Recording also drops expired entries,
// bounding the slice.
//
// Records COALESCE: an existing entry with the same provider and an identical scope has its
// expiry extended (never shortened) instead of gaining a duplicate. So many Pods failing for
// the same reason produce ONE entry — the slice is bounded by distinct live scopes, not
// failure volume. A half-open entry, its probe refused, is blocked again for ttl.
func (b *Blocklist) Record(prov string, scope provider.BlockScope, ttl time.Duration) {
	if prov == "" || ttl <= 0 || !knownPolicy(scope.Deny) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	b.gcLocked(now)

	expiresAt := now.Add(ttl)
	for i := range b.entries {
		if e := &b.entries[i]; e.provider == prov && e.scope == scope {
			if expiresAt.After(e.expiresAt) {
				e.expiresAt = expiresAt // extend; never shorten a live block
			}
			e.probeUntil = time.Time{}
			return
		}
	}
	b.entries = append(b.entries, entry{
		provider:  prov,
		scope:     scope,
		expiresAt: expiresAt,
	})
}

// Blocked reports whether c is currently excluded, without admitting a probe: a half-open
// candidate reads as not blocked. Placement asks Admit instead.
func (b *Blocklist) Blocked(c Candidate) bool {
	_, blocked := b.BlockedUntil(c)
	return blocked
}

// BlockedUntil reports whether c is currently excluded and, if so, how long until it could
// become servable: until the LATEST covering entry stops holding it (its expiry, or its
// probe's window). It never admits a probe, so it is safe for anything but placement.
func (b *Blocklist) BlockedUntil(c Candidate) (retryAfter time.Duration, blocked bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	b.gcLocked(now)
	return b.heldLocked(c, now)
}

// Admit is the placement loop's query: whether c may be tried now, and if not, how long
// until it could be. A candidate held back by any covering entry is not admitted. One that
// is half-open is admitted as the probe: every covering half-open entry then holds the
// others back for ProbeWindow, until a refusal (Record) blocks it again or the window ends.
func (b *Blocklist) Admit(c Candidate) (retryAfter time.Duration, admitted bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	b.gcLocked(now)
	if until, held := b.heldLocked(c, now); held {
		return until, false
	}
	for i := range b.entries {
		if e := &b.entries[i]; e.provider == c.Provider && scopeCovers(e.scope, c) {
			e.probeUntil = now.Add(ProbeWindow)
		}
	}
	return 0, true
}

func (b *Blocklist) heldLocked(c Candidate, now time.Time) (time.Duration, bool) {
	var latest time.Time
	for _, e := range b.entries {
		if e.provider == c.Provider && scopeCovers(e.scope, c) {
			if until := e.heldUntil(now); until.After(latest) {
				latest = until
			}
		}
	}
	if latest.IsZero() {
		return 0, false
	}
	return latest.Sub(now), true
}

// gcLocked drops the entries whose probe window passed without a refusal: the candidate is
// open. An unprobed half-open entry stays, so the next placement probes it; entries stay
// bounded by distinct scopes. Caller holds b.mu.
func (b *Blocklist) gcLocked(now time.Time) {
	if len(b.entries) == 0 {
		return
	}
	kept := b.entries[:0]
	for _, e := range b.entries {
		if e.probeUntil.IsZero() || now.Before(e.probeUntil) {
			kept = append(kept, e)
		}
	}
	// Zero the tail so dropped entries are not retained by the backing array.
	for i := len(kept); i < len(b.entries); i++ {
		b.entries[i] = entry{}
	}
	b.entries = kept
}

// scopeCovers reports whether scope (a recorded block) applies to candidate c, per its
// policy: the whole provider, one region, or one candidate. This is the match half of the
// granularity rule: as broad as the failure, no broader.
func scopeCovers(scope provider.BlockScope, c Candidate) bool {
	switch scope.Deny {
	case provider.DenyAll:
		return true
	case provider.DenyRegion:
		return scope.Region == c.Region
	case provider.DenyCandidate:
		return scope.Accelerator == c.Accelerator &&
			(scope.CapacityType == "" || scope.CapacityType == c.CapacityType) &&
			scope.Region == c.Region
	default:
		return false
	}
}

func knownPolicy(p provider.DenyPolicy) bool {
	switch p {
	case provider.DenyAll, provider.DenyRegion, provider.DenyCandidate:
		return true
	}
	return false
}
