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
// entry's scope MATCHES it. Empty scope fields are wildcards, so "no Spot H100 in
// us-east-1" never disqualifies an OnDemand request, another accelerator, or another
// region — while an auth failure (DenyAll) blocks the whole provider until its TTL
// lapses.
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

// entry is one recorded block: the provider it applies to, the scope that failed,
// and when the block lapses. It is compared against a query candidate by Blocked.
type entry struct {
	provider  string
	scope     provider.BlockScope
	expiresAt time.Time
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

// Record adds a block for prov at scope, lapsing after ttl. A DenyAll scope blocks the
// whole provider; anything else is confined to its matching fields. A non-positive ttl
// or empty provider is a no-op — defensive, so a misfire cannot install a permanent or
// provider-less block. Recording also drops expired entries, bounding the slice.
//
// Records COALESCE: an existing live entry with the same provider and an identical
// scope has its expiry extended (never shortened) instead of gaining a duplicate. So
// many Pods failing for the same reason produce ONE entry — the slice is bounded by
// distinct live scopes, not failure volume. Behaviour is unchanged, since Blocked/
// BlockedUntil already used the latest covering expiry.
func (b *Blocklist) Record(prov string, scope provider.BlockScope, ttl time.Duration) {
	if prov == "" || ttl <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	b.gcLocked(now)

	expiresAt := now.Add(ttl)
	// Coalesce into an existing live entry for the same scope. gcLocked has already
	// dropped expired entries, so every entry scanned here is still live.
	for i := range b.entries {
		if b.entries[i].provider == prov && scopeEqual(b.entries[i].scope, scope) {
			if expiresAt.After(b.entries[i].expiresAt) {
				b.entries[i].expiresAt = expiresAt // extend; never shorten a live block
			}
			return
		}
	}
	b.entries = append(b.entries, entry{
		provider:  prov,
		scope:     scope,
		expiresAt: expiresAt,
	})
}

// Blocked reports whether c is currently excluded by any live entry. It is the
// query the placement loop makes before selecting a candidate: a true result means
// "skip this (provider, accelerator, tier, region) and try the next one".
func (b *Blocklist) Blocked(c Candidate) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	b.gcLocked(now)
	for i := range b.entries {
		if b.entries[i].provider == c.Provider && scopeCovers(b.entries[i].scope, c) {
			return true
		}
	}
	return false
}

// BlockedUntil reports whether c is currently excluded and, if so, how long until
// it could become servable — the duration until the LATEST covering entry lapses
// (c stays blocked while ANY covering entry is live, so it frees only when the
// last one expires). Placement uses this to requeue a gated Pod exactly when a
// candidate frees, instead of leaving it idle until the periodic resync. A false
// result (retryAfter 0) means c is not blocked now.
func (b *Blocklist) BlockedUntil(c Candidate) (retryAfter time.Duration, blocked bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	b.gcLocked(now)
	var latest time.Time
	for i := range b.entries {
		if b.entries[i].provider == c.Provider && scopeCovers(b.entries[i].scope, c) {
			if b.entries[i].expiresAt.After(latest) {
				latest = b.entries[i].expiresAt
			}
		}
	}
	if latest.IsZero() {
		return 0, false
	}
	// gcLocked has dropped every expired entry, so latest is strictly after now.
	return latest.Sub(now), true
}

// gcLocked drops entries that have expired as of now. Caller holds b.mu.
func (b *Blocklist) gcLocked(now time.Time) {
	if len(b.entries) == 0 {
		return
	}
	kept := b.entries[:0]
	for _, e := range b.entries {
		if e.expiresAt.After(now) {
			kept = append(kept, e)
		}
	}
	// Zero the tail so dropped entries are not retained by the backing array.
	for i := len(kept); i < len(b.entries); i++ {
		b.entries[i] = entry{}
	}
	b.entries = kept
}

// scopeCovers reports whether scope (a recorded block) applies to candidate c. DenyAll
// covers the whole provider; otherwise every field must match, per BlockScope's
// three-state pointer semantics:
//
//   - nil   => axis not applicable: matches ONLY a candidate whose field is empty, so
//     a CPU-only Pod or a region-simple provider blocks without widening.
//   - &""   => wildcard: matches any value on that axis.
//   - &"v"  => exact: matches only a candidate whose field equals "v".
//
// This is the match half of the granularity rule: as broad as the failure, no broader.
func scopeCovers(scope provider.BlockScope, c Candidate) bool {
	if scope.DenyAll {
		return true
	}
	if !ptrMatches(scope.Accelerator, c.Accelerator) {
		return false
	}
	if scope.CapacityType != "" && scope.CapacityType != c.CapacityType {
		return false
	}
	if !ptrMatches(scope.Region, c.Region) {
		return false
	}
	return true
}

// scopeEqual reports whether two BlockScopes are the SAME block — identical along
// every axis. It is used by Record to coalesce a repeated failure into an existing
// entry. The *string fields are compared by VALUE, not pointer identity: two
// independent failures for the same region allocate distinct *string pointers, so a
// pointer-wise "==" on BlockScope would never coalesce them (and pointer fields make
// "==" unsafe anyway). This is stricter than scopeCovers, which asks "does this block
// COVER that candidate"; here we need exact equality so distinct scopes stay distinct
// entries.
func scopeEqual(a, b provider.BlockScope) bool {
	return a.DenyAll == b.DenyAll &&
		a.CapacityType == b.CapacityType &&
		ptrEqual(a.Accelerator, b.Accelerator) &&
		ptrEqual(a.Region, b.Region)
}

// ptrEqual reports whether two *string are equal by value: both nil, or both non-nil
// pointing at equal strings. (nil and &"" are DIFFERENT — nil means "axis not
// applicable" while &"" means "wildcard", a distinction BlockScope depends on.)
func ptrEqual(x, y *string) bool {
	if x == nil || y == nil {
		return x == y // equal only if both nil
	}
	return *x == *y
}

// ptrMatches applies BlockScope's three-state rule to one axis: nil matches only an
// empty candidate value (the axis is not applicable), &"" is a wildcard that matches
// anything, and &"v" matches only the exact value.
func ptrMatches(pattern *string, value string) bool {
	if pattern == nil {
		return value == ""
	}
	if *pattern == "" {
		return true
	}
	return *pattern == value
}
