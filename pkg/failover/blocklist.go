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

// Package failover holds the in-memory, TTL-bounded blocklist that turns a single
// provision failure into a temporary exclusion, so placement can fail over to the
// next candidate (zone → region → tier) instead of hot-looping against a provider
// that just rejected the same request.
//
// The design mirrors SkyPilot's blocklist-granularity rule: a failure is recorded
// at the exact granularity that failed (a provider.BlockScope), and a candidate is
// blocked only if some live entry's scope MATCHES it. Empty scope fields are
// wildcards, so "no Spot capacity for H100 in us-east-1" never disqualifies an
// OnDemand request, a different accelerator, or another region — but an auth
// failure (DenyAll) blocks the whole provider until its TTL lapses.
//
// It is deliberately in-memory and per-process: the blocklist is a hint that
// bounds churn, not durable state. On a manager restart it is empty and the next
// attempt re-probes the provider — the worst case is one wasted attempt, which the
// provider itself rejects and re-records. This keeps the control plane free of a
// blocklist CRD and the reconcile hot-path free of an API round-trip.
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
type Candidate struct {
	Provider        string
	AcceleratorType string
	CapacityType    nebulav1alpha1.CapacityType
	Region          string
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

// Record adds a block for prov at scope, lapsing after ttl. A DenyAll scope blocks
// the whole provider; a scoped block confines it to the matching wildcard fields.
// A non-positive ttl or an empty provider is a no-op (nothing to bound, or nothing
// to key on) — the caller is expected to skip zero scopes, but Record is defensive
// so a misfire cannot install a permanent or provider-less block. Recording also
// opportunistically drops expired entries so the slice cannot grow without bound.
func (b *Blocklist) Record(prov string, scope provider.BlockScope, ttl time.Duration) {
	if prov == "" || ttl <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	b.gcLocked(now)
	b.entries = append(b.entries, entry{
		provider:  prov,
		scope:     scope,
		expiresAt: now.Add(ttl),
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

// scopeCovers reports whether scope (a recorded block) applies to candidate c.
// DenyAll covers everything on the provider. Otherwise each field must match the
// candidate's, per the three-state semantics of BlockScope's pointer fields:
//
//   - nil   => the axis is not applicable: it matches ONLY a candidate whose field
//     is empty. A CPU-only Pod (no accelerator) or a region-simple provider (no
//     region) blocks without widening across an axis it never had.
//   - &""   => wildcard: matches any value on that axis.
//   - &"v"  => exact: matches only a candidate whose field equals "v".
//
// This is the match half of the blocklist-granularity rule: the block is as broad
// as the failure was, no broader.
func scopeCovers(scope provider.BlockScope, c Candidate) bool {
	if scope.DenyAll {
		return true
	}
	if !ptrMatches(scope.AcceleratorType, c.AcceleratorType) {
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
