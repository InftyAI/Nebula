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

package provider

import (
	"errors"
	"strings"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// Provision failure categories, shared by every adapter. The CATEGORIES are
// universal — no-capacity, unsupported, auth, quota describe outcomes any
// NeoCloud can return — so they live here, not in a single adapter. Each adapter
// recognizes its provider-specific API conditions and wraps the matching
// sentinel (via fmt.Errorf("...: %w", provider.ErrX)); the control plane can then
// errors.Is against these without importing any adapter, and BlockScope is
// derived uniformly by ClassifyError below.
var (
	// ErrNoCapacity: the provider could not allocate the requested accelerator
	// right now. Accelerator-scoped and transient.
	ErrNoCapacity = errors.New("provider: no capacity for requested accelerator")
	// ErrUnsupportedAccelerator: the provider does not offer the accelerator at
	// all. Accelerator-scoped, durable until the pool changes.
	ErrUnsupportedAccelerator = errors.New("provider: unsupported accelerator")
	// ErrAuth: credentials/authorization failed. Whole-provider — nothing will
	// succeed until it is fixed.
	ErrAuth = errors.New("provider: authentication failed")
	// ErrQuota: a resource quota/limit was reached. Scoped like a capacity failure
	// (accelerator + tier), NOT whole-provider: cloud quotas are per-resource and,
	// for a multi-region adapter, per-region — e.g. an AWS vCPU limit is a regional,
	// per-instance-family, per-tier ceiling, so exhausting it in one region says
	// nothing about the same request in another. The adapter confines it to the
	// failing region (see aws.ClassifyProvisionError). Transient until quota frees up.
	ErrQuota = errors.New("provider: quota exceeded")
)

// ClassifyError maps a provision error to the BlockScope it should be
// blocklisted at, using the shared sentinels first and a string-heuristic
// fallback for raw API messages that were not wrapped. It encodes the rule that
// a narrow failure (this accelerator has no capacity, or its quota is exhausted)
// must not disqualify other accelerators — or, for a multi-region adapter, other
// regions — on the same provider. Only auth (which fails everywhere) widens to the
// whole provider via DenyAll; every other case — including an unrecognized error —
// is scoped to the failing accelerator/tier/region so failover can route around it
// rather than fencing off the entire provider (the blocklist TTL bounds it either
// way).
//
// It is the single place the SHARED part of a scope is derived — category
// (DenyAll vs capacity-scoped), the capacity tier, and the accelerator pool — so
// every adapter's ClassifyProvisionError delegates here and then only decorates
// with what is provider-specific (AWS adds its region). No scope is assembled
// anywhere else: the vnode handler resolves the accelerator pool off the Pod and
// passes it in, rather than mutating the returned scope.
//
// capacityType is the tier the failing request used; it is stamped onto
// accelerator-scoped scopes so the block is precise (a Spot failure does not
// block OnDemand). accelerator is the request's POOL identity (type:count, e.g.
// "H100:8"; "" for a CPU-only Pod), NOT the provider's SKU id — so the block stays
// truthful when a launch spans several interchangeable instance types: on a
// capacity-scoped block a non-empty pool becomes an exact-match pointer (narrowing
// the block to the (type, count) pool that actually failed), while "" leaves
// Accelerator nil ("not applicable") so the block does not widen across every
// accelerator. Auth (DenyAll) ignores both, since bad credentials fail for
// every accelerator and tier; quota is scoped like capacity (per accelerator and
// tier, and per region once the adapter confines it).
func ClassifyError(err error, capacityType nebulav1alpha1.CapacityType, accelerator string) BlockScope {
	if err == nil {
		return BlockScope{}
	}

	// capacityScope builds an accelerator/tier-scoped block, promoting a non-empty
	// accelerator pool to an exact-match pointer and leaving it nil otherwise.
	capacityScope := func() BlockScope {
		s := BlockScope{CapacityType: capacityType}
		if accelerator != "" {
			s.Accelerator = &accelerator
		}
		return s
	}

	switch {
	case errors.Is(err, ErrAuth):
		return BlockScope{DenyAll: true}
	case errors.Is(err, ErrNoCapacity), errors.Is(err, ErrUnsupportedAccelerator), errors.Is(err, ErrQuota):
		return capacityScope()
	}

	// Fall back to string heuristics for errors not wrapped with a sentinel.
	msg := strings.ToLower(err.Error())
	switch {
	case containsAny(msg, "unauthorized", "forbidden", "authentication", "invalid token", "api key"):
		return BlockScope{DenyAll: true}
	case containsAny(msg, "quota", "limit exceeded", "rate limit"):
		return capacityScope()
	case containsAny(msg, "no capacity", "capacity", "unavailable", "out of", "no gpu"):
		return capacityScope()
	default:
		// An unrecognized error is scoped like capacity (this accelerator + tier, and
		// per region once the adapter confines it), NOT DenyAll. A DenyAll on an
		// unknown error fences off the WHOLE provider — every region and accelerator —
		// which is far too broad a blast radius for a failure we can't even identify
		// (e.g. a transient malformed-request blip in one region). Failover past the
		// one failing candidate is the safer default; the TTL still bounds it.
		return capacityScope()
	}
}

// containsAny reports whether s contains any of subs.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
