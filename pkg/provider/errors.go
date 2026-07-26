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
	// ErrQuota: account quota/limit reached. Whole-provider until quota frees up.
	ErrQuota = errors.New("provider: quota exceeded")
)

// ClassifyError maps a provision error to the BlockScope it should be
// blocklisted at, using the shared sentinels first and a string-heuristic
// fallback for raw API messages that were not wrapped. It encodes the rule that
// a narrow failure (this accelerator has no capacity) must not disqualify other
// accelerators on the same provider, while auth/quota widen to the whole
// provider; an unrecognized error is treated conservatively as whole-provider so
// a failure does not hot-loop (the blocklist TTL bounds the blast radius).
//
// capacityType is the tier the failing request used; it is stamped onto
// accelerator-scoped scopes so the block is precise (a Spot failure does not
// block OnDemand). Adapters call this from ClassifyProvisionError after wrapping
// their own errors, passing the only tier they serve when they are single-tier.
func ClassifyError(err error, capacityType nebulav1alpha1.CapacityType) BlockScope {
	if err == nil {
		return BlockScope{}
	}

	switch {
	case errors.Is(err, ErrAuth), errors.Is(err, ErrQuota):
		return BlockScope{DenyAll: true}
	case errors.Is(err, ErrNoCapacity), errors.Is(err, ErrUnsupportedAccelerator):
		return BlockScope{CapacityType: capacityType}
	}

	// Fall back to string heuristics for errors not wrapped with a sentinel.
	msg := strings.ToLower(err.Error())
	switch {
	case containsAny(msg, "unauthorized", "forbidden", "authentication", "invalid token", "api key"):
		return BlockScope{DenyAll: true}
	case containsAny(msg, "quota", "limit exceeded", "rate limit"):
		return BlockScope{DenyAll: true}
	case containsAny(msg, "no capacity", "capacity", "unavailable", "out of", "no gpu"):
		return BlockScope{CapacityType: capacityType}
	default:
		return BlockScope{DenyAll: true}
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
