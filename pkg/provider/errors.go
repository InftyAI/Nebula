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
	"context"
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
	// ErrImage: the provider could not obtain the image it was asked to run — a credential it
	// cannot honour, one it was not given, a registry that refused it, or a build that failed.
	ErrImage = errors.New("provider: cannot obtain image")
)

// ClassifyError maps a provision error to the BlockScope it should be blocklisted at,
// checking the shared sentinels first and falling back to string heuristics for raw API
// messages. The rule it encodes: a narrow failure must not disqualify other accelerators, or
// other regions. Only auth widens to the whole provider via DenyAll; a capacity refusal is
// scoped to the failing accelerator/tier/region so failover can route around it; and two cases
// block nothing at all — a failure that belongs to the REQUEST rather than the candidate, and
// one that could not be attributed to either.
//
// The zero scope is the ONLY signal for "block nothing": recordBlock no-ops on it, so callers
// classify every failure and act on the result instead of pre-filtering with a second
// predicate.
//
// This is the single place the SHARED part of a scope is derived, so every adapter delegates
// here and then only adds what is provider-specific (AWS adds its region). Nothing assembles
// a scope elsewhere. An adapter that decorates the result must leave the ZERO scope alone —
// it means "block nothing", and any field added to it becomes a block.
//
// capacityType is stamped onto accelerator-scoped blocks so a Spot failure does not block
// OnDemand. accelerator is the request's POOL identity (type:count, "" for a CPU-only Pod),
// not the provider's SKU id, so the block stays truthful when a launch spans several
// interchangeable instance types: non-empty becomes an exact match, "" leaves it nil so the
// block does not widen across every accelerator. DenyAll ignores both.
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

	switch categorize(err) {
	case catAuth:
		return BlockScope{DenyAll: true}
	case catCapacity:
		return capacityScope()
	case catRequest:
		// Nothing is blocklisted: the request was refused, the candidate is fine. A block
		// here would be recorded against provider/accelerator/tier/region — a key with no
		// Pod, image or credential in it — so one Pod's unusable pull credential would
		// exclude that candidate for every OTHER Pod until the TTL lapsed.
		//
		// The zero scope makes recordBlock a no-op, which is the whole mechanism. An adapter
		// must therefore not decorate this scope (see ClassifyProvisionError in each
		// adapter): adding a region would make it non-empty and install a region-wide block.
		//
		// The Pod still fails with the reason rather than retrying forever; that is the
		// caller's doing (see vnode.Handler.CreatePod), not this scope's.
		return BlockScope{}
	case catUnattributable:
		// Nothing is blocklisted, and for a different reason than catRequest above: there the
		// candidate is known to be innocent, here nothing at all was learned about it. A
		// cancelled context or a dropped connection is no evidence against a candidate, so
		// recording one would exclude a provider for a failure that was never its doing.
		//
		// This zero is what makes the scope the SINGLE answer to "should anything be blocked":
		// callers hand every failure to recordBlock and let the scope decide, rather than
		// gating on a second predicate that could disagree with it.
		return BlockScope{}
	default:
		// A category added to the enum but not handled above. Scoped like capacity (this
		// accelerator + tier, and per region once the adapter confines it) rather than zero,
		// because zero is a CLAIM — that the candidate is fine — which an unreasoned-about
		// category has not earned. NOT DenyAll either: that fences off every region and
		// accelerator on the provider, far too wide a blast radius to reach by omission.
		return capacityScope()
	}
}

// IsDeadline reports whether err is a provision deadline that fired.
func IsDeadline(err error) bool {
	return err != nil && (errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(strings.ToLower(err.Error()), "context deadline exceeded"))
}

// failureCategory is the internal classification ClassifyError drives off.
type failureCategory int

const (
	// catUnattributable: nothing in the error says what the provider decided, because
	// it may not have decided anything — a transport failure, a cancellation, an
	// unparseable API blip. It blocklists nothing: no candidate can be held responsible
	// for a failure nobody could attribute to it.
	catUnattributable failureCategory = iota
	// catAuth: credentials or authorization failed, so nothing on the provider works.
	catAuth
	// catCapacity: the provider refused this request because of something about the
	// CANDIDATE — no capacity, quota exhausted, an accelerator it does not offer, or the whole
	// provision budget spent without a usable instance. Blocking it is meaningful, because the
	// next Pod asking for the same candidate would fail the same way.
	catCapacity
	// catRequest: the provider refused this request because of something about the REQUEST
	// itself — today an image it cannot pull or cannot build. A decision, so a rejection, but
	// it says nothing about the candidate and must not blocklist one.
	catRequest
)

// categorize buckets a provision error: the deadline and cancellation checks first — they
// outrank any label an adapter attached, since an adapter cannot see whose clock fired — then
// the sentinels, then string heuristics.
func categorize(err error) failureCategory {
	msg := strings.ToLower(err.Error())

	// A deadline is a capacity failure, not an unknown: the candidate was given the whole
	// provision budget and did not produce a usable instance.
	if IsDeadline(err) {
		return catCapacity
	}

	// Cancellation stays unattributable, unlike the deadline above: it means WE stopped asking
	// — a manager shutdown, a leader handoff — and the provider may well have accepted the
	// request. Nothing about the candidate was learned, so blocklisting would punish it for
	// our own exit. Only the block scope; the caller still fails the Pod.
	if errors.Is(err, context.Canceled) || containsAny(msg, "context canceled") {
		return catUnattributable
	}

	switch {
	case errors.Is(err, ErrAuth):
		return catAuth
	case errors.Is(err, ErrImage):
		return catRequest
	case errors.Is(err, ErrNoCapacity), errors.Is(err, ErrUnsupportedAccelerator),
		errors.Is(err, ErrQuota):
		return catCapacity
	}

	if containsAny(msg,
		"rpc error", "connection refused", "connection reset", "broken pipe",
		"no such host", "i/o timeout", "eof", "tls handshake",
		"service unavailable", "bad gateway", "gateway timeout", "internal server error") {
		return catUnattributable
	}

	switch {
	case containsAny(msg, "unauthorized", "forbidden", "authentication", "invalid token", "api key"):
		return catAuth
	case containsAny(msg, "quota", "limit exceeded", "rate limit"):
		return catCapacity
	case containsAny(msg, "no capacity", "capacity", "unavailable", "out of", "no gpu"):
		return catCapacity
	default:
		return catUnattributable
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
