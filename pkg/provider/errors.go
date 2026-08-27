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
	// ErrImagePull: the image could not be pulled — a credential the provider cannot honour,
	// one it was not given, or a registry that refused it.
	//
	// REQUEST-scoped, and the only sentinel that is: it describes the Pod, not the candidate.
	// So it blocklists NOTHING (see ClassifyError). Both alternatives are wrong — ErrAuth
	// widens to DenyAll, fencing off the whole provider because one Pod named a role it
	// cannot assume, and a capacity scope evicts an accelerator/tier/region that is serving
	// other Pods perfectly well, since the blocklist key carries no Pod, image or credential
	// identity. A failure that belongs to one request cannot be recorded against a candidate.
	ErrImagePull = errors.New("provider: cannot pull image")
)

// ClassifyError maps a provision error to the BlockScope it should be blocklisted at,
// checking the shared sentinels first and falling back to string heuristics for raw API
// messages. The rule it encodes: a narrow failure must not disqualify other accelerators, or
// other regions. Only auth widens to the whole provider via DenyAll; a failure that belongs to
// the REQUEST rather than the candidate blocks nothing at all; everything else — including an
// unrecognized error — is scoped to the failing accelerator/tier/region so failover can route
// around it.
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
		// Still a rejection, so the Pod fails with the reason rather than retrying forever —
		// the two questions are separate. See IsRejection.
		return BlockScope{}
	default:
		// An unrecognized error is scoped like capacity (this accelerator + tier, and
		// per region once the adapter confines it), NOT DenyAll. A DenyAll on an
		// unknown error fences off the WHOLE provider — every region and accelerator —
		// which is far too broad a blast radius for a failure we can't even identify
		// (e.g. a transient malformed-request blip in one region). Failover past the
		// one failing candidate is the safer default; the TTL still bounds it.
		//
		// This answers "how widely, IF we block", not "should we block": a caller that
		// cannot attribute the failure to the request at all should not be filing a block
		// in the first place — see IsRejection.
		return capacityScope()
	}
}

// failureCategory is the internal classification ClassifyError and IsRejection both
// drive off, so the two can never disagree about whether an error was recognized.
type failureCategory int

const (
	// catUnattributable: nothing in the error says what the provider decided, because
	// it may not have decided anything — a transport failure, a timeout, an
	// unparseable API blip. Kept distinct from the scope ClassifyError ultimately
	// returns for it.
	catUnattributable failureCategory = iota
	// catAuth: credentials or authorization failed, so nothing on the provider works.
	catAuth
	// catCapacity: the provider refused this request because of something about the
	// CANDIDATE — no capacity, quota exhausted, an accelerator it does not offer. Blocking
	// it is meaningful, because the next Pod asking for the same candidate would fail too.
	catCapacity
	// catRequest: the provider refused this request because of something about the REQUEST
	// itself — today only an image it cannot pull. A decision, so a rejection, but it says
	// nothing about the candidate and must not blocklist one.
	catRequest
)

// categorize buckets a provision error, sentinels first and string heuristics after.
func categorize(err error) failureCategory {
	// Sentinels first. An adapter that wrapped one has made an explicit decision, and
	// it outranks anything the raw message text happens to contain.
	switch {
	case errors.Is(err, ErrAuth):
		return catAuth
	case errors.Is(err, ErrImagePull):
		return catRequest
	case errors.Is(err, ErrNoCapacity), errors.Is(err, ErrUnsupportedAccelerator),
		errors.Is(err, ErrQuota):
		return catCapacity
	}

	msg := strings.ToLower(err.Error())

	// Transport and timeout markers are checked BEFORE the category heuristics,
	// because they are the failures those heuristics most reliably MISREAD: a gRPC
	// status renders as "rpc error: code = Unavailable desc = ...", whose
	// "unavailable" would otherwise match the capacity bucket below and turn "we could
	// not reach the provider" into "the provider has no capacity" — the exact
	// misattribution IsRejection exists to prevent. A deadline is unattributable for a
	// second reason too: our own ProvisionTimeout can fire on a call the provider went
	// on to honour, so the instance may well exist.
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return catUnattributable
	case containsAny(msg,
		"rpc error", "connection refused", "connection reset", "broken pipe",
		"no such host", "i/o timeout", "eof", "tls handshake",
		"service unavailable", "bad gateway", "gateway timeout", "internal server error"):
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

// IsRejection reports whether err is a provider DECISION about this request — "no
// capacity", "over quota", "bad credentials", "I do not offer that accelerator" — as
// opposed to a failure to find out what the provider would have decided: a transport
// error, a timeout, a 503, an unparseable response.
//
// The distinction exists because the two call for opposite handling and the costs are
// asymmetric. A rejection is authoritative, so the Pod is failed with the reason — correct,
// because the provider said no. Whether a CANDIDATE is also blocklisted is a separate
// question, answered by ClassifyError: a capacity rejection blocks one and failover routes
// around it, while a request-scoped one (an unusable image credential) blocks nothing, because
// the candidate did nothing wrong. An unattributable failure is authoritative about nothing,
// so the same writes
// stamp a terminal status onto a request the provider may have accepted (leaving a
// paid instance running behind a Pod that is about to be reaped) and fence off a
// candidate that never misbehaved. Retrying costs a request and Provision is
// idempotent on ClaimName; acting on a guess costs an instance.
//
// A nil error is not a rejection.
func IsRejection(err error) bool {
	return err != nil && categorize(err) != catUnattributable
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
