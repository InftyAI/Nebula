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
	"fmt"
	"reflect"
	"testing"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

func TestClassifyError(t *testing.T) {
	const tier = nebulav1alpha1.CapacityOnDemand
	const accel = "H100"
	// capacityScope is the expected accelerator-scoped block: the accelerator id is
	// promoted to an exact-match pointer.
	capacityScope := BlockScope{CapacityType: tier, Accelerator: ptrStr(accel)}
	tests := []struct {
		name string
		err  error
		want BlockScope
	}{
		{"nil", nil, BlockScope{}},
		{"auth sentinel", ErrAuth, BlockScope{DenyAll: true}},
		// Quota is scoped like capacity (per accelerator + tier), NOT DenyAll: cloud
		// quotas are per-resource and, for a multi-region adapter, per-region, so one
		// exhausted quota must not fence off other regions/accelerators.
		{"quota sentinel", ErrQuota, capacityScope},
		{"no-capacity sentinel", ErrNoCapacity, capacityScope},
		{"unsupported sentinel", ErrUnsupportedAccelerator, capacityScope},
		// An unusable image credential belongs to the POD. The blocklist key carries no Pod,
		// image or credential identity, so ANY scope here would exclude the candidate for
		// unrelated Pods that pull perfectly well — hence the zero scope, which blocks
		// nothing. Still a rejection: see TestIsRejection.
		{"image-pull sentinel blocks nothing", ErrImagePull, BlockScope{}},
		{
			"wrapped image-pull sentinel blocks nothing",
			fmt.Errorf("modal: unsupported image pull credential: %w", ErrImagePull),
			BlockScope{},
		},
		// Same scope as a pull failure, and it must stay that way: the image is the Pod's,
		// and a builder that refused this one has nothing to say about the candidate. The
		// wrapped case is the shape Modal produces — the SDK's own verdict, then the label
		// (see modal.sdkClient.buildImage) — and it is the one that regressed to DenyAll when the
		// registry's "unauthorized" text was left to the heuristics.
		{"image-build sentinel blocks nothing", ErrImageBuild, BlockScope{}},
		{
			"wrapped image-build sentinel blocks nothing",
			fmt.Errorf("modal: RemoteError: unauthorized: authentication required: %w", ErrImageBuild),
			BlockScope{},
		},
		{"wrapped sentinel", fmt.Errorf("provision failed: %w", ErrNoCapacity), capacityScope},
		{"string unauthorized", fmt.Errorf("HTTP 401 unauthorized"), BlockScope{DenyAll: true}},
		{"string quota", fmt.Errorf("account limit exceeded"), capacityScope},
		{"string capacity", fmt.Errorf("no capacity available"), capacityScope},
		// An unrecognized error is scoped like capacity (this accelerator + tier), NOT
		// DenyAll: a DenyAll would fence off the whole provider on a failure we can't
		// even identify, so failover past the one failing candidate is the safer default.
		{"unknown capacity-scoped", fmt.Errorf("weird transient blip"), capacityScope},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err, tier, accel); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ClassifyError(%v) = %+v, want %+v", tt.err, got, tt.want)
			}
		})
	}
}

// Spot tier must be stamped onto accelerator-scoped blocks so a Spot failure
// does not block OnDemand on the same provider.
func TestClassifyError_TierStamped(t *testing.T) {
	got := ClassifyError(ErrNoCapacity, nebulav1alpha1.CapacitySpot, "H100")
	if got.CapacityType != nebulav1alpha1.CapacitySpot || got.DenyAll {
		t.Fatalf("expected Spot accelerator-scoped block, got %+v", got)
	}
}

// An empty accelerator (CPU-only Pod) must leave Accelerator nil ("not
// applicable"), never a wildcard that would widen the block across every GPU.
func TestClassifyError_EmptyAcceleratorStaysNil(t *testing.T) {
	got := ClassifyError(ErrNoCapacity, nebulav1alpha1.CapacityOnDemand, "")
	if got.Accelerator != nil {
		t.Fatalf("expected nil Accelerator for a CPU-only request, got %+v", got.Accelerator)
	}
	if got.DenyAll || got.CapacityType != nebulav1alpha1.CapacityOnDemand {
		t.Fatalf("expected an OnDemand capacity scope, got %+v", got)
	}
}

// IsRejection separates a provider DECISION about the request from a failure to
// learn what it would have decided. The vnode handler fails the Pod and blocklists
// the candidate only for the former, so a transport error misclassified as a
// rejection stamps a terminal status on a request that may have been accepted.
func TestIsRejection(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not a rejection", nil, false},
		{"auth sentinel", ErrAuth, true},
		{"no-capacity sentinel", ErrNoCapacity, true},
		{"quota sentinel", ErrQuota, true},
		{"unsupported sentinel", ErrUnsupportedAccelerator, true},
		{"wrapped sentinel", fmt.Errorf("create sandbox: %w", ErrNoCapacity), true},
		{"string capacity", errors.New("InsufficientInstanceCapacity"), true},
		{"string auth", errors.New("HTTP 401 unauthorized"), true},
		// Blocks nothing (see TestClassifyError), yet still a rejection: the provider DID
		// decide, and a Pod whose credential it cannot use must fail with that reason rather
		// than retry it forever. Blocking and rejecting are separate questions.
		{"image-pull sentinel", ErrImagePull, true},
		{"image-build sentinel", ErrImageBuild, true},

		// The failures this predicate exists for.
		{"deadline exceeded", context.DeadlineExceeded, false},
		{"wrapped deadline", fmt.Errorf("provision: %w", context.DeadlineExceeded), false},
		{"canceled", context.Canceled, false},
		{"connection refused", errors.New("dial tcp 10.0.0.1:443: connect: connection refused"), false},
		{"eof", errors.New("unexpected EOF"), false},
		{"http 503", errors.New("503 Service Unavailable"), false},
		{"unrecognized", errors.New("weird transient blip"), false},

		// A gRPC status renders "Unavailable" in its text, which the capacity heuristic
		// would otherwise match — this is the misread the transport check precedes it for.
		{"grpc unavailable", errors.New("rpc error: code = Unavailable desc = transport is closing"), false},
		// ...but a sentinel the adapter wrapped still wins over the raw text, so an
		// adapter that classified a gRPC error itself is not second-guessed.
		{"grpc unavailable wrapping a sentinel",
			fmt.Errorf("rpc error: code = Unavailable desc = no gpu: %w", ErrNoCapacity), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRejection(tt.err); got != tt.want {
				t.Fatalf("IsRejection(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// ClassifyError answers "how widely, IF we block" and keeps its capacity-shaped
// default for an unattributable error: a caller that has decided to block something
// still wants the narrow blast radius. IsRejection is the separate question of
// whether to block at all, so the two must not be collapsed.
func TestClassifyError_UnattributableStillScopesNarrow(t *testing.T) {
	got := ClassifyError(
		errors.New("rpc error: code = Unavailable desc = transport is closing"),
		nebulav1alpha1.CapacitySpot, "H100:8")
	if got.DenyAll {
		t.Fatalf("an unattributable error must never widen to DenyAll, got %+v", got)
	}
	if got.Accelerator == nil || *got.Accelerator != "H100:8" ||
		got.CapacityType != nebulav1alpha1.CapacitySpot {
		t.Fatalf("expected a Spot/H100:8-scoped block, got %+v", got)
	}
}

func ptrStr(s string) *string { return &s }
