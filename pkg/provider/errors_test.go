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
		// An image the provider cannot obtain belongs to the POD. The blocklist key carries no
		// Pod, image or credential identity, so ANY scope here would exclude the candidate for
		// unrelated Pods that pull perfectly well — hence the zero scope, which blocks nothing.
		{"image sentinel blocks nothing", ErrImage, BlockScope{}},
		{
			"unsupported pull credential blocks nothing",
			fmt.Errorf("modal: unsupported image pull credential: %w", ErrImage),
			BlockScope{},
		},
		// The one that matters: a build refused by the REGISTRY, which is the shape Modal
		// produces — the SDK's own verdict, then the label (see modal.sdkClient.buildImage).
		// This regressed to DenyAll once, when the registry's "unauthorized" text was left to
		// the heuristics and one Pod's bad Secret fenced off the whole provider.
		{
			"registry refusal during a build blocks nothing",
			fmt.Errorf("modal: RemoteError: unauthorized: authentication required: %w", ErrImage),
			BlockScope{},
		},
		{"wrapped sentinel", fmt.Errorf("provision failed: %w", ErrNoCapacity), capacityScope},
		{"string unauthorized", fmt.Errorf("HTTP 401 unauthorized"), BlockScope{DenyAll: true}},
		{"string quota", fmt.Errorf("account limit exceeded"), capacityScope},
		{"string capacity", fmt.Errorf("no capacity available"), capacityScope},
		// An error we cannot attribute blocks NOTHING: it is no evidence against the
		// candidate, and the zero scope is what keeps recordBlock from acting on it.
		{"unknown blocks nothing", fmt.Errorf("weird transient blip"), BlockScope{}},

		// A DEADLINE is scoped like capacity: the candidate had the whole provision budget
		// and produced no usable instance, so the next attempt should go elsewhere instead
		// of spending another full budget here. Cancellation is the opposite — that is US
		// stopping, no evidence against the candidate — so it blocks nothing.
		{"deadline exceeded", context.DeadlineExceeded, capacityScope},
		{"wrapped deadline", fmt.Errorf("provision: %w", context.DeadlineExceeded), capacityScope},
		{"canceled blocks nothing", context.Canceled, BlockScope{}},

		// OUR clock outranks any label a sentinel carries: an adapter reports the failure it
		// saw and cannot see whose deadline fired. So a build that ran out of budget is a
		// capacity failure, not the zero-scope image failure its label suggests.
		{"deadline wrapped in an image label",
			fmt.Errorf("modal: image build: %w: %w", context.DeadlineExceeded, ErrImage), capacityScope},
		{"cancellation wrapped in a capacity label",
			fmt.Errorf("sweep: %w: %w", context.Canceled, ErrNoCapacity), BlockScope{}},
		// The form errors.Is CANNOT see: grpc-go turns a dead context into a status error
		// that wraps nothing, and it is the likelier arrival — an SDK blocked in Recv finds
		// out from gRPC, not from its own ctx.Err() poll.
		{"grpc deadline wrapping an image label",
			fmt.Errorf("modal: image build: %w: %w",
				errors.New("rpc error: code = DeadlineExceeded desc = context deadline exceeded"),
				ErrImage), capacityScope},
		// ...but a verdict Modal actually reached keeps the label's zero scope: the override
		// fires only when the context is what died, so a registry refusal must not fence off
		// the accelerator.
		{"image label on a remote verdict",
			fmt.Errorf("modal: image build: %w: %w",
				errors.New("Image build for im-1 failed with the exception:\nunauthorized"),
				ErrImage), BlockScope{}},
		// A mint failure carries no label, so it is scoped by its cause. That is the point: the
		// mint uses OUR provider credentials, so an auth or rate-limit refusal there is
		// provider-wide, and a request scope would fence off nothing while every replacement
		// Pod picked the same broken provider.
		{"mint failure on an auth refusal",
			fmt.Errorf("modal: mint connect credential for sandbox sb-1: %w",
				errors.New("unauthorized")), BlockScope{DenyAll: true}},
		{"mint failure on a rate limit",
			fmt.Errorf("modal: mint connect credential for sandbox sb-1: %w",
				errors.New("429 rate limit exceeded")), capacityScope},
		// Only a mint failure that names nothing recognizable stays at zero.
		{"mint failure on a transport error",
			fmt.Errorf("modal: mint connect credential for sandbox sb-1: %w",
				errors.New("rpc error: code = Unavailable")), BlockScope{}},
		{"mint failure with no cause", errors.New("modal: connect credential minted without a token"),
			BlockScope{}},

		// A gRPC status renders "Unavailable" in its text, which the capacity heuristic
		// would otherwise match — the misread the transport check precedes it for. A sentinel
		// the adapter wrapped still wins over the raw text, so an adapter that classified a
		// gRPC error itself is not second-guessed.
		{"grpc unavailable wrapping a sentinel",
			fmt.Errorf("rpc error: code = Unavailable desc = no gpu: %w", ErrNoCapacity), capacityScope},
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

// The scope is the single answer to "should anything be blocked", so an unattributable
// error must classify to ZERO — callers hand every failure to recordBlock and rely on this
// to record nothing. A narrow-but-non-empty scope here would fence off a candidate for a
// transport failure that was never its doing.
func TestClassifyError_UnattributableBlocksNothing(t *testing.T) {
	got := ClassifyError(
		errors.New("rpc error: code = Unavailable desc = transport is closing"),
		nebulav1alpha1.CapacitySpot, "H100:8")
	if got != (BlockScope{}) {
		t.Fatalf("an unattributable error must block nothing, got %+v", got)
	}
}

func ptrStr(s string) *string { return &s }
