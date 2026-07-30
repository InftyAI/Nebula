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

func ptrStr(s string) *string { return &s }
