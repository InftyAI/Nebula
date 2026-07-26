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
	"testing"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

func TestClassifyError(t *testing.T) {
	const tier = nebulav1alpha1.CapacityOnDemand
	tests := []struct {
		name string
		err  error
		want BlockScope
	}{
		{"nil", nil, BlockScope{}},
		{"auth sentinel", ErrAuth, BlockScope{DenyAll: true}},
		{"quota sentinel", ErrQuota, BlockScope{DenyAll: true}},
		{"no-capacity sentinel", ErrNoCapacity, BlockScope{CapacityType: tier}},
		{"unsupported sentinel", ErrUnsupportedAccelerator, BlockScope{CapacityType: tier}},
		{"wrapped sentinel", fmt.Errorf("provision failed: %w", ErrNoCapacity), BlockScope{CapacityType: tier}},
		{"string unauthorized", fmt.Errorf("HTTP 401 unauthorized"), BlockScope{DenyAll: true}},
		{"string quota", fmt.Errorf("account limit exceeded"), BlockScope{DenyAll: true}},
		{"string capacity", fmt.Errorf("no capacity available"), BlockScope{CapacityType: tier}},
		{"unknown conservative", fmt.Errorf("weird transient blip"), BlockScope{DenyAll: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err, tier); got != tt.want {
				t.Fatalf("ClassifyError(%v) = %+v, want %+v", tt.err, got, tt.want)
			}
		})
	}
}

// Spot tier must be stamped onto accelerator-scoped blocks so a Spot failure
// does not block OnDemand on the same provider.
func TestClassifyError_TierStamped(t *testing.T) {
	got := ClassifyError(ErrNoCapacity, nebulav1alpha1.CapacitySpot)
	if got.CapacityType != nebulav1alpha1.CapacitySpot || got.DenyAll {
		t.Fatalf("expected Spot accelerator-scoped block, got %+v", got)
	}
}
