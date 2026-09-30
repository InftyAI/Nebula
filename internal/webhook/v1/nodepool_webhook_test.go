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

package v1

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

func nodePool(capacity []nebulav1alpha1.CapacityType, providers ...string) *nebulav1alpha1.NodePool {
	np := &nebulav1alpha1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np"}}
	for _, p := range providers {
		np.Spec.Providers = append(np.Spec.Providers, nebulav1alpha1.ProviderSpec{Name: p})
	}
	np.Spec.CapacityTypes = capacity
	return np
}

func TestNodePoolValidator(t *testing.T) {
	spot, onDemand := nebulav1alpha1.CapacitySpot, nebulav1alpha1.CapacityOnDemand
	cases := []struct {
		name    string
		np      *nebulav1alpha1.NodePool
		wantErr bool
	}{
		{"modal with spot", nodePool([]nebulav1alpha1.CapacityType{spot}, "modal"), true},
		{"modal among others with spot", nodePool([]nebulav1alpha1.CapacityType{onDemand, spot}, "runpod", "modal"), true},
		{"modal name is case-insensitive", nodePool([]nebulav1alpha1.CapacityType{spot}, "Modal"), true},
		{"modal with on-demand only", nodePool([]nebulav1alpha1.CapacityType{onDemand}, "modal"), false},
		{"other provider with spot", nodePool([]nebulav1alpha1.CapacityType{spot, onDemand}, "runpod"), false},
	}
	v := &NodePoolCustomValidator{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := v.ValidateCreate(context.Background(), tc.np); (err != nil) != tc.wantErr {
				t.Fatalf("ValidateCreate err = %v, wantErr %v", err, tc.wantErr)
			}
			old := nodePool([]nebulav1alpha1.CapacityType{onDemand}, "modal")
			if _, err := v.ValidateUpdate(context.Background(), old, tc.np); (err != nil) != tc.wantErr {
				t.Fatalf("ValidateUpdate err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestNodePoolValidatorAllowsDelete(t *testing.T) {
	np := nodePool([]nebulav1alpha1.CapacityType{nebulav1alpha1.CapacitySpot}, "modal")
	if _, err := (&NodePoolCustomValidator{}).ValidateDelete(context.Background(), np); err != nil {
		t.Fatalf("ValidateDelete err = %v, want nil", err)
	}
}

func TestNodePoolValidatorRejectsWrongType(t *testing.T) {
	if _, err := (&NodePoolCustomValidator{}).ValidateCreate(context.Background(), &nebulav1alpha1.NodePoolList{}); err == nil {
		t.Fatal("expected an error for a non-NodePool object")
	}
}
