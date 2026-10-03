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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// capsOnly is a provider whose only behaviour is its Capabilities; the webhook reads nothing else.
type capsOnly struct {
	provider.Provider
	caps provider.Capabilities
}

func (p capsOnly) Capabilities() provider.Capabilities { return p.caps }

// Shaped like the real adapters: Modal enforces egress but has no Spot, RunPod neither, AWS Spot only.
var testProviders = map[string]provider.Provider{
	"modal":  capsOnly{caps: provider.Capabilities{SupportsEgressPolicy: true}},
	"runpod": capsOnly{},
	"aws":    capsOnly{caps: provider.Capabilities{SupportsSpot: true}},
}

func lookup(name string) (provider.Provider, bool) {
	p, ok := testProviders[name]
	return p, ok
}

func pool(capacity []nebulav1alpha1.CapacityType, egress nebulav1alpha1.EgressMode, providers ...string) *nebulav1alpha1.NodePool {
	np := &nebulav1alpha1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "np"}}
	for _, p := range providers {
		np.Spec.Providers = append(np.Spec.Providers, nebulav1alpha1.ProviderSpec{Name: p})
	}
	np.Spec.CapacityTypes = capacity
	if egress != "" {
		np.Spec.Egress = &nebulav1alpha1.EgressPolicy{Mode: egress}
	}
	return np
}

// A setting a provider cannot serve is never an error, only a warning naming the provider
// placement will skip, plus one more when nothing in the pool is left.
func TestNodePoolValidatorWarnings(t *testing.T) {
	spot, onDemand := nebulav1alpha1.CapacitySpot, nebulav1alpha1.CapacityOnDemand
	both := []nebulav1alpha1.CapacityType{onDemand, spot}
	cases := []struct {
		name string
		np   *nebulav1alpha1.NodePool
		want []string // substrings, one per expected warning, in order
	}{
		{"every provider usable", pool(both, "", "modal", "runpod", "aws"), nil},
		// Modal still serves the OnDemand tier, so the default capacityTypes must stay quiet.
		{"modal with spot as a fallback", pool(both, "", "modal"), nil},
		{"modal with spot only", pool([]nebulav1alpha1.CapacityType{spot}, "", "modal", "aws"),
			[]string{`"modal" serves none of capacityTypes`}},
		{"runpod under a restricted egress", pool(both, nebulav1alpha1.EgressBlocked, "runpod", "modal"),
			[]string{`"runpod" cannot enforce egress mode "Blocked"`}},
		{"nothing can place", pool(both, nebulav1alpha1.EgressAllowlist, "runpod", "aws"),
			[]string{`"runpod" cannot enforce`, `"aws" cannot enforce`, "will stay unplaceable"}},
		// Unregistered is placement's skip and status's report, not a warning.
		{"unregistered provider", pool(both, "", "lambda"), nil},
		{"empty capacityTypes is the default tier", pool(nil, "", "modal"), nil},
	}
	v := &NodePoolCustomValidator{Providers: lookup}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			warns, err := v.ValidateCreate(context.Background(), tc.np)
			if err != nil {
				t.Fatalf("ValidateCreate err = %v, want nil", err)
			}
			if len(warns) != len(tc.want) {
				t.Fatalf("warnings = %q, want %d matching %q", warns, len(tc.want), tc.want)
			}
			for i, w := range tc.want {
				if !strings.Contains(warns[i], w) {
					t.Errorf("warning %d = %q, want it to contain %q", i, warns[i], w)
				}
			}
			upd, err := v.ValidateUpdate(context.Background(), pool(both, "", "modal"), tc.np)
			if err != nil || len(upd) != len(warns) {
				t.Errorf("ValidateUpdate = %q, %v; want the create result %q", upd, err, warns)
			}
		})
	}
}

func TestNodePoolValidatorRejectsWrongType(t *testing.T) {
	v := &NodePoolCustomValidator{Providers: lookup}
	if _, err := v.ValidateCreate(context.Background(), &nebulav1alpha1.NodePoolList{}); err == nil {
		t.Fatal("expected an error for a non-NodePool object")
	}
}
