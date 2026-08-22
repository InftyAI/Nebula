/*
Copyright 2026.

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

package controller

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// These specs exercise CRD validation on NodePoolSpec, which is only enforced
// by a real apiserver (the fake client used by the unit tests does not apply the
// structural schema or CEL rules). They require envtest binaries; when those
// are absent the whole suite is skipped in BeforeSuite.
var _ = Describe("NodePool spec validation", func() {
	newWeightedPool := func(name string, refs ...nebulav1alpha1.ProviderSpec) *nebulav1alpha1.NodePool {
		return &nebulav1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: nebulav1alpha1.NodePoolSpec{
				Providers: refs,
				Strategy:  nebulav1alpha1.StrategyWeighted,
			},
		}
	}
	weight := func(w int32) *int32 { return &w }
	providerRefs := func(count int) []nebulav1alpha1.ProviderSpec {
		refs := make([]nebulav1alpha1.ProviderSpec, count)
		for i := range refs {
			refs[i].Name = fmt.Sprintf("provider-%d", i)
		}
		return refs
	}

	It("admits a pool with eight providers", func() {
		pool := newPool("eight-providers", nebulav1alpha1.StrategyOrdered, providerRefs(8)...)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
	})

	It("rejects a pool with more than eight providers", func() {
		pool := newPool("nine-providers", nebulav1alpha1.StrategyOrdered, providerRefs(9)...)
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("must have at most 8 items"))
	})

	// Strategy admits only Ordered today (see NodePoolSpec.Strategy). These assert
	// the restriction itself, because it is the enum — not the Weighted weight CEL
	// rule — that now rejects the other two. The weight rule is retained but
	// unreachable, so it has no admission behaviour left to test: a Weighted pool
	// WITH weights on every provider is rejected just the same, which is what the
	// second spec below pins.
	It("rejects a Weighted pool even with a weight on every provider", func() {
		pool := newWeightedPool("weighted-ok",
			nebulav1alpha1.ProviderSpec{Name: "modal", Weight: weight(3)},
			nebulav1alpha1.ProviderSpec{Name: "runpod", Weight: weight(1)},
		)
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`Unsupported value: "Weighted"`))
	})

	It("rejects a LowestPrice pool", func() {
		pool := newPool("lowest-price", nebulav1alpha1.StrategyLowestPrice,
			nebulav1alpha1.ProviderSpec{Name: "modal"})
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`Unsupported value: "LowestPrice"`))
	})

	It("admits an Ordered pool regardless of weights", func() {
		pool := &nebulav1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: "ordered-noweights"},
			Spec: nebulav1alpha1.NodePoolSpec{
				Providers: []nebulav1alpha1.ProviderSpec{{Name: "modal"}},
				Strategy:  nebulav1alpha1.StrategyOrdered,
			},
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
	})

	It("rejects a capacityType outside the Spot;OnDemand enum", func() {
		// "Reserved" was a formerly-declared tier that never worked (the AWS adapter
		// aliased it to a plain OnDemand launch) and has been removed from the enum.
		// The CRD's x-kubernetes enum must now reject it so a stale spec cannot be
		// admitted and silently degrade to OnDemand.
		pool := &nebulav1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-capacity-type"},
			Spec: nebulav1alpha1.NodePoolSpec{
				Providers:     []nebulav1alpha1.ProviderSpec{{Name: "modal"}},
				CapacityTypes: []nebulav1alpha1.CapacityType{"Reserved"},
				Strategy:      nebulav1alpha1.StrategyOrdered,
			},
		}
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Unsupported value: \"Reserved\""))
	})

	// The egress rules. The comma one is the load-bearing case: Targets rides to the VK
	// handler as ONE comma-separated annotation, so an entry containing a comma would be
	// decoded as several permitted targets rather than the single invalid one the author
	// wrote — a policy wider than the pool declares, which is the wrong way for a
	// containment control to fail. Admission is the only thing standing between the two.
	newEgressPool := func(name string, egress *nebulav1alpha1.EgressPolicy) *nebulav1alpha1.NodePool {
		return &nebulav1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: nebulav1alpha1.NodePoolSpec{
				Providers: []nebulav1alpha1.ProviderSpec{{Name: "modal"}},
				Strategy:  nebulav1alpha1.StrategyOrdered,
				Egress:    egress,
			},
		}
	}

	It("rejects a target containing a comma", func() {
		pool := newEgressPool("egress-packed-target", &nebulav1alpha1.EgressPolicy{
			Mode:    nebulav1alpha1.EgressAllowlist,
			Targets: []string{"10.0.0.0/8,api.openai.com"},
		})
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("must not contain a comma"))
	})

	It("admits the same two targets listed separately", func() {
		pool := newEgressPool("egress-split-targets", &nebulav1alpha1.EgressPolicy{
			Mode:    nebulav1alpha1.EgressAllowlist,
			Targets: []string{"10.0.0.0/8", "*.huggingface.co"},
		})
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
	})

	It("rejects targets without mode Allowlist", func() {
		pool := newEgressPool("egress-blocked-with-targets", &nebulav1alpha1.EgressPolicy{
			Mode:    nebulav1alpha1.EgressBlocked,
			Targets: []string{"10.0.0.0/8"},
		})
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("targets is only valid with mode Allowlist"))
	})

	It("rejects mode Allowlist with no targets", func() {
		pool := newEgressPool("egress-allowlist-empty", &nebulav1alpha1.EgressPolicy{
			Mode: nebulav1alpha1.EgressAllowlist,
		})
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("requires at least one target"))
	})

	It("admits the Spot and OnDemand capacity tiers", func() {
		pool := &nebulav1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: "ok-capacity-types"},
			Spec: nebulav1alpha1.NodePoolSpec{
				Providers: []nebulav1alpha1.ProviderSpec{{Name: "modal"}},
				CapacityTypes: []nebulav1alpha1.CapacityType{
					nebulav1alpha1.CapacityOnDemand,
					nebulav1alpha1.CapacitySpot,
				},
				Strategy: nebulav1alpha1.StrategyOrdered,
			},
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
	})
})
