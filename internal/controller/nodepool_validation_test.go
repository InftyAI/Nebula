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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// These specs exercise the CEL validation rule on NodePoolSpec, which is only
// enforced by a real apiserver (the fake client used by the unit tests does not
// run CRD x-kubernetes-validations). They require envtest binaries; when those
// are absent the whole suite is skipped in BeforeSuite.
var _ = Describe("NodePool spec validation (CEL)", func() {
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

	It("rejects a Weighted pool with a provider missing a weight", func() {
		pool := newWeightedPool("weighted-missing",
			nebulav1alpha1.ProviderSpec{Name: "modal", Weight: weight(3)},
			nebulav1alpha1.ProviderSpec{Name: "runpod"}, // no weight
		)
		err := k8sClient.Create(ctx, pool)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("strategy Weighted requires a weight on every provider"))
	})

	It("admits a Weighted pool with a weight on every provider", func() {
		pool := newWeightedPool("weighted-ok",
			nebulav1alpha1.ProviderSpec{Name: "modal", Weight: weight(3)},
			nebulav1alpha1.ProviderSpec{Name: "runpod", Weight: weight(1)},
		)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
	})

	It("admits a non-Weighted pool regardless of weights", func() {
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
})
