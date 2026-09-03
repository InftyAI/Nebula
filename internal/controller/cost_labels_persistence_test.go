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

package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"sigs.k8s.io/controller-runtime/pkg/client"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// status.costLabels distinguishes nil (nothing observed yet) from empty (observed, the Pod carried
// none of the configured keys), and stampCostLabels relies on that to write attribution exactly
// once. Only a real apiserver can prove it: the fake client the unit tests use stores deep copies
// rather than marshalling, so an empty map survives there whatever the json tag says — while an
// omitempty would drop it on the way to etcd and leave the claim looking unstamped forever.
// Requires envtest binaries; the whole suite is skipped in BeforeSuite when they are absent.
var _ = Describe("NodeClaim status.costLabels", func() {
	claim := func(name string) *nebulav1alpha1.NodeClaim {
		return newClaimNoFinalizer(name, "p1", "default", "uid-1", "modal")
	}
	readBack := func(name string) map[string]string {
		var nc nebulav1alpha1.NodeClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, &nc)).To(Succeed())
		return nc.Status.CostLabels
	}

	It("keeps an empty map distinct from an unset one", func() {
		nc := claim("cost-labels-empty")
		Expect(k8sClient.Create(ctx, nc)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, nc)).To(Succeed()) })

		// Unset: nothing observed yet.
		Expect(readBack(nc.Name)).To(BeNil())

		nc.Status.Phase = nebulav1alpha1.NodeClaimBound
		nc.Status.CostLabels = map[string]string{}
		Expect(k8sClient.Status().Update(ctx, nc)).To(Succeed())

		// Stamped and empty. A nil here is the bug this spec exists for: stampCostLabels would
		// re-read the Pod on every reconcile, and a label added later would move the attribution of
		// a claim whose earlier windows were already booked to "none".
		Expect(readBack(nc.Name)).NotTo(BeNil())
		Expect(readBack(nc.Name)).To(BeEmpty())
	})

	It("keeps a stamped value verbatim, qualified key and all", func() {
		nc := claim("cost-labels-stamped")
		Expect(k8sClient.Create(ctx, nc)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, nc)).To(Succeed()) })

		nc.Status.Phase = nebulav1alpha1.NodeClaimBound
		nc.Status.CostLabels = map[string]string{"example.com/org-id": "acme"}
		Expect(k8sClient.Status().Update(ctx, nc)).To(Succeed())

		Expect(readBack(nc.Name)).To(Equal(map[string]string{"example.com/org-id": "acme"}))
	})
})
