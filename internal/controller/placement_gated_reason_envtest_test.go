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
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// Against a real API server, which the fake client is not: it must set the SchedulingGated
// condition at create, and take the status patch without touching the transition time.
var _ = Describe("a gated Pod's reason", func() {
	It("lands in the PodScheduled condition the API server set", func() {
		ctx := context.Background()
		pod := gatedPod("gated-reason", "default", "", "no-such-pool", "")
		pod.UID = ""
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, pod) })

		key := types.NamespacedName{Namespace: "default", Name: "gated-reason"}
		var created corev1.Pod
		Expect(k8sClient.Get(ctx, key, &created)).To(Succeed())
		before := podScheduled(&created)
		Expect(before).NotTo(BeNil(), "the API server sets PodScheduled on a gated Pod")
		Expect(before.Reason).To(Equal(corev1.PodReasonSchedulingGated))

		r := &PodPlacementReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var got corev1.Pod
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		after := podScheduled(&got)
		Expect(after).NotTo(BeNil())
		Expect(after.Message).To(ContainSubstring(`NodePool "no-such-pool"`))
		Expect(after.Reason).To(Equal(corev1.PodReasonSchedulingGated))
		Expect(after.Status).To(Equal(corev1.ConditionFalse))
		Expect(after.LastTransitionTime.Equal(&before.LastTransitionTime)).To(BeTrue())
		Expect(got.Labels[nebulav1alpha1.PoolLabel]).To(Equal("no-such-pool"))
	})
})

func podScheduled(pod *corev1.Pod) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodScheduled {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}
