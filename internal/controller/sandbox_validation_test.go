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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/util"
)

// These specs exercise Sandbox admission — the CEL rule on SandboxSpec and the
// schema-level guarantees (image defaulting, and the absence of a command field).
// None of it is enforced by the fake client the unit tests use: it runs neither
// x-kubernetes-validations nor structural-schema defaulting/pruning, so a real
// apiserver is the only thing that can prove these hold. Requires envtest binaries;
// the whole suite is skipped in BeforeSuite when they are absent.
var _ = Describe("Sandbox admission", func() {
	newSandbox := func(name string) *nebulav1alpha1.Sandbox {
		return &nebulav1alpha1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: nebulav1alpha1.SandboxSpec{
				NodePoolRef: "sample",
			},
		}
	}
	gpuLimit := func(sbx *nebulav1alpha1.Sandbox, n string) {
		sbx.Spec.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{util.NvidiaGPUResource: resource.MustParse(n)},
		}
	}

	It("rejects a GPU count with no acceleratorType", func() {
		// The contradictory pair: util.AcceleratorRequest errors on it, so admitting
		// this would defer the failure to placement — minutes later, and reported on the
		// Pod rather than on the object the user actually wrote.
		sbx := newSandbox("gpu-no-type")
		gpuLimit(sbx, "1")

		err := k8sClient.Create(ctx, sbx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nvidia.com/gpu requires acceleratorType to be set"))
	})

	It("rejects a GPU request (not just a limit) with no acceleratorType", func() {
		// gpuCount reads limits OR requests, so the rule has to cover both; a
		// requests-only spec would otherwise slip through and fail at placement.
		sbx := newSandbox("gpu-request-no-type")
		sbx.Spec.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{util.NvidiaGPUResource: resource.MustParse("1")},
		}

		err := k8sClient.Create(ctx, sbx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nvidia.com/gpu requires acceleratorType to be set"))
	})

	It("admits a GPU count together with an acceleratorType", func() {
		sbx := newSandbox("gpu-with-type")
		sbx.Spec.AcceleratorType = "a100-40gb"
		gpuLimit(sbx, "1")

		Expect(k8sClient.Create(ctx, sbx)).To(Succeed())
		Expect(k8sClient.Delete(ctx, sbx)).To(Succeed())
	})

	It("admits an acceleratorType with no count (which means one accelerator)", func() {
		sbx := newSandbox("type-no-count")
		sbx.Spec.AcceleratorType = "h100"

		Expect(k8sClient.Create(ctx, sbx)).To(Succeed())
		Expect(k8sClient.Delete(ctx, sbx)).To(Succeed())
	})

	It("admits a CPU-only sandbox with non-GPU resources", func() {
		sbx := newSandbox("cpu-only")
		sbx.Spec.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
		}

		Expect(k8sClient.Create(ctx, sbx)).To(Succeed())
		Expect(k8sClient.Delete(ctx, sbx)).To(Succeed())
	})

	It("defaults the image so a bare spec is usable", func() {
		sbx := newSandbox("default-image")
		Expect(k8sClient.Create(ctx, sbx)).To(Succeed())
		defer func() { Expect(k8sClient.Delete(ctx, sbx)).To(Succeed()) }()

		var got nebulav1alpha1.Sandbox
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sbx), &got)).To(Succeed())
		Expect(got.Spec.Image).To(Equal("ubuntu:24.04"))
	})

	It("rejects an explicitly empty image rather than defaulting it", func() {
		// `image: ""` is a mistake, not a request for the default, and MinLength must
		// still bite — otherwise the box would boot something its author never named.
		//
		// This has to go through unstructured: the field is `omitempty`, so a Go zero
		// value is dropped before the request is sent and would be DEFAULTED instead of
		// rejected. Only an explicit empty string on the wire reaches MinLength, which is
		// exactly the distinction the apiserver draws between unset and empty.
		sbx := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": nebulav1alpha1.GroupVersion.String(),
			"kind":       "Sandbox",
			"metadata":   map[string]any{"name": "empty-image", "namespace": "default"},
			"spec": map[string]any{
				"nodePoolRef": "sample",
				"image":       "",
			},
		}}

		err := k8sClient.Create(ctx, sbx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("should be at least 1 chars long"))
	})

	It("rejects a spec with no nodePoolRef", func() {
		// Placing a paid GPU instance against a guessed policy is not a safe default,
		// so the field is required rather than defaulted.
		sbx := newSandbox("no-pool")
		sbx.Spec.NodePoolRef = ""

		err := k8sClient.Create(ctx, sbx)
		Expect(err).To(HaveOccurred())
	})

	It("rejects a user-supplied command", func() {
		// The process model depends on SandD being PID 1 — it is what serves exec and
		// logs — so a command must never reach the container. SandboxSpec simply has no
		// command field, and because the CRD is a structural schema the apiserver rejects
		// the unknown field itself. This spec exists because that guarantee is the reason
		// no validating webhook was written: if the schema ever stopped rejecting it (a
		// stray x-kubernetes-preserve-unknown-fields would do it), the command would be
		// silently pruned instead, and the failure would surface as "exec does not work"
		// rather than as a rejected object.
		sbx := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": nebulav1alpha1.GroupVersion.String(),
			"kind":       "Sandbox",
			"metadata":   map[string]any{"name": "with-command", "namespace": "default"},
			"spec": map[string]any{
				"nodePoolRef": "sample",
				"command":     []any{"/bin/sleep", "infinity"},
			},
		}}

		// Strict field validation is what turns "unknown field" from a silent prune into
		// an error; kubectl applies it by default, so this matches what a user sees.
		err := k8sClient.Create(ctx, sbx, client.FieldValidation(metav1.FieldValidationStrict))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unknown field"))
	})
})
