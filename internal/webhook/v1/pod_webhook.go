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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

var podlog = logf.Log.WithName("pod-webhook")

// SetupPodWebhookWithManager registers the webhook for Pod in the manager.
func SetupPodWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&corev1.Pod{}).
		WithDefaulter(&PodCustomDefaulter{}).
		Complete()
}

// NOTE: the path is /mutate--v1-pod (double dash), not /mutate-core-v1-pod.
// controller-runtime derives the served path from the GVK, and Pod's API group
// is the empty string "", so the group segment is empty. The generated manifest
// must match that served path or the API server gets a 404 ("could not find the
// requested resource") when calling the webhook.
// +kubebuilder:webhook:path=/mutate--v1-pod,mutating=true,failurePolicy=fail,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpod-v1.nebula.inftyai.com,admissionReviewVersions=v1

// PodCustomDefaulter injects Nebula's provider-selection scheduling gate onto
// opted-in Pods at CREATE. The gate holds the Pod as SchedulingGated until the
// placement controller has chosen a provider (adding a provider nodeSelector)
// and removes the gate, releasing the Pod to the native scheduler. This is the
// entry point of the whole placement flow: no gate means the Pod is scheduled
// immediately by vanilla Kubernetes with no Nebula involvement.
//
// The webhook is scoped narrowly by an objectSelector on EnabledLabel in the
// generated manifest, so in practice only opted-in Pods reach it; the label
// check here is defence-in-depth for the case the selector is misconfigured.
type PodCustomDefaulter struct{}

var _ webhook.CustomDefaulter = &PodCustomDefaulter{}

// Default implements webhook.CustomDefaulter. For an opted-in Pod at CREATE it
// (a) adds the provider-selection scheduling gate and (b) adds a toleration for
// the per-provider virtual-node taint, but only when it is safe and meaningful:
//   - the Pod carries the opt-in label (EnabledLabel=="true");
//   - the Pod is not already scheduled (spec.nodeName empty) — the API server
//     rejects adding a scheduling gate to an already-bound Pod, and a Pod that
//     is already scheduled has bypassed placement anyway;
//   - each mutation is idempotent (safe under retries and the
//     create;update-vs-create verb narrowing).
func (d *PodCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return fmt.Errorf("expected a Pod object but got %T", obj)
	}

	if pod.Labels[nebulav1alpha1.EnabledLabel] != "true" {
		return nil // not opted in; leave the Pod untouched
	}
	if pod.Spec.NodeName != "" {
		return nil // already scheduled; a gate can no longer be added
	}

	// Tolerate the virtual node's taint. Every provider's virtual node carries
	// nebula.inftyai.com/provider=<name>:NoSchedule so that only Nebula-placed
	// Pods land there; without a matching toleration the scheduler would never
	// bind this Pod once the placement controller sets the provider nodeSelector.
	// The provider is not chosen yet at admission, so the toleration is key-only
	// (Operator=Exists), matching the taint for any provider value.
	if !hasProviderToleration(pod) {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
			Key:      nebulav1alpha1.ProviderLabel,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		})
	}

	if hasGate(pod, nebulav1alpha1.ProviderSelectionGate) {
		return nil // already gated; nothing more to do
	}

	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{
		Name: nebulav1alpha1.ProviderSelectionGate,
	})
	podlog.Info("injected provider-selection scheduling gate and virtual-node toleration",
		"namespace", pod.Namespace, "name", pod.Name)
	return nil
}

// hasGate reports whether the Pod already carries the named scheduling gate.
func hasGate(pod *corev1.Pod, name string) bool {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == name {
			return true
		}
	}
	return false
}

// hasProviderToleration reports whether the Pod already tolerates the
// provider-node taint. An existing toleration counts if it would match the
// NoSchedule taint on key nebula.inftyai.com/provider — either a key-only
// Exists toleration or the user's own equivalent — so we never add a duplicate.
func hasProviderToleration(pod *corev1.Pod) bool {
	for _, t := range pod.Spec.Tolerations {
		if t.Key != nebulav1alpha1.ProviderLabel {
			continue
		}
		if t.Effect != "" && t.Effect != corev1.TaintEffectNoSchedule {
			continue
		}
		return true
	}
	return false
}
