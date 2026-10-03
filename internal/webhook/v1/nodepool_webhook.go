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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// SetupNodePoolWebhookWithManager registers the webhook for NodePool in the manager.
func SetupNodePoolWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&nebulav1alpha1.NodePool{}).
		WithValidator(&NodePoolCustomValidator{}).
		Complete()
}

// failurePolicy=ignore: the validator only warns, so a webhook outage must not block
// NodePool writes. The Pod validator enforces runtime limits and stays Fail.
// +kubebuilder:webhook:path=/validate-nebula-inftyai-com-v1alpha1-nodepool,mutating=false,failurePolicy=ignore,sideEffects=None,groups=nebula.inftyai.com,resources=nodepools,verbs=create;update,versions=v1alpha1,name=vnodepool-v1alpha1.nebula.inftyai.com,admissionReviewVersions=v1

// NodePoolCustomValidator validates NodePools on create and update. A setting a listed
// provider cannot serve (Spot on Modal, a restricted egress on RunPod) is NOT rejected:
// placement skips that provider instead (see provider.Capabilities), so the rest of the pool
// still works. It only WARNS, so a provider placement will never use is not a surprise.
type NodePoolCustomValidator struct {
	// Providers resolves a provider name to its backend; defaults to the registry. An
	// unresolved name draws no warning: placement skips it too, and NodePool status reports it.
	Providers func(name string) (provider.Provider, bool)
}

var _ webhook.CustomValidator = &NodePoolCustomValidator{}

// ValidateCreate implements webhook.CustomValidator.
func (v *NodePoolCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	return v.validate(obj)
}

// ValidateUpdate implements webhook.CustomValidator.
func (v *NodePoolCustomValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	return v.validate(newObj)
}

// ValidateDelete implements webhook.CustomValidator. Deletes are always allowed.
func (v *NodePoolCustomValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *NodePoolCustomValidator) validate(obj runtime.Object) (admission.Warnings, error) {
	np, ok := obj.(*nebulav1alpha1.NodePool)
	if !ok {
		return nil, fmt.Errorf("expected a NodePool object but got %T", obj)
	}
	return v.warnings(np), nil
}

// warnings names every listed provider placement will never use, and says so once more when
// that is all of them, since such a pool admits Pods it can never place.
func (v *NodePoolCustomValidator) warnings(np *nebulav1alpha1.NodePool) admission.Warnings {
	var warns admission.Warnings
	known, usable := 0, 0
	for _, ref := range np.Spec.Providers {
		prov, ok := v.provider(ref.Name)
		if !ok {
			continue
		}
		known++
		caps := prov.Capabilities()
		switch {
		case !caps.ServesEgress(np.Spec.Egress):
			warns = append(warns, fmt.Sprintf("provider %q cannot enforce egress mode %q, so placement will never use it",
				ref.Name, np.Spec.Egress.ModeOrOpen()))
		case !servesAnyTier(caps, np.Spec.CapacityTypes):
			warns = append(warns, fmt.Sprintf("provider %q serves none of capacityTypes %v, so placement will never use it",
				ref.Name, np.Spec.CapacityTypes))
		default:
			usable++
		}
	}
	if known > 0 && usable == 0 {
		warns = append(warns, "no provider in this pool can serve it; its Pods will stay unplaceable")
	}
	return warns
}

func (v *NodePoolCustomValidator) provider(name string) (provider.Provider, bool) {
	if v.Providers != nil {
		return v.Providers(name)
	}
	return provider.Get(name)
}

// servesAnyTier mirrors placement's tier walk, where an empty list is one default tier.
func servesAnyTier(caps provider.Capabilities, tiers []nebulav1alpha1.CapacityType) bool {
	if len(tiers) == 0 {
		return true
	}
	for _, t := range tiers {
		if caps.ServesCapacityTier(t) {
			return true
		}
	}
	return false
}
