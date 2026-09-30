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
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// modalProvider is the provider name Modal registers its virtual node under.
const modalProvider = "modal"

// SetupNodePoolWebhookWithManager registers the webhook for NodePool in the manager.
func SetupNodePoolWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&nebulav1alpha1.NodePool{}).
		WithValidator(&NodePoolCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-nebula-inftyai-com-v1alpha1-nodepool,mutating=false,failurePolicy=fail,sideEffects=None,groups=nebula.inftyai.com,resources=nodepools,verbs=create;update,versions=v1alpha1,name=vnodepool-v1alpha1.nebula.inftyai.com,admissionReviewVersions=v1

// NodePoolCustomValidator rejects NodePools that ask for capacity a provider
// cannot supply. Modal has no spot instances, so a pool that lists Modal and
// Spot would hold a Spot tier that Modal can never fill.
//
// The API server applies the capacityTypes default ([OnDemand, Spot]) before
// admission, so a Modal pool that leaves capacityTypes empty is rejected too;
// it must list [OnDemand] explicitly.
type NodePoolCustomValidator struct{}

var _ webhook.CustomValidator = &NodePoolCustomValidator{}

// ValidateCreate implements webhook.CustomValidator.
func (v *NodePoolCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, validateNodePool(obj)
}

// ValidateUpdate implements webhook.CustomValidator.
func (v *NodePoolCustomValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	return nil, validateNodePool(newObj)
}

// ValidateDelete implements webhook.CustomValidator. Deletes are always allowed.
func (v *NodePoolCustomValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func validateNodePool(obj runtime.Object) error {
	np, ok := obj.(*nebulav1alpha1.NodePool)
	if !ok {
		return fmt.Errorf("expected a NodePool object but got %T", obj)
	}
	if hasProvider(np, modalProvider) && hasCapacityType(np, nebulav1alpha1.CapacitySpot) {
		return fmt.Errorf("modal does not support spot instances; remove modal from providers or set capacityTypes to [OnDemand]")
	}
	return nil
}

func hasProvider(np *nebulav1alpha1.NodePool, name string) bool {
	for _, p := range np.Spec.Providers {
		if strings.EqualFold(p.Name, name) {
			return true
		}
	}
	return false
}

func hasCapacityType(np *nebulav1alpha1.NodePool, ct nebulav1alpha1.CapacityType) bool {
	for _, c := range np.Spec.CapacityTypes {
		if c == ct {
			return true
		}
	}
	return false
}
