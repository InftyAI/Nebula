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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SandboxPoolSpec keeps N Sandboxes alive. It exists because provisioning a
// remote instance takes MINUTES while an agent's exec call wants sub-second: the
// only way to serve that is to hold boxes ready before anyone asks. A pool is
// also the unit fan-out is expressed on ("twenty boxes for this batch") and the
// thing an autoscaler can drive.
//
// It creates Sandbox OBJECTS, not replicas inside itself, and that is the whole
// point of having two types. The count is genuinely a different concern from the
// box: a pool answers "how many", a Sandbox answers "which one, running what, for
// whom". Because each box stays its own object underneath the pool, per-box RBAC,
// per-box status and a failure that stays visible all keep working — none of which
// survives being flattened into a replica index.
//
// Boxes get GENERATED names (pool-a4f2x), not ordinals. Ordinals would imply a
// slot that gets refilled, so a box that died would be replaced by an empty one
// wearing the same name — the same address with a different filesystem, which is
// the most confusing thing this API could do. A generated name means a replacement
// is visibly a NEW box, and callers that need a stable handle hold the Sandbox
// name they were given rather than an index into a set.
type SandboxPoolSpec struct {
	// Replicas is how many Sandboxes to keep. Zero is legal and useful: it releases
	// every box while keeping the pool's definition, which is how a pool is parked
	// overnight without being forgotten.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`

	// Template is the shape of every Sandbox this pool creates. All boxes in one
	// pool are the same shape by construction — a pool exists to make boxes
	// interchangeable at the point of HANDOUT, so a caller can take any ready box
	// without inspecting it. Two shapes means two pools.
	//
	// A template is right here for the same reason it is wrong on Sandbox itself:
	// this object does not describe a box, it describes how to make them.
	Template SandboxTemplateSpec `json:"template"`
}

// SandboxTemplateSpec is the Sandbox a pool stamps out: the standard Kubernetes
// template shape (metadata + spec), so pool-created boxes can carry the labels a
// caller selects them by.
type SandboxTemplateSpec struct {
	// Metadata is the labels and annotations applied to each created Sandbox. Only
	// labels and annotations are honoured; a name here is ignored, since names are
	// generated per box.
	// +optional
	Metadata SandboxTemplateMetadata `json:"metadata,omitempty"`

	// Spec is the SandboxSpec of every box in the pool.
	Spec SandboxSpec `json:"spec"`
}

// SandboxTemplateMetadata is the subset of ObjectMeta a template may set. It is
// spelled out rather than embedding metav1.ObjectMeta because embedding would
// advertise fields a template cannot honour (name, ownerReferences, resourceVersion)
// and bloat the CRD schema with them.
type SandboxTemplateMetadata struct {
	// Labels are applied to each created Sandbox, on top of the pool-ownership
	// labels the controller adds.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations are applied to each created Sandbox.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// SandboxPool condition types (standard Kubernetes condition convention).
const (
	// SandboxPoolConditionReady is True when every desired box is ready, i.e. the
	// pool is fully warm. Callers that can start work with a partially warm pool
	// should read status.ReadyReplicas instead of waiting on this.
	SandboxPoolConditionReady = "Ready"
)

// SandboxPool condition reasons.
const (
	// ReasonPoolWarm: every desired box is Ready.
	ReasonPoolWarm = "Warm"
	// ReasonPoolWarming: at least one box is still coming up. Not an error — a cold
	// pool takes minutes by nature.
	ReasonPoolWarming = "Warming"
	// ReasonPoolScaledToZero: spec.Replicas is 0, so there is nothing to be ready.
	// Distinguished from Warming so a parked pool does not read as a stuck one.
	ReasonPoolScaledToZero = "ScaledToZero"
)

// SandboxPoolStatus is the observed state of the pool.
type SandboxPoolStatus struct {
	// Replicas is how many Sandboxes the pool currently owns, ready or not. It is
	// the /scale subresource's status counterpart.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is how many owned Sandboxes are Ready — the number of boxes
	// that can actually serve an exec right now, which is what "is the pool warm"
	// really means.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Selector is the label selector matching this pool's Sandboxes, serialized in
	// the string form the /scale subresource requires. HPA and KEDA read the target's
	// selector from here, so autoscaling a pool does not work without it.
	// +optional
	Selector string `json:"selector,omitempty"`

	// Sandboxes names the boxes this pool owns, so the pool is a usable handout
	// list: a caller reads it to find a box to claim without listing and filtering
	// Sandboxes itself. Ordered by name for a stable diff.
	// +optional
	Sandboxes []string `json:"sandboxes,omitempty"`

	// Conditions follows the standard Kubernetes condition convention.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=sbxp
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.template.spec.nodePoolRef`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SandboxPool keeps N warm Sandboxes ready to hand out, so a consumer does not
// wait minutes for an instance to provision.
type SandboxPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxPoolSpec   `json:"spec,omitempty"`
	Status SandboxPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxPoolList contains a list of SandboxPool.
type SandboxPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SandboxPool{}, &SandboxPoolList{})
}
