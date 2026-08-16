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

// SandboxSetSpec maintains N Sandboxes. That is the whole contract — a SET, not a
// pool: there are no lease semantics here (claim a box, hold it, return it). Keeping
// N boxes alive is what ENABLES warm pooling and fan-out, but those are uses of a
// set, not its job. "Pool" is also already taken in this API group by NodePool.
//
// It creates Sandbox OBJECTS, not replicas inside itself: a set answers "how many",
// a Sandbox answers "which one, running what, for whom". Because each box stays its
// own object, per-box RBAC, per-box status, and a visible failure keep working —
// none of which survives being flattened into a replica index.
//
// Boxes get GENERATED names (myset-a4f2x), not ordinals. An ordinal implies a slot
// that gets refilled, so a dead box would be replaced by an empty one wearing the
// same name — same address, different filesystem. A generated name makes a
// replacement visibly a NEW box.
type SandboxSetSpec struct {
	// Replicas is how many Sandboxes to maintain. Zero is legal and useful: it
	// releases every box while keeping the set's definition, which is how a set is
	// parked overnight without being forgotten.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`

	// Template is the shape of every Sandbox this set creates. All boxes in one set
	// are the same shape by construction — the set exists to make boxes
	// interchangeable at the point of HANDOUT, so a caller can take any ready box
	// without inspecting it. Two shapes means two sets.
	//
	// A template is right here for the same reason it is wrong on Sandbox itself:
	// this object does not describe a box, it describes how to make them.
	Template SandboxTemplateSpec `json:"template"`
}

// SandboxTemplateSpec is the Sandbox a set stamps out: the standard Kubernetes
// template shape (metadata + spec), so created boxes can carry the labels a caller
// selects them by.
type SandboxTemplateSpec struct {
	// Metadata is the labels and annotations applied to each created Sandbox. Only
	// labels and annotations are honoured; a name here is ignored, since names are
	// generated per box.
	// +optional
	Metadata SandboxTemplateMetadata `json:"metadata,omitempty"`

	// Spec is the SandboxSpec of every box in the set.
	Spec SandboxSpec `json:"spec"`
}

// SandboxTemplateMetadata is the subset of ObjectMeta a template may set. It is
// spelled out rather than embedding metav1.ObjectMeta because embedding would
// advertise fields a template cannot honour (name, ownerReferences,
// resourceVersion) and bloat the CRD schema with them.
type SandboxTemplateMetadata struct {
	// Labels are applied to each created Sandbox, on top of the set-ownership
	// labels the controller adds.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations are applied to each created Sandbox.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// SandboxSet condition types (standard Kubernetes condition convention).
const (
	// SandboxSetConditionReady is True when every desired box is Ready. Callers that
	// can start work with a partially ready set should read status.ReadyReplicas
	// instead of waiting on this.
	SandboxSetConditionReady = "Ready"
)

// SandboxSet condition reasons.
const (
	// ReasonSandboxSetReady: every desired box is Ready.
	ReasonSandboxSetReady = "Ready"
	// ReasonSandboxSetProgressing: at least one box is still coming up. Not an error
	// — a cold set takes minutes by nature, since each box is a real instance.
	ReasonSandboxSetProgressing = "Progressing"
	// ReasonSandboxSetScaledToZero: spec.Replicas is 0, so there is nothing to be
	// ready. Distinguished from Progressing so a parked set does not read as a stuck
	// one.
	ReasonSandboxSetScaledToZero = "ScaledToZero"
)

// SandboxSetStatus is the observed state of the set.
type SandboxSetStatus struct {
	// Replicas is how many Sandboxes the set currently owns, ready or not. It is the
	// /scale subresource's status counterpart.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is how many owned Sandboxes are Ready — the number of boxes that
	// can actually serve an exec right now.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Selector is the label selector matching this set's Sandboxes, serialized in the
	// string form the /scale subresource requires. HPA and KEDA read the target's
	// selector from there, so autoscaling a set does not work without it.
	// +optional
	Selector string `json:"selector,omitempty"`

	// Sandboxes names the boxes this set owns, so the set is a usable handout list: a
	// caller reads it to find a box to use without listing and filtering Sandboxes
	// itself. Ordered by name for a stable diff.
	// +optional
	Sandboxes []string `json:"sandboxes,omitempty"`

	// Conditions follows the standard Kubernetes condition convention.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=sbs
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="NodePool",type=string,JSONPath=`.spec.template.spec.nodePoolRef`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SandboxSet maintains N Sandboxes, so boxes can be kept ready ahead of demand
// instead of making a consumer wait minutes for an instance to provision.
type SandboxSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxSetSpec   `json:"spec,omitempty"`
	Status SandboxSetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxSetList contains a list of SandboxSet.
type SandboxSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxSet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SandboxSet{}, &SandboxSetList{})
}
