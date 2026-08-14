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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SandboxSpec is one long-lived, interactive remote box: an agent's workspace, a
// shell, a scratch GPU machine. It is the first workload class Nebula serves
// beyond a hand-written Pod.
//
// A Sandbox is SINGULAR — one object, one instance. There is deliberately no
// replicas field and no pod template, because a sandbox is not fungible: someone
// is attached to it, it accumulates state in its filesystem, and the object's own
// name IS its stable identity (`kubectl exec sandbox-alice` always reaches the
// same box). Set-shaped controllers exist because containers are interchangeable,
// which is exactly the property a sandbox lacks — a rolling update would evict a
// live session, and "scale in by one" would have to guess whose box to kill. A
// caller that wants N boxes creates N Sandboxes, each with its own image,
// lifetime and identity.
//
// The count lives one level up, in SandboxSet, which maintains N Sandboxes and
// owns /scale so `kubectl scale` and HPA work. That split is what lets this type
// stay singular: because the set creates Sandbox OBJECTS rather than replicas
// inside one object, everything that depends on a box being its own object —
// per-box RBAC (grant a user their sandbox and not their neighbour's), a
// per-box image and TTL, and a failure that stays visible instead of being
// papered over by a replacement — keeps working underneath a pool.
//
// The spec deliberately reuses corev1 types (ResourceRequirements, EnvVar) rather
// than inventing parallel fields. The controller synthesizes a Pod, so anything
// it accepts must ultimately BE PodSpec-shaped; re-declaring resources or env
// would fork the vocabulary and, worse, fork the source of truth for the
// accelerator COUNT — which placement and the scheduler's fit check both read
// from the container's nvidia.com/gpu limit (see util.AcceleratorRequest).
//
// The CEL rule below rejects a GPU count with no accelerator type. That pair is
// contradictory rather than merely incomplete — util.AcceleratorRequest returns an
// error for it — so without the rule the object is admitted and then fails at
// PLACEMENT, minutes later and one object removed from the mistake. Note the
// inverse is fine and deliberately allowed: a type with no count means one
// accelerator.
// +kubebuilder:validation:XValidation:rule="has(self.acceleratorType) || !has(self.resources) || ((!has(self.resources.limits) || !('nvidia.com/gpu' in self.resources.limits)) && (!has(self.resources.requests) || !('nvidia.com/gpu' in self.resources.requests)))",message="nvidia.com/gpu requires acceleratorType to be set"
type SandboxSpec struct {
	// NodePoolRef names the NodePool whose policy places this sandbox: which
	// providers are allowed, which capacity tiers, how to rank them. Required —
	// there is no implicit default pool, because placing a paid GPU instance
	// against a guessed policy is not a safe default.
	// +kubebuilder:validation:MinLength=1
	NodePoolRef string `json:"nodePoolRef"`

	// Image is the container image the sandbox runs. It defaults to a plain Ubuntu,
	// because unlike the accelerator the image is not a decision a caller has to make
	// to get a useful box: `kubectl exec` into a bare distro is exactly the "give me a
	// remote shell" case, and anything else can be installed from inside it. Defaulting
	// a paid GPU shape would be guessing at spend; defaulting a shell is not.
	//
	// Note it deliberately does NOT default to a CUDA image even when an accelerator is
	// requested. A conditional default would make the image depend on another field,
	// which structural-schema defaulting cannot express and which would surprise anyone
	// reading the object back. Ask for a CUDA image explicitly when you want one.
	//
	// There is deliberately no command field, and one cannot be set: the CRD is a
	// structural schema, so `command:` in a Sandbox spec is rejected as an unknown
	// field by the apiserver itself — no webhook required. That is not a
	// simplification, it is the process model: a sandbox has nothing to run at boot,
	// so the controller supplies a placeholder command whose only job is to keep the
	// container from exiting (today this is implemented with a long-running `sleep`,
	// which must exist in the chosen image). Nebula does not currently support
	// `kubectl exec`/`kubectl logs` against sandboxes; a user-supplied command would
	// displace the placeholder and take the instance down with it.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:default="ubuntu:24.04"
	// +optional
	Image string `json:"image,omitempty"`

	// AcceleratorType is the requested accelerator TYPE (e.g. "a100-40gb",
	// "h100"), matched case-insensitively against the provider catalog. The COUNT
	// is NOT here: it is a standard nvidia.com/gpu entry in Resources, so exactly
	// one number drives scheduling fit and provisioning. The controller stamps
	// this onto the synthesized Pod's AcceleratorTypeLabel, so a Sandbox and a
	// hand-written Nebula Pod go through identical placement.
	//
	// Empty means a CPU-only sandbox, which is a legitimate (and cheap) thing to
	// want for a shell or an agent that only needs a filesystem.
	// +optional
	AcceleratorType string `json:"acceleratorType,omitempty"`

	// Resources is the standard Kubernetes resource requirements for the sandbox
	// container, verbatim. The accelerator count rides here as an nvidia.com/gpu
	// limit (`limits: {nvidia.com/gpu: "1"}`), which is where both the placement
	// controller and the scheduler already read it from.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Env is passed to the sandbox container verbatim, including valueFrom
	// references — a sandbox usually needs at least a registry or Hugging Face
	// token, and re-inventing secret indirection here would be strictly worse than
	// reusing the field everyone already knows.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// TTL bounds the sandbox's total lifetime, measured from the moment it first
	// became Ready (NOT from creation, so a slow provision does not eat into the
	// user's time). On expiry the controller releases the instance and the sandbox
	// reports phase Expired.
	//
	// This exists because the failure mode of a remote GPU box is financial: an
	// abandoned sandbox bills until someone notices. Omit it for an unbounded
	// sandbox, which is a deliberate choice rather than the default.
	// +optional
	TTL *metav1.Duration `json:"ttl,omitempty"`
}

// SandboxPhase is the coarse, user-facing lifecycle state, derived from the
// synthesized Pod rather than tracked independently. The Pod (via the virtual
// kubelet) is the source of truth for what the external instance is doing — see
// pkg/vnode/status.go — so this is a projection, and the vocabulary intentionally
// mirrors the Pod status reasons the vnode stamps.
type SandboxPhase string

const (
	// SandboxPending: the sandbox exists but its Pod has not been placed yet —
	// typically waiting on the provider-selection gate, e.g. because no provider in
	// the pool can currently serve the requested accelerator. A sandbox that sits
	// here points at placement, not at the provider.
	SandboxPending SandboxPhase = "Pending"
	// SandboxProvisioning: a provider Provision call is in flight; the external
	// instance does not exist yet.
	SandboxProvisioning SandboxPhase = "Provisioning"
	// SandboxInitializing: the instance exists at the provider but is not yet
	// reachable — booting, or up but not yet passing reachability checks. Kept
	// distinct from Provisioning so a stuck sandbox distinguishes "cannot get
	// capacity" from "capacity granted, slow boot".
	SandboxInitializing SandboxPhase = "Initializing"
	// SandboxReady: the instance is running and reachable. This is the only phase
	// in which exec/logs can succeed.
	SandboxReady SandboxPhase = "Ready"
	// SandboxFailed: the instance failed or vanished (terminated out-of-band,
	// reclaimed, or the provision was rejected). Terminal: a sandbox holds
	// filesystem state that a fresh instance would not have, so it is never
	// silently recreated underneath its user. Delete and recreate it explicitly.
	SandboxFailed SandboxPhase = "Failed"
	// SandboxExpired: spec.TTL elapsed and the instance was released. Terminal, and
	// deliberately not garbage: the object stays as the record of why the box went
	// away, so a user who returns to a dead sandbox gets an answer instead of a
	// NotFound.
	SandboxExpired SandboxPhase = "Expired"
)

// Sandbox condition types (standard Kubernetes condition convention).
const (
	// SandboxConditionReady is True exactly when the sandbox is usable — the
	// instance is running and reachable. It is the condition to wait on
	// (`kubectl wait --for=condition=Ready sandbox/x`) and mirrors the Pod's own
	// Ready condition.
	SandboxConditionReady = "Ready"
)

// Sandbox condition reasons.
const (
	// ReasonSandboxReady: the instance is running and reachable.
	ReasonSandboxReady = "Ready"
	// ReasonSandboxProvisioning: still bringing the instance up (covers both
	// placement and boot; the phase distinguishes them).
	ReasonSandboxProvisioning = "Provisioning"
	// ReasonSandboxFailed: the instance failed, was rejected, or vanished.
	ReasonSandboxFailed = "Failed"
	// ReasonSandboxExpired: spec.TTL elapsed and the instance was released.
	ReasonSandboxExpired = "Expired"
	// ReasonPodConflict: a Pod of the required name already exists and is NOT owned
	// by this Sandbox. The controller refuses to adopt it — it could be an unrelated
	// workload, and adopting would hand someone else's Pod a terminate finalizer —
	// so the sandbox surfaces the collision instead of acting on a guess.
	ReasonPodConflict = "PodConflict"
)

// SandboxStatus is the observed state, projected from the synthesized Pod.
type SandboxStatus struct {
	// Phase is the coarse lifecycle state.
	// +optional
	Phase SandboxPhase `json:"phase,omitempty"`

	// PodName is the synthesized Pod backing this sandbox. It is recorded even
	// though it currently equals the Sandbox name, so tooling (and `kubectl exec`
	// wrappers) read the pod identity from status rather than reconstructing it
	// from a naming convention this controller would then be unable to change.
	// +optional
	PodName string `json:"podName,omitempty"`

	// Endpoint is the reachable address of the external instance once it is
	// running, in the provider's own form (a public DNS name or an IP). Mirrored
	// from the Pod's EndpointAnnotation, which is where the virtual kubelet
	// publishes it.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// ReadyTime is when the sandbox first became Ready. It is the anchor TTL is
	// measured from, so it is durable status rather than a derived value: if it
	// were recomputed from the Pod, a Pod status blip could silently restart the
	// user's clock.
	// +optional
	ReadyTime *metav1.Time `json:"readyTime,omitempty"`

	// ExpiryTime is when TTL will elapse (ReadyTime + TTL), surfaced so a user can
	// see the deadline without doing the arithmetic. Absent when no TTL is set or
	// the sandbox has not become Ready yet.
	// +optional
	ExpiryTime *metav1.Time `json:"expiryTime,omitempty"`

	// Conditions follows the standard Kubernetes condition convention.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName={sb,sbx}
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.nodePoolRef`
// +kubebuilder:printcolumn:name="Accelerator",type=string,JSONPath=`.spec.acceleratorType`
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.endpoint`
// +kubebuilder:printcolumn:name="Expires",type=date,JSONPath=`.status.expiryTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Sandbox is one interactive remote instance — an agent workspace, a shell, a
// scratch GPU box — reachable with the same `kubectl exec` / `kubectl logs` a
// local Pod would be.
type Sandbox struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxSpec   `json:"spec,omitempty"`
	Status SandboxStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxList contains a list of Sandbox.
type SandboxList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Sandbox `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Sandbox{}, &SandboxList{})
}
