package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodeClaimSpec is the durable identity of one external instance: who it serves,
// which provider it lives on, and which policy produced it. Controller-created (not
// user-facing), one per placed Pod. The workload shape (image, resources, GPU
// type/count, spot) is NOT duplicated here — it lives on the Pod, which the provider
// controller reads directly. This is a ledger, not a spec: it exists to survive the
// Node so teardown can reclaim the instance and never leak a paid GPU.
type NodeClaimSpec struct {
	// PodRef links this claim to the Pod it serves. UID pins the exact Pod so a
	// recreated Pod of the same name gets a fresh claim rather than adopting the
	// old instance.
	PodRef PodReference `json:"podRef"`

	// Provider is the chosen NeoCloud. Immutable: a NodeClaim never migrates.
	// Recovery from preemption is delete-and-recreate. Held durably so teardown
	// knows which provider API to call even after status is lost.
	Provider string `json:"provider"`

	// CapacityType is the purchase tier placement selected (Spot/OnDemand). Stored
	// durably because it is a provisioning input that cannot be read off the Pod, and
	// Provision needs it to re-issue the request after a controller restart. Immutable,
	// like Provider. Empty means "use the provider's default" (Modal is OnDemand-only
	// and ignores it).
	// +optional
	CapacityType CapacityType `json:"capacityType,omitempty"`

	// Region is the region candidate placement selected, in the provider's own
	// vocabulary (e.g. AWS "us-east-1"). Durable and immutable for the same reason as
	// CapacityType: Provision must re-issue in the same region after a restart. Empty
	// means the provider's default — a pool that declared no region constraint.
	//
	// Not always a single region NAME: a provider whose create cannot fail over
	// collapses every declared region into ONE candidate, joined by a provider-private
	// separator (Modal uses "|", so us-east + us-west records "us-east|us-west"). Only
	// that provider can split it back. Treat the value as an opaque token.
	// +optional
	Region string `json:"region,omitempty"`

	// Accelerator is the accelerator pool this claim serves, as "type:count" (e.g.
	// "H100:8"), resolved at placement time from the Pod. It names the POOL, not the
	// SKU: a launch may span several interchangeable instance types (AWS's fleet tries
	// alternates), so this stays truthful regardless of which alternate lands. Unlike
	// Provider/CapacityType/Region it is NOT a provisioning input (the provider
	// re-derives it from the Pod) — it is recorded so `kubectl get nc` shows what each
	// instance serves. Empty for a CPU-only claim.
	// +optional
	Accelerator string `json:"accelerator,omitempty"`

	// PoolRef is the NodePool whose policy produced this claim, for reporting.
	// +optional
	PoolRef string `json:"poolRef,omitempty"`
}

// PodReference identifies the namespaced Pod a cluster-scoped NodeClaim serves.
type PodReference struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	// UID pins the exact Pod object; a recreated Pod of the same name gets a new
	// NodeClaim rather than silently adopting the old instance.
	UID string `json:"uid"`
}

// NodeClaimPhase is the coarse, user-facing lifecycle state.
//
// The claim is a passive teardown ledger, not a status mirror: workload runtime
// status (CPU/logs/restarts/readiness) belongs to the Pod (see pkg/vnode/status.go).
// It tracks only what its own job needs, keyed off the served Pod: Provisioning
// (no instance yet), Bound (an instance EXISTS — the guard the teardown backstop
// trusts), Terminated (gone). Finer states like Preempted are absent because nothing
// can detect them: InstanceState has no Preempted value, and an absent instance only
// says it is gone, not why. Add a phase only when something actually sets it.
//
// The ledger's question is EXISTENCE, not readiness — a booting instance is just as
// billable as a serving one, so both are Bound and readiness is left to the Pod.
// (Hence no Initializing phase: a readiness distinction on an object that does not
// track readiness.)
type NodeClaimPhase string

const (
	// NodeClaimProvisioning: the served Pod has been observed but the external
	// instance does not yet exist — provisioning is still allocating it. The claim
	// does NOT earn the Bound teardown guard here: a Pod that vanishes while still
	// provisioning is treated as possible cache lag (grace window), not a real
	// teardown, because we never confirmed an instance was actually created.
	NodeClaimProvisioning NodeClaimPhase = "Provisioning"
	// NOTE: there is deliberately no "Initializing" phase. It meant "exists but not
	// reachable yet" and did NOT earn the teardown guard, which stranded a real,
	// billable instance behind the grace window whenever its Pod vanished mid-boot.
	// That state is now Bound; readiness lives on the Pod alone.
	//
	// NodeClaimBound: an external instance EXISTS at the provider. This is the durable
	// guard the backstop trusts — a Bound claim whose Pod later disappears is a real
	// teardown, not cache lag, so it is reclaimed immediately instead of after the
	// grace window.
	//
	// Existence, NOT readiness: a booting instance (EC2 "pending", or running with its
	// 2/2 checks outstanding; a Modal sandbox whose probe has not passed) is Bound,
	// because it bills the same as one that is serving. Usability is the Pod's Ready
	// condition.
	NodeClaimBound NodeClaimPhase = "Bound"
	// NodeClaimTerminating: the served Pod is being deleted but the instance may not be
	// reclaimed yet — teardown is in flight. Distinct from Terminated (already GONE):
	// here the Pod object still exists (grace period draining, VK's DeletePod running,
	// a finalizer pending), so the claim reads "going away" rather than being stranded
	// on a stale Provisioning/Bound. A forward transition from ANY prior phase, since a
	// deleting Pod is on its way out regardless of provisioning progress. The claim
	// self-deletes (firing the backstop) once the Pod object is fully gone.
	NodeClaimTerminating NodeClaimPhase = "Terminating"
	// NodeClaimTerminated: the instance is gone. Set when the served Pod reaches a
	// terminal phase (Failed/Succeeded), which VK reports when the provider's instance
	// disappears. The claim stays as the ledger of the vanished instance until its Pod
	// is deleted, and does NOT self-delete here — there is nothing left to reclaim.
	NodeClaimTerminated NodeClaimPhase = "Terminated"
)

// NodeClaimStatus is the durable record reconciled against the provider by the
// poll loop.
type NodeClaimStatus struct {
	// Phase is the coarse lifecycle state.
	// +optional
	Phase NodeClaimPhase `json:"phase,omitempty"`

	// InstanceID is the provider's identifier for the external instance (e.g. a
	// RunPod pod id). This is the field that must not be lost: the terminate
	// finalizer uses it to reclaim the instance.
	// +optional
	InstanceID string `json:"instanceID,omitempty"`

	// NodeName is the virtual Node created for this instance, once it exists.
	// +optional
	NodeName string `json:"nodeName,omitempty"`

	// Endpoint is the reachable address (e.g. SSH host:port) once ready.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=nc
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.provider`
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.spec.region`
// +kubebuilder:printcolumn:name="ACCELERATOR",type=string,JSONPath=`.spec.accelerator`
// +kubebuilder:printcolumn:name="CAPACITY_TYPE",type=string,JSONPath=`.spec.capacityType`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=`.status.instanceID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NodeClaim represents one external GPU instance and its lifecycle.
type NodeClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodeClaimSpec   `json:"spec,omitempty"`
	Status NodeClaimStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NodeClaimList contains a list of NodeClaim.
type NodeClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeClaim `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NodeClaim{}, &NodeClaimList{})
}
