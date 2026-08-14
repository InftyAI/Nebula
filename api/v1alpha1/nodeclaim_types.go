package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodeClaimSpec is the durable identity of one external instance: who it serves,
// which provider it lives on, and which policy produced it. It is created and
// owned by the controller (not users), one per placed Pod. The workload shape
// (image, resources, GPU type/count, spot) is NOT duplicated here — that lives
// on the Pod, which the provider controller reads directly. NodeClaim is a
// ledger, not a spec: its reason to exist is to survive the Node so teardown can
// reclaim the external instance and never leak a paid GPU.
type NodeClaimSpec struct {
	// PodRef links this claim to the Pod it serves. UID pins the exact Pod so a
	// recreated Pod of the same name gets a fresh claim rather than adopting the
	// old instance.
	PodRef PodReference `json:"podRef"`

	// Provider is the chosen NeoCloud. Immutable: a NodeClaim never migrates.
	// Recovery from preemption is delete-and-recreate. Held durably so teardown
	// knows which provider API to call even after status is lost.
	Provider string `json:"provider"`

	// CapacityType is the purchase tier the placement optimizer selected
	// (Spot/OnDemand). It is stored durably here because it is the one
	// provisioning input that cannot be read off the Pod, and Provision needs it
	// to re-issue the request after a controller restart. Immutable, like
	// Provider. Empty means "let the provider use its default" (e.g. Modal is
	// OnDemand-only and ignores it).
	// +optional
	CapacityType CapacityType `json:"capacityType,omitempty"`

	// Region is the region candidate the placement optimizer selected, in the
	// provider's own vocabulary (e.g. AWS "us-east-1"). Stored durably alongside
	// Provider/CapacityType because it is a provisioning input that cannot be read
	// off the Pod, and Provision needs it to re-issue the request in the same
	// region after a controller restart. Immutable, like Provider. Empty means
	// "the provider's configured default region" — a provider with no region
	// constraint declared on the pool leaves it empty.
	//
	// It is not always a single region NAME: a provider whose create cannot fail
	// over collapses every region the pool declared into ONE candidate, and stores
	// them joined by a provider-private separator (Modal uses "|", so a pool
	// declaring us-east and us-west records "us-east|us-west"). Only that provider
	// can split the value back, which it does at the API boundary. Treat the field
	// as an opaque provider-scoped token rather than parsing it.
	// +optional
	Region string `json:"region,omitempty"`

	// Accelerator is the requested accelerator pool this claim serves, as
	// "type:count" (e.g. "H100:8"), resolved at placement time from the Pod's
	// accelerator type + count. It names the POOL, not the concrete SKU: a launch
	// may span several interchangeable provider instance types (AWS's fleet tries
	// alternates), so the exact instance type is only known post-launch from the
	// observed instance — this field stays truthful regardless of which alternate
	// lands. Unlike Provider/CapacityType/Region it is NOT a provisioning input (the
	// provider re-derives it from the Pod) — it is recorded for reporting, like
	// PoolRef, so `kubectl get nc` shows what each instance serves without
	// cross-referencing the Pod. Empty for a CPU-only claim, which requests no
	// accelerator.
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
// The NodeClaim is a passive teardown ledger, not a status mirror: it does NOT
// track finer workload runtime status (CPU/logs/restarts/readiness) — the Pod is
// the source of truth for that (see pkg/vnode/status.go). It tracks only the
// coarse states that matter to its own job as a ledger, keyed off the served
// Pod's phase/reason: Provisioning (instance does not exist yet), Bound (an
// instance EXISTS at the provider — the guard the teardown backstop trusts), and
// Terminated (instance gone). Finer states (e.g. Preempted) are deliberately
// absent: preemption cannot be detected — the provider contract's InstanceState
// has no Preempted value, and an absent instance only tells us it is gone, not
// why. Reintroduce a phase only when something actually sets it.
//
// The ledger's question is EXISTENCE, not readiness: what the backstop must know
// is whether there is an instance out there to reclaim. A booting instance and a
// serving one are equally real — equally billable, equally in need of teardown —
// so both are Bound, and readiness is left entirely to the Pod. (This is why
// there is no Initializing phase: it would be a readiness distinction on an
// object that does not track readiness.)
type NodeClaimPhase string

const (
	// NodeClaimProvisioning: the served Pod has been observed but the external
	// instance does not yet exist — provisioning is still allocating it. The claim
	// does NOT earn the Bound teardown guard here: a Pod that vanishes while still
	// provisioning is treated as possible cache lag (grace window), not a real
	// teardown, because we never confirmed an instance was actually created.
	NodeClaimProvisioning NodeClaimPhase = "Provisioning"
	// NOTE: there is deliberately no "Initializing" phase. It used to mean "the
	// instance exists but is not reachable yet" and did NOT earn the teardown guard,
	// which stranded a real, billable instance behind the grace window whenever its
	// Pod vanished mid-boot. Existence is what the ledger tracks, so that state is
	// now Bound; readiness lives on the Pod alone.
	//
	// NodeClaimBound: an external instance EXISTS at the provider for this claim.
	// This is the durable guard the backstop trusts — a Bound claim whose Pod later
	// disappears is a real teardown, not cache lag, so it is reclaimed immediately
	// rather than after the grace window.
	//
	// Existence, NOT readiness: an instance that is booting (EC2 "pending", or
	// running with its 2/2 status checks still pending; a Modal sandbox whose
	// readiness probe has not passed) is Bound, because it is just as real and just
	// as billable as one that is serving. Whether the workload is actually usable is
	// the Pod's Ready condition, not this phase.
	NodeClaimBound NodeClaimPhase = "Bound"
	// NodeClaimTerminating: the served Pod is being deleted (its DeletionTimestamp
	// is set) but the external instance may not be reclaimed yet — teardown is in
	// flight. This is distinct from Terminated, which means the instance is already
	// GONE: here the Pod object still exists (draining its grace period / VK's
	// DeletePod running / a finalizer pending), so the phase reflects "going away"
	// rather than stranding the claim on a stale Provisioning/Bound. It is a
	// forward transition from ANY prior phase, since a deleting Pod is on its way
	// out regardless of how far provisioning had progressed. The claim self-deletes
	// (firing the terminate backstop) once the Pod object is fully gone.
	NodeClaimTerminating NodeClaimPhase = "Terminating"
	// NodeClaimTerminated: the external instance is gone. Set when the served Pod
	// has reached a terminal phase (Failed/Succeeded) — VK reports that when the
	// provider's instance disappears (torn down, reclaimed, or exited). The claim
	// stays around as a ledger of the vanished instance until its Pod is deleted;
	// it does NOT self-delete on this transition (the instance is already gone, so
	// there is nothing left to reclaim).
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
