// Package v1alpha1 contains the Nebula API types.
//
// Nebula is an operator that orchestrates GPU workloads across NeoClouds
// (RunPod, Modal, Kubernetes, ...). It follows a Karpenter-style split:
//
//	NodePool  - policy: which providers are allowed, how to choose between
//	            them (cost/availability), failover behaviour, and the GPU shape.
//	NodeClaim - one provisioned external instance and its lifecycle. Owns the
//	            terminate finalizer so a paid instance is never leaked.
//
// +kubebuilder:object:generate=true
// +groupName=nebula.inftyai.com
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group/version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "nebula.inftyai.com", Version: "v1alpha1"}

	// SchemeBuilder registers the types with a Scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// Well-known keys used across the project. Kept here so the webhook, the
// placement controller and the NodeClaim controller share one source of truth.
const (
	// EnabledLabel opts a Pod into Nebula. It doubles as the webhook's
	// objectSelector so only opted-in Pods ever hit the mutating webhook.
	EnabledLabel = "nebula.inftyai.com/enabled"

	// ProviderSelectionGate is the scheduling gate the webhook injects at Pod
	// CREATE. The placement controller removes it once it has chosen a
	// provider (by adding a provider nodeSelector), releasing the Pod to the
	// scheduler.
	ProviderSelectionGate = "nebula.inftyai.com/provider-selection"

	// ProviderLabel is set on each provider's virtual node and added to a Pod's
	// nodeSelector by the placement controller to route it to that provider.
	ProviderLabel = "nebula.inftyai.com/provider"

	// ManagedByLabel marks every object Nebula creates and owns (starting with
	// the virtual nodes). It uses the well-known app.kubernetes.io/managed-by key
	// so standard tooling recognizes it; its value is always ManagedByValue. This
	// is the stable, management-scoped selector for "everything Nebula manages",
	// independent of provider routing — for NetworkPolicies, monitoring scrape
	// configs, and operator queries.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	// ManagedByValue is the sole value of ManagedByLabel.
	ManagedByValue = "nebula"

	// PoolLabel records which NodePool a Pod (and its NodeClaim) belongs to. Its
	// value is the NodePool name, so the key mirrors the CRD kind.
	PoolLabel = "nebula.inftyai.com/nodepool"

	// AcceleratorTypeLabel carries the requested accelerator TYPE only (e.g.
	// "a100-40gb" or "h100"). The COUNT is expressed separately as a standard
	// resource request/limit on the container (nvidia.com/gpu for the NVIDIA
	// accelerators the wired providers serve today) — so scheduling fit and
	// provisioning read the same number, and there is no bespoke count grammar. It
	// is a label (not an annotation) so Pods can be selected/validated by
	// accelerator type; label values forbid ":", which is exactly why the count
	// could never live here. The name is provider-neutral (accelerator, not GPU)
	// so it also fits non-GPU accelerators (TPUs, etc.) when such a provider lands.
	// The type is matched case-insensitively against the provider catalog, so
	// "a100", "A100" both resolve; the provider's canonical casing is what actually
	// gets provisioned (see catalog.Base.MapAccelerator). Read type+count together
	// via util.AcceleratorRequest.
	AcceleratorTypeLabel = "nebula.inftyai.com/accelerator-type"

	// CapacityTypeAnnotation carries the optimizer-chosen purchase tier
	// (Spot/OnDemand/Reserved). It is the one provisioning input that cannot be
	// read off the Pod's own spec, so the placement controller writes it here
	// when it ungates the Pod. The virtual kubelet — which provisions solely from
	// the Pod — reads it back on CreatePod. Empty means "let the provider use its
	// default" (e.g. Modal is OnDemand-only and ignores it).
	CapacityTypeAnnotation = "nebula.inftyai.com/capacity-type"

	// RegionAnnotation carries the optimizer-chosen provider region. Like
	// CapacityTypeAnnotation it is a provisioning input absent from the Pod's own
	// spec, so the placement controller writes it when it ungates the Pod and the
	// virtual kubelet reads it back on CreatePod into ProvisionRequest.Region.
	// Empty/absent means "the provider's configured default region" — region-simple
	// providers (Modal, RunPod) ignore it.
	RegionAnnotation = "nebula.inftyai.com/region"

	// BlocklistTTLAnnotation carries the pool's FailoverPolicy.BlocklistTTL down to
	// the virtual kubelet. Like the two annotations above it is a provisioning-time
	// input the Pod cannot otherwise express: the TTL is a NodePool policy, but the
	// VK handler (which provisions per-Pod and never sees the pool) needs it to know
	// how long to blocklist a placement that just failed. The placement controller
	// stamps it when it ungates the Pod; the handler reads it on a Provision failure
	// to bound the block it records. Absent/unparseable means the handler's built-in
	// default TTL.
	BlocklistTTLAnnotation = "nebula.inftyai.com/blocklist-ttl"

	// TerminateInstanceFinalizer is held by every NodeClaim to guarantee teardown.
	// The virtual kubelet owns the happy path (DeletePod → provider.Terminate,
	// keyed on the Pod-derived claim name), but its teardown is edge-triggered and
	// its instance tracking is in-memory, so a Pod force-deleted during a VK outage
	// would leak a paid instance. This finalizer makes teardown level-triggered:
	// the cluster-scoped claim outlives the namespaced Pod, so on delete the
	// NodeClaim controller resolves the provider, finds the instance by claim name
	// via List, and Terminates it before releasing the finalizer — independent of
	// VK liveness (see docs/architecture.md §3).
	TerminateInstanceFinalizer = "nebula.inftyai.com/terminate-instance"
)
