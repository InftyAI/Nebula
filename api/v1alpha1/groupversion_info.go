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
// On top of that provisioning core sit the workload types, each synthesizing
// Pods onto the same placement path rather than bypassing it:
//
//	Sandbox    - one interactive remote box (agent workspace, shell, scratch GPU),
//	             reachable with the same kubectl exec/logs as a local Pod.
//	SandboxSet - maintains N Sandboxes, and owns /scale so `kubectl scale` and HPA
//	             drive the count. Keeping boxes ready ahead of demand is a USE of
//	             this, not its definition — there are no lease semantics here.
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

	// EnabledValue is the only value of EnabledLabel that opts a Pod in. The
	// comparison is exact, so a Pod labelled "True" or "1" is NOT opted in — the
	// label is the webhook's objectSelector, and the API server matches it
	// literally, so anything else would make the controllers and the selector
	// disagree about which Pods are Nebula's.
	EnabledValue = "true"

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

	// SandboxLabel records which Sandbox a Pod belongs to. Its value is the Sandbox
	// name, so the key mirrors the CRD kind. The Sandbox controller selects its own
	// Pod by it, and it is what makes `kubectl get pods -l
	// nebula.inftyai.com/sandbox=alice` work.
	SandboxLabel = "nebula.inftyai.com/sandbox"

	// SandboxSetLabel records which SandboxSet created a Sandbox. Its value is the
	// set name. It is the selector the set's /scale subresource publishes in status
	// (so HPA can find the set's members) and how the set controller enumerates the
	// boxes it owns — ownerReferences alone would not support a label-selector query.
	SandboxSetLabel = "nebula.inftyai.com/sandboxset"

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
	// (Spot/OnDemand). It is the one provisioning input that cannot be
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

	// EndpointAnnotation carries the reachable address of the external instance
	// once it is running (a public DNS name or IP, in the provider's own form).
	// It is the ONLY way to reach the workload, so it must be visible on the Pod:
	// PodIP cannot hold it because the API server validates PodIP as a literal IP
	// and rejects a DNS name (the common AWS case), so the endpoint rides an
	// annotation instead. Written by the virtual kubelet when it first observes the
	// instance running; absent until then. Unlike the provisioning-input
	// annotations above (which the placement controller stamps and VK reads), this
	// flows the other way — VK writes it for operators/tooling to read.
	EndpointAnnotation = "nebula.inftyai.com/endpoint"

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

// Pod status reasons the virtual kubelet stamps on the Pods it reports, projecting
// the external instance's lifecycle onto standard Pod status (pkg/vnode/status.go
// is the only writer).
//
// They live here, not privately in pkg/vnode, for two reasons. They are a CONTRACT
// between packages: the Pod phase is lossy — Provisioning and "booting" both
// surface as PodPending — so the reason is the only thing separating "no instance
// exists yet" from "an instance exists and is coming up", and the NodeClaim
// controller keys its teardown guard off exactly that distinction (see
// desiredPhase). A rename on the writing side that the reading side did not follow
// would still compile, still pass tests, and silently leak paid instances: every
// booting instance would read as Provisioning, so a Pod that vanished mid-boot
// would be left running behind the cache-lag grace window. And they are user-facing
// — operators match on status.reason in jsonpath and alerts — so every value is
// public API whether or not Nebula's own code currently reads it. That is why the
// whole set is here rather than the subset with in-tree readers: these are the
// values status.reason can take, and a reader should find them in one place.
const (
	// PodReasonProvisioning: capacity has not been allocated yet. Stamped by CreatePod
	// before it calls Provision, and HELD if Provision returns an id without reserving
	// capacity — a Modal sandbox the control plane accepted but that is still queued
	// for a GPU. So the instance may exist (and then must be reclaimed) even under this
	// reason; what has not happened is the allocation. Replaced by Initializing as soon
	// as capacity is committed: at once for a provider that allocates synchronously
	// (AWS), otherwise when the first poll observes the instance.
	PodReasonProvisioning = "Provisioning"
	// PodReasonInitializing: the instance EXISTS at the provider but is not yet
	// reachable — it is booting (EC2 "pending"), running-but-not-yet-passing its
	// reachability checks (running, <2/2, EC2's own "Initializing" status), or a Modal
	// sandbox whose readiness probe has not passed. It mirrors that EC2 status-check
	// term. Provisioning is done; the instance is coming up. Distinct from
	// Provisioning so a Pod stuck here points at a slow boot / failing status checks,
	// not a stuck allocation — and so the NodeClaim controller can tell that an
	// instance exists. The virtual kubelet stamps it only on EVIDENCE of existence:
	// either the provider observed the instance in its List, or Provision reported it
	// reserved (capacity committed, not merely requested). That is what makes it
	// trustworthy for the claim to key Bound off.
	PodReasonInitializing = "Initializing"
	// PodReasonRunning: the provider reports the instance running.
	PodReasonRunning = "Running"
	// PodReasonProvisionFailed: the provider rejected or failed the Provision call.
	PodReasonProvisionFailed = "ProvisionFailed"
	// PodReasonFailed: the provider reports the instance in a failed state.
	PodReasonFailed = "Failed"
	// PodReasonTerminated: the instance is gone from the provider (torn down,
	// reclaimed, or exited). Disappearance alone does not say WHY, so this is the
	// neutral term rather than "Preempted".
	PodReasonTerminated = "Terminated"
)
