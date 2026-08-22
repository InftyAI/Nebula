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

	// AcceleratorTypeLabel carries the accelerator TYPE only (e.g. "a100-40gb",
	// "h100"). The COUNT is a standard container resource request/limit
	// (nvidia.com/gpu today), so scheduling fit and provisioning read the same number
	// and there is no bespoke count grammar. A label rather than an annotation so Pods
	// can be selected by accelerator type — and label values forbid ":", which is why
	// the count could never live here. The name says "accelerator", not GPU, so TPUs
	// and friends fit when such a provider lands. Matched case-insensitively against
	// the catalog ("a100" and "A100" both resolve); the provider's canonical casing is
	// what gets provisioned (see catalog.Base.MapAccelerator). Read type+count together
	// via util.AcceleratorRequest.
	AcceleratorTypeLabel = "nebula.inftyai.com/accelerator-type"

	// EndpointAnnotation carries the reachable address of the external instance (a DNS
	// name, an IP, or a URL, in the provider's own form). It is the only way to reach
	// the workload, and PodIP cannot hold it — the API server validates PodIP as a
	// literal IP and rejects a DNS name, the common AWS case — so it rides an
	// annotation. The virtual kubelet writes it as soon as it knows the address, which
	// is NOT tied to the phase: a provider that mints a connect URL at create time
	// (Modal) publishes from CreatePod, before Running; one whose address only exists
	// after boot (AWS) publishes from the poll loop. Absent until then, never cleared.
	// This one flows outward — VK writes, operators read.
	EndpointAnnotation = "nebula.inftyai.com/endpoint"

	// InstanceIDAnnotation carries the provider's id for the external instance backing
	// this Pod. Written by the virtual kubelet as soon as Provision returns an id — which
	// is the only place it is ever learned, since VK otherwise holds it in memory — and
	// never cleared.
	//
	// It exists so the NodeClaim controller can record status.InstanceID from the Pod it
	// has already fetched. Before this it asked the PROVIDER, listing every instance and
	// matching on claim name, on every reconcile until the id resolved: correct, but a
	// provider API call per reconcile per claim, which against a real backend means
	// hundreds of DescribeInstances/list calls for one large batch, into APIs that rate
	// limit. The id is a fact VK already knows, so it flows outward on the Pod like the
	// endpoint does rather than being searched for.
	//
	// The claim's own copy is still the durable one: teardown runs after the Pod is gone,
	// so it reads status.InstanceID, falling back to List-by-claim-name when the id never
	// made it across.
	InstanceIDAnnotation = "nebula.inftyai.com/instance-id"

	// TerminateInstanceFinalizer is held by every NodeClaim to guarantee teardown. VK
	// owns the happy path (DeletePod → provider.Terminate), but its teardown is
	// edge-triggered and its tracking in-memory, so a Pod force-deleted during a VK
	// outage would leak a paid instance. This finalizer makes teardown
	// level-triggered: the cluster-scoped claim outlives the namespaced Pod, so on
	// delete the NodeClaim controller resolves the provider, finds the instance by
	// claim name via List, and Terminates before releasing — independent of VK
	// liveness (see docs/architecture.md §3).
	TerminateInstanceFinalizer = "nebula.inftyai.com/terminate-instance"
)

// Pod status reasons the virtual kubelet stamps on the Pods it reports, projecting
// the external instance's lifecycle onto standard Pod status (pkg/vnode/status.go is
// the only writer).
//
// They are public rather than private to pkg/vnode because they are a CONTRACT
// between packages. The Pod phase is lossy — provisioning and booting both surface as
// PodPending — so the reason is the only thing separating "no instance yet" from "an
// instance exists and is coming up", and the NodeClaim controller keys its teardown
// guard off exactly that (see desiredPhase). A rename on the writing side that the
// reader missed would still compile, still pass tests, and silently leak paid
// instances. They are also user-facing (operators match status.reason in jsonpath and
// alerts), so the whole set lives here — not just the values with in-tree readers.
const (
	// PodReasonProvisioning: capacity has not been allocated yet. Stamped by CreatePod
	// before it calls Provision, and HELD if Provision returns an id without reserving
	// capacity — a Modal sandbox the control plane accepted but that is still queued
	// for a GPU. So the instance may exist (and then must be reclaimed) even under this
	// reason; what has not happened is the allocation. Replaced by Initializing as soon
	// as capacity is committed: at once for a provider that allocates synchronously
	// (AWS), otherwise when the first poll observes the instance.
	PodReasonProvisioning = "Provisioning"
	// PodReasonInitializing: the instance EXISTS but is not yet reachable — booting
	// (EC2 "pending"), running with reachability checks outstanding (<2/2, EC2's own
	// "Initializing" term, which this mirrors), or a Modal sandbox whose probe has not
	// passed. Distinct from Provisioning so a Pod stuck here points at a slow boot
	// rather than a stuck allocation, and so the NodeClaim controller can tell an
	// instance exists. VK stamps it only on EVIDENCE of existence: the provider
	// observed the instance in List, or Provision reported it reserved (capacity
	// committed, not merely requested). That is what makes it safe to key Bound off.
	PodReasonInitializing = "Initializing"
	// PodReasonRunning: the provider reports the instance running.
	PodReasonRunning = "Running"
	// PodReasonProvisionFailed: the provider rejected or failed the Provision call.
	PodReasonProvisionFailed = "ProvisionFailed"
	// PodReasonConfigError: the Pod references something unreadable — a missing Secret or
	// ConfigMap behind an env var, or a downward-API field this node cannot answer — so
	// nothing was requested from the provider. The kubelet's CreateContainerConfigError,
	// and non-terminal for the same reason: the reference usually appears moments later, and
	// waiting is free while nothing exists to bill.
	PodReasonConfigError = "ConfigError"
	// PodReasonFailed: the provider reports the instance in a failed state.
	PodReasonFailed = "Failed"
	// PodReasonTerminated: the instance is gone from the provider (torn down,
	// reclaimed, or exited). Disappearance alone does not say WHY, so this is the
	// neutral term rather than "Preempted".
	PodReasonTerminated = "Terminated"
)
