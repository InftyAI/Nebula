// Package provider defines the abstraction every compute provider (RunPod,
// Modal, Kubernetes, ...) implements. It is the single narrow seam between
// Nebula's provider-agnostic control plane (placement controller, NodeClaim
// controller, poll loop) and the heterogeneous cloud APIs underneath.
//
// Scope: v1 targets NeoClouds (RunPod, Modal, CoreWeave, Lambda), which are
// region-simple, so region/zone is intentionally NOT modeled yet. Hyperscalers
// (AWS/GCP/Azure) are a planned near-term expansion; when they land, Region/Zone
// become additive fields on the request/Offering/BlockScope structs and the
// optimizer's candidate key widens to include them — the method signatures here
// are designed not to change. Do not hard-code NeoCloud-only assumptions into
// the control plane; keep provider quirks behind Capabilities.
//
// Design rules learned from SkyPilot / the Nebula design discussion:
//   - The Pod is the source of truth for the workload shape. Provision takes the
//     Pod and reads image/command/env/ports/resources and the accelerators
//     annotation off it; the control plane never re-encodes that into a provider
//     call itself.
//   - Providers differ in capabilities (RunPod cannot stop, has no native tags,
//     no preemption push). Those quirks are declared via Capabilities and
//     handled here, not leaked into the control plane.
//   - Detection is poll-based everywhere (no provider gives a reliable
//     preemption push, and even those with a spot notice are polled uniformly),
//     so List must return all of this provider's instances in as few calls as
//     possible.
package provider

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// Provider is one compute-provider backend. Implementations are registered by name
// (matching NodePool.spec.providers[].name and the ProviderLabel on the virtual
// node). All methods must be safe for concurrent use.
type Provider interface {
	// Name is the stable identifier, e.g. "runpod".
	Name() string

	// Capabilities declares provider quirks so the control plane can behave
	// generically instead of branching on provider name.
	Capabilities() Capabilities

	// --- Lifecycle -------------------------------------------------------

	// Provision creates exactly one external instance for the given Pod. The Pod
	// is the source of truth for the entire workload shape: the implementation
	// reads image/command/env/ports/cpu/memory from pod.Spec, the accelerator type
	// from the accelerator-type label, and the accelerator count from the
	// nvidia.com/gpu resource (via util.AcceleratorRequest; the type is then
	// translated via MapAccelerator). The request carries
	// only what the Pod cannot express: the optimizer-chosen capacity tier and
	// the claim identity. It returns the provider instance id on success.
	// Idempotency: if an instance already exists for req.ClaimName (encoded in
	// the provider's naming scheme, since most providers lack tags), return that
	// id instead of creating a second.
	Provision(ctx context.Context, pod *corev1.Pod, req ProvisionRequest) (instanceID string, err error)

	// Terminate destroys the instance by id. Must be idempotent: terminating an
	// already-gone instance returns nil (so the NodeClaim finalizer can retry
	// safely). This is the call the terminate finalizer relies on to never leak
	// a paid instance.
	Terminate(ctx context.Context, instanceID string) error

	// Get returns the current state of one instance, or (nil, nil) if it no
	// longer exists (treat absence as terminated).
	Get(ctx context.Context, instanceID string) (*Instance, error)

	// List returns every instance owned by Nebula on this provider, in as few
	// API calls as possible (ideally one — e.g. RunPod's myPods). This is the
	// engine of the poll loop: preemption/termination is detected by an instance
	// disappearing or changing state here, since no provider pushes such events.
	List(ctx context.Context) ([]Instance, error)

	// --- Catalog ---------------------------------------------------------

	// Offerings returns the current price/availability rows this provider can
	// serve, feeding the optimizer's {provider,accelerator,capacityType}->
	// {price,avail} table. Cached + periodically refreshed by the caller;
	// implementations may combine a static catalog with a live availability probe.
	Offerings(ctx context.Context) ([]Offering, error)

	// --- Translation -----------------------------------------------------

	// MapAccelerator translates a canonical Nebula accelerator type ("H100",
	// "A100-80GB", "TPU-v4") into this provider's identifier (e.g. RunPod's
	// "NVIDIA H100 80GB HBM3"). Returns ok=false if the provider does not offer
	// that accelerator.
	MapAccelerator(canonical string) (providerAcceleratorID string, ok bool)

	// ClassifyProvisionError maps a Provision error to the granularity at which
	// the failing placement should be blocklisted. This keeps failover precise:
	// a "no H100 capacity" error blocks only {provider, H100, capacityType},
	// while an auth/quota error blocks the whole provider. See BlockScope.
	ClassifyProvisionError(err error) BlockScope
}

// ProvisionRequest carries only the decisions the placement controller made
// that are NOT already on the Pod. Everything about the workload — image,
// command, env, ports, cpu/memory, accelerator type (accelerator-type label) and
// count (nvidia.com/gpu resource) — is read from the Pod itself, which
// is the single source of truth; duplicating it here would repeat the mistake
// NodeClaim deliberately avoids. That leaves exactly two fields: the optimizer's
// capacity-tier choice (nowhere on the Pod) and the claim identity.
type ProvisionRequest struct {
	// ClaimName is the NodeClaim name; providers without native tags encode it
	// into the instance name so List/Terminate can find the instance later.
	ClaimName string
	// CapacityType is the tier the optimizer selected (Spot/OnDemand/Reserved).
	// This is the one workload-independent decision that cannot be expressed on
	// the Pod, so it must be passed explicitly.
	CapacityType nebulav1alpha1.CapacityType
}

// Capabilities declares provider quirks as data, so the control plane filters
// and behaves generically rather than branching on provider name.
type Capabilities struct {
	// SupportsStop is true if instances can be stopped/resumed (RunPod: false —
	// lifecycle is create/terminate only).
	SupportsStop bool
	// SupportsSpot is true if the provider offers interruptible capacity.
	SupportsSpot bool
	// NativeTags is true if the provider has real instance tags/labels; when
	// false, identity is encoded in the instance name (RunPod: false).
	NativeTags bool
	// PreemptionNotice is the advance warning before a spot reclaim; zero means
	// none (RunPod: 0 — abrupt, detected only by polling).
	PreemptionNotice time.Duration
	// PollInterval is how often the virtual node re-lists this provider to detect
	// out-of-band state changes (Pending→Running, preemption, external teardown);
	// see pkg/vnode. It is a per-provider knob because the trade-off differs by
	// provider: a spot-heavy backend where preemption is common and costly wants a
	// short interval to notice reclaims quickly, while an OnDemand-only backend
	// that never preempts can poll lazily. Zero means "use the vnode default"
	// (30s). The happy-path transitions (provision start, teardown) do not wait on
	// this — they are pushed synchronously by CreatePod/DeletePod — so this only
	// bounds detection latency for events no provider pushes.
	PollInterval time.Duration
}

// Instance is the provider-agnostic view of one external instance, as observed.
type Instance struct {
	ID        string
	ClaimName string // recovered from the naming scheme (for tag-less providers)
	State     InstanceState
	// Endpoint is the reachable address once ready (e.g. SSH host:port).
	Endpoint string
	// CapacityType reflects how the instance was provisioned, when known.
	CapacityType nebulav1alpha1.CapacityType
}

// InstanceState is the provider-agnostic lifecycle state, normalized from each
// provider's own status strings.
type InstanceState string

const (
	// InstancePending: created but not yet reachable/ready.
	InstancePending InstanceState = "Pending"
	// InstanceRunning: up and reachable.
	InstanceRunning InstanceState = "Running"
	// InstanceTerminated: gone (also the mapping for "absent from List").
	InstanceTerminated InstanceState = "Terminated"
	// InstanceFailed: entered a terminal error state.
	InstanceFailed InstanceState = "Failed"
)

// Offering is one row of the price/availability catalog. The price/availability
// lookup seam and the embeddable catalog-backed base (Name/Offerings/default
// MapAccelerator) live in the pkg/provider/catalog package, alongside the
// concrete CSV catalog, so all catalog-shaped types share one home.
type Offering struct {
	AcceleratorType string
	CapacityType    nebulav1alpha1.CapacityType
	PricePerHour    float64
	Available       bool
}

// BlockScope is the granularity at which a failed placement is excluded, matched
// to what actually failed (SkyPilot's blocklist-granularity rule). Empty fields
// act as wildcards in the in-memory blocklist match: a failed H100 request must
// not disqualify A100 requests on the same provider.
type BlockScope struct {
	// AcceleratorType empty => blocks all accelerator types on this provider.
	AcceleratorType string
	// CapacityType empty => blocks all capacity types.
	CapacityType nebulav1alpha1.CapacityType
	// DenyAll true => block everything on this provider (e.g. auth/quota errors),
	// ignoring AcceleratorType/CapacityType. The scope is still this one provider;
	// it never spans providers.
	DenyAll bool
}
