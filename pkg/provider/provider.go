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

	// MapAccelerator translates a canonical Nebula accelerator request (type +
	// count) into the provider ids that can serve it — the PRIMARY first, then any
	// interchangeable alternates. Count is part of the key because on some providers
	// the GPU count is baked into the offering rather than a free knob: on AWS
	// (L4, 1) and (L4, 8) resolve to DIFFERENT instance types (g6.xlarge vs
	// g6.48xlarge) — distinct capacity pools — so the id must distinguish them.
	// Providers that attach an arbitrary count to a single offering (Modal) ignore
	// count and return the same id for every count. Returns ok=false if the provider
	// offers no id for that (type, count).
	//
	// Most of the system uses ids[0], the PRIMARY: it is what failover keys a
	// capacity block on, so two requests that share a capacity pool must return the
	// same primary and two that do not must differ. Any further ids are equivalent
	// alternates a provider may try within a SINGLE provisioning attempt (AWS's
	// launch fleet spans several instance types so EC2 lands on whichever has
	// capacity) — they broaden one launch, they do NOT widen the blocklist: an
	// alternate running dry never disables the primary. A provider with no alternates
	// returns a single-element slice.
	MapAccelerator(canonical string, count int32) (providerAcceleratorIDs []string, ok bool)

	// ClassifyProvisionError maps a Provision error to the granularity at which
	// the failing placement should be blocklisted. This keeps failover precise:
	// a "no H100 capacity" error blocks only {provider, H100, capacityType, region},
	// while an auth/quota error blocks the whole provider. See BlockScope.
	//
	// accelerator and region are the two request facts the error itself does not
	// carry: the accelerator the failing request asked for ("" for a CPU-only Pod)
	// and the region it targeted ("" for a region-simple provider, or when the
	// request did not pin one). The provider is the single owner of the complete
	// scope — it stamps the accelerator on a capacity block, confines the block to
	// the region that actually failed (a region-aware provider), and widens
	// auth/quota to the whole provider. No caller assembles a scope piecemeal after
	// this returns.
	ClassifyProvisionError(err error, accelerator, region string) BlockScope
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
	// CapacityType is the tier the optimizer selected (Spot/OnDemand).
	// This is the one workload-independent decision that cannot be expressed on
	// the Pod, so it must be passed explicitly.
	CapacityType nebulav1alpha1.CapacityType
	// Region is the provider region the optimizer chose to provision in, in the
	// provider's own vocabulary (e.g. AWS "us-east-1"). Like CapacityType it is a
	// workload-independent decision absent from the Pod. Empty means "use the
	// provider's configured default region" — region-simple NeoClouds (Modal,
	// RunPod) ignore it, and a region-aware adapter falls back to the region its
	// client was built with (see the AWS adapter's NewSDKClient).
	Region string
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
	// (15s). The happy-path transitions (provision start, teardown) do not wait on
	// this — they are pushed synchronously by CreatePod/DeletePod — so this only
	// bounds detection latency for events no provider pushes.
	PollInterval time.Duration
	// ProvisionTimeout bounds a single Provision call end to end. It exists for
	// region-aware providers that fail over across capacity pools (e.g. AWS tries
	// each availability zone in turn on a capacity error): the deadline caps how
	// long that inner failover loop may spend before giving up so the outer
	// region-level failover can proceed, rather than a slow provider stalling a
	// Pod indefinitely. It bounds the PROVISIONING attempt (the launch API calls
	// and any per-zone retries), NOT "the workload became healthy" — that remains
	// the poll loop's job. Zero means "no adapter-imposed deadline" (the caller's
	// context still applies); providers with a single capacity pool (Modal) leave
	// it zero.
	ProvisionTimeout time.Duration
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
	// Region is where the instance actually lives, in the provider's own
	// vocabulary. Empty for region-simple providers that do not report one.
	Region string
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
	// Region is the provider region this row prices, in the provider's own
	// vocabulary (e.g. AWS "us-east-1"). Empty for region-simple providers whose
	// catalog is not region-partitioned (Modal, RunPod); a region-aware provider
	// emits one row per {accelerator, capacityType, region}.
	Region string
	// AcceleratorID is this provider's own name for what serves the canonical
	// AcceleratorType — the per-provider half of the mapping the canonical GPU
	// vocabulary is translated through (e.g. AWS "p5.48xlarge" for H100). It is
	// the lookup data a diverging provider's MapAccelerator override returns. Left
	// unset by providers whose mapping is identity (Modal names its GPUs exactly
	// like the canonical types), which reuse catalog.Base's default MapAccelerator
	// and so need no per-name id. Mirrors the providerAcceleratorID that
	// MapAccelerator returns.
	AcceleratorID string
	// GPUCount is how many accelerators the AcceleratorID provides. It matters for
	// providers where the accelerator count is not a free knob but is baked into
	// the offering: on AWS you do not ask for "N T4s", you pick an instance type
	// whose GPU count is fixed (T4x1 = g4dn.xlarge, T4x8 = g4dn.metal), so the
	// mapping key is (AcceleratorType, GPUCount) and the same accelerator appears
	// once per count. Left 0 by providers that attach an arbitrary count to a
	// single offering (Modal takes the count as a parameter), for whom it is not a
	// lookup dimension.
	GPUCount int32
}

// BlockScope is the granularity at which a failed placement is excluded, matched
// to what actually failed (SkyPilot's blocklist-granularity rule). It is a partial
// MATCH PATTERN, not a value, and AcceleratorID/Region are THREE-STATE pointers
// so a pattern can distinguish "any value" from "no value":
//
//   - nil   => the axis is NOT APPLICABLE to this block: it matches only a
//     candidate whose corresponding field is also empty. A CPU-only Pod (no
//     accelerator) and a region-simple provider (no region) both leave the axis
//     nil, so the block never spuriously widens across it.
//   - &""   => WILDCARD: matches any value on that axis (a Spot-capacity block that
//     spans every accelerator, say).
//   - &"v"  => EXACT: matches only candidates whose field equals "v" (so a failed
//     H100 request must not disqualify A100 on the same provider).
//
// This is why the scope is assembled in two places: the adapter classifies from the
// error alone (which knows the region — AWS is bound to one — but never the
// accelerator, a property of the request), and the vnode handler fills the
// accelerator in from the failing Pod. Neither can produce the full scope, so the
// unset fields must be distinguishable from a deliberate wildcard — hence pointers.
// DenyAll is the broadest scope and ignores the pattern fields entirely.
//
// This is the opposite sense to a value field like NodePoolSpec's
// ProviderSpec.Regions, where empty means "the one default region" — because a
// pattern matches, while a request takes a single default. The blocklist never
// mixes the two: a NodePool candidate is resolved to fully-concrete values before
// it is matched against these patterns.
type BlockScope struct {
	// Accelerator: nil => not applicable (CPU-only Pod, or a DenyAll scope);
	// &"" => wildcard, matches every accelerator; &"H100:8" => exactly that one
	// pool. It is the request's POOL identity (type:count), NOT the provider's SKU
	// id. Keying on the pool is what confines a capacity block to what actually ran
	// out: an L4:8 Spot shortage must not exclude L4:1, a different pool, though both
	// are "L4" — and it stays stable when a launch spans several interchangeable
	// provider instance types (so a post-launch SKU choice never desyncs the key). A
	// capacity error carries no accelerator (it is classified from the error alone),
	// so the adapter leaves this nil and the vnode handler fills it in from the
	// failing Pod's request.
	Accelerator *string
	// CapacityType empty => blocks all capacity types.
	CapacityType nebulav1alpha1.CapacityType
	// Region: nil => the provider has no region axis (a region-simple provider like
	// Modal/RunPod, whose candidate carries an empty region too, so nil matches it);
	// &"us-east-1" => confines the block to that one region, so a "no capacity in
	// us-east-1" failure does not disqualify the same request in us-west-2. A
	// region-aware provider (AWS) sets it; a region-simple one leaves it nil.
	Region *string
	// DenyAll true => block everything on this provider (e.g. auth/quota errors),
	// ignoring AcceleratorID/CapacityType/Region. The scope is still this one
	// provider; it never spans providers.
	DenyAll bool
}

// SanddConfig configures the SandD sandbox daemon (see github.com/InftyAI/SandD)
// shared by every provider adapter: when set, a launched instance also runs SandD
// in tunnel mode, giving remote command execution and interactive shell/PTY
// sessions on the box with NO inbound access — the daemon dials OUT to the
// controller over the Tailscale/headscale mesh, which is what makes it work for
// instances in a private VPC with no public IP or open port. It is the access and
// control channel for the machine, not a troubleshooting-only add-on.
//
// It lives here, in the provider-agnostic seam, because it is not AWS-specific: it
// is a property of "let an agent run commands / shell into a Nebula-provisioned
// box" that any adapter can honour. Each adapter injects it in whatever way fits its
// launch model (AWS bakes it into cloud-init as a host daemon alongside the
// workload container; a serverless-sandbox provider would prepend it to the sandbox
// command). AWS is the first to wire it in.
//
// It is OPT-IN: a zero value (empty AuthKey) injects nothing, so the bootstrap is
// unchanged for clusters that do not enable it. AuthKey is a Tailscale/headscale
// auth key — a secret — delivered to the controller as one and never logged. The
// daemon runs ALONGSIDE the workload (it does not run it), so an adapter that
// honours it MUST launch the daemon such that its own failure can never abort the
// workload itself.
type SanddConfig struct {
	// AuthKey is the Tailscale/headscale pre-auth key the daemon joins the mesh
	// with (sandd --tunnel-authkey). Empty disables SandD injection entirely.
	AuthKey string
	// ControlServer is the headscale control-plane URL (sandd --tunnel-server),
	// e.g. "http://headscale.internal:8080". Required when AuthKey is set.
	ControlServer string
	// ServerURL is the controller's SandD WebSocket URL reachable OVER the mesh
	// (sandd --server-url), e.g. "ws://100.64.0.1:8765/ws". Required when AuthKey
	// is set.
	ServerURL string
}

// Enabled reports whether the SandD daemon should be injected. A missing AuthKey
// means the operator did not opt in, so an adapter emits its plain bootstrap.
func (s SanddConfig) Enabled() bool { return s.AuthKey != "" }
