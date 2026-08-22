// Package provider defines the abstraction every compute provider (Modal, AWS, ...)
// implements — the one narrow seam between Nebula's provider-agnostic control plane and
// the heterogeneous cloud APIs underneath.
//
// Region IS modeled, as one axis of the placement candidate and of BlockScope, but it
// stays OPTIONAL: an empty region means "let the provider place freely", a normal mode
// (the cheapest one on Modal). Zone is not modeled — AWS's CreateFleet already spreads
// across a region's AZs and no NeoCloud exposes zones. Region vocabularies differ per
// provider, and the pool speaks group tokens ("us") on top, so translation lives behind
// ExpandRegions rather than in the control plane.
//
// Design rules:
//   - The Pod is the source of truth for the workload shape. Provision reads
//     image/command/env/ports/resources off it; the control plane never re-encodes that.
//   - Provider quirks are declared via Capabilities and handled here, never leaked into
//     the control plane.
//   - Detection is poll-based everywhere (no provider pushes preemption reliably), so
//     List must return every instance in as few calls as possible.
package provider

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
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

	// Provision creates exactly one external instance for the Pod, which is the source of
	// truth for the whole workload shape: the implementation reads
	// image/command/env/ports/cpu/memory off pod.Spec, and the accelerator type+count via
	// util.AcceleratorRequest (then MapAccelerator). The request carries only what the Pod
	// cannot express: the chosen capacity tier and the claim identity.
	//
	// It returns the instance id, whether capacity is RESERVED, and any minted connect
	// credential. Reserved means the provider committed real capacity, not merely accepted
	// the request — two different guarantees an id alone cannot express:
	//
	//   - AWS reserves: an *instant* CreateFleet is synchronous, so an id means EC2 found
	//     capacity and the instance is booting; a shortfall is an error.
	//   - Modal does not: Create returns once the control plane accepts the sandbox, and
	//     the GPU may stay queued for minutes on a large shape.
	//
	// Callers use it for honest status: an unreserved instance is not yet initializing, so
	// its Pod stays at the provisioning reason. It says nothing about readiness, which is
	// observed later through List/Get. reserved=false still carries the full teardown
	// obligation; a provider that cannot tell the two apart returns true.
	//
	// Idempotency: if an instance already exists for req.ClaimName (encoded in the
	// provider's naming scheme, since most lack tags), return that id rather than creating
	// a second. Such a call returns NO credential — the original cannot be re-read.
	Provision(ctx context.Context, pod *corev1.Pod, req ProvisionRequest) (ProvisionResult, error)

	// Terminate destroys the instance by id. Must be idempotent — terminating an
	// already-gone instance returns nil, so the finalizer that guarantees no paid instance
	// leaks can retry safely.
	Terminate(ctx context.Context, instanceID string) error

	// Get returns the current state of one instance, or (nil, nil) if it no
	// longer exists (treat absence as terminated).
	Get(ctx context.Context, instanceID string) (*Instance, error)

	// List returns every instance Nebula owns on this provider, in as few API calls as
	// possible (ideally one). This is the engine of the poll loop: since no provider pushes
	// events, preemption and termination are detected by an instance disappearing or
	// changing state here.
	List(ctx context.Context) ([]Instance, error)

	// --- Catalog ---------------------------------------------------------

	// Offerings returns the price/availability rows this provider can serve, feeding the
	// optimizer's {provider,accelerator,capacityType} -> {price,avail} table. The caller
	// caches and refreshes it; an implementation may combine a static catalog with a live
	// availability probe.
	Offerings(ctx context.Context) ([]Offering, error)

	// --- Translation -----------------------------------------------------

	// MapAccelerator translates a canonical accelerator request (type + count) into the
	// provider ids that can serve it: the PRIMARY first, then interchangeable alternates.
	//
	// Count is part of the key because on some providers the GPU count is baked into the
	// offering: on AWS (L4, 1) and (L4, 8) are different instance types (g6.xlarge vs
	// g6.48xlarge) and so distinct capacity pools. A provider that attaches an arbitrary
	// count to one offering (Modal) ignores count. ok=false means no id for that pair.
	//
	// Most of the system uses ids[0], the PRIMARY, because failover keys a capacity block
	// on it: requests sharing a capacity pool must return the same primary, and ones that
	// do not must differ. The alternates broaden a SINGLE launch (AWS's fleet spans
	// instance types so EC2 lands on whichever has capacity) but never widen the
	// blocklist — an alternate running dry does not disable the primary.
	MapAccelerator(canonical string, count int32) (providerAcceleratorIDs []string, ok bool)

	// ExpandRegions resolves a pool's declared region constraint (ProviderSpec.Regions)
	// into the concrete regions placement may walk, in this provider's own vocabulary —
	// only the provider knows its geography:
	//
	//   - nil/empty  => unconstrained: every region this provider serves.
	//   - a GROUP token ("us", "eu", "ap") => that geography's regions.
	//   - anything else => a literal region name, passed through UNVALIDATED.
	//
	// That last case is deliberate: region names change faster than this code, so an
	// unrecognized one is forwarded and a genuinely bad name fails at provision time with
	// the provider's own error. Better than refusing a region that shipped last week.
	//
	// Expanding HERE, at the pool boundary, keeps everything downstream single-valued —
	// NodeClaimSpec.Region, ProvisionRequest.Region and the blocklist key — so a capacity
	// failure blocks the one candidate that failed, not the group it came from.
	//
	// How many candidates a declaration becomes depends on whether the provider can FAIL
	// OVER between regions. One that reports a shortage synchronously (AWS) returns one
	// candidate per region, so the next is tried. One that just queues the request with no
	// error (Modal) must not: nothing would re-drive placement, so only the first
	// candidate would ever be tried. It returns ONE opaque candidate carrying the whole
	// set and lets its own scheduler choose.
	//
	// Pure (no API calls, no ctx), because the result feeds both placement's candidate walk
	// and the List/Offerings fan-out, and those MUST agree: a region provisioned into but
	// not swept is absent from List, which reports a live instance as Terminated.
	ExpandRegions(declared []string) []string

	// ClassifyProvisionError maps a Provision error to the granularity at which
	// the failing placement should be blocklisted. This keeps failover precise:
	// a "no H100 capacity" error blocks only {provider, H100, capacityType, region},
	// while an auth/quota error blocks the whole provider. See BlockScope.
	//
	// accelerator and region are the two request facts the error does not carry, both ""
	// when absent (a CPU-only Pod, a region-simple provider). The provider returns the
	// COMPLETE scope; no caller assembles one piecemeal afterwards.
	ClassifyProvisionError(err error, accelerator, region string) BlockScope
}

// LogStreamer is the OPTIONAL half of a provider: one instance's console output, which is
// what makes `kubectl logs` work. A backend with no console history simply does not
// implement it, and the virtual node answers NotFound.
//
// The returned stream must:
//
//   - start at the instance's FIRST byte, not at "now";
//   - FOLLOW until the instance exits, ctx is cancelled, or Close;
//   - MERGE stdout and stderr (the kubelet API has one stream);
//   - release everything on Close, which the caller always calls.
//
// A missing instance must return an error, never an empty stream — silence reads as
// "the workload logged nothing".
//
// No options here: --follow/--tail/--limit-bytes are applied by pkg/vnode/logs.go so
// every provider behaves the same and no adapter can quietly ignore one.
type LogStreamer interface {
	Logs(ctx context.Context, instanceID string) (io.ReadCloser, error)
}

// Executor is the other OPTIONAL half: run one command inside a live instance, which is
// what makes `kubectl exec` work. A backend with no way in (no agent, no SSH key) does
// not implement it, and the virtual node answers NotFound.
//
// Exec only STARTS the command; the caller pumps the streams and waits. That keeps the
// copy loop in pkg/vnode/exec.go, so stdin EOF, output draining and exit-code reporting
// are identical for every provider and no adapter can quietly get one wrong.
//
// ctx covers the START ONLY, and the caller bounds it — an interactive shell outlives it
// by hours. So the returned Process must not tie its streams or Wait to ctx, or a long
// exec would die the moment the start budget ran out.
type Executor interface {
	Exec(ctx context.Context, instanceID string, cmd []string, opts ExecOptions) (Process, error)
}

// ExecOptions is what the client asked for that the provider must know at start time.
// Everything else about the command is in cmd.
type ExecOptions struct {
	// TTY asks for a pseudo-terminal, so the command believes it is interactive and line
	// editing works (`kubectl exec -it`). A provider that cannot allocate one still runs
	// the command: a shell without a TTY beats no shell.
	TTY bool
}

// Process is one command running inside an instance. The caller always calls Close.
type Process interface {
	// Stdin is the command's input; closing it is how the command sees EOF. Nil when the
	// provider cannot write stdin, which makes the exec output-only.
	Stdin() io.WriteCloser
	// Stdout is the command's output — under a TTY, everything it writes.
	Stdout() io.Reader
	// Stderr is separate error output, or nil when there is none to separate. Nil is the
	// normal case under a TTY, where one terminal carries both.
	Stderr() io.Reader
	// Wait blocks until the command exits and returns its exit code. A non-zero code is
	// the command's own answer, NOT a failure — err is for "we could not find out".
	Wait(ctx context.Context) (int, error)
	// Close releases the streams and whatever connection carried them. It must unblock a
	// parked Read/Write, so a disconnected client cannot leave the transport running.
	Close() error
}

// ProvisionRequest carries what a provider cannot read off the Pod: the placement decisions
// (tier, region), the claim identity, and the one part of the workload whose value is not in
// the Pod — env behind a reference. Everything else — image, command, ports, cpu/memory,
// accelerator type and count — comes from the Pod, the single source of truth.
type ProvisionRequest struct {
	// ClaimName is the NodeClaim name; providers without native tags encode it
	// into the instance name so List/Terminate can find the instance later.
	ClaimName string
	// CapacityType is the tier the optimizer chose (Spot/OnDemand) — a decision that has
	// nowhere to live on the Pod.
	CapacityType nebulav1alpha1.CapacityType
	// Region is the ONE candidate placement chose, exactly as this provider's own
	// ExpandRegions minted it — already resolved (never a group token) and opaque to the
	// control plane. Usually one concrete region (AWS "us-east-1"), which is what lets a
	// capacity failure blocklist just that region; a provider that cannot fail over
	// (Modal) may encode several for its own scheduler, and only that adapter parses it.
	//
	// Empty means "no region constraint" — common, not a fallback: a pool declaring no
	// regions leaves it empty, which on Modal is the widest and cheapest option (pinning
	// costs 1.5-1.75x). AWS cannot honour it, but its ExpandRegions never produces it.
	Region string
	// Egress is the pool's outbound policy, or nil for Open. Placement has already checked
	// that this provider can enforce it (Capabilities.SupportsEgressPolicy), so an adapter
	// receiving a restrictive policy must apply it or fail the Provision — never silently
	// drop it, which would leave the workload on the open internet under a policy that says
	// otherwise. Resolved from the NodePool at provision time, never from the Pod, which
	// the workload's own owner can patch (see vnode.Handler.egressFor).
	Egress *nebulav1alpha1.EgressPolicy
	// Env is the container's environment, fully RESOLVED: literals plus everything
	// envFrom/valueFrom referenced, merged in kubelet precedence (envFrom in listed order,
	// then env overriding it). A provider forwards it and never re-reads the Pod's env,
	// which holds references it cannot follow.
	//
	// Resolved by the caller (pkg/vnode): reading a Secret needs cluster access an adapter
	// deliberately lacks, and kubelet precedence should be implemented once.
	//
	// SECRET-BEARING, and why this struct redacts itself — a secretKeyRef's value lands here
	// in the clear. Never log the map, never put it on the Pod, never in an error string,
	// the same rule as ProvisionResult.ConnectToken. Nil is normal: no env, or an
	// unresolving caller.
	Env map[string]string
}

// String redacts Env so a ProvisionRequest can be logged safely — nothing stops a future
// log.Info("...", "req", req). Key names print, since they are in the Pod spec already and
// are what makes a "wrong env" report actionable; only values are withheld.
func (r ProvisionRequest) String() string {
	return fmt.Sprintf("ProvisionRequest{ClaimName:%s CapacityType:%s Region:%s Egress:%s Env:%s}",
		r.ClaimName, r.CapacityType, r.Region, r.Egress.ModeOrOpen(), RedactedEnv(r.Env))
}

// GoString implements fmt.GoStringer so %#v is redacted too.
func (r ProvisionRequest) GoString() string { return r.String() }

// ProvisionResult is what one Provision call produced. A struct rather than more
// positional returns because the credential below is delivered ONCE, and a value that can
// only be observed here deserves a name.
type ProvisionResult struct {
	// InstanceID is the provider's id for the instance. Non-empty on success, and
	// carrying the full teardown obligation from that moment on.
	InstanceID string
	// Reserved is whether the provider committed actual capacity, as opposed to
	// merely accepting the request. See Provision.
	Reserved bool
	// ConnectURL and ConnectToken are the credential for reaching the instance — an address
	// plus the bearer token authenticating against it, which is all a consumer needs:
	//
	//	curl -H "Authorization: Bearer $token" $url
	//
	// Returned here and ONLY here: minting is one-shot with no read-back (Modal's
	// CreateConnectToken returns a DIFFERENT token each call). The caller must persist it
	// durably — the virtual kubelet writes both to a Secret — or it is gone. Empty when
	// the instance needs no credential, or on an idempotent re-Provision.
	//
	// The TOKEN is a SECRET: never log it, never put it on the Pod (annotations are
	// readable with `get pod` and unencrypted in etcd), never in an error string.
	//
	// ConnectURL is not secret, and is NOT Instance.Endpoint even when they agree: this is
	// the one-shot create path, while the endpoint is observed, so it is what reports an
	// address that only exists after boot and survives a manager restart.
	ConnectURL   string
	ConnectToken string
}

// String redacts ConnectToken so a ProvisionResult can be logged safely: the compiler
// cannot stop a future log.Info("...", "result", res), so %v/%s go through here. The URL is
// not secret and prints as-is.
func (r ProvisionResult) String() string {
	return fmt.Sprintf("ProvisionResult{InstanceID:%s Reserved:%t ConnectURL:%s ConnectToken:%s}",
		r.InstanceID, r.Reserved, r.ConnectURL, redacted(r.ConnectToken))
}

// GoString implements fmt.GoStringer so %#v is redacted too.
func (r ProvisionResult) GoString() string { return r.String() }

// Capabilities declares provider quirks as data, so the control plane filters
// and behaves generically rather than branching on provider name.
type Capabilities struct {
	// SupportsStop is true if instances can be stopped/resumed (RunPod: false —
	// lifecycle is create/terminate only).
	SupportsStop bool
	// SupportsSpot is true if the provider offers interruptible capacity.
	SupportsSpot bool
	// SupportsEgressPolicy is true if the provider can enforce NodePoolSpec.Egress on the
	// instances it creates. False means placement SKIPS this provider for any pool that
	// restricts egress, rather than provisioning something with open internet access under
	// a policy that says otherwise (AWS: false — its instances land in the default VPC, so
	// enforcement needs security-group egress rules and no NAT, not one API field).
	SupportsEgressPolicy bool
	// NativeTags is true if the provider has real instance tags/labels; when
	// false, identity is encoded in the instance name (RunPod: false).
	NativeTags bool
	// PreemptionNotice is the advance warning before a spot reclaim; zero means
	// none (RunPod: 0 — abrupt, detected only by polling).
	PreemptionNotice time.Duration
	// PollInterval is how often the virtual node re-lists this provider to catch
	// changes nobody pushes: Pending→Running, preemption, external teardown. Per
	// provider because a spot-heavy backend wants reclaims noticed fast while an
	// OnDemand-only one can poll lazily. Zero means the vnode default (15s).
	PollInterval time.Duration
	// ProvisionTimeout bounds a single Provision call end to end, so a provider that
	// retries across capacity pools internally (AWS tries each AZ) gives up in time for
	// the outer region failover to run. It bounds the launch attempt, not "the workload
	// became healthy" — that stays the poll loop's job. Zero means no deadline beyond
	// the caller's context; single-pool providers (Modal) leave it zero.
	ProvisionTimeout time.Duration
}

// Instance is the provider-agnostic view of one external instance, as observed.
type Instance struct {
	ID        string
	ClaimName string // recovered from the naming scheme (for tag-less providers)
	State     InstanceState
	// CapacityType reflects how the instance was provisioned, when known.
	CapacityType nebulav1alpha1.CapacityType
	// Region is where the instance actually lives, in the provider's own
	// vocabulary. Empty for region-simple providers that do not report one.
	Region string
	// Endpoint is the reachable address once ready (e.g. SSH host:port). It is not
	// secret, so unlike the connect credential it rides the level-triggered read path
	// and is re-reported on every tick.
	Endpoint string
}

// redacted renders a secret's presence without its value.
func redacted(s string) string {
	if s == "" {
		return ""
	}
	return "[REDACTED]"
}

// RedactedEnv renders an env map's shape — size and key names, sorted so two logs of the
// same map read alike — with every value withheld: keys are in the Pod spec, values may
// come from a Secret. Exported because every adapter carrying a resolved environment needs
// this in its own String(), and a second implementation is a second chance to leak.
func RedactedEnv(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Sprintf("{%d keys: %s}", len(keys), strings.Join(keys, ","))
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
	// AcceleratorType (AWS "p5.48xlarge" for H100) — the lookup data MapAccelerator
	// returns. Unset when the mapping is identity (Modal names its GPUs like the
	// canonical types) and catalog.Base's default MapAccelerator is used.
	AcceleratorID string
	// GPUCount is how many accelerators the AcceleratorID provides. It matters where the
	// count is baked into the offering: on AWS you pick an instance type with a fixed GPU
	// count (T4x1 = g4dn.xlarge, T4x8 = g4dn.metal), so the mapping key is
	// (AcceleratorType, GPUCount) and each accelerator appears once per count. Left 0 by
	// providers that take the count as a parameter (Modal).
	GPUCount int32
}

// BlockScope is how narrowly a failed placement is excluded, matched to what actually
// failed. It is a MATCH PATTERN, not a value, so the pointer fields are three-state:
//
//   - nil   => axis not applicable; matches only a candidate whose field is also empty
//     (a CPU-only Pod has no accelerator, a region-simple provider no region).
//   - &""   => wildcard: matches any value on that axis.
//   - &"v"  => exact: only candidates equal to "v" (a failed H100 must not block A100).
//
// Pointers are needed because the scope is built in two places: the adapter classifies
// from the error alone (which knows the region but not the accelerator, a property of the
// request), and the vnode handler fills the accelerator in from the failing Pod. So
// "unset" must be distinguishable from a deliberate wildcard. DenyAll ignores the pattern
// fields entirely.
//
// Note this is the opposite sense to a value field like ProviderSpec.Regions, where empty
// means "the default region". A candidate is resolved to concrete values before being
// matched here, so the two never mix.
type BlockScope struct {
	// Accelerator: nil => not applicable (CPU-only Pod, or DenyAll); &"" => every
	// accelerator; &"H100:8" => that one pool. The value is the request's POOL identity
	// (type:count), not the provider's SKU id, so a block stays confined to what ran out:
	// an L4:8 shortage must not exclude L4:1. It also stays stable when a launch spans
	// several interchangeable instance types. Filled in by the vnode handler, since the
	// adapter classifies from the error alone and cannot know it.
	Accelerator *string
	// CapacityType empty => blocks all capacity types.
	CapacityType nebulav1alpha1.CapacityType
	// Region: nil => the provider has no region axis (Modal/RunPod, whose candidates
	// carry an empty region too); &"us-east-1" => that region only, so a shortage there
	// does not disqualify us-west-2.
	Region *string
	// DenyAll true => block everything on this provider (auth/quota errors), ignoring the
	// fields above. Still scoped to this one provider; it never spans providers.
	DenyAll bool
}
