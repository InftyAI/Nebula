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

// Package runpod implements the provider.Provider interface for RunPod
// (https://runpod.io), a NeoCloud renting GPU containers by the second. One NodeClaim
// maps to one RunPod Pod.
//
// RunPod sits between the two adapters that came before it, and the differences are
// what drive the decisions here:
//
//   - Lifecycle is create/terminate only for our purposes, so SupportsStop=false. RunPod
//     does expose stop/start, but a stopped Pod still bills for its disk and releases the
//     GPU, so it is neither free nor resumable in the sense the capability promises.
//   - Spot is REAL and is one boolean: `interruptible: true`, with no bid to name (the
//     REST v1 API dropped bidPerGpu). So SupportsSpot=true and, unlike Modal, the
//     capacity tier on the request is actually honoured.
//   - There is NO preemption push and no notice window, so reclaims are noticed only by
//     the poll loop — hence PreemptionNotice=0 and a faster-than-default PollInterval.
//   - Create FAILS SYNCHRONOUSLY when capacity is short ("no longer any instances
//     available..."), the AWS behaviour rather than Modal's queue-and-accept. That is
//     what makes region failover meaningful, so ExpandRegions is left as catalog.Base's
//     pass-through: one candidate per declared region, each blocklistable on its own.
//   - Pods have NO tags. NativeTags=false, and Nebula's identity rides the Pod NAME
//     (see podName/claimFromName), which is what List filters on.
//   - There is no outbound-allowlist knob at all, so SupportsEgressPolicy=false and
//     placement skips RunPod for any pool that restricts egress rather than provisioning
//     something with open internet access under a policy that says otherwise.
//   - Only SECURE cloud is used. RunPod's cheaper COMMUNITY cloud prices the same GPU
//     differently, and the catalog CSV has no cloud-type axis to express that, so
//     offering both would make the price the optimizer reads a guess.
//
// kubectl logs and exec are NOT served: RunPod's REST v1 surface has no pod-log endpoint,
// and its only way into a container is SSH, which needs key material this adapter has
// nowhere to put. Both are optional halves of provider.Provider resolved by type
// assertion, so leaving them out costs nothing but a NotFound.
//
// The concrete HTTP API lives behind the Client seam, so this file holds only
// provider-agnostic translation and is unit-testable without network access.
package runpod

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
	"github.com/InftyAI/Nebula/pkg/util"
)

// namePrefix marks a RunPod Pod as Nebula's. RunPod has no tags, so the name is the
// ONLY carrier of ownership and identity: List filters on this prefix so an unrelated
// Pod in the same account is never adopted, terminated, or reported as an instance.
const namePrefix = "nebula-"

// maxNameLen is RunPod's own cap on the Pod name (191 chars). Nebula refuses a claim
// whose name would exceed it rather than truncating — see podName.
const maxNameLen = 191

// spotPollInterval is how often to re-list. RunPod's interruptible tier reclaims
// abruptly with no notice pushed to us, so a faster cadence than the vnode default is
// what turns "the Pod vanished" into a NodeClaim update promptly. Matches AWS.
const spotPollInterval = 10 * time.Second

// ErrSpotCapacity is a marker the Client wraps onto an interruptible-tier capacity
// failure (alongside provider.ErrNoCapacity) so ClassifyProvisionError — handed only the
// error, never the request — can recover that the failing tier was Spot and block only
// Spot, leaving OnDemand serviceable. The same device as aws.ErrSpotCapacity.
var ErrSpotCapacity = errors.New("runpod: spot capacity")

// compile-time assertion that Provider satisfies the interface. LogStreamer and Executor
// are deliberately absent; see the package doc.
var _ provider.Provider = (*Provider)(nil)

// Client is the narrow seam over RunPod's REST API: only the operations the adapter
// needs, in provider-agnostic terms, so the real HTTP implementation and a test fake are
// interchangeable.
type Client interface {
	// CreatePod launches one Pod from spec and returns its RunPod id. A capacity
	// shortage is an ERROR here, not a queued Pod, and must be wrapped with
	// provider.ErrNoCapacity (plus ErrSpotCapacity on the interruptible tier).
	CreatePod(ctx context.Context, spec PodSpec) (id string, err error)
	// TerminatePod deletes a Pod by id. Must be idempotent: deleting an already-gone
	// Pod returns nil, since the NodeClaim finalizer retries against it.
	TerminatePod(ctx context.Context, id string) error
	// GetPod returns one Pod, or (nil, nil) if it no longer exists.
	GetPod(ctx context.Context, id string) (*Pod, error)
	// ListPods returns every Pod in the account, in as few calls as possible. Filtering
	// down to Nebula's own is the ADAPTER's job (it owns the naming scheme), so this
	// must not filter.
	ListPods(ctx context.Context) ([]Pod, error)
	// EnsureRegistryAuth resolves auth to a RunPod containerRegistryAuth id, creating
	// the object if this credential has no id yet. RunPod's create takes an id, never an
	// inline username/password, so this indirection is unavoidable.
	EnsureRegistryAuth(ctx context.Context, auth *provider.RegistryAuth) (id string, err error)
}

// PodSpec is the resolved, RunPod-shaped request the Client turns into a Pod. The
// adapter builds it from the Pod (source of truth) plus the resolved accelerator ids.
type PodSpec struct {
	// Name is the RunPod Pod name, which carries Nebula's identity because RunPod has no
	// tags: namePrefix + the NodeClaim name. See podName.
	Name string
	// Image is the container image, from the Pod's first container.
	Image string
	// Entrypoint and StartCmd are the container's command and args, mapped onto RunPod's
	// dockerEntrypoint/dockerStartCmd. They stay SEPARATE, unlike Modal where both
	// concatenate into one command: RunPod's two fields are ENTRYPOINT and CMD, so
	// Kubernetes' command/args land on their exact Docker counterparts.
	Entrypoint []string
	StartCmd   []string
	// Env is the environment, taken whole from provider.ProvisionRequest.Env: literals
	// plus everything envFrom/valueFrom referenced, already resolved by the caller.
	//
	// SECRET-BEARING, hence the redacting String below.
	Env map[string]string
	// GPUTypeIDs are RunPod's own accelerator ids that can serve the request, PRIMARY
	// first. All of them go out in one create: RunPod takes an array and picks by
	// availability, so interchangeable ids broaden a SINGLE launch (as AWS's fleet spans
	// instance types) without widening what a failure blocklists — the block keys on the
	// canonical pool, not on whichever id RunPod landed. Empty for a CPU-only Pod.
	GPUTypeIDs []string
	// GPUCount is how many accelerators to attach; 0 selects a CPU-only Pod.
	GPUCount int32
	// VCPUPerGPU and RAMPerGPUGiB are the Pod's cpu/memory requests expressed RunPod's
	// way — PER GPU, not in total, so the adapter divides by GPUCount and rounds UP
	// (rounding down would hand the workload less than it asked for). Zero leaves
	// RunPod's own defaults (2 vCPU, 8 GiB per GPU).
	VCPUPerGPU   int
	RAMPerGPUGiB int
	// VCPUCount is the CPU-only equivalent, an absolute count rather than a per-GPU one.
	// Only read when GPUCount is 0.
	VCPUCount int
	// ContainerDiskGiB is the writable container disk, from the Pod's ephemeral-storage
	// request. Zero leaves RunPod's default (50 GiB).
	//
	// No persistent volume is ever requested: RunPod defaults to a billable 20 GiB one,
	// and a Nebula instance is cattle with nothing to persist, so the Client pins it to 0.
	ContainerDiskGiB int
	// Ports are the container ports to expose, in RunPod's "<port>/<proto>" form (see
	// containerPorts). Empty leaves RunPod's default exposure.
	Ports []string
	// DataCenterIDs and CountryCodes are the placement constraint, split out of the ONE
	// region candidate this request carries (see splitRegion). At most one is ever set,
	// and both empty means unconstrained — the widest capacity pool, and the normal case
	// for a pool that declares no regions.
	DataCenterIDs []string
	CountryCodes  []string
	// Interruptible asks for the spot tier, from ProvisionRequest.CapacityType. RunPod's
	// REST v1 takes no bid alongside it, so nothing else is needed to price it.
	Interruptible bool
	// RegistryAuthID is the RunPod containerRegistryAuth object authenticating the image
	// pull, already resolved from the canonical credential by Client.EnsureRegistryAuth.
	// Empty is an anonymous pull.
	RegistryAuthID string
}

// String redacts Env so a spec can be logged or wrapped in an error safely: key names
// print (they are in the Pod spec already), values never do. RegistryAuthID is an opaque
// object id, not a credential, so it prints as-is.
func (s PodSpec) String() string {
	return fmt.Sprintf("PodSpec{Name:%s Image:%s Entrypoint:%v StartCmd:%v Env:%s "+
		"GPUTypeIDs:%v GPUCount:%d VCPUPerGPU:%d RAMPerGPUGiB:%d VCPUCount:%d "+
		"ContainerDiskGiB:%d Ports:%v DataCenterIDs:%v CountryCodes:%v Interruptible:%t "+
		"RegistryAuthID:%s}",
		s.Name, s.Image, s.Entrypoint, s.StartCmd, provider.RedactedEnv(s.Env),
		s.GPUTypeIDs, s.GPUCount, s.VCPUPerGPU, s.RAMPerGPUGiB, s.VCPUCount,
		s.ContainerDiskGiB, s.Ports, s.DataCenterIDs, s.CountryCodes, s.Interruptible,
		s.RegistryAuthID)
}

// GoString implements fmt.GoStringer so %#v is redacted too.
func (s PodSpec) GoString() string { return s.String() }

// Pod is the adapter-level view of a RunPod Pod as observed.
type Pod struct {
	ID   string
	Name string
	// DesiredStatus is RunPod's own status string (RUNNING/EXITED/TERMINATED). It is the
	// DESIRED state, not the observed one, which is why toState also needs LastStartedAt.
	DesiredStatus string
	// LastStartedAt is when the container last started, empty until it has. Paired with
	// DesiredStatus it is the closest thing RunPod offers to a readiness signal.
	LastStartedAt string
	// Interruptible is whether this Pod is on the spot tier, so an observed instance
	// reports the tier it actually got.
	Interruptible bool
	// DataCenterID is where RunPod placed it, in RunPod's own vocabulary.
	DataCenterID string
	// Ports are the exposed ports RunPod echoes back, in the same "<port>/<proto>" form
	// PodSpec.Ports sends. They are what the proxy URL routes to, and unlike PortMappings
	// they are present from creation, which is what makes an /http-only Pod addressable.
	Ports []string
	// PublicIP and PortMappings are the DIRECT address, keyed by container port, and only
	// ever populated for a /tcp port RunPod has finished assigning. Both empty otherwise —
	// including for every /http port, which is why they are a preference and not the
	// endpoint's only source.
	PublicIP     string
	PortMappings map[string]int
}

// Provider is the RunPod implementation of provider.Provider. It embeds catalog.Base for
// the generic catalog methods — Name, Offerings, ExpandRegions and the catalog-driven
// MapAccelerator, which does real work here: RunPod's accelerator ids are marketing
// strings ("NVIDIA H100 80GB HBM3") that share nothing with Nebula's canonical names, so
// every row in runpod.csv carries an accelerator_id.
type Provider struct {
	catalog.Base
	client Client
}

// New returns a RunPod Provider backed by client and price catalog. Both must be
// non-nil; use catalog.Load() to build the catalog from the CSV/ConfigMap data. cat is
// the catalog.Lookup seam, so tests can inject a fake.
func New(client Client, cat catalog.Lookup) *Provider {
	return &Provider{
		Base:   catalog.Base{ProviderName: provider.ProviderRunPod, Catalog: cat},
		client: client,
	}
}

// Capabilities implements provider.Provider. See the package doc for why each trait is
// set the way it is.
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		SupportsStop:         false,            // a stopped Pod still bills and loses its GPU
		SupportsSpot:         true,             // `interruptible`, no bid to name
		SupportsEgressPolicy: false,            // no outbound allowlist in the API at all
		NativeTags:           false,            // identity rides the Pod name
		PreemptionNotice:     0,                // abrupt; detected only by polling
		PollInterval:         spotPollInterval, // Spot reclaims are abrupt; poll faster than default
		// No ProvisionTimeout: one create call, no internal sweep across capacity pools
		// to bound (contrast AWS, which walks a region's AZs itself).
	}
}

// Provision implements provider.Provider. The Pod is the source of truth for the
// workload; req carries only the claim identity, the capacity tier and the region.
//
// A successful create is RESERVED, unlike Modal: RunPod allocates a host machine before
// it answers, and a shortage comes back as an error rather than a queued Pod. So an id
// here means real capacity, and the Pod may honestly move on from "provisioning".
//
// The connect URL is RunPod's HTTP proxy for the first declared port — deterministic from
// the Pod id, so no read-back is needed. There is no token: the proxy is unauthenticated,
// which is why ConnectToken stays empty rather than carrying a placeholder.
func (p *Provider) Provision(
	ctx context.Context, pod *corev1.Pod, req provider.ProvisionRequest,
) (provider.ProvisionResult, error) {
	if pod == nil {
		return provider.ProvisionResult{}, errors.New("runpod: nil pod")
	}
	if req.ClaimName == "" {
		return provider.ProvisionResult{}, errors.New("runpod: empty ClaimName in ProvisionRequest")
	}
	// Refuse a restrictive egress policy rather than dropping it. Placement checks
	// SupportsEgressPolicy and should never route such a pool here, but the request can be
	// built by anyone, and silently ignoring it would put the workload on the open
	// internet under a policy that says otherwise.
	if mode := req.Egress.ModeOrOpen(); mode != nebulav1alpha1.EgressOpen {
		return provider.ProvisionResult{}, fmt.Errorf(
			"runpod: cannot enforce egress mode %q; RunPod exposes no outbound policy", mode)
	}

	// Idempotency: RunPod has no tags, so the claim is looked up by the Pod NAME this
	// adapter mints. A repeat after a partial create returns the existing Pod rather than
	// paying for a second.
	//
	// Reserved is true for the same reason as a fresh create: the Pod exists, so a machine
	// was allocated. That is the capacity question; readiness is separate and observed
	// through List. No credential comes back — the interface forbids it on a re-Provision,
	// and here there is nothing to re-mint anyway, since the proxy URL is derivable.
	existing, err := p.findByClaim(ctx, req.ClaimName)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	if existing != nil {
		return provider.ProvisionResult{InstanceID: existing.ID, Reserved: true}, nil
	}

	spec, err := p.podSpecFromPod(pod, req)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	// Resolve the pull credential LAST among the request-shaping steps, because unlike
	// everything else in the spec it costs API calls; a spec that was going to be rejected
	// for its accelerator or its name has already failed by now.
	if req.RegistryAuth != nil {
		if err := checkRegistryAuth(req.RegistryAuth); err != nil {
			return provider.ProvisionResult{}, err
		}
		authID, err := p.client.EnsureRegistryAuth(ctx, req.RegistryAuth)
		if err != nil {
			return provider.ProvisionResult{}, err
		}
		spec.RegistryAuthID = authID
	}

	id, err := p.client.CreatePod(ctx, spec)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	return provider.ProvisionResult{
		InstanceID: id,
		Reserved:   true,
		ConnectURL: proxyURL(id, spec.Ports),
	}, nil
}

// Terminate implements provider.Provider. Idempotent by the Client contract. The region
// is ignored: RunPod's API is global and a Pod id addresses it from anywhere, so region is
// only ever a placement input.
func (p *Provider) Terminate(ctx context.Context, instanceID, _ string) error {
	if instanceID == "" {
		return nil // nothing provisioned yet; treat as already gone
	}
	return p.client.TerminatePod(ctx, instanceID)
}

// Get implements provider.Provider. The region is ignored, as in Terminate.
func (p *Provider) Get(ctx context.Context, instanceID, _ string) (*provider.Instance, error) {
	pd, err := p.client.GetPod(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if pd == nil {
		return nil, nil // absent => terminated, per interface contract
	}
	inst := toInstance(*pd)
	return &inst, nil
}

// List implements provider.Provider. One API call, then the name filter that stands in for
// the tags RunPod does not have: a Pod without Nebula's prefix belongs to someone else in
// the same account and must never be reported (the poll loop would adopt it, and the
// NodeClaim controller would eventually terminate it).
func (p *Provider) List(ctx context.Context) ([]provider.Instance, error) {
	pods, err := p.client.ListPods(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]provider.Instance, 0, len(pods))
	for _, pd := range pods {
		if !strings.HasPrefix(pd.Name, namePrefix) {
			continue
		}
		out = append(out, toInstance(pd))
	}
	return out, nil
}

// ClassifyProvisionError implements provider.Provider. The categories and the
// scope-derivation rule are shared (provider.ClassifyError and the sentinels the Client
// wraps), so this supplies only the two RunPod-specific facts: which tier failed, and the
// region axis.
func (p *Provider) ClassifyProvisionError(err error, accelerator, region string) provider.BlockScope {
	// No failure, no block. ClassifyError already returns the zero scope, but the region
	// decoration below would repopulate it into a scope recordBlock would install.
	if err == nil {
		return provider.BlockScope{}
	}
	// The failing tier is not on the error's face, so the Client marks an interruptible
	// shortage. Getting this wrong in the safe direction matters: a Spot failure blocked
	// as OnDemand would disable capacity that is still purchasable.
	tier := nebulav1alpha1.CapacityOnDemand
	if errors.Is(err, ErrSpotCapacity) {
		tier = nebulav1alpha1.CapacitySpot
	}
	scope := provider.ClassifyError(err, tier, accelerator)
	// The zero scope means BLOCK NOTHING — a rejection of this request that says nothing
	// about the candidate, such as an image credential RunPod cannot use. Stamping a region
	// onto it would make it non-empty, and recordBlock would install a region-wide block
	// across every accelerator: the same trap as the err == nil guard above.
	if scope == (provider.BlockScope{}) {
		return scope
	}
	// DenyAll already covers every region (auth fails everywhere), so narrowing it would
	// contradict the category. An empty region leaves Region nil, which per BlockScope
	// matches only candidates that carry no region either — the unconstrained pool — so
	// the block never leaks onto region-pinned candidates.
	if region != "" && !scope.DenyAll {
		scope.Region = &region
	}
	return scope
}

// findByClaim returns the Nebula-owned Pod for claimName, or nil if none. RunPod has no
// server-side tag filter, so this is List plus a name comparison.
func (p *Provider) findByClaim(ctx context.Context, claimName string) (*provider.Instance, error) {
	name, err := podName(claimName)
	if err != nil {
		return nil, err
	}
	pods, err := p.client.ListPods(ctx)
	if err != nil {
		return nil, err
	}
	for _, pd := range pods {
		if pd.Name == name {
			inst := toInstance(pd)
			return &inst, nil
		}
	}
	return nil, nil
}

// podName is the RunPod Pod name for a NodeClaim: the prefix that marks ownership plus
// the claim name, which is what makes List/findByClaim work on a backend with no tags.
//
// A name that would exceed RunPod's cap is an ERROR, never a truncation. Truncating would
// map two long claim names onto one Pod name, and every consequence of that collision is
// severe: findByClaim adopts the other claim's instance, so one Pod is billed twice and
// the other claim's teardown reaps the survivor. Refusing is loud and fixable (claim names
// derive from the Pod's, so the workload can be renamed); a collision is silent.
func podName(claimName string) (string, error) {
	name := namePrefix + claimName
	if len(name) > maxNameLen {
		return "", fmt.Errorf(
			"runpod: claim name %q is too long: RunPod caps a pod name at %d characters and identity "+
				"rides that name, so it cannot be shortened", claimName, maxNameLen)
	}
	return name, nil
}

// claimFromName recovers the NodeClaim name podName encoded, or "" for a Pod that is not
// Nebula's. It is the tag read that RunPod's lack of tags forces into the naming scheme.
func claimFromName(name string) string {
	return strings.TrimPrefix(name, namePrefix)
}

// podSpecFromPod reads the workload off the Pod (source of truth) and the placement
// decisions off req, then maps the accelerator to RunPod's own ids.
func (p *Provider) podSpecFromPod(pod *corev1.Pod, req provider.ProvisionRequest) (PodSpec, error) {
	if len(pod.Spec.Containers) == 0 {
		return PodSpec{}, errors.New("runpod: pod has no containers")
	}
	c := pod.Spec.Containers[0]
	if c.Image == "" {
		return PodSpec{}, errors.New("runpod: pod's first container has no image")
	}
	name, err := podName(req.ClaimName)
	if err != nil {
		return PodSpec{}, err
	}

	dcs, countries := splitRegion(req.Region)
	spec := PodSpec{
		Name:  name,
		Image: c.Image,
		// command → ENTRYPOINT, args → CMD: Kubernetes' two fields land on the Docker
		// fields they are defined in terms of, so an image whose own CMD supplies the args
		// keeps working when only command is overridden.
		Entrypoint: c.Command,
		StartCmd:   c.Args,
		// The caller's resolved map is the whole environment — the Pod's literals plus
		// everything envFrom/valueFrom referenced. pod.Spec.Containers[0].Env is NOT read
		// here: it holds references this adapter has no cluster access to follow.
		Env:              req.Env,
		ContainerDiskGiB: ephemeralGiB(&c),
		Ports:            containerPorts(&c),
		DataCenterIDs:    dcs,
		CountryCodes:     countries,
		Interruptible:    req.CapacityType == nebulav1alpha1.CapacitySpot,
	}

	// Accelerator type comes from the AcceleratorTypeLabel; count from the container's
	// nvidia.com/gpu resource (see util.AcceleratorRequest).
	canonical, count, err := util.AcceleratorRequest(pod)
	if err != nil {
		return PodSpec{}, fmt.Errorf("runpod: %w", err)
	}
	if canonical != "" {
		// Every id, not just the primary: RunPod's gpuTypeIds is an array it selects from
		// by availability, so alternates widen this one launch. ids[0] stays the pool
		// identity failover blocks on, which is the caller's business, not RunPod's.
		ids, ok := p.MapAccelerator(canonical, count)
		if !ok {
			return PodSpec{}, fmt.Errorf("runpod: unsupported accelerator %q: %w",
				canonical, provider.ErrUnsupportedAccelerator)
		}
		spec.GPUTypeIDs = ids
		spec.GPUCount = count
		// RunPod sizes a GPU Pod's cpu/memory PER GPU, so the Pod's totals are divided by
		// the count. Zero (nothing requested) leaves RunPod's own per-GPU defaults.
		spec.VCPUPerGPU = perGPU(cores(resourceQty(&c, corev1.ResourceCPU)), count)
		spec.RAMPerGPUGiB = perGPU(gib(resourceQty(&c, corev1.ResourceMemory)), count)
		return spec, nil
	}
	// No accelerator label => a CPU-only Pod, which RunPod sizes with an absolute vCPU
	// count instead of a per-GPU one.
	spec.VCPUCount = cores(resourceQty(&c, corev1.ResourceCPU))
	return spec, nil
}

// checkRegistryAuth reports whether RunPod can honour a pull credential, so Provision only
// pays for an EnsureRegistryAuth call on a kind that can work.
//
// A refusal, never a fallback: an anonymous pull of a private image either 401s opaquely or
// succeeds against a PUBLIC image of the same name.
func checkRegistryAuth(a *provider.RegistryAuth) error {
	switch {
	case a.Basic != nil:
		if err := a.Validate(); err != nil {
			return fmt.Errorf("runpod: %w", err) // every error out of this adapter is prefixed
		}
		return nil
	default:
		// AWSRole is the kind the canonical form carries that RunPod has no equivalent for:
		// its registry credentials are a static username/password object, with nothing that
		// assumes an IAM role on the workload's behalf. An ECR "password" is a 12-hour
		// token, so smuggling one in as Basic would provision a Pod that stops being able
		// to pull halfway through the day.
		return a.Unsupported("runpod")
	}
}

// splitRegion turns the ONE region candidate placement chose into RunPod's two placement
// fields. Empty means unconstrained (no pool regions declared) and yields neither, which is
// the widest capacity pool.
//
// The split is by SHAPE, not by a lookup table: RunPod's data-center ids are compound
// ("EU-RO-1", "US-KS-2"), while a bare two-letter token is an ISO country code its
// countryCodes field takes ("us", "se"). So a pool declaring a geography gets one, a pool
// naming a data center gets the other, and no static list of data centers has to be
// maintained here — which matters because such a list rots silently: a stale entry only
// surfaces as a rejected create for a region that exists.
//
// This is also why ExpandRegions is left as catalog.Base's pass-through. AWS has to expand
// its group tokens because "us" is not a callable region name; for RunPod it IS callable,
// as a country code, so there is nothing to expand and one declared token stays one
// blocklistable candidate.
func splitRegion(region string) (dataCenterIDs, countryCodes []string) {
	region = strings.TrimSpace(region)
	if region == "" {
		return nil, nil
	}
	if len(region) == 2 {
		return nil, []string{strings.ToUpper(region)}
	}
	return []string{region}, nil
}

// containerPorts renders the container's declared ports in RunPod's "<port>/<proto>" form.
//
// Every port goes out as /http, not /tcp, and that is a real choice: RunPod's http scheme
// publishes a proxy URL derivable from the Pod id (see proxyURL), so the workload is
// reachable the moment it comes up, whereas /tcp reaches it only through a randomly
// assigned public port that must be read back after boot. A Pod serving raw TCP is
// therefore not addressable today; a containerPort carries no hint of its protocol above
// TCP/UDP, so the common case is what gets served.
func containerPorts(c *corev1.Container) []string {
	if len(c.Ports) == 0 {
		return nil
	}
	ports := make([]string, 0, len(c.Ports))
	for _, p := range c.Ports {
		ports = append(ports, fmt.Sprintf("%d/http", p.ContainerPort))
	}
	return ports
}

// proxyURL is RunPod's HTTP proxy address for a Pod's first declared port. It is derived,
// not read back: the form is fixed, so the URL is known at create time and survives a
// manager restart without an API call.
//
// Empty when the container declares no port — there is then no port to route to, and a
// guess would publish an endpoint that answers nothing.
func proxyURL(id string, ports []string) string {
	if id == "" || len(ports) == 0 {
		return ""
	}
	port, _, _ := strings.Cut(ports[0], "/")
	return fmt.Sprintf("https://%s-%s.proxy.runpod.net", id, port)
}

// resourceQty returns the container's request for name, falling back to its limit, or nil
// when neither is present. Requests first because that is the floor the workload declared;
// RunPod has no separate ceiling to set, so limits are only a fallback source of a number.
func resourceQty(c *corev1.Container, name corev1.ResourceName) *resource.Quantity {
	if q, ok := c.Resources.Requests[name]; ok {
		return &q
	}
	if q, ok := c.Resources.Limits[name]; ok {
		return &q
	}
	return nil
}

// cores converts a CPU quantity to whole vCPUs, RunPod's unit, rounding UP: a request of
// 500m is one vCPU, and 1500m is two. Rounding down would hand the workload less CPU than
// it asked for, and a fractional vCPU is not something RunPod can express. Nil (unset) is 0,
// which leaves RunPod's own default.
func cores(q *resource.Quantity) int {
	if q == nil {
		return 0
	}
	return ceilDiv(int(q.MilliValue()), 1000)
}

// gib converts a memory quantity to whole GiB, RunPod's unit, rounding up as cores does and
// for the same reason. Nil is 0.
func gib(q *resource.Quantity) int {
	if q == nil {
		return 0
	}
	const giB = 1024 * 1024 * 1024
	return ceilDiv(int(q.Value()), giB)
}

// ephemeralGiB reads the container's ephemeral-storage request as the container disk size.
// Zero (unset) leaves RunPod's 50 GiB default, which is generous enough that most workloads
// never need to state one.
func ephemeralGiB(c *corev1.Container) int {
	return gib(resourceQty(c, corev1.ResourceEphemeralStorage))
}

// perGPU divides a Pod-wide total by the accelerator count, rounding up, because RunPod
// sizes cpu and memory PER GPU. Rounding up keeps the total at or above what the Pod asked
// for; rounding down would under-provision every request that does not divide evenly.
//
// A zero total stays zero (unset → RunPod's default), and a zero count is treated as one so
// a malformed request cannot divide by zero.
func perGPU(total int, count int32) int {
	if total <= 0 {
		return 0
	}
	if count <= 0 {
		return total
	}
	return ceilDiv(total, int(count))
}

// ceilDiv divides rounding away from zero for positive inputs. Its own function because
// every conversion above rounds the same way, and an inlined `(a+b-1)/b` is easy to get
// subtly wrong once.
func ceilDiv(a, b int) int {
	if a <= 0 || b <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

// RunPod's desiredStatus values, the only three the API documents.
const (
	// statusRunning: RunPod WANTS this Pod running. It says nothing about whether the
	// container is up yet, which is why toState pairs it with LastStartedAt.
	statusRunning = "RUNNING"
	// statusExited: the container exited. RunPod does not distinguish a clean exit from a
	// crash here, so it maps to Terminated — "gone", with no claim about why.
	statusExited = "EXITED"
	// statusTerminated: the Pod was destroyed (our own Terminate, or a spot reclaim).
	statusTerminated = "TERMINATED"
)

// toState maps an observed Pod to the provider-agnostic lifecycle state.
//
// The subtlety is that desiredStatus is DESIRED, not observed: RunPod reports RUNNING from
// the moment it accepts the Pod, while the image may still be pulling. Reporting Running
// then would advance the Pod — and its Deployment's ready replicas — before anything is
// listening. LastStartedAt is the one field that only appears once the container has
// actually started, so it is the gate. Same shape as AWS holding an instance at Pending
// until its status checks clear.
//
// Everything unrecognized falls to Pending, so a status this adapter has not seen keeps the
// poll loop watching rather than going terminal on a live, billing Pod.
func toState(pd Pod) provider.InstanceState {
	switch strings.ToUpper(pd.DesiredStatus) {
	case statusRunning:
		if pd.LastStartedAt == "" {
			return provider.InstancePending
		}
		return provider.InstanceRunning
	case statusExited, statusTerminated:
		return provider.InstanceTerminated
	default:
		return provider.InstancePending
	}
}

// toInstance normalizes an observed RunPod Pod into the provider-agnostic Instance.
//
// Endpoint prefers the public IP and mapped port when RunPod has assigned them, because
// that is the address that reaches a Pod directly; the derived proxy URL is the fallback,
// and the only address a /http-only Pod ever has. Either way it is re-reported on every
// tick, and an empty value never clears what is already on the Pod (the write paths skip "").
func toInstance(pd Pod) provider.Instance {
	tier := nebulav1alpha1.CapacityOnDemand
	if pd.Interruptible {
		tier = nebulav1alpha1.CapacitySpot
	}
	return provider.Instance{
		ID:           pd.ID,
		ClaimName:    claimFromName(pd.Name),
		State:        toState(pd),
		CapacityType: tier,
		Region:       pd.DataCenterID,
		Endpoint:     endpointOf(pd),
	}
}

// endpointOf renders the Pod's reachable address: host:port from the public IP and its
// first port mapping, else the derived HTTP proxy URL, else empty while the Pod is still
// coming up.
//
// The mapping is walked in sorted key order because Go randomizes map iteration and this
// value is written to the Pod: an unsorted pick would rewrite the endpoint on alternating
// poll ticks for a Pod exposing two ports, which reads as flapping.
func endpointOf(pd Pod) string {
	if pd.PublicIP != "" && len(pd.PortMappings) > 0 {
		keys := make([]string, 0, len(pd.PortMappings))
		for k := range pd.PortMappings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return fmt.Sprintf("%s:%d", pd.PublicIP, pd.PortMappings[keys[0]])
	}
	return proxyURL(pd.ID, pd.Ports)
}
