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

// Package aws implements the provider.Provider interface for Amazon EC2, the
// first hyperscaler (region-aware) backend.
//
// AWS's shape drives several adapter decisions:
//   - You do not request a GPU by accelerator type; you request an INSTANCE TYPE
//     (p5.48xlarge = 8x H100). The canonical-accelerator -> instance-type mapping
//     lives in the catalog's accelerator_id column, so the shared
//     catalog.Base.MapAccelerator resolves it and this adapter needs no override.
//   - EC2 is region-aware. Every ProvisionRequest carries the optimizer-chosen
//     Region (empty => the region this adapter's client was configured with), and
//     List/Get report the region an instance actually runs in. Capacity failures
//     are per-region, so ClassifyProvisionError scopes the block to one region.
//   - EC2 has a real Spot tier with a ~2-minute interruption notice, and instances
//     can be stopped/started — so SupportsSpot and SupportsStop are true, unlike
//     the NeoCloud adapters.
//   - EC2 has native tags, so the ClaimName rides in a tag (no name-encoding hack).
//
// The concrete EC2 API lives behind the Client seam so this package holds only
// provider-agnostic translation and is unit-testable without AWS credentials or
// the SDK. A real Client wrapping the AWS SDK is wired in via NewSDKClient.
package aws

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
	"github.com/InftyAI/Nebula/pkg/util"
)

// preemptionNotice is EC2's Spot interruption warning lead time (the "2-minute
// warning"). Declared so the control plane can poll Spot claims faster and drain
// ahead of a reclaim.
const preemptionNotice = 2 * time.Minute

// spotPollInterval is how often to re-list on this adapter. Because EC2 Spot
// interruptions are common and abrupt (the 2-minute notice is not pushed to us),
// we poll faster than the vnode default so a reclaim is noticed promptly.
const spotPollInterval = 10 * time.Second

// provisionTimeout overrides the vnode handler's generic Provision deadline. EC2 capacity
// is per-zone, so RunInstance sweeps the region's AZs on a capacity error; this leaves room
// for that sweep while still capping it, so a starved region yields to the outer
// region-level failover instead of stalling a Pod. It bounds the launch attempts, not "the
// container became healthy" — that is the poll loop's job.
const provisionTimeout = 2 * time.Minute

// regionGroups maps a NodePool geography token to the EC2 regions it covers. A group token
// is not an EC2 region name and cannot be derived from one ("us" is not an endpoint, London
// is eu-west-2), so the mapping is data.
//
// Only DEFAULT-enabled regions are listed. Opt-in ones (af-south-1, ap-east-1, ca-west-1,
// eu-south-1, me-central-1, …) are excluded because clientFor resolves a GPU AMI and the
// default VPC's subnets on first use and does not cache failures — a region the account has
// not enabled would fail and retry every poll tick, forever, for a region nobody asked for.
// An operator who HAS enabled one names it explicitly; literal names pass through untouched.
//
// GovCloud (us-gov-*) and China (cn-*) are absent for a stronger reason: separate IAM
// partitions, so one credential set cannot reach them at all.
//
// Kept sorted so the failover walk order within a group is stable and reviewable.
var regionGroups = map[string][]string{
	"us": {"us-east-1", "us-east-2", "us-west-1", "us-west-2"},
	"eu": {"eu-central-1", "eu-north-1", "eu-west-1", "eu-west-2", "eu-west-3"},
	"ap": {"ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-south-1", "ap-southeast-1", "ap-southeast-2"},
	"ca": {"ca-central-1"},
	"sa": {"sa-east-1"},
}

// ErrSpotCapacity is a marker the Client wraps onto a Spot-tier capacity failure
// (alongside provider.ErrNoCapacity) so ClassifyProvisionError — which the
// interface hands only the error, not the request — can recover that the failing
// tier was Spot and block only Spot, leaving OnDemand serviceable. A real client
// wraps it as fmt.Errorf("...: %w: %w", provider.ErrNoCapacity, aws.ErrSpotCapacity).
var ErrSpotCapacity = errors.New("aws: spot capacity")

// compile-time assertion that Provider satisfies the interface.
var _ provider.Provider = (*Provider)(nil)

// ClaimTagKey is the EC2 tag under which the NodeClaim name is stored, so
// List/Get can recover Nebula identity. EC2 has native tags, so no name-encoding
// hack is needed.
const ClaimTagKey = "nebula.inftyai.com/claim"

// Client is the narrow seam over EC2's API, expressed in provider-agnostic terms
// so a real SDK-backed implementation and a test fake are interchangeable. Only
// the operations the adapter needs are exposed.
type Client interface {
	// RunInstance launches exactly one instance from spec and returns its EC2
	// instance id.
	RunInstance(ctx context.Context, spec InstanceSpec) (id string, err error)
	// TerminateInstance terminates an instance by id. Must be idempotent:
	// terminating an already-gone instance returns nil.
	TerminateInstance(ctx context.Context, id string) error
	// DescribeInstance returns one instance, or (nil, nil) if it no longer exists.
	DescribeInstance(ctx context.Context, id string) (*EC2Instance, error)
	// ListInstances returns every Nebula-owned instance (filtered by the
	// ClaimTagKey tag) across the region, in as few calls as possible.
	ListInstances(ctx context.Context) ([]EC2Instance, error)
	// AvailableInstanceTypes returns the set of EC2 instance types the client's
	// region actually offers, as a set keyed by instance type. It backs the
	// per-region availability filter in Offerings: a static catalog row whose
	// instance type is absent here is not offered in this region. Which types a
	// region offers is AWS-authoritative and changes over time, so it is queried
	// live rather than hand-maintained in the catalog.
	AvailableInstanceTypes(ctx context.Context) (map[string]bool, error)
}

// InstanceSpec is the resolved, EC2-shaped request the Client turns into a
// RunInstances call. The adapter builds it from the Pod (source of truth) plus
// the resolved instance type and capacity tier.
type InstanceSpec struct {
	// InstanceTypes are the EC2 instance types that can serve the accelerator (e.g.
	// "p5.48xlarge"), resolved from the catalog's accelerator_id column, PRIMARY
	// first then interchangeable alternates. The launch fleet spans them all in one
	// request so EC2 lands on whichever (type, AZ) pair has capacity; the primary
	// (InstanceTypes[0]) is the one the blocklist keys on. Always has at least one
	// element for a GPU launch.
	InstanceTypes []string
	// Image is the container image, from the Pod's first container. The Client is
	// responsible for launching it (e.g. via a GPU AMI + user-data, or ECS/EKS).
	Image string
	// Command is the Pod container's command, kept SEPARATE from Args to preserve
	// Kubernetes container semantics: command overrides the image ENTRYPOINT (Docker
	// --entrypoint), not its CMD. Empty means "use the image's own ENTRYPOINT".
	Command []string
	// Args is the Pod container's args, appended after the entrypoint just as CMD
	// arguments. Empty means "use the image's own CMD".
	Args []string
	// Env is the environment, taken whole from provider.ProvisionRequest.Env: literals plus
	// everything envFrom/valueFrom referenced, already resolved by the caller. See where it is
	// set for the user-data exposure this implies.
	Env map[string]string
	// Spot requests interruptible capacity when true (OnDemand otherwise).
	Spot bool
	// Region is where to launch, in EC2's vocabulary. Empty => the Client's
	// configured default region.
	Region string
	// Tags carry Nebula identity; ClaimTagKey holds the NodeClaim name.
	Tags map[string]string
}

// EC2Instance is the adapter-level view of one EC2 instance as observed.
type EC2Instance struct {
	ID     string
	Tags   map[string]string
	State  string // EC2's own state name, normalized by toState.
	Region string
	// PublicEndpoint is the reachable address once running (e.g. public DNS/IP).
	PublicEndpoint string
	// Spot is true when the instance was launched as Spot capacity.
	Spot bool
	// StatusChecksPassed is true once BOTH EC2 reachability checks (system and
	// instance, the "2/2 checks passed") report ok. An instance enters the running
	// state a minute or two before its checks pass, so toState holds a running-but-
	// unchecked instance at Pending: "running" is not yet "reachable".
	StatusChecksPassed bool
}

// ClientFactory builds a Client bound to one region. It is the seam through which
// the Provider lazily materializes a per-region EC2 client on first use: the real
// factory (NewSDKClient's) resolves that region's GPU AMI and default-VPC subnets;
// a test injects a fake. Credentials are NOT a parameter — the factory resolves
// them from the SDK's default chain, which is account-global, so ONE credential
// set serves every region and only the region endpoint differs.
type ClientFactory func(ctx context.Context, region string) (Client, error)

// RegionSource reports the regions the adapter should sweep in List and Offerings. The
// NodePool is the source of truth: cmd/main.go backs this with a lister over
// ProviderSpec.Regions across every "aws" pool, so the set is DYNAMIC (a pool added at
// runtime widens the sweep) and needs no env var. Provisioning never needed it — the target
// region rides on the request — only the fan-out does.
//
// It may return empty (no aws pool yet, or an unsynced cache); sweepRegions then falls back
// to the lazy client cache's keys, so a fleet placed by a prior generation is still swept.
// Must be safe to call concurrently.
type RegionSource func() []string

// Provider is the EC2 implementation of provider.Provider. It embeds catalog.Base
// for the generic catalog methods (Name, Offerings, and MapAccelerator — which
// resolves the accelerator_id/instance-type mapping straight from the catalog, so
// no override is needed here) and implements the EC2-specific lifecycle.
//
// Multi-region: EC2 is region-partitioned (instance, AMI, subnets, capacity all live in one
// region), so the Provider holds a region -> Client map, each Client pinned to one region
// and built lazily by newClient. One registry entry ("aws") and one virtual node still front
// them all, because the region is a per-Pod placement choice on the ProvisionRequest, not a
// per-node fact — so it has to be an axis inside the adapter, not a provider per region.
//
// The region set is not configured at construction but read from the NodePool at call time
// via regionSource, since ProviderSpec.Regions changes at runtime.
type Provider struct {
	catalog.Base
	// newClient lazily builds the Client for a region (AMI/subnet resolution). The
	// factory seam keeps the adapter SDK-free and unit-testable.
	newClient ClientFactory
	// regionSource reports the NodePool-declared regions to sweep in List/Offerings.
	// May be nil in tests, in which case sweepRegions uses only the cache keys.
	regionSource RegionSource

	mu      sync.Mutex
	clients map[string]Client // region -> Client, populated lazily by clientFor
}

// New returns an EC2 Provider backed by a client factory and price catalog.
// regionSource supplies the NodePool-declared region set the List/Offerings fan-out
// sweeps (nil is tolerated — the sweep then uses only the regions already
// provisioned into). cat is the catalog.Lookup seam so tests can inject a fake.
//
// There is deliberately NO default region: every request carries its own region
// (ExpandRegions turns even an omitted pool declaration into concrete regions, and
// placement stamps one onto the ProvisionRequest), and observed instances report
// their region from the region-pinned client — so nothing needs a fallback, and no
// AWS_REGION env is read.
func New(newClient ClientFactory, cat catalog.Lookup, regionSource RegionSource) *Provider {
	return &Provider{
		Base:         catalog.Base{ProviderName: provider.ProviderAWS, Catalog: cat},
		newClient:    newClient,
		regionSource: regionSource,
		clients:      make(map[string]Client),
	}
}

// newSingleRegion is a test/back-compat convenience: a Provider serving exactly one
// region backed by an already-built Client (no lazy factory). It underpins the
// existing single-region unit tests, which inject a fakeClient directly. The region
// is the sole swept region (via a constant regionSource), so List/Offerings behave
// as the pre-multi-region tests expect.
func newSingleRegion(client Client, cat catalog.Lookup, region string) *Provider {
	p := New(
		func(context.Context, string) (Client, error) { return client, nil },
		cat,
		func() []string { return []string{region} },
	)
	// Pre-seed the cache so even a stray region lookup returns the fake rather than
	// invoking the (constant) factory.
	p.clients[region] = client
	return p
}

// ExpandRegions implements provider.Provider, overriding catalog.Base's pass-through:
// EC2 region names do not contain the pool's group tokens, so AWS needs the
// regionGroups table. Three levels, in the order they are checked:
//
//	nil/[]          => every default-enabled region (the union of regionGroups)
//	["us"]          => that group's regions
//	["us-east-1"]   => itself, verbatim and unvalidated
//
// A non-group value is a literal region name, NOT validated against any list: EC2 gains
// regions faster than this table is edited, so validating would reject a region that
// exists, while an impossible name simply fails at clientFor with AWS's own error. That is
// also the escape hatch for opt-in regions, which no group contains.
//
// The result is deduped (["us", "us-east-1"] is 4 regions, not 5) and order-stable, so the
// failover walk is reproducible.
//
// Unconstrained is wide: ~17 regions per capacity tier, each walked as a candidate and
// swept by List/Offerings every tick. Prefer a group unless the workload needs global reach.
//
// It delegates to the package-level ExpandRegions, which cmd/main.go's region source also
// needs — it must expand each pool BEFORE unioning across pools (a pool declaring nothing
// means "all", a meaning lost if raw lists were unioned first).
func (p *Provider) ExpandRegions(declared []string) []string { return ExpandRegions(declared) }

// ExpandRegions is Provider.ExpandRegions as a package-level function; see that
// method for the semantics. It is exported because the NodePool-backed RegionSource
// in cmd/main.go must apply the identical expansion, and it needs it per-pool at a
// point where no Provider is in hand.
func ExpandRegions(declared []string) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(r string) {
		if r == "" || seen[r] {
			return
		}
		seen[r] = true
		out = append(out, r)
	}
	// Unconstrained: every default-enabled region. Walk the group table in sorted key
	// order so the union is deterministic (Go randomizes map iteration).
	if len(declared) == 0 {
		groups := make([]string, 0, len(regionGroups))
		for g := range regionGroups {
			groups = append(groups, g)
		}
		sort.Strings(groups)
		for _, g := range groups {
			for _, r := range regionGroups[g] {
				add(r)
			}
		}
		return out
	}
	for _, d := range declared {
		d = strings.TrimSpace(d)
		if group, ok := regionGroups[strings.ToLower(d)]; ok {
			for _, r := range group {
				add(r)
			}
			continue
		}
		add(d) // a literal region name: forwarded unvalidated
	}
	return out
}

// sweepRegions returns the regions List and Offerings fan out across: the union of
// the NodePool-declared set (regionSource) and every region already in the lazy
// client cache. The cache half is what makes teardown survive a NodePool edit — an
// instance still running in a region just dropped from every pool is still swept and
// so still observed/reclaimed, rather than being stranded because the region left
// the declared set. Order is not significant (callers concatenate results).
func (p *Provider) sweepRegions() []string {
	seen := make(map[string]bool)
	var out []string
	add := func(r string) {
		if r == "" || seen[r] {
			return
		}
		seen[r] = true
		out = append(out, r)
	}
	if p.regionSource != nil {
		for _, r := range p.regionSource() {
			add(strings.TrimSpace(r))
		}
	}
	p.mu.Lock()
	for r := range p.clients {
		add(r)
	}
	p.mu.Unlock()
	return out
}

// clientFor returns the Client for region (defaulting an empty region), building
// and caching it on first use. Construction is serialized under mu: a region's
// Client is built at most once, and a concurrent caller for the same or another
// region waits — acceptable because a build is a rare one-time, per-region event
// (an AMI + subnet resolution), not a hot path. A build failure is NOT cached, so
// a transient resolution error (throttle, a not-yet-enabled region) is retried on
// the next call rather than poisoning the region permanently.
func (p *Provider) clientFor(ctx context.Context, region string) (Client, error) {
	if region == "" {
		// No region to build a client for. Unreachable on the normal path (every
		// request carries a region), so this only guards a legacy unqualified instance
		// id reaching Terminate/Get — surface it rather than silently guessing.
		return nil, fmt.Errorf("aws: no region for client: %w", ErrConfig)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[region]; ok {
		return c, nil
	}
	c, err := p.newClient(ctx, region)
	if err != nil {
		return nil, fmt.Errorf("aws: build client for region %s: %w", region, err)
	}
	p.clients[region] = c
	return c, nil
}

// Capabilities implements provider.Provider. See the package doc for why each
// trait is set the way it is.
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		SupportsStop:     true,             // EC2 instances stop/start
		SupportsSpot:     true,             // real interruptible tier
		NativeTags:       true,             // EC2 tags carry identity
		PreemptionNotice: preemptionNotice, // Spot 2-minute warning
		PollInterval:     spotPollInterval, // Spot reclaims are abrupt; poll faster than default
		ProvisionTimeout: provisionTimeout, // caps the per-zone capacity failover loop
	}
}

// Offerings implements provider.Provider, overriding the generic
// catalog.Base.Offerings. It combines the two halves of AWS's price/availability
// truth:
//
//   - The catalog CSV holds the DURABLE facts — the (accelerator, count) ->
//     instance-type mapping and a seed price — with a BLANK region, since the same
//     instance type serves the same pair in every region.
//   - AWS itself holds the PER-REGION truth: which instance types the configured
//     region actually offers, queried live via the AvailableInstanceTypes probe.
//
// So each static row is emitted once PER REGION, with Available AND-ed against that
// region's live probe. Rows for types a region does not offer are still returned, marked
// unavailable, so the optimizer can still see the price.
//
// A region that fails its probe is skipped rather than failing the whole call, as in List.
// Only if every region fails is an error returned.
func (p *Provider) Offerings(ctx context.Context) ([]provider.Offering, error) {
	rows := p.Catalog.Offerings(p.ProviderName)
	if len(rows) == 0 {
		return nil, nil
	}
	log := logf.FromContext(ctx).WithName("aws-offerings")

	regions := p.sweepRegions()
	out := make([]provider.Offering, 0, len(rows)*len(regions))
	failed := 0
	for _, region := range regions {
		client, err := p.clientFor(ctx, region)
		if err != nil {
			failed++
			log.Error(err, "build client for offerings failed; skipping region", "region", region)
			continue
		}
		avail, err := client.AvailableInstanceTypes(ctx)
		if err != nil {
			failed++
			log.Error(err, "probe instance-type availability failed; skipping region", "region", region)
			continue
		}
		for _, o := range rows {
			o.Region = region
			o.Available = o.Available && avail[o.AcceleratorID]
			out = append(out, o)
		}
	}
	if failed == len(regions) && failed > 0 {
		return nil, fmt.Errorf("aws: offerings probe failed in all %d region(s)", failed)
	}
	return out, nil
}

// Provision implements provider.Provider. The Pod is the source of truth for the
// workload; req carries the claim identity, capacity tier, and region.
//
// The returned id is the RAW EC2 id ("i-..."), not region-qualified: it is what the
// NodeClaim ledger records and the user sees. Terminate/Get find the instance by sweeping
// regions instead, since a wrong-region lookup is a harmless no-op.
//
// Every id this returns is RESERVED. RunInstance goes through a CreateFleet *instant*
// request, which is synchronous: the response either carries an id — EC2 found capacity in
// some (type, AZ) cell and the instance is booting — or the reason it could not launch,
// which becomes the error driving AZ/region/tier failover. An EC2 instance has no queued
// state to sit in, so reserved is unconditionally true on success.
//
// No connect credential: an instance is reached at its public DNS name or IP, which EC2
// does not know until boot, so the address is observed by List/Get into Instance.Endpoint
// and access is authenticated by the key pair and security group, not a bearer token.
func (p *Provider) Provision(
	ctx context.Context, pod *corev1.Pod, req provider.ProvisionRequest,
) (provider.ProvisionResult, error) {
	if pod == nil {
		return provider.ProvisionResult{}, errors.New("aws: nil pod")
	}
	if req.ClaimName == "" {
		return provider.ProvisionResult{}, errors.New("aws: empty ClaimName in ProvisionRequest")
	}

	region := req.Region
	client, err := p.clientFor(ctx, region)
	if err != nil {
		return provider.ProvisionResult{}, err
	}

	// Idempotency: if an instance already carries this claim tag IN THIS REGION,
	// return it rather than launching a second (guards a retry after a partial
	// create). A claim is placed in exactly one region per attempt, so scanning the
	// target region's client is sufficient. It is reserved for the same reason a
	// fresh launch is: it only exists because some earlier instant fleet succeeded.
	if existing, err := findByClaim(ctx, client, req.ClaimName); err != nil {
		return provider.ProvisionResult{}, err
	} else if existing != nil {
		return provider.ProvisionResult{InstanceID: existing.ID, Reserved: true}, nil
	}

	spec, err := p.instanceSpecFromPod(pod, req)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	// The Provision deadline is enforced generically by the vnode handler (from
	// Capabilities.ProvisionTimeout), so RunInstance simply honors ctx as it fails
	// over across zones — no adapter-local WithTimeout here.
	id, err := client.RunInstance(ctx, spec)
	if err != nil {
		return provider.ProvisionResult{}, err
	}
	return provider.ProvisionResult{InstanceID: id, Reserved: true}, nil
}

// Terminate implements provider.Provider. Idempotent by the Client contract. The
// instanceID is a raw EC2 id, which does not carry its region, so teardown sweeps
// the swept regions and terminates the instance wherever it lives. This is safe
// and cheap: TerminateInstance is idempotent and a wrong-region lookup returns
// InvalidInstanceID.NotFound, which the Client maps to nil — so terminating in a
// region the instance is not in is a harmless no-op. It stops at the first region
// that actually owns the instance.
//
// A legacy region-qualified id ("<region>/i-...") from before this change is still
// honored: splitID peels the region off and it routes straight to that region.
func (p *Provider) Terminate(ctx context.Context, instanceID string) error {
	if instanceID == "" {
		return nil // nothing provisioned yet; treat as already gone
	}
	// Back-compat: a legacy qualified id routes directly to its region.
	if region, rawID := splitID(instanceID); region != "" {
		client, err := p.clientFor(ctx, region)
		if err != nil {
			return err
		}
		return client.TerminateInstance(ctx, rawID)
	}

	// Raw id: sweep the regions and terminate wherever it lives. Confirm ownership
	// with a Describe first so we only issue TerminateInstance against the region
	// that actually has it — and so a region whose client cannot be built does not
	// mask a successful terminate elsewhere.
	var lastErr error
	for _, region := range p.sweepRegions() {
		client, err := p.clientFor(ctx, region)
		if err != nil {
			lastErr = err
			continue
		}
		ec2, err := client.DescribeInstance(ctx, instanceID)
		if err != nil {
			lastErr = err
			continue
		}
		if ec2 == nil {
			continue // not in this region
		}
		return client.TerminateInstance(ctx, instanceID)
	}
	// Not found in any region we could reach: already gone (idempotent success),
	// unless every reachable region errored, in which case surface that for retry.
	return lastErr
}

// Get implements provider.Provider. instanceID is a raw EC2 id, which does not
// carry its region, so the lookup sweeps the swept regions and returns the first
// region's view of the instance. A per-region client-build/describe error is
// tolerated and the sweep continues; only if every region errored (and none held
// the instance) is that error surfaced.
//
// A legacy region-qualified id ("<region>/i-...") routes directly to its region.
func (p *Provider) Get(ctx context.Context, instanceID string) (*provider.Instance, error) {
	if region, rawID := splitID(instanceID); region != "" {
		client, err := p.clientFor(ctx, region)
		if err != nil {
			return nil, err
		}
		ec2, err := client.DescribeInstance(ctx, rawID)
		if err != nil {
			return nil, err
		}
		if ec2 == nil {
			return nil, nil // absent => terminated, per interface contract
		}
		inst := p.toInstance(*ec2)
		return &inst, nil
	}

	var lastErr error
	for _, region := range p.sweepRegions() {
		client, err := p.clientFor(ctx, region)
		if err != nil {
			lastErr = err
			continue
		}
		ec2, err := client.DescribeInstance(ctx, instanceID)
		if err != nil {
			lastErr = err
			continue
		}
		if ec2 == nil {
			continue // not in this region
		}
		inst := p.toInstance(*ec2)
		return &inst, nil
	}
	// Absent from every reachable region => terminated (nil,nil), unless a region
	// errored and might have held it, in which case surface the error for retry.
	return nil, lastErr
}

// List implements provider.Provider. It FANS OUT across every region sweepRegions
// yields (the NodePool-declared set unioned with regions already provisioned into)
// and concatenates the results, so the poll loop and the NodeClaim teardown backstop
// see instances wherever they live — including regions this process never
// provisioned into itself (a restart, or an instance placed by a prior leader).
//
// Per-region errors are TOLERATED: one region throttling must not blank the whole list,
// which the poll loop would read as "every tracked pod's instance vanished". A failing
// region is logged and skipped for this tick. Only if EVERY region fails is an error
// returned, so a total outage still surfaces instead of reporting an empty fleet.
func (p *Provider) List(ctx context.Context) ([]provider.Instance, error) {
	log := logf.FromContext(ctx).WithName("aws-list")

	type result struct {
		region    string
		instances []provider.Instance
		err       error
	}
	regions := p.sweepRegions()
	// No region to sweep (no aws NodePool yet and nothing provisioned): an empty
	// fleet, not an error. Returning nil here is correct — there are provably no
	// Nebula instances to observe — and avoids the "all regions failed" path below
	// firing on a zero-length set.
	if len(regions) == 0 {
		return nil, nil
	}
	results := make([]result, len(regions))
	var wg sync.WaitGroup
	for i, region := range regions {
		wg.Add(1)
		go func(i int, region string) {
			defer wg.Done()
			client, err := p.clientFor(ctx, region)
			if err != nil {
				results[i] = result{region: region, err: err}
				return
			}
			raw, err := client.ListInstances(ctx)
			if err != nil {
				results[i] = result{region: region, err: err}
				return
			}
			insts := make([]provider.Instance, 0, len(raw))
			for _, ec2 := range raw {
				insts = append(insts, p.toInstance(ec2))
			}
			results[i] = result{region: region, instances: insts}
		}(i, region)
	}
	wg.Wait()

	out := make([]provider.Instance, 0)
	failed := 0
	for _, r := range results {
		if r.err != nil {
			failed++
			log.Error(r.err, "list instances in region failed; skipping this region for this tick", "region", r.region)
			continue
		}
		out = append(out, r.instances...)
	}
	// Every region failed => surface the outage rather than reporting an empty
	// fleet, which would strand or falsely terminate tracked pods.
	if failed == len(regions) && failed > 0 {
		return nil, fmt.Errorf("aws: list failed in all %d region(s)", failed)
	}
	return out, nil
}

// ClassifyProvisionError implements provider.Provider. The failure CATEGORIES
// and scope-derivation rule are shared (provider.ClassifyError / the ErrX
// sentinels), so this method supplies only what is EC2-specific. Two things set
// AWS apart from the OnDemand-only NeoClouds:
//
//   - Both tiers exist, so the tier stamped on an accelerator block comes from the wrapped
//     sentinel: a Spot shortage must block only Spot, leaving OnDemand serviceable.
//   - EC2 capacity is per-region, so a capacity or quota block is confined to the region
//     that actually failed — a shortage in us-east-1 must not disqualify us-west-2, and
//     vCPU limits are regional too. Only DenyAll (auth) stays region-wide, since bad
//     credentials fail everywhere. The region is a parameter because the error does not
//     carry it and there is no default region to assume.
func (p *Provider) ClassifyProvisionError(err error, accelerator, region string) provider.BlockScope {
	if err == nil {
		return provider.BlockScope{}
	}
	tier := nebulav1alpha1.CapacityOnDemand
	if errors.Is(err, ErrSpotCapacity) {
		tier = nebulav1alpha1.CapacitySpot
	}
	scope := provider.ClassifyError(err, tier, accelerator)
	// Confine an accelerator/capacity/quota block to the region that failed. A
	// DenyAll (auth) fails in every region, so it stays region-wide (Region left nil).
	// An empty region (should not happen — every request carries one) maps to &"" =
	// the wildcard, which blocks the accelerator in ALL regions: the safe over-broad
	// choice, since a stale-but-effective block beats a block pinned to a guessed
	// region that never matches.
	if !scope.DenyAll {
		r := region
		scope.Region = &r
	}
	return scope
}

// findByClaim returns the instance in this client's region tagged with claimName,
// or nil if none. It scans one region's client (the launch target), since a claim
// is placed in exactly one region per provision attempt.
func findByClaim(ctx context.Context, client Client, claimName string) (*EC2Instance, error) {
	instances, err := client.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	for i := range instances {
		if instances[i].Tags[ClaimTagKey] == claimName {
			return &instances[i], nil
		}
	}
	return nil, nil
}

// idSep separates the region prefix from the raw EC2 id in a legacy region-qualified
// instance id ("<region>/<ec2-id>"). "/" cannot appear in either a region name or
// an EC2 instance id, so it is an unambiguous delimiter. Current ids are raw EC2
// ids; this exists only so splitID can still route ids recorded before that change.
const idSep = "/"

// splitID recognizes a LEGACY region-qualified id ("<region>/i-..."): it returns
// the region and the raw EC2 id. A current, raw EC2 id (no separator) yields an
// empty region, signaling the caller to locate the instance by sweeping regions
// instead. It is the one remaining reader of the old format, kept so ids persisted
// on a NodeClaim before the id stopped being qualified still terminate correctly.
func splitID(instanceID string) (region, rawID string) {
	if i := strings.Index(instanceID, idSep); i >= 0 {
		return instanceID[:i], instanceID[i+len(idSep):]
	}
	return "", instanceID
}

// instanceSpecFromPod reads the workload off the Pod (source of truth) and the
// accelerator type (from the AcceleratorTypeLabel), maps it to an EC2 instance
// type via the catalog, and stamps the claim tag, capacity tier, and region.
func (p *Provider) instanceSpecFromPod(
	pod *corev1.Pod, req provider.ProvisionRequest,
) (InstanceSpec, error) {
	if len(pod.Spec.Containers) == 0 {
		return InstanceSpec{}, errors.New("aws: pod has no containers")
	}
	c := pod.Spec.Containers[0]

	// Accelerator type comes from the AcceleratorTypeLabel; the count rides on the
	// nvidia.com/gpu resource. On EC2 both are lookup keys: the instance type is the
	// one whose (accelerator_type, gpu_count) pair matches, since the GPU count is
	// baked into the instance type (T4x1 = g4dn.xlarge, T4x8 = g4dn.metal) rather
	// than a free knob. MapAccelerator (from the embedded catalog.Base) resolves by
	// that (type, count) key straight from the catalog, returning the PRIMARY instance
	// type first then any interchangeable alternates — the launch fleet spans them all
	// so EC2 can land on whichever (type, AZ) pair has capacity.
	canonical, count, err := util.AcceleratorRequest(pod)
	if err != nil {
		return InstanceSpec{}, fmt.Errorf("aws: %w", err)
	}
	if canonical == "" || count <= 0 {
		return InstanceSpec{}, errors.New(
			"aws: pod requests no accelerator; EC2 GPU provisioning needs an accelerator type and count")
	}
	instanceTypes, ok := p.MapAccelerator(canonical, count)
	if !ok {
		return InstanceSpec{}, fmt.Errorf("aws: no EC2 instance type for %s x%d", canonical, count)
	}

	return InstanceSpec{
		InstanceTypes: instanceTypes,
		Image:         c.Image,
		Command:       append([]string{}, c.Command...),
		Args:          append([]string{}, c.Args...),
		// The caller's resolved environment, whole (provider.ProvisionRequest.Env). The Pod's
		// own env is not read: it holds references this adapter cannot follow.
		//
		// CAVEAT, and the reason this is called out: buildUserData renders env into cloud-init
		// user-data, which EC2 stores unencrypted and serves through IMDS and
		// DescribeInstanceAttribute. A value resolved from a Secret therefore lands somewhere
		// readable by anything on the instance and by any principal holding that IAM
		// permission. Acceptable while nothing sensitive rides on it; not a place to put a
		// long-lived credential.
		// TODO: deliver Secret-derived values out-of-band — SSM Parameter Store / Secrets
		// Manager under the claim, fetched at boot with the instance profile — and keep only
		// non-sensitive values in user-data.
		Env:    req.Env,
		Spot:   req.CapacityType == nebulav1alpha1.CapacitySpot,
		Region: req.Region,
		Tags:   map[string]string{ClaimTagKey: req.ClaimName},
	}, nil
}

// toInstance normalizes an observed EC2 instance into the provider-agnostic
// Instance.
func (p *Provider) toInstance(ec2 EC2Instance) provider.Instance {
	tier := nebulav1alpha1.CapacityOnDemand
	if ec2.Spot {
		tier = nebulav1alpha1.CapacitySpot
	}
	// The region-pinned SDK client always stamps its region onto observed instances
	// (see sdkClient.observe), so ec2.Region is set; no default fallback is needed.
	region := ec2.Region
	return provider.Instance{
		ID:           ec2.ID,
		ClaimName:    ec2.Tags[ClaimTagKey],
		State:        toState(ec2.State, ec2.StatusChecksPassed),
		Endpoint:     ec2.PublicEndpoint,
		CapacityType: tier,
		Region:       region,
	}
}

// EC2 instance state names (a subset; see the EC2 API InstanceState). toState
// normalizes these to the provider-agnostic lifecycle state.
const (
	stateRunning      = "running"
	statePending      = "pending"
	stateStopping     = "stopping"
	stateStopped      = "stopped"
	stateShuttingDown = "shutting-down"
	stateTerminated   = "terminated"
)

// toState maps EC2's instance-state name to the provider-agnostic lifecycle
// state. "running" is up but not necessarily reachable; "pending" is coming up;
// "terminated"/"shutting-down" are gone; "stopping"/"stopped" are treated as
// Terminated for scheduling purposes (a stopped instance is not serving the
// workload, and the NodeClaim ledger's recovery model is delete-and-recreate).
// An unrecognized state maps to Pending so the poll loop keeps watching rather
// than declaring a premature terminal state.
//
// A running instance is only reported Running once its reachability checks pass
// (statusChecksPassed). EC2 flips an instance to "running" a minute or two before
// its 2/2 checks clear; reporting Running that early would advance the Pod (and
// the owning Deployment) before the instance can actually be reached, so a
// running-but-unchecked instance is held at Pending until the checks pass.
func toState(ec2State string, statusChecksPassed bool) provider.InstanceState {
	switch ec2State {
	case stateRunning:
		if !statusChecksPassed {
			return provider.InstancePending
		}
		return provider.InstanceRunning
	case statePending:
		return provider.InstancePending
	case stateTerminated, stateShuttingDown, stateStopping, stateStopped:
		return provider.InstanceTerminated
	default:
		return provider.InstancePending
	}
}
