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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

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

// provisionTimeout is this adapter's override of the vnode handler's generic
// Provision deadline (Capabilities.ProvisionTimeout). EC2 capacity is per-zone,
// so RunInstance fails over across the region's availability zones on a capacity
// error; AWS raises the deadline above the generic default to leave room for that
// inner zone sweep, while still capping it so a capacity-starved region yields
// promptly to the outer region-level failover rather than stalling a Pod. It
// bounds the launch attempts, not "the container became healthy" (that is the
// poll loop's job).
const provisionTimeout = 2 * time.Minute

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
	// InstanceType is the EC2 instance type resolved from the accelerator (e.g.
	// "p5.48xlarge"), via the catalog's accelerator_id column.
	InstanceType string
	// Image is the container image, from the Pod's first container. The Client is
	// responsible for launching it (e.g. via a GPU AMI + user-data, or ECS/EKS).
	Image string
	// Command is the container command+args, from the Pod.
	Command []string
	// Env is the environment, flattened from the Pod's container env.
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

// Provider is the EC2 implementation of provider.Provider. It embeds catalog.Base
// for the generic catalog methods (Name, Offerings, and MapAccelerator — which
// resolves the accelerator_id/instance-type mapping straight from the catalog, so
// no override is needed here) and implements the EC2-specific lifecycle.
type Provider struct {
	catalog.Base
	client Client
	// region is the adapter's configured default region, used when a
	// ProvisionRequest does not pin one and stamped onto observed instances that
	// do not report their own.
	region string
}

// New returns an EC2 Provider backed by client and price catalog. region is the
// default region the client was configured with (used when a request omits one);
// cat is the catalog.Lookup seam so tests can inject a fake.
func New(client Client, cat catalog.Lookup, region string) *Provider {
	return &Provider{
		Base:   catalog.Base{ProviderName: provider.ProviderAWS, Catalog: cat},
		client: client,
		region: region,
	}
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
// So each static row is stamped with this adapter's region and its Available flag
// is AND-ed with the live probe: a row survives as available only if the catalog
// seeded it available AND the region currently offers its instance type. Rows for
// types the region does not offer are still returned (so the optimizer can see the
// price) but marked unavailable. A probe failure is surfaced as an error rather
// than silently reporting the stale seed as truth.
func (p *Provider) Offerings(ctx context.Context) ([]provider.Offering, error) {
	rows := p.Catalog.Offerings(p.ProviderName)
	if len(rows) == 0 {
		return nil, nil
	}
	avail, err := p.client.AvailableInstanceTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("aws: probe instance-type availability in %s: %w", p.region, err)
	}
	out := make([]provider.Offering, len(rows))
	for i, o := range rows {
		o.Region = p.region
		o.Available = o.Available && avail[o.AcceleratorID]
		out[i] = o
	}
	return out, nil
}

// Provision implements provider.Provider. The Pod is the source of truth for the
// workload; req carries the claim identity, capacity tier, and region.
func (p *Provider) Provision(ctx context.Context, pod *corev1.Pod, req provider.ProvisionRequest) (string, error) {
	if pod == nil {
		return "", errors.New("aws: nil pod")
	}
	if req.ClaimName == "" {
		return "", errors.New("aws: empty ClaimName in ProvisionRequest")
	}

	// Idempotency: if an instance already carries this claim tag, return it rather
	// than launching a second (guards a retry after a partial create).
	if existing, err := p.findByClaim(ctx, req.ClaimName); err != nil {
		return "", err
	} else if existing != nil {
		return existing.ID, nil
	}

	spec, err := p.instanceSpecFromPod(pod, req)
	if err != nil {
		return "", err
	}
	// The Provision deadline is enforced generically by the vnode handler (from
	// Capabilities.ProvisionTimeout), so RunInstance simply honors ctx as it fails
	// over across zones — no adapter-local WithTimeout here.
	return p.client.RunInstance(ctx, spec)
}

// Terminate implements provider.Provider. Idempotent by the Client contract.
func (p *Provider) Terminate(ctx context.Context, instanceID string) error {
	if instanceID == "" {
		return nil // nothing provisioned yet; treat as already gone
	}
	return p.client.TerminateInstance(ctx, instanceID)
}

// Get implements provider.Provider.
func (p *Provider) Get(ctx context.Context, instanceID string) (*provider.Instance, error) {
	ec2, err := p.client.DescribeInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if ec2 == nil {
		return nil, nil // absent => terminated, per interface contract
	}
	inst := p.toInstance(*ec2)
	return &inst, nil
}

// List implements provider.Provider.
func (p *Provider) List(ctx context.Context) ([]provider.Instance, error) {
	instances, err := p.client.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]provider.Instance, 0, len(instances))
	for _, ec2 := range instances {
		out = append(out, p.toInstance(ec2))
	}
	return out, nil
}

// ClassifyProvisionError implements provider.Provider. The failure CATEGORIES
// and scope-derivation rule are shared (provider.ClassifyError / the ErrX
// sentinels), so this method supplies only what is EC2-specific. Two things set
// AWS apart from the OnDemand-only NeoClouds:
//
//   - Both tiers exist, so the tier to stamp on an accelerator-scoped block is
//     read from the wrapped sentinel: a Spot no-capacity error must block only
//     Spot, leaving OnDemand serviceable. ClassifyError does not see the request,
//     so we detect the Spot case here (ErrNoCapacity wrapped with the spot marker)
//     and pass the right tier.
//   - EC2 capacity is per-region and this adapter is bound to one region (its
//     client's), so an accelerator/capacity block is confined to p.region — a
//     "no capacity in us-east-1" failure does not disqualify the same request
//     against the us-west-2 adapter. Whole-provider blocks (auth/quota) are left
//     region-wide, since bad credentials fail in every region.
func (p *Provider) ClassifyProvisionError(err error, accelerator string) provider.BlockScope {
	if err == nil {
		return provider.BlockScope{}
	}
	tier := nebulav1alpha1.CapacityOnDemand
	if errors.Is(err, ErrSpotCapacity) {
		tier = nebulav1alpha1.CapacitySpot
	}
	scope := provider.ClassifyError(err, tier, accelerator)
	// Accelerator/capacity blocks are per-region (EC2 capacity is regional and
	// this adapter is bound to one region); a DenyAll (auth/quota) fails in every
	// region, so it stays region-wide (Region left nil). The exact-region pointer
	// confines the block to p.region so the same request survives in another region.
	//
	// p.region is correct ONLY because this adapter is single-region: the client is
	// pinned to cfg.Region at construction and runInSubnet ignores spec.Region, so
	// every launch lands in p.region regardless of the request's Region — the block
	// region and the launch region are the same. If the adapter is ever made
	// multi-region (runInSubnet honoring spec.Region, re-resolving AMI/subnets per
	// region), this MUST become the request's region (regionOrDefault(req.Region)) —
	// which means threading it through ClassifyProvisionError — or failover would
	// block the wrong region and keep retrying the one that actually failed.
	if !scope.DenyAll {
		region := p.region
		scope.Region = &region
	}
	return scope
}

// findByClaim returns the instance tagged with claimName, or nil if none.
func (p *Provider) findByClaim(ctx context.Context, claimName string) (*provider.Instance, error) {
	instances, err := p.client.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	for _, ec2 := range instances {
		if ec2.Tags[ClaimTagKey] == claimName {
			inst := p.toInstance(ec2)
			return &inst, nil
		}
	}
	return nil, nil
}

// instanceSpecFromPod reads the workload off the Pod (source of truth) and the
// accelerator type (from the AcceleratorTypeLabel), maps it to an EC2 instance
// type via the catalog, and stamps the claim tag, capacity tier, and region.
func (p *Provider) instanceSpecFromPod(pod *corev1.Pod, req provider.ProvisionRequest) (InstanceSpec, error) {
	if len(pod.Spec.Containers) == 0 {
		return InstanceSpec{}, errors.New("aws: pod has no containers")
	}
	c := pod.Spec.Containers[0]

	env := make(map[string]string, len(c.Env))
	for _, e := range c.Env {
		// ValueFrom (secrets/configmaps) is not resolved here; the real Client
		// wiring must project those. Plain values are copied through.
		if e.ValueFrom == nil {
			env[e.Name] = e.Value
		}
	}

	// Accelerator type comes from the AcceleratorTypeLabel; the count rides on the
	// nvidia.com/gpu resource. On EC2 both are lookup keys: the instance type is the
	// one whose (accelerator_type, gpu_count) pair matches, since the GPU count is
	// baked into the instance type (T4x1 = g4dn.xlarge, T4x8 = g4dn.metal) rather
	// than a free knob. So — unlike the identity-mapped NeoClouds — this does NOT
	// use the type-only MapAccelerator; it resolves by type AND count.
	canonical, count, err := util.AcceleratorRequest(pod)
	if err != nil {
		return InstanceSpec{}, fmt.Errorf("aws: %w", err)
	}
	if canonical == "" || count <= 0 {
		return InstanceSpec{}, errors.New(
			"aws: pod requests no accelerator; EC2 GPU provisioning needs an accelerator type and count")
	}
	instanceType, ok := p.instanceTypeFor(canonical, count)
	if !ok {
		return InstanceSpec{}, fmt.Errorf("aws: no EC2 instance type for %s x%d", canonical, count)
	}

	return InstanceSpec{
		InstanceType: instanceType,
		Image:        c.Image,
		Command:      append(append([]string{}, c.Command...), c.Args...),
		Env:          env,
		Spot:         req.CapacityType == nebulav1alpha1.CapacitySpot,
		Region:       p.regionOrDefault(req.Region),
		Tags:         map[string]string{ClaimTagKey: req.ClaimName},
	}, nil
}

// instanceTypeFor resolves the EC2 instance type serving canonical accelerators
// at the requested count, by matching a catalog offering on BOTH the accelerator
// type (case-insensitively) and gpu_count. This is the AWS-specific replacement
// for the type-only MapAccelerator: because an EC2 instance type has a fixed GPU
// count, (type, count) — not type alone — selects it. Region and capacity tier
// are NOT part of the key here: the same (type, count) maps to the same instance
// type in every region/tier, so the first matching row's AcceleratorID is
// authoritative. Returns ok=false when no offering serves that pair (e.g. an
// unsupported accelerator, or a count with no single-instance shape — there is no
// T4x2 instance type).
func (p *Provider) instanceTypeFor(canonical string, count int32) (instanceType string, ok bool) {
	for _, o := range p.Catalog.Offerings(p.ProviderName) {
		if o.GPUCount == count && strings.EqualFold(o.AcceleratorType, canonical) && o.AcceleratorID != "" {
			return o.AcceleratorID, true
		}
	}
	return "", false
}

// regionOrDefault returns the requested region, or the adapter's configured
// default when the request does not pin one.
func (p *Provider) regionOrDefault(region string) string {
	if region != "" {
		return region
	}
	return p.region
}

// toInstance normalizes an observed EC2 instance into the provider-agnostic
// Instance.
func (p *Provider) toInstance(ec2 EC2Instance) provider.Instance {
	tier := nebulav1alpha1.CapacityOnDemand
	if ec2.Spot {
		tier = nebulav1alpha1.CapacitySpot
	}
	region := ec2.Region
	if region == "" {
		region = p.region
	}
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
