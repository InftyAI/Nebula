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

package aws

import (
	"context"
	"errors"
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"

	"github.com/InftyAI/Nebula/pkg/provider/catalog"
)

// ec2API is the subset of the EC2 SDK client the sdkClient depends on. Narrowing
// it to an interface lets client.go's translation be tested against a fake SDK
// without a live account (the real *ec2.Client satisfies it).
type ec2API interface {
	RunInstances(
		ctx context.Context, in *ec2.RunInstancesInput, optFns ...func(*ec2.Options),
	) (*ec2.RunInstancesOutput, error)
	TerminateInstances(
		ctx context.Context, in *ec2.TerminateInstancesInput, optFns ...func(*ec2.Options),
	) (*ec2.TerminateInstancesOutput, error)
	DescribeInstances(
		ctx context.Context, in *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options),
	) (*ec2.DescribeInstancesOutput, error)
	// DescribeInstanceStatus reports the system+instance reachability checks (the
	// "2/2 checks passed" a launched instance clears a minute or two AFTER it
	// enters the running state), used to gate the Running lifecycle state so a Pod
	// is not reported Running before its instance is actually reachable.
	DescribeInstanceStatus(
		ctx context.Context, in *ec2.DescribeInstanceStatusInput, optFns ...func(*ec2.Options),
	) (*ec2.DescribeInstanceStatusOutput, error)
	DescribeInstanceTypeOfferings(
		ctx context.Context, in *ec2.DescribeInstanceTypeOfferingsInput, optFns ...func(*ec2.Options),
	) (*ec2.DescribeInstanceTypeOfferingsOutput, error)
	// DescribeImages resolves the region's GPU AMI at construction (see
	// resolveGPUAMI); DescribeSubnets discovers the default VPC's per-AZ subnets
	// RunInstance fails over across (see discoverDefaultSubnets).
	DescribeImages(
		ctx context.Context, in *ec2.DescribeImagesInput, optFns ...func(*ec2.Options),
	) (*ec2.DescribeImagesOutput, error)
	DescribeSubnets(
		ctx context.Context, in *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options),
	) (*ec2.DescribeSubnetsOutput, error)
}

// sdkClient is the real Client, backed by aws-sdk-go-v2's EC2 API. All
// EC2-specific SDK calls live here so the adapter (aws.go) and its tests stay
// SDK-free; the pure translation (user-data, error mapping) lives in translate.go.
//
// Execution model (see the package doc): a NodeClaim maps to one EC2 instance
// launched DIRECTLY from RunInstances — no Launch Template, no pre-created infra.
// Everything the launch needs is SELF-CONFIGURED from credentials alone at
// construction, so registering AWS in a new region requires nothing but access:
//
//   - amiID: the region's GPU AMI (NVIDIA driver + container runtime baked in),
//     resolved live via DescribeImages against the Amazon-owned ECS GPU-optimized
//     AMI (see resolveGPUAMI). AMI ids are per-region, so this cannot be a constant.
//   - subnets: the default VPC's subnets, one per availability zone, discovered
//     via DescribeSubnets (default-for-az=true). EC2 capacity is per-AZ, so trying
//     another zone's subnet is the cheapest recovery from InsufficientInstance
//     capacity before condemning the whole region.
//   - security group: left UNSET. Launching into a default-VPC subnet with no
//     SecurityGroupIds makes EC2 attach that VPC's default SG (outbound-open,
//     inbound-from-itself) — exactly what a fire-and-forget `docker pull && run`
//     needs, with no inbound exposure.
//
// The adapter overrides only what is workload-specific: instance type, user-data
// (the container bootstrap), Spot market option, and the identity tags.
type sdkClient struct {
	ec2 ec2API
	// region is the resolved region this client operates in, echoed onto observed
	// instances that do not otherwise report one.
	region string
	// amiID is the region's GPU AMI, resolved at construction. Every instance
	// launches from it; it is NON-SECRET, AWS-published config, not a credential.
	amiID string
	// subnets are the default VPC's per-AZ subnets RunInstance fails over across on
	// a capacity error, discovered at construction. Empty when the region has no
	// default VPC: RunInstance then makes a single attempt letting EC2 pick the
	// subnet (still valid, just no zone failover).
	subnets []subnet
}

// subnet is one candidate launch target: its id and the AZ it lives in (for
// logging/ordering). RunInstance launches into one of these per attempt.
type subnet struct {
	id string
	az string
}

// compile-time assertion that sdkClient satisfies the adapter's Client seam.
var _ Client = (*sdkClient)(nil)

// NewSDKClient builds an EC2-backed Provider for region, ready to register.
//
// It is fully self-configuring: given only credentials and a region, it resolves
// the GPU AMI and the default VPC's subnets itself, so registering AWS in a new
// region needs no pre-created Launch Template, tagged subnets, or any other infra
// — matching the creds-only UX of the NeoCloud adapters.
//
// Security contract (fixed regardless of backing):
//   - region is the ONLY non-secret config, passed as a plain argument (flag/env).
//     Empty means "resolve the SDK's default region" from the standard chain
//     (AWS_REGION / shared config / IMDS); the resolved value is echoed on
//     observed instances and stamped onto region-scoped blocks. A region that
//     resolves to empty is a config skip (ErrConfig).
//   - Credentials are SECRETS and are NEVER accepted here. The SDK's default
//     credential chain (config.LoadDefaultConfig) is used, preferring IRSA /
//     instance-role and falling back to AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//     from the environment (a Kubernetes Secret / .env, not a flag), keeping
//     secrets out of process arguments and logs.
//
// The catalog is loaded via catalog.Load() (embedded CSV / mounted ConfigMap),
// identical to the other adapters.
func NewSDKClient(ctx context.Context, region string) (*Provider, error) {
	cat, err := catalog.Load()
	if err != nil {
		return nil, fmt.Errorf("aws: load price catalog: %w", err)
	}

	opts := []func(*config.LoadOptions) error{}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("aws: load SDK config: %w", err)
	}
	if cfg.Region == "" {
		// Neither the argument nor the SDK chain resolved a region; without one the
		// EC2 endpoint is undefined. Treat as unconfigured (non-fatal skip).
		return nil, fmt.Errorf("aws: no region resolved: %w", ErrConfig)
	}

	c := &sdkClient{
		ec2:    ec2.NewFromConfig(cfg),
		region: cfg.Region,
	}

	// Resolve the region's GPU AMI (required — no AMI, nothing to launch) and the
	// default VPC's per-AZ subnets (best-effort — no default VPC leaves c.subnets
	// empty and RunInstance lets EC2 pick the subnet, just without zone failover).
	amiID, err := c.resolveGPUAMI(ctx)
	if err != nil {
		return nil, fmt.Errorf("aws: resolve GPU AMI in %s: %w", cfg.Region, err)
	}
	c.amiID = amiID

	subnets, err := c.discoverDefaultSubnets(ctx)
	if err != nil {
		return nil, fmt.Errorf("aws: discover default-VPC subnets in %s: %w", cfg.Region, err)
	}
	c.subnets = subnets

	return New(c, cat, cfg.Region), nil
}

// gpuAMINameFilter matches the Amazon-owned Amazon-Linux-2 ECS GPU-optimized AMI.
// DescribeImages is filtered to this pattern and owner "amazon", then the newest
// by creation date is chosen — so the AMI stays current without a hand-maintained
// per-region id table. This AMI ships the NVIDIA driver and a Docker runtime,
// which is all buildUserData's `docker pull && docker run --gpus all` needs.
const gpuAMINameFilter = "amzn2-ami-ecs-gpu-hvm-*-x86_64-ebs"

// ErrConfig marks a missing/invalid non-secret AWS configuration (no resolvable
// region, no GPU AMI in the region). It is a distinct sentinel so cmd/main.go can
// treat "AWS not configured" as a non-fatal skip (errors.Is) — AWS simply is not
// registered — rather than crashing the manager, mirroring a creds-absent skip
// for the other providers.
var ErrConfig = errors.New("aws: not configured")

// RunInstance implements Client. It launches exactly one instance directly (from
// the resolved GPU AMI into a default-VPC subnet), overriding only the
// workload-specific fields.
//
// EC2 capacity is per-availability-zone, so a launch is not a single shot: when a
// zone reports InsufficientInstanceCapacity (or a Spot-capacity variant),
// RunInstance retries in the next discovered zone before giving up. Only when
// every candidate zone is capacity-starved does it return the wrapped
// ErrNoCapacity, so the outer region-level failover (ClassifyProvisionError +
// NodePool regions) fires only after this cheaper inner failover is exhausted. A
// non-capacity failure (auth, quota, bad config) is terminal and returned
// immediately — retrying other zones would not help and only burns the deadline.
// The ctx deadline (set by the vnode handler from Capabilities.ProvisionTimeout)
// bounds the whole sweep.
//
// When no subnets were discovered (no default VPC), the loop runs once with a
// zero-value target, letting EC2 pick the subnet — a valid single-shot launch,
// just without zone failover.
func (c *sdkClient) RunInstance(ctx context.Context, spec InstanceSpec) (string, error) {
	userData, err := buildUserData(spec)
	if err != nil {
		return "", err
	}

	targets := c.subnets
	if len(targets) == 0 {
		targets = []subnet{{}} // single attempt, letting EC2 pick the subnet
	}

	var lastErr error
	for _, sn := range targets {
		// Honor the deadline between attempts: a slow first zone must not let the
		// sweep overrun the provision timeout.
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return "", lastErr
			}
			return "", err
		}

		id, err := c.runInSubnet(ctx, spec, userData, sn)
		if err == nil {
			return id, nil
		}
		classified := classifyEC2Error(err, spec.Spot)
		// Only a ZONE-LOCAL capacity shortfall justifies trying the next zone: a
		// sibling AZ's default subnet may still satisfy the launch. Everything else
		// is terminal for this sweep and surfaced immediately — auth/quota/unknown
		// (retrying other zones would not help), and crucially a region/account-
		// scoped capacity error (a Spot price ceiling or per-region Spot limit), for
		// which every AZ would fail identically. Stopping now hands such an error
		// straight to region/tier failover instead of burning the deadline on a
		// futile sweep. It still carries ErrNoCapacity, so ClassifyProvisionError
		// blocks the region correctly.
		if !errors.Is(classified, errZoneLocal) {
			return "", classified
		}
		lastErr = classified
	}
	// Every zone was capacity-starved; return the last capacity error so
	// ClassifyProvisionError can block the region and region-failover can proceed.
	return "", lastErr
}

// runInSubnet issues one RunInstances attempt. sn.id empty => let EC2 pick the
// subnet (no default VPC discovered); otherwise launch into that subnet (and thus
// its AZ). SecurityGroupIds is deliberately unset: launching into a default-VPC
// subnet attaches that VPC's default SG (outbound-open, no inbound exposure),
// which is all a fire-and-forget container launch needs.
func (c *sdkClient) runInSubnet(ctx context.Context, spec InstanceSpec, userData string, sn subnet) (string, error) {
	in := &ec2.RunInstancesInput{
		MinCount:     awssdk.Int32(1),
		MaxCount:     awssdk.Int32(1),
		ImageId:      awssdk.String(c.amiID),
		InstanceType: ec2types.InstanceType(spec.InstanceType),
		UserData:     awssdk.String(userData),
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeInstance,
			Tags:         ec2Tags(spec.Tags),
		}},
	}
	if sn.id != "" {
		in.SubnetId = awssdk.String(sn.id)
	}
	if spec.Spot {
		in.InstanceMarketOptions = &ec2types.InstanceMarketOptionsRequest{
			MarketType: ec2types.MarketTypeSpot,
		}
	}

	out, err := c.ec2.RunInstances(ctx, in)
	if err != nil {
		return "", err // caller classifies (needs the raw error to detect capacity)
	}
	if len(out.Instances) == 0 || out.Instances[0].InstanceId == nil {
		return "", errors.New("aws: RunInstances returned no instance id")
	}
	return *out.Instances[0].InstanceId, nil
}

// resolveGPUAMI finds the region's GPU AMI: the newest Amazon-owned image whose
// name matches gpuAMINameFilter (the ECS GPU-optimized AMI). AMI ids are
// per-region, so this is queried live rather than hardcoded — the whole point of
// the self-configuring model. A region that returns no matching image is a config
// error (ErrConfig): AWS is effectively not usable there, and the caller skips it
// non-fatally rather than launching from a missing AMI.
func (c *sdkClient) resolveGPUAMI(ctx context.Context) (string, error) {
	out, err := c.ec2.DescribeImages(ctx, &ec2.DescribeImagesInput{
		Owners: []string{"amazon"},
		Filters: []ec2types.Filter{
			{Name: awssdk.String("name"), Values: []string{gpuAMINameFilter}},
			{Name: awssdk.String("state"), Values: []string{"available"}},
		},
	})
	if err != nil {
		return "", err
	}
	// Pick the newest by CreationDate (RFC3339 strings sort lexicographically in
	// chronological order), so a driver/runtime refresh is picked up automatically.
	var newest ec2types.Image
	for _, img := range out.Images {
		if img.ImageId == nil || img.CreationDate == nil {
			continue
		}
		if newest.CreationDate == nil || *img.CreationDate > *newest.CreationDate {
			newest = img
		}
	}
	if newest.ImageId == nil {
		return "", fmt.Errorf("no GPU AMI (%s) offered: %w", gpuAMINameFilter, ErrConfig)
	}
	return *newest.ImageId, nil
}

// discoverDefaultSubnets lists the default VPC's subnets — one default subnet per
// availability zone (default-for-az=true) — as RunInstance's per-zone capacity
// failover targets. It pages through all matches. An empty result (the region has
// no default VPC) is NOT an error: the caller launches without pinning a subnet,
// giving up zone failover but still functioning. Using only the default subnets
// keeps Nebula from placing instances on networks an operator did not intend.
func (c *sdkClient) discoverDefaultSubnets(ctx context.Context) ([]subnet, error) {
	in := &ec2.DescribeSubnetsInput{
		Filters: []ec2types.Filter{{
			Name:   awssdk.String("default-for-az"),
			Values: []string{"true"},
		}},
	}
	var out []subnet
	for {
		page, err := c.ec2.DescribeSubnets(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, s := range page.Subnets {
			if s.SubnetId == nil {
				continue
			}
			sn := subnet{id: *s.SubnetId}
			if s.AvailabilityZone != nil {
				sn.az = *s.AvailabilityZone
			}
			out = append(out, sn)
		}
		if page.NextToken == nil {
			break
		}
		in.NextToken = page.NextToken
	}
	return out, nil
}

// TerminateInstance implements Client. Idempotent: an already-gone instance
// (InvalidInstanceID.NotFound) returns nil so the finalizer can retry safely.
func (c *sdkClient) TerminateInstance(ctx context.Context, id string) error {
	_, err := c.ec2.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{id},
	})
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

// DescribeInstance implements Client. Returns (nil, nil) when the instance no
// longer exists (absent == terminated per the interface contract).
func (c *sdkClient) DescribeInstance(ctx context.Context, id string) (*EC2Instance, error) {
	out, err := c.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{id},
	})
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	for _, r := range out.Reservations {
		if len(r.Instances) == 0 {
			continue
		}
		inst := c.observe(r.Instances[0])
		// Fold in reachability checks so a single-instance Get reports Running only
		// once the 2/2 checks pass, matching ListInstances. A status probe failure
		// is non-fatal: the instance is still returned (checks stay false).
		if inst.ID != "" {
			if passed, err := c.statusChecksByID(ctx, []string{inst.ID}); err == nil {
				inst.StatusChecksPassed = passed[inst.ID]
			}
		}
		return &inst, nil
	}
	return nil, nil
}

// ListInstances implements Client. It scopes the list server-side to instances
// carrying the ClaimTagKey tag — the tag every Nebula instance is launched with —
// so instances Nebula does not own are never returned, and pages through all
// results. This is the engine of the poll loop.
func (c *sdkClient) ListInstances(ctx context.Context) ([]EC2Instance, error) {
	in := &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{{
			Name:   awssdk.String("tag-key"),
			Values: []string{ClaimTagKey},
		}},
	}
	var out []EC2Instance
	var ids []string
	for {
		page, err := c.ec2.DescribeInstances(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, r := range page.Reservations {
			for i := range r.Instances {
				inst := c.observe(r.Instances[i])
				out = append(out, inst)
				if inst.ID != "" {
					ids = append(ids, inst.ID)
				}
			}
		}
		if page.NextToken == nil {
			break
		}
		in.NextToken = page.NextToken
	}

	// Fold in reachability checks: DescribeInstances does not report them, so a
	// separate DescribeInstanceStatus call decides which running instances have
	// actually passed their 2/2 checks. A failure here is not fatal — the instances
	// are still returned (StatusChecksPassed stays false), so the poll loop simply
	// holds them at Pending one more tick rather than the whole List erroring out.
	passed, err := c.statusChecksByID(ctx, ids)
	if err != nil {
		return out, nil
	}
	for i := range out {
		out[i].StatusChecksPassed = passed[out[i].ID]
	}
	return out, nil
}

// statusChecksByID returns the subset of ids whose system AND instance
// reachability checks both report "ok" (the "2/2 checks passed"). It pages
// through DescribeInstanceStatus; an id absent from the result (or not ok) maps
// to false. Called with no ids it returns an empty map without hitting the API.
func (c *sdkClient) statusChecksByID(ctx context.Context, ids []string) (map[string]bool, error) {
	passed := make(map[string]bool, len(ids))
	if len(ids) == 0 {
		return passed, nil
	}
	in := &ec2.DescribeInstanceStatusInput{InstanceIds: ids}
	for {
		page, err := c.ec2.DescribeInstanceStatus(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, s := range page.InstanceStatuses {
			if s.InstanceId == nil {
				continue
			}
			passed[*s.InstanceId] = statusChecksOK(s)
		}
		if page.NextToken == nil {
			break
		}
		in.NextToken = page.NextToken
	}
	return passed, nil
}

// statusChecksOK reports whether both reachability checks (system and instance)
// are "ok". EC2 exposes them as an overall summary plus per-check details; the
// summary is "ok" only when both underlying checks pass, so it is the single
// field to read.
func statusChecksOK(s ec2types.InstanceStatus) bool {
	return s.SystemStatus != nil && s.SystemStatus.Status == ec2types.SummaryStatusOk &&
		s.InstanceStatus != nil && s.InstanceStatus.Status == ec2types.SummaryStatusOk
}

// AvailableInstanceTypes implements Client via DescribeInstanceTypeOfferings,
// scoped to this client's region (LocationType=region), paging through all
// results. The returned set is keyed by instance type; a type present here is
// offered in the region.
func (c *sdkClient) AvailableInstanceTypes(ctx context.Context) (map[string]bool, error) {
	in := &ec2.DescribeInstanceTypeOfferingsInput{
		LocationType: ec2types.LocationTypeRegion,
		Filters: []ec2types.Filter{{
			Name:   awssdk.String("location"),
			Values: []string{c.region},
		}},
	}
	out := map[string]bool{}
	for {
		page, err := c.ec2.DescribeInstanceTypeOfferings(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, o := range page.InstanceTypeOfferings {
			out[string(o.InstanceType)] = true
		}
		if page.NextToken == nil {
			break
		}
		in.NextToken = page.NextToken
	}
	return out, nil
}

// observe normalizes a live EC2 SDK instance into the adapter-level EC2Instance
// view: id, tags, state name, region (from the placement AZ), a best-effort
// public endpoint, and whether it is Spot. It is a point-in-time read and never
// blocks.
func (c *sdkClient) observe(inst ec2types.Instance) EC2Instance {
	out := EC2Instance{
		Region: c.region,
		Tags:   map[string]string{},
	}
	if inst.InstanceId != nil {
		out.ID = *inst.InstanceId
	}
	if inst.State != nil {
		out.State = string(inst.State.Name)
	}
	for _, t := range inst.Tags {
		if t.Key != nil && t.Value != nil {
			out.Tags[*t.Key] = *t.Value
		}
	}
	// Region is a coarser fact than the AZ; the AZ (e.g. "us-east-1a") is prefixed
	// by the region, but we already know the region this client operates in, so we
	// keep c.region rather than trimming the AZ.
	if inst.PublicDnsName != nil && *inst.PublicDnsName != "" {
		out.PublicEndpoint = *inst.PublicDnsName
	} else if inst.PublicIpAddress != nil {
		out.PublicEndpoint = *inst.PublicIpAddress
	}
	out.Spot = inst.InstanceLifecycle == ec2types.InstanceLifecycleTypeSpot
	return out
}

// ec2Tags converts the adapter's tag map into EC2's tag slice.
func ec2Tags(tags map[string]string) []ec2types.Tag {
	out := make([]ec2types.Tag, 0, len(tags))
	for _, k := range sortedKeys(tags) {
		v := tags[k]
		out = append(out, ec2types.Tag{Key: awssdk.String(k), Value: awssdk.String(v)})
	}
	return out
}

// isNotFound reports whether err is EC2's "instance does not exist" condition, so
// Terminate/Describe can treat it as already gone (idempotency).
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed":
		return true
	default:
		return false
	}
}
