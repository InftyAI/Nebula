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
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

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
	// CreateLaunchTemplate / DeleteLaunchTemplate manage the EPHEMERAL launch
	// template a CreateFleet launch references: RunInstance creates one carrying the
	// per-Pod fields (AMI, user-data, tags) that CreateFleet's overrides cannot
	// express, fleets against it, then deletes it — so no launch template survives
	// the call and the self-configuring model is preserved.
	CreateLaunchTemplate(
		ctx context.Context, in *ec2.CreateLaunchTemplateInput, optFns ...func(*ec2.Options),
	) (*ec2.CreateLaunchTemplateOutput, error)
	DeleteLaunchTemplate(
		ctx context.Context, in *ec2.DeleteLaunchTemplateInput, optFns ...func(*ec2.Options),
	) (*ec2.DeleteLaunchTemplateOutput, error)
	// DescribeLaunchTemplates lists the Nebula-tagged ephemeral templates so
	// RunInstance can sweep any leaked by a crash between create and delete (see
	// sweepStaleLaunchTemplates).
	DescribeLaunchTemplates(
		ctx context.Context, in *ec2.DescribeLaunchTemplatesInput, optFns ...func(*ec2.Options),
	) (*ec2.DescribeLaunchTemplatesOutput, error)
	// CreateFleet launches instances across candidate AZs in ONE server-side call.
	// RunInstance uses an "instant" fleet (synchronous: instances/errors returned in
	// the response) to collapse the former per-AZ RunInstances sweep into a single
	// request — EC2 itself iterates the subnet overrides for capacity.
	CreateFleet(
		ctx context.Context, in *ec2.CreateFleetInput, optFns ...func(*ec2.Options),
	) (*ec2.CreateFleetOutput, error)
}

// sdkClient is the real Client, backed by aws-sdk-go-v2's EC2 API. All
// EC2-specific SDK calls live here so the adapter (aws.go) and its tests stay
// SDK-free; the pure translation (user-data, error mapping) lives in translate.go.
//
// Execution model (see the package doc): a NodeClaim maps to one EC2 instance
// launched via a CreateFleet "instant" request, which lets EC2 do the per-AZ
// capacity search server-side in ONE call. CreateFleet cannot take an inline AMI /
// user-data (its overrides only cover instance type / subnet / price), so the
// launch references a launch template — but an EPHEMERAL one, created for this
// provision and deleted before the call returns. No launch template or other
// pre-created infra survives, so the self-configuring model holds: everything the
// launch needs is derived from credentials alone at construction, and registering
// AWS in a new region requires nothing but access:
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

	// azOfferings caches, per instance type, the AZ-offering lookup for that type in
	// this region (DescribeInstanceTypeOfferings at AZ granularity). RunInstance
	// consults it to prune fleet overrides that target an AZ where the type is not
	// offered at all — e.g. g6.48xlarge in us-east-1e — which EC2 would otherwise
	// reject per-override with InvalidFleetConfiguration, wasting a grid slot on a
	// permanently-dead pair. Offerings are stable per region, so each type is looked
	// up at most once (populated lazily on first use).
	//
	// offeringsMu guards ONLY the map bookkeeping — never a network call. Each entry
	// is single-flight: the first caller for a type inserts an entry with an open
	// `done` channel, releases the lock, runs the one Describe, fills the entry, and
	// closes `done`; concurrent callers for the same type find the entry and wait on
	// `done` without the lock and without issuing a duplicate Describe. So a burst of
	// N pods for a new type costs ONE lookup, and cache hits never block on I/O.
	offeringsMu sync.Mutex
	azOfferings map[string]*offeringEntry
}

// offeringEntry is a single-flight cache slot for one instance type's AZ offerings.
// done is closed once the lookup finishes; readers block on it, then read azs/err
// (which are written exactly once, before done closes, so no lock is needed to read
// them afterwards). azs is nil on a failed lookup (err set) — a fail-open signal
// fleetOverrides reads as "unknown, don't prune". A resolved-but-empty azs means the
// type is offered in no AZ of this region.
type offeringEntry struct {
	done chan struct{}
	azs  map[string]bool
	err  error
}

// subnet is one candidate launch target: its id and the AZ it lives in (for
// logging/ordering). RunInstance launches into one of these per attempt.
type subnet struct {
	id string
	az string
}

// compile-time assertion that sdkClient satisfies the adapter's Client seam.
var _ Client = (*sdkClient)(nil)

// NewSDKClient builds an EC2-backed multi-region Provider, ready to register.
//
// regionSource supplies the regions the List/Offerings fan-out sweeps, sourced from
// the NodePool at call time (ProviderSpec.Regions). It is NOT env/flag config:
// regions are the operator's per-pool declaration and change at runtime, and the
// region a Pod lands in already flows NodePool -> placement -> ProvisionRequest, so
// provisioning never needed a configured set. See RegionSource and Provider.sweepRegions.
//
// Per-region clients are built LAZILY (see Provider.clientFor): this constructor
// only loads the catalog — startup pays no per-region AMI+subnet resolution, no
// region is resolved or validated here (the set is dynamic and comes from the
// NodePool). The first Provision/List into a region resolves that region's GPU AMI
// and default-VPC subnets on demand.
//
// There is NO default region and NO region env (AWS_REGION is not read): every
// request carries its own region (admission requires each aws pool to list ≥1 region;
// placement stamps it), so a fallback would be dead config. This constructor fails
// ONLY if the catalog cannot load — never on region config.
//
// Security contract (and why ONE credential set spans all regions):
//   - NO non-secret config is accepted as an argument; regions come from the NodePool
//     (API objects), not env/flags.
//   - Credentials are SECRETS and are NEVER accepted here. The SDK's default
//     credential chain (config.LoadDefaultConfig, used per-region in the factory) is
//     used, preferring IRSA / instance-role and falling back to AWS_ACCESS_KEY_ID /
//     AWS_SECRET_ACCESS_KEY from the environment. An IAM principal is ACCOUNT-GLOBAL,
//     so the same credentials authorize every region — only the endpoint
//     (config.WithRegion) differs per client. No per-region credential is needed.
//
// The catalog is loaded via catalog.Load() (embedded CSV / mounted ConfigMap),
// identical to the other adapters.
func NewSDKClient(_ context.Context, regionSource RegionSource) (*Provider, error) {
	cat, err := catalog.Load()
	if err != nil {
		return nil, fmt.Errorf("aws: load price catalog: %w", err)
	}

	factory := func(ctx context.Context, region string) (Client, error) {
		return newSDKClientForRegion(ctx, region)
	}
	return New(factory, cat, regionSource), nil
}

// newSDKClientForRegion builds one region-pinned sdkClient: it loads SDK config for
// that region (same account-global credential chain, region endpoint overridden),
// then resolves the region's GPU AMI (required) and default-VPC subnets
// (best-effort). This is the ClientFactory body the Provider calls lazily per region.
func newSDKClientForRegion(ctx context.Context, region string) (Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("aws: load SDK config for %s: %w", region, err)
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("aws: no region resolved for %q: %w", region, ErrConfig)
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

	return c, nil
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

// RunInstance implements Client. It launches exactly one instance via a
// CreateFleet "instant" request, so EC2 does the per-availability-zone capacity
// search server-side in ONE call rather than the adapter issuing a RunInstances
// per zone and sweeping them serially.
//
// EC2 capacity is per-AZ, so the fleet is given one launch-template OVERRIDE per
// discovered default-VPC subnet: EC2 tries them itself and returns whichever it
// lands, collapsing the former N-call zone sweep into a single round-trip that no
// longer burns the provision deadline attempt-by-attempt. When no subnets were
// discovered (no default VPC) the fleet runs with no overrides, letting EC2 pick
// the subnet — a valid launch, just without cross-AZ capacity search.
//
// Because CreateFleet's overrides cannot carry an AMI or user-data, the launch
// references an ephemeral launch template built here (createLaunchTemplate) and
// torn down before returning (best-effort; a stray template is swept by
// name/tag). An instant fleet reports capacity shortfalls in the response's
// Errors (not as a Go error), so a fleet that returns no instance is mapped back
// through classifyEC2Error to the wrapped ErrNoCapacity the region-level failover
// (ClassifyProvisionError + NodePool regions) expects. A non-capacity failure
// (auth, quota, bad config) surfaces the same way and is terminal.
func (c *sdkClient) RunInstance(ctx context.Context, spec InstanceSpec) (string, error) {
	userData, err := buildUserData(spec)
	if err != nil {
		return "", err
	}

	log := logf.FromContext(ctx).WithName("aws-run").WithValues(
		"region", c.region, "instanceTypes", spec.InstanceTypes, "spot", spec.Spot)

	// Opportunistically reap any ephemeral launch template a prior provision leaked
	// (crash between create and delete). Best-effort and staleness-gated, so it
	// cannot touch a concurrent provision's own template; see sweepStaleLaunchTemplates.
	c.sweepStaleLaunchTemplates(ctx)

	ltName, err := c.createLaunchTemplate(ctx, spec, userData)
	if err != nil {
		return "", err // classifyEC2Error not needed: template creation is not a capacity path
	}
	// Best-effort teardown: no launch template must survive the provision. A delete
	// failure only leaves a cheap, uniquely-named artifact behind, so it is logged,
	// not surfaced — failing the provision over a dangling template would be worse.
	defer func() {
		if _, derr := c.ec2.DeleteLaunchTemplate(ctx, &ec2.DeleteLaunchTemplateInput{
			LaunchTemplateName: awssdk.String(ltName),
		}); derr != nil {
			log.Error(derr, "deleting ephemeral launch template; it will need sweeping", "launchTemplate", ltName)
		}
	}()

	// Prune subnets whose AZ does not OFFER a requested type before building the grid,
	// so we never emit a (type, subnet) override EC2 would reject outright with
	// InvalidFleetConfiguration (e.g. g6.48xlarge in us-east-1e). Fail-open: a lookup
	// error yields nil offerings and offeredAZs keeps every subnet, so a transient
	// DescribeInstanceTypeOfferings failure can never shrink the grid below today's.
	offerings := c.instanceTypeAZs(ctx, spec.InstanceTypes)

	// One override per (instance type, subnet) pair => EC2 searches that whole grid
	// for capacity in a single call, landing on whichever pair is available. The
	// instance types are interchangeable alternates the accelerator maps to (primary
	// first); spanning them broadens the launch beyond a single type's per-AZ
	// capacity. The launch template carries no instance type — it lives here in the
	// overrides — so a template built once serves every type. No subnets (no default
	// VPC) => one override per type with the subnet unset, letting EC2 pick it (still
	// no cross-AZ search).
	overrides := fleetOverrides(spec.InstanceTypes, c.subnets, offerings)
	log.V(1).Info("launching via instant fleet",
		"candidateInstanceTypes", len(spec.InstanceTypes), "candidateSubnets", len(c.subnets),
		"overrides", len(overrides))

	out, err := c.ec2.CreateFleet(ctx, &ec2.CreateFleetInput{
		Type: ec2types.FleetTypeInstant, // synchronous: instances/errors in the response
		TargetCapacitySpecification: &ec2types.TargetCapacitySpecificationRequest{
			TotalTargetCapacity:       awssdk.Int32(1),
			DefaultTargetCapacityType: fleetCapacityType(spec.Spot),
		},
		LaunchTemplateConfigs: []ec2types.FleetLaunchTemplateConfigRequest{{
			LaunchTemplateSpecification: &ec2types.FleetLaunchTemplateSpecificationRequest{
				LaunchTemplateName: awssdk.String(ltName),
				Version:            awssdk.String("$Latest"),
			},
			Overrides: overrides,
		}},
	})
	if err != nil {
		// A top-level error is the whole request failing (throttle, malformed, auth
		// on the fleet API itself); classify so a capacity-shaped error still fails
		// over correctly.
		return "", classifyEC2Error(err, spec.Spot)
	}

	if id := firstFleetInstanceID(out); id != "" {
		log.Info("instant fleet launched instance", "instanceID", id)
		return id, nil
	}

	// No instance: an instant fleet reports why in out.Errors. Map the reported
	// error code back through the same classifier so a capacity shortfall carries
	// ErrNoCapacity (and, for Spot, ErrSpotCapacity) exactly as the old per-zone
	// sweep did, letting region/tier failover proceed.
	err = fleetError(out, spec.Spot)
	log.Info("instant fleet launched no instance", "error", err.Error())
	return "", err
}

// staleLaunchTemplateAge bounds how old an ephemeral launch template must be
// before sweepStaleLaunchTemplates deletes it. It is far longer than any in-flight
// provision could take (the provision deadline is ProvisionTimeout, a couple of
// minutes), so the sweep can never race a live provision's own template: anything
// older than this was provably orphaned by a crash between create and delete.
const staleLaunchTemplateAge = 30 * time.Minute

// sweepStaleLaunchTemplates deletes Nebula-tagged ephemeral launch templates older
// than staleLaunchTemplateAge. RunInstance's defer already deletes the template it
// created in the happy path; this reaps the rare leak — a crash (or a persistent
// DeleteLaunchTemplate failure) between create and delete — that the defer cannot.
//
// It runs opportunistically on the (infrequent) provision path rather than the
// per-tick poll loop, so a leak is bounded to "cleaned up by the next provision in
// this region" without adding a DescribeLaunchTemplates to every poll. It is
// entirely best-effort: any error is logged and swallowed so a sweep hiccup never
// fails the provision it is piggybacking on. The age gate is what keeps it safe —
// it never touches a template young enough to belong to a concurrent provision.
func (c *sdkClient) sweepStaleLaunchTemplates(ctx context.Context) {
	log := logf.FromContext(ctx).WithName("aws-lt-sweep").WithValues("region", c.region)

	in := &ec2.DescribeLaunchTemplatesInput{
		Filters: []ec2types.Filter{{
			Name:   awssdk.String("tag-key"),
			Values: []string{ClaimTagKey},
		}},
	}
	cutoff := time.Now().Add(-staleLaunchTemplateAge)
	for {
		page, err := c.ec2.DescribeLaunchTemplates(ctx, in)
		if err != nil {
			log.V(1).Info("stale launch-template sweep skipped; describe failed", "error", err.Error())
			return
		}
		for _, lt := range page.LaunchTemplates {
			if lt.LaunchTemplateName == nil || lt.CreateTime == nil {
				continue
			}
			if lt.CreateTime.After(cutoff) {
				continue // young enough to belong to an in-flight provision; leave it
			}
			if _, err := c.ec2.DeleteLaunchTemplate(ctx, &ec2.DeleteLaunchTemplateInput{
				LaunchTemplateName: lt.LaunchTemplateName,
			}); err != nil {
				log.V(1).Info("failed to delete a stale launch template; will retry next sweep",
					"launchTemplate", *lt.LaunchTemplateName, "error", err.Error())
				continue
			}
			log.Info("swept a leaked ephemeral launch template", "launchTemplate", *lt.LaunchTemplateName)
		}
		if page.NextToken == nil {
			break
		}
		in.NextToken = page.NextToken
	}
}

// createLaunchTemplate creates the ephemeral launch template a CreateFleet launch
// references, carrying the per-Pod fields the fleet's overrides cannot express
// (AMI, user-data, identity tags), and returns its name. The
// name is derived from the claim so a retried provision reuses a stable name; a
// leftover template from a crashed prior attempt collides on create, which is
// treated as reuse rather than an error.
//
// SecurityGroupIds is deliberately unset: launching into a default-VPC subnet
// attaches that VPC's default SG (outbound-open, no inbound exposure), which is
// all a fire-and-forget container launch needs. The subnet AND the instance type
// are chosen by the fleet overrides, not the template — one template serves every
// candidate instance type — so both are intentionally absent here.
func (c *sdkClient) createLaunchTemplate(ctx context.Context, spec InstanceSpec, userData string) (string, error) {
	name := launchTemplateName(spec.Tags[ClaimTagKey])

	// InstanceMarketOptions is deliberately NOT set here. With EC2 Fleet the Spot
	// vs OnDemand market is selected by the fleet's DefaultTargetCapacityType (see
	// RunInstance), and specifying it BOTH in the launch template and on the fleet
	// is rejected with InvalidFleetConfiguration. Leaving it out also keeps the
	// template tier-agnostic, so the stable per-claim template is safely reused when
	// failover retries the same claim under the other tier.
	ltData := &ec2types.RequestLaunchTemplateData{
		ImageId:  awssdk.String(c.amiID),
		UserData: awssdk.String(userData),
		TagSpecifications: []ec2types.LaunchTemplateTagSpecificationRequest{{
			ResourceType: ec2types.ResourceTypeInstance,
			Tags:         ec2Tags(spec.Tags),
		}},
	}

	_, err := c.ec2.CreateLaunchTemplate(ctx, &ec2.CreateLaunchTemplateInput{
		LaunchTemplateName: awssdk.String(name),
		LaunchTemplateData: ltData,
		// Tag the template itself so a leaked one (delete failed / process crashed
		// between create and delete) is identifiable for an out-of-band sweep.
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeLaunchTemplate,
			Tags:         ec2Tags(spec.Tags),
		}},
	})
	if err != nil {
		// A stable name means a crashed prior attempt may have left the template;
		// AlreadyExists is reuse, not a failure — the fleet references it by name.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidLaunchTemplateName.AlreadyExistsException" {
			return name, nil
		}
		return "", err
	}
	return name, nil
}

// launchTemplateName derives the ephemeral template's name from the claim so it is
// stable across a retried provision (create then collides => reuse) and greppable
// for an out-of-band sweep of leaked templates.
func launchTemplateName(claim string) string {
	return "nebula-" + claim
}

// fleetOverrides builds the CreateFleet launch-template overrides: one per
// (instance type, subnet) pair so EC2 searches that whole grid for capacity in a
// single call and lands on whichever pair is available. The instance types are the
// interchangeable alternates the accelerator resolves to (primary first); spanning
// them widens the launch past any single type's per-AZ capacity. When no subnets
// were discovered (no default VPC) it emits one override per instance type with the
// subnet unset, so EC2 still tries every type — it just cannot search across AZs.
//
// offerings maps a type to the set of AZs that OFFER it (nil = unknown, don't prune;
// see instanceTypeAZs): a (type, subnet) pair whose AZ is not in the type's offered
// set is dropped so a permanently-unavailable pair (e.g. g6.48xlarge in us-east-1e)
// never becomes an override EC2 would reject with InvalidFleetConfiguration. Pruning
// is skipped for a type whose offered set is unknown (fail-open) OR when it would
// leave the type with no subnet at all — in that last case the type keeps its full
// subnet list so a stale/empty offerings map can never zero out the launch.
func fleetOverrides(
	instanceTypes []string, subnets []subnet, offerings map[string]map[string]bool,
) []ec2types.FleetLaunchTemplateOverridesRequest {
	var overrides []ec2types.FleetLaunchTemplateOverridesRequest
	for _, it := range instanceTypes {
		if len(subnets) == 0 {
			overrides = append(overrides, ec2types.FleetLaunchTemplateOverridesRequest{
				InstanceType: ec2types.InstanceType(it),
			})
			continue
		}
		usable := offeredSubnets(it, subnets, offerings)
		for _, sn := range usable {
			overrides = append(overrides, ec2types.FleetLaunchTemplateOverridesRequest{
				InstanceType: ec2types.InstanceType(it),
				SubnetId:     awssdk.String(sn.id),
			})
		}
	}
	return overrides
}

// offeredSubnets filters subnets to those in an AZ that offers it, per offerings.
// It fails open: an unknown offered set (nil — lookup skipped or failed) keeps every
// subnet, and a filter that would remove ALL subnets also keeps every subnet, so
// pruning only ever narrows a launch that still has at least one usable AZ left.
func offeredSubnets(it string, subnets []subnet, offerings map[string]map[string]bool) []subnet {
	azs := offerings[it]
	if azs == nil {
		return subnets // offered set unknown: do not prune
	}
	kept := make([]subnet, 0, len(subnets))
	for _, sn := range subnets {
		if azs[sn.az] {
			kept = append(kept, sn)
		}
	}
	if len(kept) == 0 {
		return subnets // pruning would zero the launch: keep all, let EC2 decide
	}
	return kept
}

// fleetCapacityType maps the Spot flag to the fleet's default target-capacity
// type. CreateFleet takes the tier here rather than a per-instance market option.
func fleetCapacityType(spot bool) ec2types.DefaultTargetCapacityType {
	if spot {
		return ec2types.DefaultTargetCapacityTypeSpot
	}
	return ec2types.DefaultTargetCapacityTypeOnDemand
}

// firstFleetInstanceID digs the launched instance id out of an instant fleet's
// response, or "" when the fleet placed nothing (every candidate was starved /
// rejected — the reason is in out.Errors, read by fleetError).
func firstFleetInstanceID(out *ec2.CreateFleetOutput) string {
	if out == nil {
		return ""
	}
	for _, inst := range out.Instances {
		for _, id := range inst.InstanceIds {
			return id
		}
	}
	return ""
}

// fleetError turns an instant fleet that launched nothing into the classified
// error region/tier failover expects. An instant fleet does not fail the API call
// on a capacity shortfall — it succeeds and reports a PER-OVERRIDE reason for every
// (instance type, subnet) pair it could not place in out.Errors — so the reported
// codes are run back through classifyEC2Error to recover ErrNoCapacity /
// ErrSpotCapacity / ErrQuota / ErrAuth. An empty Errors list (no instance and no
// reason) is treated as a generic no-capacity so the caller still fails over rather
// than wedging.
//
// The grid's errors are usually a MIX — one subnet's AZ may not offer the type
// (InvalidFleetConfiguration) while the rest are genuinely capacity-starved
// (InsufficientInstanceCapacity), and a terminal reason (auth/quota) can hide
// behind them. Picking out.Errors[0] blindly would surface whichever AZ EC2 listed
// first, so the most AUTHORITATIVE error is chosen instead (auth > quota > capacity
// > unknown): a real terminal reason is never masked by a per-AZ availability gap,
// and its true message is preserved for the log/Event.
func fleetError(out *ec2.CreateFleetOutput, spot bool) error {
	if out != nil {
		var best *ec2types.CreateFleetError
		bestRank := -1
		for i := range out.Errors {
			e := &out.Errors[i]
			if e.ErrorCode == nil {
				continue
			}
			if r := fleetErrorRank(*e.ErrorCode); r > bestRank {
				bestRank, best = r, e
			}
		}
		if best != nil {
			msg := ""
			if best.ErrorMessage != nil {
				msg = *best.ErrorMessage
			}
			return classifyEC2Error(apiErrorFromFleet(*best.ErrorCode, msg), spot)
		}
	}
	// No reason reported: assume capacity so the region is blocked and failover runs.
	return wrapNoCapacity(errors.New("aws: instant fleet returned no instance"), spot, false)
}

// fleetErrorRank orders CreateFleet per-override error codes by how authoritative
// the reason is, so fleetError surfaces the one that best explains the whole
// launch: auth (fails everywhere) > quota (region/account ceiling) > capacity
// (per-AZ shortfall, incl. an AZ that doesn't offer the type) > unknown. A higher
// rank wins. This never changes the derived BlockScope for a uniform grid; it only
// picks the representative error when the grid's reasons differ.
func fleetErrorRank(code string) int {
	switch code {
	case "UnauthorizedOperation", "AuthFailure", "Blocked", "OptInRequired", "PendingVerification":
		return 3
	case "InstanceLimitExceeded", "VcpuLimitExceeded", "RequestLimitExceeded",
		"SpotMaxPriceTooLow", "MaxSpotInstanceCountExceeded":
		return 2
	case "InsufficientInstanceCapacity", "InsufficientHostCapacity", "Unsupported", "InvalidFleetConfiguration":
		return 1
	default:
		return 0
	}
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
//
// The instance-state filter excludes terminated/shutting-down instances, which EC2
// keeps visible in DescribeInstances for ~1h after teardown. Without it a torn-down
// instance would still be counted as "live" every tick (inflating the poll count)
// and — worse, since claim names are reused across pod restarts — findByClaim could
// match a corpse and mis-report a freshly re-created Pod off a dead instance. Only
// states that represent a live (or resumable) instance are returned.
func (c *sdkClient) ListInstances(ctx context.Context) ([]EC2Instance, error) {
	in := &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{
				Name:   awssdk.String("tag-key"),
				Values: []string{ClaimTagKey},
			},
			{
				Name:   awssdk.String("instance-state-name"),
				Values: []string{"pending", "running", "stopping", "stopped"},
			},
		},
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
	//
	// It IS logged (not silently swallowed): a PERSISTENT failure here — most often a
	// missing ec2:DescribeInstanceStatus IAM permission, distinct from the
	// DescribeInstances grant this same List already relied on — pins every running
	// instance at StatusChecksPassed=false forever, so its Pod is stranded at
	// Pending/Provisioning even though the instance is healthy. Without this log that
	// failure is invisible (List still returns the instances), so surface it.
	passed, err := c.statusChecksByID(ctx, ids)
	if err != nil {
		logf.FromContext(ctx).WithName("aws-list").Error(err,
			"describe instance status checks failed; instances held at Pending until this recovers "+
				"(often a missing ec2:DescribeInstanceStatus permission)",
			"region", c.region, "instances", len(ids))
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

// instanceTypeAZs returns, for each of the requested types, the set of AZs in this
// region that offer it — used by RunInstance to prune fleet overrides targeting an
// AZ that does not offer the type. Offerings are stable per region, so a resolved
// type is cached on the client (azOfferings) and never re-queried.
//
// It is fail-open by construction: a type whose DescribeInstanceTypeOfferings lookup
// fails is left OUT of the returned map (its entry stays nil), which fleetOverrides
// reads as "unknown, don't prune". A transient API error therefore degrades to
// today's behavior (full grid), never to a shrunk or empty launch. Each miss is one
// AZ-scoped Describe (filtered to the single type), cached thereafter.
func (c *sdkClient) instanceTypeAZs(ctx context.Context, instanceTypes []string) map[string]map[string]bool {
	log := logf.FromContext(ctx).WithName("aws-offerings").WithValues("region", c.region)

	out := map[string]map[string]bool{}
	for _, it := range instanceTypes {
		e, mine := c.offeringSlot(it)
		if mine {
			// This goroutine owns the single-flight lookup for it: run the one Describe
			// OUTSIDE the lock, fill the entry, and release everyone waiting on done.
			e.azs, e.err = c.describeTypeAZs(ctx, it)
			if e.err != nil {
				// Do NOT cache a failure: drop the slot so a later provision retries
				// rather than fail-open forever on a transient throttle.
				c.offeringsMu.Lock()
				delete(c.azOfferings, it)
				c.offeringsMu.Unlock()
			}
			close(e.done)
		} else {
			<-e.done // another goroutine is (or has) resolved it; wait without the lock.
		}

		if e.err != nil {
			// Fail-open: leave this type unresolved so fleetOverrides does not prune it.
			log.V(1).Info("offering lookup failed; not pruning this type", "instanceType", it, "error", e.err.Error())
			continue
		}
		out[it] = e.azs // resolved set (may be empty: offered in no AZ of this region)
	}
	return out
}

// offeringSlot returns the single-flight cache slot for an instance type, creating
// it if absent. The bool is true for the goroutine that CREATED the slot — the one
// responsible for running the lookup and closing e.done; all others receive the
// existing slot (mine=false) and must wait on e.done. Holds offeringsMu only for the
// map read/insert, never across the network call.
func (c *sdkClient) offeringSlot(it string) (*offeringEntry, bool) {
	c.offeringsMu.Lock()
	defer c.offeringsMu.Unlock()
	if c.azOfferings == nil {
		c.azOfferings = map[string]*offeringEntry{}
	}
	if e, ok := c.azOfferings[it]; ok {
		return e, false
	}
	e := &offeringEntry{done: make(chan struct{})}
	c.azOfferings[it] = e
	return e, true
}

// describeTypeAZs lists the AZs in this region that offer one instance type, via
// DescribeInstanceTypeOfferings at AZ granularity (LocationType=availability-zone),
// paging through all results. An empty result means the type is offered in no AZ of
// this region.
func (c *sdkClient) describeTypeAZs(ctx context.Context, instanceType string) (map[string]bool, error) {
	in := &ec2.DescribeInstanceTypeOfferingsInput{
		LocationType: ec2types.LocationTypeAvailabilityZone,
		Filters: []ec2types.Filter{{
			Name:   awssdk.String("instance-type"),
			Values: []string{instanceType},
		}},
	}
	azs := map[string]bool{}
	for {
		page, err := c.ec2.DescribeInstanceTypeOfferings(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, o := range page.InstanceTypeOfferings {
			if o.Location != nil {
				azs[*o.Location] = true
			}
		}
		if page.NextToken == nil {
			break
		}
		in.NextToken = page.NextToken
	}
	return azs, nil
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
