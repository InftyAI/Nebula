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
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// fakeEC2 is an in-memory ec2API for testing the sdkClient translation without a
// live AWS account. It records the last RunInstances input and lets tests seed
// describe results and inject errors.
type fakeEC2 struct {
	lastRun       *ec2.RunInstancesInput
	runOut        *ec2.RunInstancesOutput
	runErr        error
	runCalls      int
	terminated    []string
	terminateErr  error
	describePages []*ec2.DescribeInstancesOutput
	describeErr   error
	describeIdx   int
	lastDescribe  *ec2.DescribeInstancesInput

	// statusPages seeds DescribeInstanceStatus (reachability checks); statusErr
	// injects a failure. Left nil, the fake reports no status rows — every instance
	// then reads as checks-not-passed, matching a freshly launched instance.
	statusPages []*ec2.DescribeInstanceStatusOutput
	statusErr   error
	statusIdx   int
	// lastStatusIn records the last DescribeInstanceStatus input so a test can
	// assert which instance ids were queried.
	lastStatusIn *ec2.DescribeInstanceStatusInput

	offeringPages []*ec2.DescribeInstanceTypeOfferingsOutput
	offeringErr   error
	offeringIdx   int
	// offeringMu guards the offering* fields so a concurrency test can drive the
	// single-flight lookup from many goroutines without racing the fake's counters.
	offeringMu sync.Mutex
	// offeringCalls counts DescribeInstanceTypeOfferings invocations (distinct calls,
	// not pages) so a test can assert single-flight coalescing.
	offeringCalls int
	// lastOfferingIn records the last DescribeInstanceTypeOfferings input so a test
	// can assert the region filter was applied.
	lastOfferingIn *ec2.DescribeInstanceTypeOfferingsInput

	imagesOut  *ec2.DescribeImagesOutput
	imagesErr  error
	subnetsOut *ec2.DescribeSubnetsOutput
	subnetsErr error

	// fleet path. lastFleet records the last CreateFleet input; fleetOut/fleetErr
	// seed its result. createdLTs/deletedLTs record ephemeral launch-template names
	// so a test can assert one is created and torn down; ltCreateErr injects a
	// CreateLaunchTemplate failure.
	lastFleet   *ec2.CreateFleetInput
	fleetOut    *ec2.CreateFleetOutput
	fleetErr    error
	fleetCalls  int
	createdLTs  []string
	lastLTData  ec2types.RequestLaunchTemplateData // last CreateLaunchTemplate data
	deletedLTs  []string
	ltCreateErr error

	// describeLTsOut/Err seed DescribeLaunchTemplates for the stale-template sweep.
	describeLTsOut *ec2.DescribeLaunchTemplatesOutput
	describeLTsErr error
}

func (f *fakeEC2) DescribeImages(
	_ context.Context, _ *ec2.DescribeImagesInput, _ ...func(*ec2.Options),
) (*ec2.DescribeImagesOutput, error) {
	if f.imagesErr != nil {
		return nil, f.imagesErr
	}
	if f.imagesOut != nil {
		return f.imagesOut, nil
	}
	return &ec2.DescribeImagesOutput{}, nil
}

func (f *fakeEC2) DescribeSubnets(
	_ context.Context, _ *ec2.DescribeSubnetsInput, _ ...func(*ec2.Options),
) (*ec2.DescribeSubnetsOutput, error) {
	if f.subnetsErr != nil {
		return nil, f.subnetsErr
	}
	if f.subnetsOut != nil {
		return f.subnetsOut, nil
	}
	return &ec2.DescribeSubnetsOutput{}, nil
}

func (f *fakeEC2) RunInstances(
	_ context.Context, in *ec2.RunInstancesInput, _ ...func(*ec2.Options),
) (*ec2.RunInstancesOutput, error) {
	f.lastRun = in
	f.runCalls++
	if f.runErr != nil {
		return nil, f.runErr
	}
	return f.runOut, nil
}

func (f *fakeEC2) CreateLaunchTemplate(
	_ context.Context, in *ec2.CreateLaunchTemplateInput, _ ...func(*ec2.Options),
) (*ec2.CreateLaunchTemplateOutput, error) {
	if f.ltCreateErr != nil {
		return nil, f.ltCreateErr
	}
	if in.LaunchTemplateName != nil {
		f.createdLTs = append(f.createdLTs, *in.LaunchTemplateName)
	}
	if in.LaunchTemplateData != nil {
		f.lastLTData = *in.LaunchTemplateData
	}
	return &ec2.CreateLaunchTemplateOutput{}, nil
}

func (f *fakeEC2) DeleteLaunchTemplate(
	_ context.Context, in *ec2.DeleteLaunchTemplateInput, _ ...func(*ec2.Options),
) (*ec2.DeleteLaunchTemplateOutput, error) {
	if in.LaunchTemplateName != nil {
		f.deletedLTs = append(f.deletedLTs, *in.LaunchTemplateName)
	}
	return &ec2.DeleteLaunchTemplateOutput{}, nil
}

func (f *fakeEC2) CreateFleet(
	_ context.Context, in *ec2.CreateFleetInput, _ ...func(*ec2.Options),
) (*ec2.CreateFleetOutput, error) {
	f.lastFleet = in
	f.fleetCalls++
	if f.fleetErr != nil {
		return nil, f.fleetErr
	}
	return f.fleetOut, nil
}

func (f *fakeEC2) DescribeLaunchTemplates(
	_ context.Context, _ *ec2.DescribeLaunchTemplatesInput, _ ...func(*ec2.Options),
) (*ec2.DescribeLaunchTemplatesOutput, error) {
	if f.describeLTsErr != nil {
		return nil, f.describeLTsErr
	}
	if f.describeLTsOut != nil {
		return f.describeLTsOut, nil
	}
	return &ec2.DescribeLaunchTemplatesOutput{}, nil
}

func (f *fakeEC2) TerminateInstances(
	_ context.Context, in *ec2.TerminateInstancesInput, _ ...func(*ec2.Options),
) (*ec2.TerminateInstancesOutput, error) {
	if f.terminateErr != nil {
		return nil, f.terminateErr
	}
	f.terminated = append(f.terminated, in.InstanceIds...)
	return &ec2.TerminateInstancesOutput{}, nil
}

func (f *fakeEC2) DescribeInstances(
	_ context.Context, in *ec2.DescribeInstancesInput, _ ...func(*ec2.Options),
) (*ec2.DescribeInstancesOutput, error) {
	f.lastDescribe = in
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	if f.describeIdx >= len(f.describePages) {
		return &ec2.DescribeInstancesOutput{}, nil
	}
	page := f.describePages[f.describeIdx]
	f.describeIdx++
	return page, nil
}

func (f *fakeEC2) DescribeInstanceStatus(
	_ context.Context, in *ec2.DescribeInstanceStatusInput, _ ...func(*ec2.Options),
) (*ec2.DescribeInstanceStatusOutput, error) {
	f.lastStatusIn = in
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	if f.statusIdx >= len(f.statusPages) {
		return &ec2.DescribeInstanceStatusOutput{}, nil
	}
	page := f.statusPages[f.statusIdx]
	f.statusIdx++
	return page, nil
}

func (f *fakeEC2) DescribeInstanceTypeOfferings(
	_ context.Context, in *ec2.DescribeInstanceTypeOfferingsInput, _ ...func(*ec2.Options),
) (*ec2.DescribeInstanceTypeOfferingsOutput, error) {
	f.offeringMu.Lock()
	defer f.offeringMu.Unlock()
	f.lastOfferingIn = in
	f.offeringCalls++
	if f.offeringErr != nil {
		return nil, f.offeringErr
	}
	if f.offeringIdx >= len(f.offeringPages) {
		return &ec2.DescribeInstanceTypeOfferingsOutput{}, nil
	}
	page := f.offeringPages[f.offeringIdx]
	f.offeringIdx++
	return page, nil
}

func apiErr(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: code}
}

func newSDKClient(f *fakeEC2) *sdkClient {
	return &sdkClient{ec2: f, region: testRegion, amiID: "ami-123"}
}

func TestBuildUserData(t *testing.T) {
	spec := InstanceSpec{
		Image:   "myrepo/train:v1",
		Command: []string{"python", "train.py"},
		Args:    []string{"--epochs", "3"},
		Env:     map[string]string{"B": "2", "A": "1"},
	}
	encoded, err := buildUserData(spec)
	if err != nil {
		t.Fatalf("buildUserData: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("user-data is not valid base64: %v", err)
	}
	script := string(raw)

	if !strings.Contains(script, "docker pull 'myrepo/train:v1'") {
		t.Fatalf("script missing image pull:\n%s", script)
	}
	if !strings.Contains(script, "--gpus all") {
		t.Fatalf("script missing GPU attach:\n%s", script)
	}
	// Env is rendered in sorted key order for determinism (A before B).
	if idxA, idxB := strings.Index(script, "'A=1'"), strings.Index(script, "'B=2'"); idxA < 0 || idxB < 0 || idxA > idxB {
		t.Fatalf("env not rendered in sorted order:\n%s", script)
	}
	// Kubernetes semantics: Command[0] overrides the image ENTRYPOINT (--entrypoint),
	// Command[1:] are its leading args, and Args follow as CMD arguments — the whole
	// tail rendered after the image, in that order.
	if !strings.Contains(script, "--entrypoint 'python'") {
		t.Fatalf("command[0] not mapped to --entrypoint:\n%s", script)
	}
	if !strings.Contains(script, "'myrepo/train:v1' 'train.py' '--epochs' '3'") {
		t.Fatalf("command tail/args not rendered after the image in order:\n%s", script)
	}
}

// TestBuildUserData_ArgsWithoutCommand covers a Pod that sets args but not command:
// the image's own ENTRYPOINT must run (no --entrypoint emitted) with args appended
// as CMD, exactly as a kubelet would launch it.
func TestBuildUserData_ArgsWithoutCommand(t *testing.T) {
	spec := InstanceSpec{
		Image: "srv:v2",
		Args:  []string{"--port", "8080"},
	}
	encoded, err := buildUserData(spec)
	if err != nil {
		t.Fatalf("buildUserData: %v", err)
	}
	raw, _ := base64.StdEncoding.DecodeString(encoded)
	script := string(raw)

	if strings.Contains(script, "--entrypoint") {
		t.Fatalf("no command was set; --entrypoint must not be emitted:\n%s", script)
	}
	if !strings.Contains(script, "'srv:v2' '--port' '8080'") {
		t.Fatalf("args not appended after the image as CMD:\n%s", script)
	}
}

// TestBuildUserData_NoCommandOrArgs covers a bare image: the image's own
// ENTRYPOINT/CMD run, so the run line is just the image with no trailing tokens.
func TestBuildUserData_NoCommandOrArgs(t *testing.T) {
	encoded, err := buildUserData(InstanceSpec{Image: "bare:v1"})
	if err != nil {
		t.Fatalf("buildUserData: %v", err)
	}
	raw, _ := base64.StdEncoding.DecodeString(encoded)
	script := string(raw)

	if strings.Contains(script, "--entrypoint") {
		t.Fatalf("no command was set; --entrypoint must not be emitted:\n%s", script)
	}
	if !strings.Contains(script, "--gpus all 'bare:v1'\n") {
		t.Fatalf("bare image must run with no trailing command/args:\n%s", script)
	}
}

func TestBuildUserData_EmptyImageErrors(t *testing.T) {
	if _, err := buildUserData(InstanceSpec{}); err == nil {
		t.Fatal("expected an error for an empty image")
	}
}

func TestBuildUserData_QuotesHostileValues(t *testing.T) {
	// A value with a single quote must not break out of its shell argument.
	spec := InstanceSpec{
		Image: "img",
		Env:   map[string]string{"X": "a'; rm -rf /; echo '"},
	}
	encoded, err := buildUserData(spec)
	if err != nil {
		t.Fatalf("buildUserData: %v", err)
	}
	raw, _ := base64.StdEncoding.DecodeString(encoded)
	script := string(raw)
	// The dangerous substring must appear only inside a quoted argument, never as
	// a bare `rm -rf /` command line.
	if strings.Contains(script, "\nrm -rf /") {
		t.Fatalf("hostile env value escaped quoting:\n%s", script)
	}
}

// TestBuildUserData_PlainBootstrap pins the ENTIRE rendered script for the simplest
// spec, byte for byte. The sibling tests above assert individual fragments are
// present; this one asserts nothing ELSE is, which no amount of Contains checks can
// establish. Earlier bootstraps prepended a setup preamble (fetching an agent binary,
// writing a credentials file) before the docker lines, and a fragment-only suite
// stayed green through all of it.
//
// That is worth pinning exactly because of where this string ends up: it is cloud-init
// on a paid GPU instance with no kubelet, so anything that appears here runs
// unsupervised and is visible only through the serial console. An intentional change
// should update this expectation in the same commit — that edit is the review prompt.
func TestBuildUserData_PlainBootstrap(t *testing.T) {
	encoded, err := buildUserData(InstanceSpec{Image: "img"})
	if err != nil {
		t.Fatalf("buildUserData: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("user-data is not valid base64: %v", err)
	}

	const want = "#!/bin/bash\n" +
		"set -euo pipefail\n" +
		"docker pull 'img'\n" +
		"docker run --rm --gpus all 'img'\n"
	if got := string(raw); got != want {
		t.Fatalf("rendered bootstrap changed:\n got: %q\nwant: %q", got, want)
	}
}

func TestClassifyEC2Error(t *testing.T) {
	tests := []struct {
		name      string
		code      string
		spot      bool
		wantNoCap bool
		wantSpot  bool
		wantQuota bool
		wantAuth  bool
		wantZone  bool
	}{
		{"capacity ondemand is zone-local", "InsufficientInstanceCapacity", false, true, false, false, false, true},
		{"capacity spot is zone-local", "InsufficientInstanceCapacity", true, true, true, false, false, true},
		{"host capacity is zone-local", "InsufficientHostCapacity", false, true, false, false, false, true},
		{"type unsupported in AZ is zone-local", "Unsupported", false, true, false, false, false, true},
		// Region/account-scoped Spot limits: capacity + Spot, but NOT zone-local, so
		// the adapter's per-zone sweep must stop rather than iterate every AZ. Spot
		// applies even when the caller flagged the request OnDemand — these codes only
		// arise on Spot requests.
		{"spot price too low is region-scoped", "SpotMaxPriceTooLow", true, true, true, false, false, false},
		{"max spot count is region-scoped", "MaxSpotInstanceCountExceeded", true, true, true, false, false, false},
		{"instance limit is quota", "InstanceLimitExceeded", false, false, false, true, false, false},
		{"unauthorized is auth", "UnauthorizedOperation", false, false, false, false, true, false},
		{"unknown code passes through", "SomethingElse", false, false, false, false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyEC2Error(apiErr(tt.code), tt.spot)
			if errors.Is(got, provider.ErrNoCapacity) != tt.wantNoCap {
				t.Errorf("ErrNoCapacity: got %v, want %v (%v)", errors.Is(got, provider.ErrNoCapacity), tt.wantNoCap, got)
			}
			if errors.Is(got, ErrSpotCapacity) != tt.wantSpot {
				t.Errorf("ErrSpotCapacity: got %v, want %v", errors.Is(got, ErrSpotCapacity), tt.wantSpot)
			}
			if errors.Is(got, provider.ErrQuota) != tt.wantQuota {
				t.Errorf("ErrQuota: got %v, want %v", errors.Is(got, provider.ErrQuota), tt.wantQuota)
			}
			if errors.Is(got, provider.ErrAuth) != tt.wantAuth {
				t.Errorf("ErrAuth: got %v, want %v", errors.Is(got, provider.ErrAuth), tt.wantAuth)
			}
			if errors.Is(got, errZoneLocal) != tt.wantZone {
				t.Errorf("errZoneLocal: got %v, want %v", errors.Is(got, errZoneLocal), tt.wantZone)
			}
		})
	}

	if classifyEC2Error(nil, false) != nil {
		t.Fatal("classifyEC2Error(nil) must be nil")
	}
	// A non-API error is returned unchanged so string heuristics can still apply.
	plain := errors.New("dial tcp: timeout")
	if got := classifyEC2Error(plain, false); got != plain {
		t.Fatalf("plain error should pass through unchanged, got %v", got)
	}
}

// fleetWith builds an instant-fleet output that launched a single instance id.
func fleetWith(id string) *ec2.CreateFleetOutput {
	return &ec2.CreateFleetOutput{
		Instances: []ec2types.CreateFleetInstance{{InstanceIds: []string{id}}},
	}
}

// fleetNoCapacity builds an instant-fleet output that launched NOTHING and reports
// the reason in Errors — how CreateFleet(instant) surfaces a capacity shortfall
// (the API call itself succeeds).
func fleetNoCapacity(code string) *ec2.CreateFleetOutput {
	return &ec2.CreateFleetOutput{
		Errors: []ec2types.CreateFleetError{{ErrorCode: awssdk.String(code)}},
	}
}

// offeringPage builds a DescribeInstanceTypeOfferings page listing the AZs (Location
// values) that offer a type — the AZ-granularity shape instanceTypeAZs reads.
func offeringPage(azs ...string) *ec2.DescribeInstanceTypeOfferingsOutput {
	out := &ec2.DescribeInstanceTypeOfferingsOutput{}
	for _, az := range azs {
		out.InstanceTypeOfferings = append(out.InstanceTypeOfferings, ec2types.InstanceTypeOffering{
			Location: awssdk.String(az),
		})
	}
	return out
}

func TestFleetOverrides_PrunesUnofferedAZ(t *testing.T) {
	subnets := []subnet{
		{id: "sn-a", az: "us-east-1a"},
		{id: "sn-e", az: "us-east-1e"}, // g6.48xlarge not offered here
	}
	// Offerings say the type lives only in us-east-1a.
	offerings := map[string]map[string]bool{"g6.48xlarge": {"us-east-1a": true}}
	got := fleetOverrides([]string{"g6.48xlarge"}, subnets, offerings)
	if len(got) != 1 || got[0].SubnetId == nil || *got[0].SubnetId != "sn-a" {
		t.Fatalf("expected only the us-east-1a subnet to survive pruning, got %+v", got)
	}
}

func TestFleetOverrides_UnknownOfferingsDoesNotPrune(t *testing.T) {
	subnets := []subnet{{id: "sn-a", az: "us-east-1a"}, {id: "sn-e", az: "us-east-1e"}}
	// nil offered set for the type => unknown => fail-open, keep every subnet.
	got := fleetOverrides([]string{"g6.48xlarge"}, subnets, nil)
	if len(got) != 2 {
		t.Fatalf("unknown offerings must not prune; want 2 overrides, got %d", len(got))
	}
}

func TestFleetOverrides_KnownEmptyOfferedSetPrunesAll(t *testing.T) {
	subnets := []subnet{{id: "sn-a", az: "us-east-1a"}, {id: "sn-e", az: "us-east-1e"}}
	// Resolved-but-empty offered set: the type is offered in NO configured AZ. Unlike
	// an unknown (nil) set, this is trustworthy, so it prunes to nothing rather than
	// failing open — RunInstance reads the empty grid as a region no-capacity and skips
	// the doomed CreateFleet instead of letting EC2 bounce it.
	offerings := map[string]map[string]bool{"g6.48xlarge": {}}
	got := fleetOverrides([]string{"g6.48xlarge"}, subnets, offerings)
	if len(got) != 0 {
		t.Fatalf("a known-empty offered set must prune every subnet; got %d", len(got))
	}
}

// RunInstance prunes the dead (type, AZ) pair using DescribeInstanceTypeOfferings,
// so the fleet grid never carries a subnet whose AZ does not offer the type; the
// resolved offering set is cached so a second provision issues no further lookup.
func TestSDKRunInstance_PrunesUnofferedAZAndCaches(t *testing.T) {
	f := &fakeEC2{
		fleetOut:      fleetWith("i-1"),
		offeringPages: []*ec2.DescribeInstanceTypeOfferingsOutput{offeringPage("us-east-1a")},
	}
	c := newSDKClient(f)
	c.subnets = []subnet{{id: "sn-a", az: "us-east-1a"}, {id: "sn-e", az: "us-east-1e"}}

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"g6.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	}); err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	ovs := f.lastFleet.LaunchTemplateConfigs[0].Overrides
	if len(ovs) != 1 || ovs[0].SubnetId == nil || *ovs[0].SubnetId != "sn-a" {
		t.Fatalf("expected the us-east-1e override pruned, got %+v", ovs)
	}
	// The AZ-scoped offerings query filters to the single type.
	if f.offeringCalls != 1 {
		t.Fatalf("expected one offerings lookup, got %d", f.offeringCalls)
	}

	// A second provision reuses the cached offered set — no further lookup.
	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"g6.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c2"},
	}); err != nil {
		t.Fatalf("second RunInstance: %v", err)
	}
	if f.offeringCalls != 1 {
		t.Fatalf("offerings must be cached across provisions; got %d lookups", f.offeringCalls)
	}
}

// When a requested type is offered in NO configured AZ of the region (a known-empty
// offered set, e.g. p4d.24xlarge in us-west-1), RunInstance must short-circuit to a
// region-scoped no-capacity WITHOUT creating a launch template or calling CreateFleet
// — the fleet would only bounce with InvalidFleetConfiguration. The error must carry
// ErrNoCapacity (so failover proceeds) but NOT errZoneLocal (no sibling AZ can help).
func TestSDKRunInstance_NoOfferedAZShortCircuits(t *testing.T) {
	f := &fakeEC2{
		fleetOut:      fleetWith("i-1"), // would succeed IF a fleet were launched
		offeringPages: []*ec2.DescribeInstanceTypeOfferingsOutput{offeringPage()},
	}
	c := newSDKClient(f)
	c.subnets = []subnet{{id: "sn-a", az: "us-west-1a"}, {id: "sn-b", az: "us-west-1b"}}

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p4d.24xlarge"}, Image: "img", Spot: true,
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrNoCapacity) {
		t.Fatalf("want ErrNoCapacity, got %v", err)
	}
	if errors.Is(err, errZoneLocal) {
		t.Fatal("a region-wide unavailability must not be zone-local (no sibling AZ helps)")
	}
	// No doomed API calls: neither a launch template nor a fleet.
	if f.fleetCalls != 0 {
		t.Fatalf("CreateFleet must be skipped when no AZ offers the type; got %d calls", f.fleetCalls)
	}
	if len(f.createdLTs) != 0 {
		t.Fatalf("no launch template must be created on the short-circuit; got %v", f.createdLTs)
	}
}

// A burst of concurrent provisions for the same NEW type must coalesce into ONE
// DescribeInstanceTypeOfferings (single-flight): the first caller runs the lookup
// while the rest wait on it, so the shared cache is populated with a single call
// rather than one per goroutine.
func TestInstanceTypeAZs_SingleFlightCoalesces(t *testing.T) {
	f := &fakeEC2{offeringPages: []*ec2.DescribeInstanceTypeOfferingsOutput{offeringPage("us-east-1a")}}
	c := newSDKClient(f)

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			got := c.instanceTypeAZs(context.Background(), []string{"g6.48xlarge"})
			if !got["g6.48xlarge"]["us-east-1a"] {
				t.Errorf("expected us-east-1a in the resolved offered set, got %+v", got)
			}
		}()
	}
	wg.Wait()

	if f.offeringCalls != 1 {
		t.Fatalf("single-flight must coalesce %d concurrent lookups into 1, got %d", n, f.offeringCalls)
	}
}

// A failed lookup must NOT be cached: the slot is dropped so a later provision
// retries the Describe rather than fail-open forever on a transient throttle.
func TestInstanceTypeAZs_ErrorNotCachedRetries(t *testing.T) {
	f := &fakeEC2{
		offeringErr:   errors.New("throttled"),
		offeringPages: []*ec2.DescribeInstanceTypeOfferingsOutput{offeringPage("us-east-1a")},
	}
	c := newSDKClient(f)

	// First lookup fails -> unresolved (fail-open) and not cached.
	if got := c.instanceTypeAZs(context.Background(), []string{"g6.48xlarge"}); got["g6.48xlarge"] != nil {
		t.Fatalf("a failed lookup must be unresolved (nil), got %+v", got["g6.48xlarge"])
	}
	// Clear the injected error; the retry must re-issue the Describe and now succeed.
	f.offeringMu.Lock()
	f.offeringErr = nil
	f.offeringMu.Unlock()

	got := c.instanceTypeAZs(context.Background(), []string{"g6.48xlarge"})
	if !got["g6.48xlarge"]["us-east-1a"] {
		t.Fatalf("a retry after a transient error must resolve the offered set, got %+v", got)
	}
	if f.offeringCalls != 2 {
		t.Fatalf("expected the failed lookup to be retried (2 calls), got %d", f.offeringCalls)
	}
}

// A DescribeInstanceTypeOfferings failure must fail open: the launch keeps every
// subnet (today's behavior) rather than shrinking or wedging.
func TestSDKRunInstance_OfferingLookupErrorFailsOpen(t *testing.T) {
	f := &fakeEC2{fleetOut: fleetWith("i-1"), offeringErr: errors.New("throttled")}
	c := newSDKClient(f)
	c.subnets = []subnet{{id: "sn-a", az: "us-east-1a"}, {id: "sn-e", az: "us-east-1e"}}

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"g6.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	}); err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	if ovs := f.lastFleet.LaunchTemplateConfigs[0].Overrides; len(ovs) != 2 {
		t.Fatalf("an offerings lookup error must not prune; want 2 overrides, got %d", len(ovs))
	}
}

func TestSDKRunInstance_OnDemand(t *testing.T) {
	f := &fakeEC2{fleetOut: fleetWith("i-42")}
	c := newSDKClient(f)

	id, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"},
		Image:         "img",
		Tags:          map[string]string{ClaimTagKey: "claim-a"},
	})
	if err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	if id != "i-42" {
		t.Fatalf("id = %q, want i-42", id)
	}
	// The launch goes through an instant fleet, not RunInstances.
	if f.fleetCalls != 1 {
		t.Fatalf("fleetCalls = %d, want 1", f.fleetCalls)
	}
	if f.lastFleet.Type != ec2types.FleetTypeInstant {
		t.Fatalf("fleet Type = %q, want instant", f.lastFleet.Type)
	}
	// OnDemand: the fleet's default target-capacity type is on-demand.
	if f.lastFleet.TargetCapacitySpecification.DefaultTargetCapacityType != ec2types.DefaultTargetCapacityTypeOnDemand {
		t.Fatalf("capacity type = %q, want on-demand", f.lastFleet.TargetCapacitySpecification.DefaultTargetCapacityType)
	}
	// Exactly one instance requested.
	if got := f.lastFleet.TargetCapacitySpecification.TotalTargetCapacity; got == nil || *got != 1 {
		t.Fatalf("TotalTargetCapacity = %v, want 1", got)
	}
	// The ephemeral launch template is created and torn down: none survives.
	if len(f.createdLTs) != 1 {
		t.Fatalf("createdLTs = %v, want exactly one ephemeral template", f.createdLTs)
	}
	if len(f.deletedLTs) != 1 || f.deletedLTs[0] != f.createdLTs[0] {
		t.Fatalf("deletedLTs = %v, want the created template %v torn down", f.deletedLTs, f.createdLTs)
	}
	// The fleet references that template by name.
	cfg := f.lastFleet.LaunchTemplateConfigs[0]
	if cfg.LaunchTemplateSpecification.LaunchTemplateName == nil ||
		*cfg.LaunchTemplateSpecification.LaunchTemplateName != f.createdLTs[0] {
		t.Fatalf("fleet references %+v, want template %q", cfg.LaunchTemplateSpecification, f.createdLTs[0])
	}
}

func TestSDKRunInstance_SpotSetsCapacityType(t *testing.T) {
	f := &fakeEC2{fleetOut: fleetWith("i-spot")}
	c := newSDKClient(f)

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"}, Image: "img", Spot: true,
		Tags: map[string]string{ClaimTagKey: "c"},
	}); err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	// Spot is requested via the fleet's default target-capacity type.
	if f.lastFleet.TargetCapacitySpecification.DefaultTargetCapacityType != ec2types.DefaultTargetCapacityTypeSpot {
		t.Fatalf("Spot request must set spot capacity type: %q",
			f.lastFleet.TargetCapacitySpecification.DefaultTargetCapacityType)
	}
}

// One override per discovered subnet: EC2 does the per-AZ capacity search inside
// the single fleet call rather than the adapter sweeping RunInstances per zone.
func TestSDKRunInstance_OneOverridePerSubnet(t *testing.T) {
	f := &fakeEC2{
		fleetOut:      fleetWith("i-1"),
		offeringPages: []*ec2.DescribeInstanceTypeOfferingsOutput{offeringPage("us-east-1a", "us-east-1b", "us-east-1c")},
	}
	c := &sdkClient{ec2: f, region: testRegion, amiID: "ami-123", subnets: []subnet{
		{id: "subnet-a", az: "us-east-1a"},
		{id: "subnet-b", az: "us-east-1b"},
		{id: "subnet-c", az: "us-east-1c"},
	}}

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	}); err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	// Still ONE fleet call, regardless of subnet count.
	if f.fleetCalls != 1 {
		t.Fatalf("fleetCalls = %d, want 1 (server-side sweep, not per-zone)", f.fleetCalls)
	}
	overrides := f.lastFleet.LaunchTemplateConfigs[0].Overrides
	if len(overrides) != 3 {
		t.Fatalf("overrides = %d, want one per subnet (3)", len(overrides))
	}
	for i, want := range []string{"subnet-a", "subnet-b", "subnet-c"} {
		if overrides[i].SubnetId == nil || *overrides[i].SubnetId != want {
			t.Fatalf("override[%d] subnet = %v, want %q", i, overrides[i].SubnetId, want)
		}
	}
}

// Several interchangeable instance types are spanned in ONE fleet: the overrides
// are the (instance type, subnet) grid, so EC2 lands on whichever pair has
// capacity. The launch template carries no instance type — it lives in the
// overrides — so one template serves every candidate type.
func TestSDKRunInstance_SpansInstanceTypesAndSubnets(t *testing.T) {
	f := &fakeEC2{
		fleetOut: fleetWith("i-1"),
		// One offering page per type lookup (p5 then p4d), each offered in both AZs.
		offeringPages: []*ec2.DescribeInstanceTypeOfferingsOutput{
			offeringPage("us-east-1a", "us-east-1b"),
			offeringPage("us-east-1a", "us-east-1b"),
		},
	}
	c := &sdkClient{ec2: f, region: testRegion, amiID: "ami-123", subnets: []subnet{
		{id: "subnet-a", az: "us-east-1a"},
		{id: "subnet-b", az: "us-east-1b"},
	}}

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge", "p4d.24xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	}); err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	overrides := f.lastFleet.LaunchTemplateConfigs[0].Overrides
	// 2 types x 2 subnets = 4 overrides, primary type first.
	type pair struct{ it, sn string }
	got := make([]pair, 0, len(overrides))
	for _, o := range overrides {
		sn := ""
		if o.SubnetId != nil {
			sn = *o.SubnetId
		}
		got = append(got, pair{string(o.InstanceType), sn})
	}
	want := []pair{
		{"p5.48xlarge", "subnet-a"}, {"p5.48xlarge", "subnet-b"},
		{"p4d.24xlarge", "subnet-a"}, {"p4d.24xlarge", "subnet-b"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("overrides = %v, want the (type, subnet) grid %v", got, want)
	}
	// The launch template must NOT pin an instance type (the overrides do).
	if it := f.lastLTData.InstanceType; it != "" {
		t.Fatalf("launch template InstanceType = %q, want unset (overrides carry it)", it)
	}
}

// With no discovered subnets (no default VPC) the fleet still spans every
// instance type — one override per type, subnet unset for EC2 to pick.
func TestSDKRunInstance_NoSubnetsStillSpansTypes(t *testing.T) {
	f := &fakeEC2{fleetOut: fleetWith("i-1")}
	c := &sdkClient{ec2: f, region: testRegion, amiID: "ami-123"} // no subnets

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge", "p4d.24xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	}); err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	overrides := f.lastFleet.LaunchTemplateConfigs[0].Overrides
	if len(overrides) != 2 {
		t.Fatalf("overrides = %d, want one per instance type (2) when no subnets", len(overrides))
	}
	for i, want := range []string{"p5.48xlarge", "p4d.24xlarge"} {
		if string(overrides[i].InstanceType) != want {
			t.Fatalf("override[%d] type = %q, want %q", i, overrides[i].InstanceType, want)
		}
		if overrides[i].SubnetId != nil {
			t.Fatalf("override[%d] subnet = %v, want unset (no default VPC)", i, *overrides[i].SubnetId)
		}
	}
}

// A fleet that launches nothing reports the reason in out.Errors (the API call
// succeeds). A capacity code must map back to the wrapped ErrNoCapacity (+ Spot)
// so region/tier failover still fires — same contract the old per-zone sweep had.
func TestSDKRunInstance_FleetNoCapacityWrapped(t *testing.T) {
	f := &fakeEC2{fleetOut: fleetNoCapacity("InsufficientInstanceCapacity")}
	c := newSDKClient(f)

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"}, Image: "img", Spot: true,
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrNoCapacity) || !errors.Is(err, ErrSpotCapacity) {
		t.Fatalf("err = %v, want wrapped ErrNoCapacity + ErrSpotCapacity", err)
	}
	// The ephemeral template is torn down even on the no-capacity path.
	if len(f.deletedLTs) != 1 {
		t.Fatalf("deletedLTs = %v, want the template torn down on failure too", f.deletedLTs)
	}
}

// A CreateFleet grid returns a MIX of per-override reasons: one AZ does not offer
// the type (InvalidFleetConfiguration) while the rest are capacity-starved. Both are
// capacity-class, so the launch is still ErrNoCapacity (never a whole-provider block)
// and the AZ-availability error does not derail failover.
func TestSDKRunInstance_FleetMixedCapacityErrors(t *testing.T) {
	f := &fakeEC2{fleetOut: &ec2.CreateFleetOutput{Errors: []ec2types.CreateFleetError{
		{ErrorCode: awssdk.String("InvalidFleetConfiguration"),
			ErrorMessage: awssdk.String("g6.48xlarge is not supported in us-east-1e")},
		{ErrorCode: awssdk.String("InsufficientInstanceCapacity"),
			ErrorMessage: awssdk.String("insufficient capacity in us-east-1d")},
	}}}
	c := newSDKClient(f)

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"g6.48xlarge"}, Image: "img", Spot: true,
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrNoCapacity) || !errors.Is(err, ErrSpotCapacity) {
		t.Fatalf("err = %v, want wrapped ErrNoCapacity + ErrSpotCapacity for a mixed capacity grid", err)
	}
}

// A terminal reason (auth) hiding among per-AZ capacity errors must win: it fails
// everywhere, so fleetError surfaces it rather than the more numerous capacity
// errors, and RunInstance reports ErrAuth (a whole-provider block) not no-capacity.
func TestSDKRunInstance_FleetAuthOutranksCapacity(t *testing.T) {
	f := &fakeEC2{fleetOut: &ec2.CreateFleetOutput{Errors: []ec2types.CreateFleetError{
		{ErrorCode: awssdk.String("InsufficientInstanceCapacity")},
		{ErrorCode: awssdk.String("UnauthorizedOperation"),
			ErrorMessage: awssdk.String("not authorized to launch")},
	}}}
	c := newSDKClient(f)

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"g6.48xlarge"}, Image: "img", Spot: true,
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrAuth) {
		t.Fatalf("err = %v, want ErrAuth to outrank the capacity errors in the grid", err)
	}
}

// A fleet that launches nothing AND reports no reason must still fail over: map it
// to a generic no-capacity rather than wedging on a nil error.
func TestSDKRunInstance_FleetEmptyIsNoCapacity(t *testing.T) {
	f := &fakeEC2{fleetOut: &ec2.CreateFleetOutput{}}
	c := newSDKClient(f)

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrNoCapacity) {
		t.Fatalf("err = %v, want ErrNoCapacity for an empty fleet result", err)
	}
}

// A top-level CreateFleet error (throttle/malformed/auth on the fleet API) is
// classified too, so a capacity-shaped top-level error also fails over.
func TestSDKRunInstance_FleetTopLevelErrorClassified(t *testing.T) {
	f := &fakeEC2{fleetErr: apiErr("InsufficientInstanceCapacity")}
	c := newSDKClient(f)

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrNoCapacity) {
		t.Fatalf("err = %v, want classified ErrNoCapacity", err)
	}
	// Template is still cleaned up after a top-level fleet failure.
	if len(f.deletedLTs) != 1 {
		t.Fatalf("deletedLTs = %v, want cleanup after top-level fleet error", f.deletedLTs)
	}
}

// A launch-template create failure aborts the provision before any fleet call and
// leaves nothing to clean up.
func TestSDKRunInstance_LaunchTemplateCreateFails(t *testing.T) {
	f := &fakeEC2{ltCreateErr: errors.New("boom")}
	c := newSDKClient(f)

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	}); err == nil {
		t.Fatal("expected the launch-template create error to abort the provision")
	}
	if f.fleetCalls != 0 {
		t.Fatalf("fleetCalls = %d, want 0 (no fleet after template create failed)", f.fleetCalls)
	}
	if len(f.deletedLTs) != 0 {
		t.Fatalf("deletedLTs = %v, want none (nothing was created)", f.deletedLTs)
	}
}

// RunInstance opportunistically sweeps leaked ephemeral templates: one older than
// the staleness cutoff is deleted; a young one (a possible concurrent provision)
// is left alone. The sweep must never fail the provision it piggybacks on.
func TestSDKRunInstance_SweepsStaleLaunchTemplates(t *testing.T) {
	old := time.Now().Add(-2 * staleLaunchTemplateAge) // provably orphaned
	young := time.Now().Add(-1 * time.Minute)          // could be in-flight
	f := &fakeEC2{
		fleetOut: fleetWith("i-1"),
		describeLTsOut: &ec2.DescribeLaunchTemplatesOutput{
			LaunchTemplates: []ec2types.LaunchTemplate{
				{LaunchTemplateName: awssdk.String("nebula-stale"), CreateTime: &old},
				{LaunchTemplateName: awssdk.String("nebula-young"), CreateTime: &young},
			},
		},
	}
	c := newSDKClient(f)

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "claim-a"},
	}); err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	// deletedLTs holds the swept stale template plus this provision's own template.
	// The young one must NOT be deleted.
	deleted := map[string]bool{}
	for _, n := range f.deletedLTs {
		deleted[n] = true
	}
	if !deleted["nebula-stale"] {
		t.Fatalf("stale template not swept; deletedLTs = %v", f.deletedLTs)
	}
	if deleted["nebula-young"] {
		t.Fatalf("young template was swept but may be in-flight; deletedLTs = %v", f.deletedLTs)
	}
}

// A sweep describe failure must be swallowed: the provision still succeeds.
func TestSDKRunInstance_SweepFailureIsNonFatal(t *testing.T) {
	f := &fakeEC2{
		fleetOut:       fleetWith("i-1"),
		describeLTsErr: errors.New("throttled"),
	}
	c := newSDKClient(f)

	id, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceTypes: []string{"p5.48xlarge"}, Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if err != nil || id != "i-1" {
		t.Fatalf("RunInstance = (%q, %v), want a successful launch despite the sweep failing", id, err)
	}
}

func TestSDKTerminate_IdempotentOnNotFound(t *testing.T) {
	f := &fakeEC2{terminateErr: apiErr("InvalidInstanceID.NotFound")}
	c := newSDKClient(f)
	if err := c.TerminateInstance(context.Background(), "i-gone"); err != nil {
		t.Fatalf("Terminate should swallow NotFound, got %v", err)
	}

	f2 := &fakeEC2{}
	c2 := newSDKClient(f2)
	if err := c2.TerminateInstance(context.Background(), "i-1"); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if len(f2.terminated) != 1 || f2.terminated[0] != "i-1" {
		t.Fatalf("terminated = %v, want [i-1]", f2.terminated)
	}
}

func TestSDKDescribe_AbsentIsNil(t *testing.T) {
	// NotFound => (nil, nil): absence == terminated.
	f := &fakeEC2{describeErr: apiErr("InvalidInstanceID.NotFound")}
	c := newSDKClient(f)
	got, err := c.DescribeInstance(context.Background(), "i-gone")
	if err != nil || got != nil {
		t.Fatalf("DescribeInstance(absent) = (%+v, %v), want (nil, nil)", got, err)
	}

	// Empty reservations also => nil.
	f2 := &fakeEC2{describePages: []*ec2.DescribeInstancesOutput{{}}}
	c2 := newSDKClient(f2)
	got, err = c2.DescribeInstance(context.Background(), "i-x")
	if err != nil || got != nil {
		t.Fatalf("DescribeInstance(empty) = (%+v, %v), want (nil, nil)", got, err)
	}
}

func TestSDKDescribe_ObservesInstance(t *testing.T) {
	f := &fakeEC2{describePages: []*ec2.DescribeInstancesOutput{{
		Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{{
			InstanceId:        awssdk.String("i-1"),
			State:             &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
			PublicDnsName:     awssdk.String("ec2-1-2-3-4.compute.amazonaws.com"),
			InstanceLifecycle: ec2types.InstanceLifecycleTypeSpot,
			Tags:              []ec2types.Tag{{Key: awssdk.String(ClaimTagKey), Value: awssdk.String("claim-a")}},
		}}}},
	}}}
	c := newSDKClient(f)

	got, err := c.DescribeInstance(context.Background(), "i-1")
	if err != nil || got == nil {
		t.Fatalf("DescribeInstance = (%+v, %v)", got, err)
	}
	if got.ID != "i-1" || got.State != string(ec2types.InstanceStateNameRunning) {
		t.Fatalf("observed = %+v", got)
	}
	if got.PublicEndpoint != "ec2-1-2-3-4.compute.amazonaws.com" {
		t.Fatalf("endpoint = %q", got.PublicEndpoint)
	}
	if !got.Spot {
		t.Fatalf("Spot lifecycle not detected")
	}
	if got.Tags[ClaimTagKey] != "claim-a" {
		t.Fatalf("claim tag = %q", got.Tags[ClaimTagKey])
	}
	if got.Region != testRegion {
		t.Fatalf("region = %q, want %q", got.Region, testRegion)
	}
}

func TestSDKAvailableInstanceTypes_PagesAndScopesToRegion(t *testing.T) {
	f := &fakeEC2{offeringPages: []*ec2.DescribeInstanceTypeOfferingsOutput{
		{
			InstanceTypeOfferings: []ec2types.InstanceTypeOffering{
				{InstanceType: ec2types.InstanceType("g4dn.xlarge")},
			},
			NextToken: awssdk.String("page2"),
		},
		{
			InstanceTypeOfferings: []ec2types.InstanceTypeOffering{
				{InstanceType: ec2types.InstanceType("p5.48xlarge")},
			},
		},
	}}
	c := newSDKClient(f)

	got, err := c.AvailableInstanceTypes(context.Background())
	if err != nil {
		t.Fatalf("AvailableInstanceTypes: %v", err)
	}
	if len(got) != 2 || !got["g4dn.xlarge"] || !got["p5.48xlarge"] {
		t.Fatalf("available = %+v, want both paged types", got)
	}
	// The probe must be scoped to the client's region (LocationType=region +
	// location filter) so it reports THIS region's availability, not global.
	if f.lastOfferingIn.LocationType != ec2types.LocationTypeRegion {
		t.Fatalf("LocationType = %q, want region", f.lastOfferingIn.LocationType)
	}
	if len(f.lastOfferingIn.Filters) == 0 ||
		len(f.lastOfferingIn.Filters[0].Values) == 0 ||
		f.lastOfferingIn.Filters[0].Values[0] != testRegion {
		t.Fatalf("location filter = %+v, want %q", f.lastOfferingIn.Filters, testRegion)
	}
}

func TestSDKAvailableInstanceTypes_ErrorPropagates(t *testing.T) {
	f := &fakeEC2{offeringErr: errors.New("boom")}
	c := newSDKClient(f)
	if _, err := c.AvailableInstanceTypes(context.Background()); err == nil {
		t.Fatal("expected the probe error to propagate")
	}
}

func TestSDKList_PagesAndFiltersByClaimTag(t *testing.T) {
	f := &fakeEC2{describePages: []*ec2.DescribeInstancesOutput{
		{
			Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{{
				InstanceId: awssdk.String("i-1"),
				State:      &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
			}}}},
			NextToken: awssdk.String("page2"),
		},
		{
			Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{{
				InstanceId: awssdk.String("i-2"),
				State:      &ec2types.InstanceState{Name: ec2types.InstanceStateNamePending},
			}}}},
		},
	}}
	c := newSDKClient(f)

	list, err := c.ListInstances(context.Background())
	if err != nil {
		t.Fatalf("ListInstances: %v", err)
	}
	if len(list) != 2 || list[0].ID != "i-1" || list[1].ID != "i-2" {
		t.Fatalf("list = %+v, want both pages", list)
	}

	// The query must be scoped server-side to (a) the Nebula claim tag and (b) live
	// instance states — terminated/shutting-down are excluded so torn-down instances
	// (which EC2 keeps visible for ~1h) are neither counted nor matched by claim.
	filters := map[string][]string{}
	for _, fl := range f.lastDescribe.Filters {
		filters[awssdk.ToString(fl.Name)] = fl.Values
	}
	if got := filters["tag-key"]; len(got) != 1 || got[0] != ClaimTagKey {
		t.Fatalf("tag-key filter = %v, want [%s]", got, ClaimTagKey)
	}
	states := filters["instance-state-name"]
	if len(states) == 0 {
		t.Fatal("expected an instance-state-name filter to exclude terminated instances")
	}
	for _, s := range states {
		if s == "terminated" || s == "shutting-down" {
			t.Fatalf("instance-state-name filter must not include %q; got %v", s, states)
		}
	}
	wantStates := map[string]bool{"pending": true, "running": true, "stopping": true, "stopped": true}
	for _, s := range states {
		if !wantStates[s] {
			t.Fatalf("unexpected instance-state-name value %q; got %v", s, states)
		}
		delete(wantStates, s)
	}
	if len(wantStates) != 0 {
		t.Fatalf("instance-state-name filter missing states %v; got %v", wantStates, states)
	}
}

func TestSDKList_FoldsInStatusChecks(t *testing.T) {
	// Two running instances; only i-1 has passed both reachability checks. List must
	// report i-1 as checks-passed and i-2 as not, so the poll loop holds i-2 at
	// Pending until its checks clear.
	f := &fakeEC2{
		describePages: []*ec2.DescribeInstancesOutput{{
			Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{
				{
					InstanceId: awssdk.String("i-1"),
					State:      &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
				},
				{
					InstanceId: awssdk.String("i-2"),
					State:      &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
				},
			}}}},
		},
		statusPages: []*ec2.DescribeInstanceStatusOutput{{
			InstanceStatuses: []ec2types.InstanceStatus{
				{
					InstanceId:     awssdk.String("i-1"),
					SystemStatus:   &ec2types.InstanceStatusSummary{Status: ec2types.SummaryStatusOk},
					InstanceStatus: &ec2types.InstanceStatusSummary{Status: ec2types.SummaryStatusOk},
				},
				{
					InstanceId:     awssdk.String("i-2"),
					SystemStatus:   &ec2types.InstanceStatusSummary{Status: ec2types.SummaryStatusOk},
					InstanceStatus: &ec2types.InstanceStatusSummary{Status: ec2types.SummaryStatusInitializing},
				},
			},
		}},
	}
	c := newSDKClient(f)

	list, err := c.ListInstances(context.Background())
	if err != nil {
		t.Fatalf("ListInstances: %v", err)
	}
	byID := map[string]EC2Instance{}
	for _, inst := range list {
		byID[inst.ID] = inst
	}
	if !byID["i-1"].StatusChecksPassed {
		t.Error("i-1 has 2/2 checks ok; want StatusChecksPassed=true")
	}
	if byID["i-2"].StatusChecksPassed {
		t.Error("i-2 instance check is still initializing; want StatusChecksPassed=false")
	}
	// The status probe must be scoped to exactly the listed ids.
	if got := f.lastStatusIn.InstanceIds; len(got) != 2 {
		t.Fatalf("status probe ids = %v, want the two listed instances", got)
	}
}

func TestSDKList_StatusProbeFailureIsNonFatal(t *testing.T) {
	// A DescribeInstanceStatus failure must NOT fail the whole List: the instances
	// are still returned (checks default to not-passed), so a transient status-API
	// hiccup costs at most one extra Pending tick rather than stranding the loop.
	f := &fakeEC2{
		describePages: []*ec2.DescribeInstancesOutput{{
			Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{{
				InstanceId: awssdk.String("i-1"),
				State:      &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
			}}}},
		}},
		statusErr: errors.New("throttled"),
	}
	c := newSDKClient(f)

	list, err := c.ListInstances(context.Background())
	if err != nil {
		t.Fatalf("ListInstances must not propagate a status-probe error: %v", err)
	}
	if len(list) != 1 || list[0].StatusChecksPassed {
		t.Fatalf("list = %+v, want the instance returned with checks not passed", list)
	}
}
