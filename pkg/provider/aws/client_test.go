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
	"strings"
	"testing"

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
	// lastOfferingIn records the last DescribeInstanceTypeOfferings input so a test
	// can assert the region filter was applied.
	lastOfferingIn *ec2.DescribeInstanceTypeOfferingsInput

	imagesOut  *ec2.DescribeImagesOutput
	imagesErr  error
	subnetsOut *ec2.DescribeSubnetsOutput
	subnetsErr error
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
	_ context.Context, _ *ec2.DescribeInstancesInput, _ ...func(*ec2.Options),
) (*ec2.DescribeInstancesOutput, error) {
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
	f.lastOfferingIn = in
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
	if !strings.Contains(script, "'python' 'train.py'") {
		t.Fatalf("script missing command:\n%s", script)
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

func TestSDKRunInstance_OnDemand(t *testing.T) {
	f := &fakeEC2{runOut: &ec2.RunInstancesOutput{
		Instances: []ec2types.Instance{{InstanceId: awssdk.String("i-42")}},
	}}
	c := newSDKClient(f)

	id, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceType: "p5.48xlarge",
		Image:        "img",
		Tags:         map[string]string{ClaimTagKey: "claim-a"},
	})
	if err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	if id != "i-42" {
		t.Fatalf("id = %q, want i-42", id)
	}
	if f.lastRun.InstanceType != ec2types.InstanceType("p5.48xlarge") {
		t.Fatalf("InstanceType = %q", f.lastRun.InstanceType)
	}
	if f.lastRun.ImageId == nil || *f.lastRun.ImageId != "ami-123" {
		t.Fatalf("AMI not set from resolved GPU AMI: %+v", f.lastRun.ImageId)
	}
	// Security groups are deliberately unset: the default-VPC subnet supplies the
	// default SG (outbound-open, no inbound exposure).
	if len(f.lastRun.SecurityGroupIds) != 0 {
		t.Fatalf("SecurityGroupIds must be unset, got %+v", f.lastRun.SecurityGroupIds)
	}
	// OnDemand: no spot market option.
	if f.lastRun.InstanceMarketOptions != nil {
		t.Fatalf("OnDemand request must not set market options")
	}
	// Claim tag rides on the instance tag spec.
	if len(f.lastRun.TagSpecifications) == 0 || *f.lastRun.TagSpecifications[0].Tags[0].Value != "claim-a" {
		t.Fatalf("claim tag not set: %+v", f.lastRun.TagSpecifications)
	}
}

func TestSDKRunInstance_SpotSetsMarketOption(t *testing.T) {
	f := &fakeEC2{runOut: &ec2.RunInstancesOutput{
		Instances: []ec2types.Instance{{InstanceId: awssdk.String("i-spot")}},
	}}
	c := newSDKClient(f)

	if _, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceType: "p5.48xlarge", Image: "img", Spot: true,
		Tags: map[string]string{ClaimTagKey: "c"},
	}); err != nil {
		t.Fatalf("RunInstance: %v", err)
	}
	if f.lastRun.InstanceMarketOptions == nil ||
		f.lastRun.InstanceMarketOptions.MarketType != ec2types.MarketTypeSpot {
		t.Fatalf("Spot request must set spot market option: %+v", f.lastRun.InstanceMarketOptions)
	}
}

func TestSDKRunInstance_CapacityErrorWrapped(t *testing.T) {
	f := &fakeEC2{runErr: apiErr("InsufficientInstanceCapacity")}
	c := newSDKClient(f)

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceType: "p5.48xlarge", Image: "img", Spot: true,
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrNoCapacity) || !errors.Is(err, ErrSpotCapacity) {
		t.Fatalf("err = %v, want wrapped ErrNoCapacity + ErrSpotCapacity", err)
	}
}

// A zone-local capacity shortfall must make RunInstance sweep EVERY discovered
// subnet before giving up — a sibling AZ may still have capacity.
func TestSDKRunInstance_ZoneLocalSweepsAllSubnets(t *testing.T) {
	f := &fakeEC2{runErr: apiErr("InsufficientInstanceCapacity")}
	c := &sdkClient{ec2: f, region: testRegion, amiID: "ami-123", subnets: []subnet{
		{id: "subnet-a", az: "us-east-1a"},
		{id: "subnet-b", az: "us-east-1b"},
		{id: "subnet-c", az: "us-east-1c"},
	}}

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceType: "p5.48xlarge", Image: "img",
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrNoCapacity) {
		t.Fatalf("err = %v, want ErrNoCapacity after exhausting all zones", err)
	}
	if f.runCalls != 3 {
		t.Fatalf("runCalls = %d, want 3 (one attempt per subnet)", f.runCalls)
	}
}

// A region/account-scoped Spot limit must STOP the sweep at the first subnet:
// every AZ would fail identically, so iterating them wastes the deadline. It still
// bubbles up ErrNoCapacity so region-level failover fires.
func TestSDKRunInstance_RegionScopedStopsSweep(t *testing.T) {
	f := &fakeEC2{runErr: apiErr("MaxSpotInstanceCountExceeded")}
	c := &sdkClient{ec2: f, region: testRegion, amiID: "ami-123", subnets: []subnet{
		{id: "subnet-a", az: "us-east-1a"},
		{id: "subnet-b", az: "us-east-1b"},
		{id: "subnet-c", az: "us-east-1c"},
	}}

	_, err := c.RunInstance(context.Background(), InstanceSpec{
		InstanceType: "p5.48xlarge", Image: "img", Spot: true,
		Tags: map[string]string{ClaimTagKey: "c"},
	})
	if !errors.Is(err, provider.ErrNoCapacity) || !errors.Is(err, ErrSpotCapacity) {
		t.Fatalf("err = %v, want ErrNoCapacity + ErrSpotCapacity", err)
	}
	if f.runCalls != 1 {
		t.Fatalf("runCalls = %d, want 1 (region-scoped error must not sweep)", f.runCalls)
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
